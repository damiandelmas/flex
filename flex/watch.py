"""
Flex watch — typed filesystem invalidation machinery.

Owns the Invalidation/WatchRegistration records, a thread-safe debounced
coalescing queue, and a watchdog-backed observer adapter. Contains no
SQLite, embedding, or module-specific ingestion logic.

Observer callbacks may ONLY validate cheap event properties (path, pattern,
root containment) and enqueue. All I/O, parsing, and database work happens
on the daemon thread that drains the queue via drain_ready() — never in a
watchdog callback thread. This keeps the single writer-thread invariant
(flex/modules/claude_code/compile/worker.py owns the only connection that
writes to the cell) intact.

Phase 1 registers exactly one cell (claude_code, JSONLs). Generic
registry-discovered watch cells stay on their existing polling/signature
paths until typed dispatch for other cell types exists.
"""

from __future__ import annotations

import fnmatch
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Optional

# ─────────────────────────────────────────────────────────────────────────
# Defaults + environment configuration
# ─────────────────────────────────────────────────────────────────────────

DEFAULT_QUIET_WINDOW_S = 0.5
DEFAULT_MAX_LATENCY_S = 2.0
DEFAULT_RECONCILE_INTERVAL_S = 60.0
DEFAULT_QUEUE_MAX = 10_000

ENV_QUIET_WINDOW_MS = "FLEX_WATCH_QUIET_WINDOW_MS"
ENV_MAX_LATENCY_MS = "FLEX_WATCH_MAX_LATENCY_MS"
ENV_RECONCILE_INTERVAL_S = "FLEX_WATCH_RECONCILE_INTERVAL_S"
ENV_QUEUE_MAX = "FLEX_WATCH_QUEUE_MAX"
ENV_DISABLE = "FLEX_WATCH_DISABLE"


@dataclass(frozen=True)
class WatchConfig:
    quiet_window: float = DEFAULT_QUIET_WINDOW_S
    max_latency: float = DEFAULT_MAX_LATENCY_S
    reconcile_interval: float = DEFAULT_RECONCILE_INTERVAL_S
    queue_max: int = DEFAULT_QUEUE_MAX
    disabled: bool = False


def _env_positive(env, name: str, cast, default):
    """Parse a positive numeric env var. Falls back to the documented
    default (with a stderr warning) on anything invalid — a malformed
    timing knob must never crash daemon startup."""
    raw = env.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        val = cast(raw)
        if val <= 0:
            raise ValueError("must be positive")
        return val
    except (TypeError, ValueError):
        print(
            f"[watch] invalid {name}={raw!r} — using default {default}",
            file=sys.stderr,
        )
        return default


def _env_bool(env, name: str, default: bool = False) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def load_watch_config(env=None) -> WatchConfig:
    """Parse and validate the watch subsystem's environment configuration.

    Invalid values fall back to documented defaults rather than raising —
    a background daemon should not fail to start over a bad env var.
    """
    env = env if env is not None else os.environ
    quiet_ms = _env_positive(env, ENV_QUIET_WINDOW_MS, float, DEFAULT_QUIET_WINDOW_S * 1000)
    latency_ms = _env_positive(env, ENV_MAX_LATENCY_MS, float, DEFAULT_MAX_LATENCY_S * 1000)
    reconcile_s = _env_positive(env, ENV_RECONCILE_INTERVAL_S, float, DEFAULT_RECONCILE_INTERVAL_S)
    queue_max = _env_positive(env, ENV_QUEUE_MAX, int, DEFAULT_QUEUE_MAX)
    disabled = _env_bool(env, ENV_DISABLE, False)

    return WatchConfig(
        quiet_window=quiet_ms / 1000.0,
        max_latency=latency_ms / 1000.0,
        reconcile_interval=reconcile_s,
        queue_max=int(queue_max),
        disabled=disabled,
    )


# ─────────────────────────────────────────────────────────────────────────
# Records
# ─────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Invalidation:
    """A notification that a source path changed. Not ingestion truth —
    the daemon validates and re-derives everything it needs at sync time.
    """
    cell_name: str
    source_path: Path
    observed_at: float  # time.monotonic(), process-local
    kind: Literal["created", "modified", "moved"]


@dataclass(frozen=True)
class WatchRegistration:
    """One watched root + relative pattern for one cell.

    Phase 1 creates exactly one registration (claude_code JSONLs). Pattern
    matching always uses the path relative to `root` — never the basename
    — so relative patterns like `**/*.jsonl` are matched correctly.
    """
    cell_name: str
    root: Path
    pattern: str
    recursive: bool = True

    def matches(self, path: Path) -> bool:
        """True if `path` falls under `root` (after resolution — a
        registration cannot escape its root) and matches `pattern` against
        the root-relative path, not the basename.
        """
        try:
            root = Path(self.root).resolve()
        except OSError:
            return False
        try:
            candidate = Path(path).resolve()
        except OSError:
            candidate = Path(path)
        try:
            rel = candidate.relative_to(root)
        except ValueError:
            return False
        rel_str = rel.as_posix()
        if fnmatch.fnmatch(rel_str, self.pattern):
            return True
        # fnmatch's translation of a leading '**/' requires a literal '/',
        # so it never matches a file directly under root (zero leading
        # directories). Glob semantics say '**' matches zero or more
        # directories — fall back to the pattern with that prefix stripped.
        if self.pattern.startswith("**/"):
            return fnmatch.fnmatch(rel_str, self.pattern[3:])
        return False


# ─────────────────────────────────────────────────────────────────────────
# Coalescing queue
# ─────────────────────────────────────────────────────────────────────────

class InvalidationQueue:
    """Thread-safe, debounced, coalescing invalidation queue.

    Identity is (cell_name, source_path). Repeated events update the
    quiet-window deadline while preserving the first-seen time used for
    maximum latency, so a continuously written session cannot starve
    indefinitely. `drain_ready(now)` is deterministic — callers (the
    daemon thread) get a stable order across calls with the same clock.

    Crossing `max_size` marks reconciliation required and stops admitting
    new distinct keys until the queue drains — bounded memory instead of
    unbounded growth; reconciliation is the correctness backstop either way.
    """

    def __init__(
        self,
        *,
        quiet_window: float = DEFAULT_QUIET_WINDOW_S,
        max_latency: float = DEFAULT_MAX_LATENCY_S,
        max_size: int = DEFAULT_QUEUE_MAX,
    ):
        self.quiet_window = quiet_window
        self.max_latency = max_latency
        self.max_size = max_size

        self._lock = threading.Lock()
        self._entries: dict[tuple, dict] = {}
        self._reconciliation_required = False
        self._queued_total = 0
        self._coalesced_total = 0
        self._drained_total = 0
        self._dropped_total = 0

    def put(self, inv: Invalidation) -> None:
        key = (inv.cell_name, inv.source_path)
        with self._lock:
            existing = self._entries.get(key)
            if existing is None:
                if len(self._entries) >= self.max_size:
                    self._reconciliation_required = True
                    self._dropped_total += 1
                    return
                self._entries[key] = {
                    "first": inv.observed_at,
                    "deadline": inv.observed_at + self.quiet_window,
                    "kind": inv.kind,
                }
                self._queued_total += 1
            else:
                existing["deadline"] = inv.observed_at + self.quiet_window
                existing["kind"] = inv.kind
                self._coalesced_total += 1

    def drain_ready(self, now: float) -> list:
        """Return invalidations whose quiet window elapsed or that hit
        maximum latency, removing them from the queue. Deterministically
        ordered by (cell_name, source_path) for stable, testable behavior.
        """
        ready = []
        with self._lock:
            for key, meta in list(self._entries.items()):
                quiet_elapsed = now >= meta["deadline"]
                max_hit = (now - meta["first"]) >= self.max_latency
                if quiet_elapsed or max_hit:
                    cell_name, source_path = key
                    ready.append(Invalidation(cell_name, source_path, meta["first"], meta["kind"]))
                    del self._entries[key]
            self._drained_total += len(ready)
        ready.sort(key=lambda inv: (inv.cell_name, str(inv.source_path)))
        return ready

    def reconciliation_required(self) -> bool:
        with self._lock:
            return self._reconciliation_required

    def mark_reconciliation_required(self) -> None:
        with self._lock:
            self._reconciliation_required = True

    def clear_reconciliation_required(self) -> None:
        with self._lock:
            self._reconciliation_required = False

    def stats(self) -> dict:
        with self._lock:
            return {
                "pending": len(self._entries),
                "queued_total": self._queued_total,
                "coalesced_total": self._coalesced_total,
                "drained_total": self._drained_total,
                "dropped_total": self._dropped_total,
                "reconciliation_required": self._reconciliation_required,
            }


# ─────────────────────────────────────────────────────────────────────────
# Watchdog observer adapter
# ─────────────────────────────────────────────────────────────────────────

def _make_handler(registration: WatchRegistration, queue: InvalidationQueue):
    """Build a watchdog FileSystemEventHandler bound to one registration
    and queue. Deferred import — watchdog is a runtime dependency but the
    rest of this module (queue, records, config) must stay importable even
    if the package is somehow absent (e.g. isolated unit tests).
    """
    from watchdog.events import FileSystemEventHandler

    class _Handler(FileSystemEventHandler):
        def _enqueue(self, path_str, kind):
            if not path_str:
                return
            path = Path(path_str)
            if not registration.matches(path):
                return
            try:
                canonical = path.resolve()
            except OSError:
                canonical = path
            queue.put(Invalidation(registration.cell_name, canonical, time.monotonic(), kind))

        def on_created(self, event):
            if event.is_directory:
                return
            self._enqueue(event.src_path, "created")

        def on_modified(self, event):
            if event.is_directory:
                return
            self._enqueue(event.src_path, "modified")

        def on_moved(self, event):
            if event.is_directory:
                return
            self._enqueue(event.dest_path, "moved")

        # Deletes carry no content to validate or sync — reconciliation
        # (and _raw_sources staying put) is the correctness path for
        # removals, not the event queue.
        def on_deleted(self, event):
            return

    return _Handler()


class Watcher:
    """Owns a watchdog Observer bound to one WatchRegistration + queue.

    `healthy` reflects whether the backend is currently expected to be
    delivering events. It goes False on start failure, backend thread
    death, or an explicit mark_unhealthy() call (e.g. queue overflow) —
    callers should force reconciliation whenever this is False.
    """

    def __init__(self, registration: WatchRegistration, queue: InvalidationQueue):
        self.registration = registration
        self.queue = queue
        self._observer = None
        self.backend: Optional[str] = None
        self.healthy: bool = False
        self.last_error: Optional[str] = None

    def start(self) -> bool:
        try:
            from watchdog.observers import Observer
        except ImportError as e:
            self.last_error = f"watchdog not installed: {e}"
            self.healthy = False
            return False

        if not Path(self.registration.root).exists():
            self.last_error = f"watch root does not exist: {self.registration.root}"
            self.healthy = False
            return False

        try:
            observer = Observer()
            handler = _make_handler(self.registration, self.queue)
            observer.schedule(
                handler, str(self.registration.root), recursive=self.registration.recursive
            )
            observer.start()
        except Exception as e:
            self.last_error = str(e)
            self.healthy = False
            try:
                observer.stop()
            except Exception:
                pass
            return False

        self._observer = observer
        self.backend = type(observer).__name__
        self.healthy = True
        self.last_error = None
        return True

    def is_alive(self) -> bool:
        if self._observer is None:
            return False
        try:
            return bool(self._observer.is_alive())
        except Exception:
            return False

    def check_health(self) -> bool:
        """Re-check backend liveness; flips healthy False (and records why)
        if the observer thread died after a successful start. Idempotent."""
        if self._observer is None:
            return self.healthy
        if self.healthy and not self.is_alive():
            self.healthy = False
            self.last_error = self.last_error or "observer thread died"
            self.queue.mark_reconciliation_required()
        return self.healthy

    def mark_unhealthy(self, reason: str) -> None:
        self.healthy = False
        self.last_error = reason
        self.queue.mark_reconciliation_required()

    def stop(self, timeout: float = 5.0) -> None:
        observer = self._observer
        self._observer = None
        if observer is None:
            return
        try:
            observer.stop()
            observer.join(timeout=timeout)
        except Exception as e:
            self.last_error = str(e)
        finally:
            self.healthy = False
