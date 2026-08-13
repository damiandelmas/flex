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

The event adapter currently registers the coding-session JSONL source.
Other registered watch cells stay on their polling/signature paths until
typed dispatch for their cell types exists.
"""

from __future__ import annotations

import fnmatch
import bisect
import json
import os
import sqlite3
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
_UNKNOWN_DEBT_CELL = "__unknown__"

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

    The current adapter creates one registration for coding-session JSONLs. Pattern
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


def registrations_for_cells(cells) -> list[WatchRegistration]:
    """Expand active watch cells into one registration per declared root.

    ``registry.watch_path`` is a compatibility projection for single-root
    consumers.  Multi-root local cells carry their complete recipe in
    ``_meta.selections``; events must cover that whole durable set.
    """
    registrations = []
    seen = set()
    for cell in cells:
        if cell.get("lifecycle") != "watch" or not cell.get("active", 1):
            continue
        roots = []
        path = cell.get("path")
        if path:
            try:
                uri = f"file:{Path(path).resolve()}?mode=ro"
                db = sqlite3.connect(uri, uri=True, timeout=2)
                row = db.execute(
                    "SELECT value FROM _meta WHERE key='selections'"
                ).fetchone()
                db.close()
                value = json.loads(row[0]) if row and row[0] else []
                if isinstance(value, list):
                    roots.extend(value)
            except (OSError, sqlite3.Error, TypeError, ValueError):
                pass
        if not roots:
            roots.extend(x for x in (cell.get("watch_path"), cell.get("corpus_path")) if x)
        pattern = cell.get("watch_pattern") or "**/*"
        for raw_root in roots:
            root_pattern = pattern
            try:
                root = Path(raw_root).expanduser().resolve()
            except OSError:
                continue
            # A registered singleton SQLite source historically used the file
            # itself as watch_path.  Watchdog schedules directories, and WAL
            # mode publishes authored changes through `<database>-wal`, so
            # normalize that durable declaration into a parent-directory
            # registration covering the database and its sidecars.
            if root.is_file():
                source_name = root.name
                if root_pattern in ("**/*", source_name):
                    root_pattern = f"{source_name}*"
                root = root.parent
            key = (cell["name"], root, root_pattern)
            if key in seen or not root.is_dir():
                continue
            seen.add(key)
            registrations.append(WatchRegistration(
                cell_name=cell["name"], root=root, pattern=root_pattern, recursive=True,
            ))
    # Session runtimes are the latency-sensitive source plane: install those
    # observers before large repository/context trees. All registrations still
    # participate; this ordering only determines how soon live conversations
    # begin feeding the shared queue during asynchronous startup.
    session_cells = {"claude_code", "codex", "goose"}
    return sorted(
        registrations,
        key=lambda r: (
            0 if r.cell_name in session_cells else 1,
            r.cell_name,
            str(r.root),
        ),
    )


def fair_batch(conn: sqlite3.Connection, lane: str, items, limit: int):
    """Return a persisted round-robin slice of keyed work items.

    ``items`` is ``[(stable_key, payload), ...]``. The cursor lives in the
    cell's existing `_meta` table, so a daemon restart cannot send the largest
    corpus back to the front of the line.
    """
    ordered = sorted(items, key=lambda item: item[0])
    if not ordered or limit <= 0:
        return []
    meta_key = f"drain_cursor:{lane}"
    try:
        row = conn.execute("SELECT value FROM _meta WHERE key=?", (meta_key,)).fetchone()
        cursor = row[0] if row else ""
    except sqlite3.OperationalError:
        return ordered[:limit]
    keys = [item[0] for item in ordered]
    start = bisect.bisect(keys, cursor)
    rotated = ordered[start:] + ordered[:start]
    batch = rotated[:limit]
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key,value) VALUES (?,?)",
        (meta_key, batch[-1][0]),
    )
    return batch


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
        # Debt is scoped and versioned by cell. A clean Claude session scan
        # must never acknowledge an unrelated corpus overflow, nor erase new
        # debt that arrived while that scan was running.
        self._reconciliation_debt: dict[str, int] = {}
        self._debt_epoch = 0
        self._queued_total = 0
        self._coalesced_total = 0
        self._drained_total = 0
        self._requeued_total = 0
        self._deferred_total = 0
        self._backoff_total = 0
        self._failed_total = 0
        self._dropped_total = 0

    def put(self, inv: Invalidation) -> None:
        key = (inv.cell_name, inv.source_path)
        with self._lock:
            existing = self._entries.get(key)
            if existing is None:
                if len(self._entries) >= self.max_size:
                    self._mark_reconciliation_required_locked({inv.cell_name})
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
        maximum latency, removing them from the queue. This is a dequeue,
        rather than an acknowledgement of successful processing: callers must
        requeue a batch if their handler fails. Deterministically ordered by
        (cell_name, source_path) for stable, testable behavior.
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

    def requeue(self, invalidations, *, reason: str = "handler_failure") -> int:
        """Restore a dequeued batch after a handler failure or deferral.

        The original observation time is retained, so a retry is immediately
        eligible when its quiet window already elapsed. If a newer callback
        has already enqueued the same key, retain that newer event's kind but
        make the combined entry eligible at the earlier deadline. Capacity
        overflow still requires reconciliation, which remains the durable
        correctness backstop when a retry cannot fit in memory.

        ``reason`` controls observability only: ``handler_failure`` increments
        ``failed_total``; ``deferred`` and ``backoff`` have separate counters.
        Returns the number of entries restored or merged back into the queue.
        """
        if reason not in {"handler_failure", "deferred", "backoff"}:
            raise ValueError(f"unknown invalidation requeue reason: {reason}")
        restored = 0
        failed = 0
        with self._lock:
            for inv in invalidations:
                failed += 1
                key = (inv.cell_name, inv.source_path)
                existing = self._entries.get(key)
                if existing is None:
                    if len(self._entries) >= self.max_size:
                        self._mark_reconciliation_required_locked({inv.cell_name})
                        self._dropped_total += 1
                        continue
                    self._entries[key] = {
                        "first": inv.observed_at,
                        "deadline": inv.observed_at + self.quiet_window,
                        "kind": inv.kind,
                    }
                else:
                    existing["first"] = min(existing["first"], inv.observed_at)
                    existing["deadline"] = min(
                        existing["deadline"], inv.observed_at + self.quiet_window,
                    )
                restored += 1
            self._requeued_total += restored
            if reason == "handler_failure":
                # Count every affected invalidation, including one that could
                # only fall back to full reconciliation because the queue was
                # full.
                self._failed_total += failed
            elif reason == "deferred":
                self._deferred_total += failed
            else:
                self._backoff_total += failed
        return restored

    @staticmethod
    def _debt_cells(cell_names) -> set[str]:
        if cell_names is None:
            return {_UNKNOWN_DEBT_CELL}
        if isinstance(cell_names, str):
            return {cell_names}
        return {str(name) for name in cell_names}

    def _mark_reconciliation_required_locked(self, cells: set[str]) -> None:
        if not cells:
            return
        self._debt_epoch += 1
        for cell in cells:
            self._reconciliation_debt[cell] = self._debt_epoch

    def reconciliation_required(self, cell_name: str | None = None) -> bool:
        """Return any debt, or debt relevant to one cell when named."""
        with self._lock:
            if cell_name is None:
                return bool(self._reconciliation_debt)
            return cell_name in self._reconciliation_debt

    def reconciliation_debt_generation(self, cell_name: str) -> int | None:
        """Return the current debt generation for compare-and-clear acks."""
        with self._lock:
            return self._reconciliation_debt.get(cell_name)

    def reconciliation_debt_cells(self) -> tuple[str, ...]:
        """Return the named debt scopes for health and authority routing."""
        with self._lock:
            return tuple(sorted(self._reconciliation_debt))

    def mark_reconciliation_required(self, cell_names=None) -> None:
        """Record reconciliation debt for one or more cells.

        Omitting scope preserves the legacy conservative behavior and marks
        unknown/global debt. Callers that know the invalidation's cell should
        always pass it so another cell cannot falsely acknowledge the debt.
        """
        with self._lock:
            self._mark_reconciliation_required_locked(self._debt_cells(cell_names))

    def clear_reconciliation_required(self, cell_names=None, *,
                                      through_generation: int | None = None) -> None:
        """Clear all legacy debt, or only a proven named generation.

        ``through_generation`` is a compare-and-clear guard: debt created
        after an authority started its scan remains owed.
        """
        with self._lock:
            if cell_names is None:
                self._reconciliation_debt.clear()
            else:
                for cell in self._debt_cells(cell_names):
                    generation = self._reconciliation_debt.get(cell)
                    if generation is not None and (
                        through_generation is None or generation <= through_generation
                    ):
                        del self._reconciliation_debt[cell]

    def stats(self) -> dict:
        with self._lock:
            return {
                "pending": len(self._entries),
                "queued_total": self._queued_total,
                "coalesced_total": self._coalesced_total,
                # `drained_total` is the legacy name for dequeues. It does
                # not imply the handler acknowledged the work successfully.
                "drained_total": self._drained_total,
                "dequeued_total": self._drained_total,
                "requeued_total": self._requeued_total,
                "deferred_total": self._deferred_total,
                "backoff_total": self._backoff_total,
                "failed_total": self._failed_total,
                "dropped_total": self._dropped_total,
                "reconciliation_required": bool(self._reconciliation_debt),
                "reconciliation_required_cells": sorted(self._reconciliation_debt),
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
            queue.mark_reconciliation_required(registration.cell_name)  # old path may be stale

        # Deletes carry no content to validate or sync — reconciliation
        # (and _raw_sources staying put) is the correctness path for
        # removals, not the event queue.
        def on_deleted(self, event):
            if not event.is_directory:
                queue.mark_reconciliation_required(registration.cell_name)

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
            # Publish the backend before start() so a concurrent service stop
            # can interrupt an expensive recursive watch initialization.
            self._observer = observer
            observer.start()
        except Exception as e:
            self.last_error = str(e)
            self.healthy = False
            try:
                observer.stop()
            except Exception:
                pass
            self._observer = None
            return False

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
            self.queue.mark_reconciliation_required(self.registration.cell_name)
        return self.healthy

    def mark_unhealthy(self, reason: str) -> None:
        self.healthy = False
        self.last_error = reason
        self.queue.mark_reconciliation_required(self.registration.cell_name)

    def request_stop(self) -> None:
        """Ask the observer to stop without waiting for its thread."""
        observer = self._observer
        if observer is None:
            return
        try:
            observer.stop()
        except Exception as e:
            self.last_error = str(e)
        self.healthy = False

    def join(self, timeout: float = 5.0) -> None:
        """Wait for a previously stopped observer and release it."""
        observer = self._observer
        self._observer = None
        if observer is None:
            return
        try:
            observer.join(timeout=max(0.0, timeout))
        except Exception as e:
            self.last_error = str(e)
        self.healthy = False

    def stop(self, timeout: float = 5.0) -> None:
        self.request_stop()
        self.join(timeout=timeout)


class WatcherSet:
    """Small lifecycle facade over several root observers sharing one queue."""

    def __init__(self, registrations, queue: InvalidationQueue):
        self.queue = queue
        self.watchers = [Watcher(reg, queue) for reg in registrations]
        self.backend = None
        self.healthy = False
        self.last_error = None
        self._starting = False
        self._start_thread = None
        self._stop_requested = False

    def start(self) -> bool:
        self._starting = True
        failures = []
        try:
            for watcher in self.watchers:
                if self._stop_requested:
                    break
                if not watcher.start():
                    failures.append(f"{watcher.registration.root}: {watcher.last_error}")
            self.backend = ",".join(sorted({w.backend for w in self.watchers if w.backend})) or None
            self.healthy = (
                bool(self.watchers) and not failures and not self._stop_requested
            )
            self.last_error = "; ".join(failures) or None
            if failures:
                self.queue.mark_reconciliation_required(
                    watcher.registration.cell_name for watcher in self.watchers
                )
            return self.healthy
        finally:
            self._starting = False

    def start_background(self) -> None:
        """Initialize recursive observers without blocking structural capture."""
        if self._start_thread is not None and self._start_thread.is_alive():
            return
        self._stop_requested = False
        self._starting = True
        self.healthy = bool(self.watchers)
        self._start_thread = threading.Thread(
            target=self.start,
            name="flex-watch-start",
            daemon=True,
        )
        self._start_thread.start()

    def check_health(self) -> bool:
        # Reconciliation already runs immediately at worker startup. Do not
        # declare every not-yet-started observer dead while recursive watch
        # installation proceeds in the background.
        if self._starting:
            return True
        dead = [w for w in self.watchers if not w.check_health()]
        self.healthy = bool(self.watchers) and not dead
        if dead:
            self.last_error = "; ".join(
                f"{w.registration.root}: {w.last_error or 'unhealthy'}" for w in dead
            )
            self.queue.mark_reconciliation_required(
                watcher.registration.cell_name for watcher in dead
            )
        return self.healthy

    def mark_unhealthy(self, reason: str) -> None:
        self.healthy = False
        self.last_error = reason
        self.queue.mark_reconciliation_required(
            watcher.registration.cell_name for watcher in self.watchers
        )

    def stop(self, timeout: float = 5.0) -> None:
        # Stop every backend first, then share one deadline across joins.
        # A per-watcher timeout makes shutdown grow as roots * timeout (49
        # live roots once did), which exceeds systemd's service deadline.
        self._stop_requested = True
        for watcher in self.watchers:
            watcher.request_stop()
        deadline = time.monotonic() + max(0.0, timeout)
        if self._start_thread is not None:
            self._start_thread.join(timeout=max(0.0, deadline - time.monotonic()))
        for watcher in self.watchers:
            watcher.join(timeout=max(0.0, deadline - time.monotonic()))
        self.healthy = False
