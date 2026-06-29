"""
Event-driven file watcher for flex daemon.

Wraps watchdog.Observer to provide inotify-based file change detection
with debouncing. The observer thread records which files changed; the
main daemon thread calls drain() to collect debounced paths and do
the actual sync work (SQLite, embedding).

Graceful degradation: if watchdog is not installed or inotify fails,
the daemon falls back to its existing polling loop.
"""

import fnmatch
import os
import sys
import threading
import time
from pathlib import Path

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent

    _WATCHDOG_AVAILABLE = True
except ImportError:
    _WATCHDOG_AVAILABLE = False


class _FlexHandler(FileSystemEventHandler):
    """Filters file events by pattern and records them for debounced drain."""

    def __init__(self, pattern: str, pending: dict, lock: threading.Lock):
        super().__init__()
        self._pattern = pattern
        self._pending = pending
        self._lock = lock

    def _handle(self, event):
        if event.is_directory:
            return
        src = event.src_path
        if Path(src).is_symlink():
            return
        if not fnmatch.fnmatch(Path(src).name, self._pattern):
            return
        with self._lock:
            # Only record the earliest event time per path (debounce window
            # starts from first event, not last — prevents starvation during
            # sustained writes).
            if src not in self._pending:
                self._pending[src] = time.monotonic()

    def on_modified(self, event):
        self._handle(event)

    def on_created(self, event):
        self._handle(event)


class FlexWatcher:
    """inotify-based file watcher with debounced drain.

    Usage:
        watcher = FlexWatcher(debounce_ms=500)
        watcher.watch("~/.claude/projects", pattern="*.jsonl")
        watcher.start()

        # In daemon loop:
        for path in watcher.drain():
            sync_session(path)

        watcher.stop()
    """

    def __init__(self, debounce_ms: int = 500):
        if not _WATCHDOG_AVAILABLE:
            raise ImportError("watchdog is not installed")
        self._observer = Observer()
        self._pending: dict[str, float] = {}
        self._lock = threading.Lock()
        self._debounce = debounce_ms / 1000.0
        self._active = False
        self._watches: list[str] = []

    def watch(self, path: str | Path, pattern: str = "*", recursive: bool = True):
        """Register a directory for inotify monitoring.

        Args:
            path: Directory to watch.
            pattern: Filename glob pattern (e.g. "*.jsonl").
            recursive: Watch subdirectories.
        """
        resolved = str(Path(path).expanduser().resolve())
        if not Path(resolved).is_dir():
            print(f"[watcher] Skipping non-directory: {resolved}", file=sys.stderr)
            return
        handler = _FlexHandler(pattern, self._pending, self._lock)
        self._observer.schedule(handler, resolved, recursive=recursive)
        self._watches.append(f"{resolved} ({pattern})")

    def start(self) -> bool:
        """Start the observer thread. Returns True on success, False on failure."""
        if not self._watches:
            return False
        try:
            self._observer.start()
            self._active = True
            return True
        except Exception as e:
            print(f"[watcher] Failed to start: {e}", file=sys.stderr)
            self._active = False
            return False

    def stop(self):
        """Stop and join the observer thread."""
        if self._active:
            try:
                self._observer.stop()
                self._observer.join(timeout=5)
            except Exception:
                pass
            self._active = False

    def drain(self) -> list[Path]:
        """Return paths whose debounce window has elapsed, clear them from buffer.

        Thread-safe. Called from the main daemon thread every tick.
        """
        if not self._pending:
            return []

        now = time.monotonic()
        ready = []
        with self._lock:
            expired = [
                path for path, ts in self._pending.items()
                if now - ts >= self._debounce
            ]
            for path in expired:
                del self._pending[path]
                ready.append(Path(path))
        return ready

    @property
    def active(self) -> bool:
        return self._active

    @property
    def watch_count(self) -> int:
        return len(self._watches)

    def status(self) -> dict:
        """Return watcher status for diagnostics."""
        with self._lock:
            pending_count = len(self._pending)
        return {
            "active": self._active,
            "watches": self._watches,
            "pending": pending_count,
            "debounce_ms": int(self._debounce * 1000),
        }
