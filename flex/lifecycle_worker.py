"""One local update scheduler: detectors notify, scoped provider jobs publish.

Periodic reconciliation is mandatory, including startup and new enrollment.
Watch delivery is optional. No provider daemon or family-wide scanner runs in
this resident process. Registry debt plus startup reconciliation recover an
interrupted queue; an event arriving during a job is never acknowledged by it.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from flex.lifecycle_jobs import (
    DOCUMENT_TYPES, SEMANTIC_MORE_EXIT, SEMANTIC_TYPES, SESSION_TYPES, local_cells,
)


def sd_notify(state: str) -> None:
    """Send a systemd notification (sd_notify protocol); a no-op outside systemd."""
    path = os.environ.get("NOTIFY_SOCKET")
    if not path:
        return
    import socket
    if path.startswith("@"):
        path = "\0" + path[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.sendto(state.encode(), path)
    except OSError:
        pass


@dataclass
class CellState:
    cell: dict
    reconcile_at: float = 0
    tick_at: float = 0
    semantic_at: float = 0
    retry_at: float = 0
    events: list = field(default_factory=list)
    future: object = None
    operation: str | None = None
    sent: list = field(default_factory=list)
    debt_generation: int | None = None
    last_reconcile_ts: float | None = None
    pause_until: float = 0


class LocalLifecycle:
    def __init__(self, queue, *, reconcile_interval=60, workers=4, run_job=None,
                 now=time.monotonic):
        self.queue = queue
        self.reconcile_interval = reconcile_interval
        self.workers = max(2, workers)
        self.now = now
        self.states = {}
        self.pool = ThreadPoolExecutor(max_workers=self.workers)
        self.run_job = run_job or self._run_child
        self.processes = set()
        self.process_lock = threading.Lock()
        self.stopping = threading.Event()
        self.last_registry = -float("inf")
        self.last_watch_signature = None
        self.watcher = None
        self.home = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))

    def enroll(self, cells):
        selected = {c["name"]: c for c in local_cells(cells)}
        for name, state in list(self.states.items()):
            if name not in selected and state.future is None:
                del self.states[name]
        for name, cell in selected.items():
            prior = self.states.get(name)
            if prior is None:
                self.states[name] = CellState(cell, semantic_at=self.now() + 120)
                print(f"[lifecycle] enrolled {name}: startup reconciliation due", file=sys.stderr)
            else:
                # Changing a recipe/locator must not leave the old source current.
                keys = ("path", "watch_path", "corpus_path", "refresh_module", "detector")
                if any(prior.cell.get(k) != cell.get(k) for k in keys):
                    prior.reconcile_at = 0
                prior.cell = cell
        return selected

    def collect(self):
        if self.queue is None:
            return
        for inv in self.queue.drain_ready(self.now()):
            state = self.states.get(inv.cell_name)
            if state is not None:
                # Bound parked paths as well as the detector queue. Repeated
                # edits to one file coalesce; overflow becomes full-cell debt,
                # never an unbounded resident list or a silent dropped update.
                by_path = {str(event.source_path): event for event in state.events}
                by_path[str(inv.source_path)] = inv
                if len(by_path) > 2048:
                    state.events = []
                    state.reconcile_at = 0
                    self.queue.mark_reconciliation_required(inv.cell_name)
                else:
                    state.events = list(by_path.values())

    def _finish(self, state):
        if state.future is None or not state.future.done():
            return
        operation = state.operation
        try:
            result = state.future.result()
            if result.get("deferred"):
                if operation == "semantic":
                    # A busy heavy lease is contention, not a broken cell: keep
                    # its structural work and retry deadline untouched.
                    state.semantic_at = self.now() + 30
                    return
                raise RuntimeError("admission busy")
            if result.get("pending") and operation == "tick":
                # A tick reports debt it did not create (it carries standing
                # reconciliation debt forward). It must not postpone the
                # reconcile that clears that debt: codex ticks every 2 s, and
                # each one pushing reconcile_at 15 s out meant no reconcile ever
                # ran and the cell stayed 'pending' indefinitely.
                state.retry_at = 0
            elif result.get("pending"):
                # Continue a partial reconcile soon, but leave the cell free in
                # between: a scan that hits its deadline every pass (codex) had
                # retried every 2 s and never left room for its semantic turn.
                state.retry_at = 0
                state.reconcile_at = self.now() + 15
                # Standing reconciliation debt must not undo this pause.
                state.pause_until = state.reconcile_at
            else:
                state.retry_at = 0
            if operation == "reconcile":
                if not result.get("pending"):
                    state.last_reconcile_ts = time.time()
                    cadence = 30 if state.cell.get("detector") == "poll" else self.reconcile_interval
                    state.reconcile_at = self.now() + cadence
                    if self.queue is not None and state.debt_generation is not None:
                        self.queue.clear_reconciliation_required(
                            state.cell["name"], through_generation=state.debt_generation,
                        )
            elif operation == "tick":
                state.tick_at = self.now() + 2
            elif operation == "semantic":
                # Debt left after a spent budget: come back soon. Peers that are
                # already due keep their turn first (see step()).
                state.semantic_at = self.now() + (5 if result.get("more") else 300)
            if operation != "tick" or result.get("pending"):
                print(f"[lifecycle] {state.cell['name']} {operation} completed: {result}", file=sys.stderr)
        except Exception as exc:
            # Keep only the failed job's paths. Events collected while it was
            # running are still separate, and a full reconcile repairs offline
            # or overflowed paths even after process loss.
            if operation == "semantic":
                # An embedding failure or timeout is not a structural failure:
                # yield only the semantic turn; leave retry_at, reconcile_at and
                # events alone so structural work never waits on embedding.
                state.semantic_at = self.now() + 300
            else:
                state.events = state.sent + state.events
                state.retry_at = self.now() + 30
                # Retained work keeps its retry deadline, not startup priority
                # zero. Otherwise a few broken cells can starve healthy due cells.
                state.reconcile_at = state.retry_at
            print(f"[lifecycle] {state.cell['name']} {operation} retained debt: {exc}", file=sys.stderr)
        finally:
            state.future = None
            state.sent = []
            state.operation = None

    def step(self, cells):
        self.enroll(cells)
        self.collect()
        for state in self.states.values():
            self._finish(state)
        active = sum(s.future is not None for s in self.states.values())
        semantic_active = any(s.operation == "semantic" for s in self.states.values())
        # Semantic owns one slot of its own: structural work (events, reconcile,
        # tick) may use at most workers-1, reconciles at most workers-2 so one
        # slot stays free for exact events.
        structural = active - (1 if semantic_active else 0)
        structural_cap = max(1, self.workers - 1)
        reconcile_cap = max(1, self.workers - 2)
        # Oldest due cells get a turn before another fast tick. Each cell has at
        # most one child, and a slow corpus cannot block the resident owner.
        for state in sorted(self.states.values(), key=lambda s: (s.reconcile_at, s.cell["name"])):
            if structural >= structural_cap:
                break
            if state.future is not None or self.now() < state.retry_at:
                continue
            if not state.cell.get("active", 1):
                continue
            name = state.cell["name"]
            debt = self.queue is not None and self.queue.reconciliation_required(name)
            if state.events:
                operation = "events"
            elif self.now() >= state.reconcile_at or (debt and self.now() >= state.pause_until):
                # Keep one execution slot available for exact events/append
                # checks, even when every broad corpus is cold or behind.
                if structural >= reconcile_cap:
                    continue
                operation = "reconcile"
            elif state.cell.get("cell_type") == "codex" and self.now() >= state.tick_at:
                operation = "tick"
            else:
                continue
            self._start(state, name, operation)
            active += 1
            structural += 1
        # Semantic is one job at a time, chosen after structural work so a cell
        # with events or a due reconcile never holds the turn: the idle due cell
        # that has waited longest takes it, so one reporting `more` cannot keep it.
        if not semantic_active and active < self.workers:
            candidates = [
                s for s in self.states.values()
                if s.future is None and self.now() >= s.retry_at and s.cell.get("active", 1)
                and s.cell.get("cell_type") in SEMANTIC_TYPES and self.now() >= s.semantic_at
                and not s.events
            ]
            if candidates:
                state = min(candidates, key=lambda s: (s.semantic_at, s.cell["name"]))
                self._start(state, state.cell["name"], "semantic")

    def _start(self, state, name, operation):
        if operation in {"events", "reconcile"}:
            state.sent, state.events = state.events[:256], state.events[256:]
        else:
            state.sent = []
        state.debt_generation = (
            self.queue.reconciliation_debt_generation(name) if self.queue else None
        )
        state.operation = operation
        state.future = self.pool.submit(
            self.run_job, name, operation, [str(inv.source_path) for inv in state.sent],
        )

    def _run_child(self, name, operation, paths):
        logs = self.home / "logs" / "lifecycle"
        logs.mkdir(parents=True, exist_ok=True)
        import hashlib
        key = hashlib.sha256(name.encode()).hexdigest()[:16]
        payload = json.dumps({"cell": name, "operation": operation, "paths": paths})
        # stderr is bounded by disk, not a resident capture_output buffer. Output
        # is one small result envelope; provider prints can be read in this log.
        with (logs / f"{key}.log").open("a") as log:
            proc = subprocess.Popen(
                [sys.executable, "-m", "flex.lifecycle_jobs"], stdin=subprocess.PIPE,
                stdout=log, stderr=log, text=True, start_new_session=True,
                env={**os.environ, "FLEX_EMBED_SERVICE_ONLY": "1"},
            )
            with self.process_lock:
                self.processes.add(proc)
                if self.stopping.is_set():
                    os.killpg(proc.pid, signal.SIGKILL)
            try:
                from flex.registry import get_cell_metadata
                declared = get_cell_metadata(name) or {}
                # Derived compilers retain the existing bounded refresh budget;
                # they may build a complete multi-repository candidate. A
                # capture-path timeout must not make that publication impossible.
                from flex.refresh import _refresh_timeout
                default_timeout = _refresh_timeout() if declared.get("refresh_module") and operation == "reconcile" else 300
                timeout = float(os.environ.get("FLEX_LOCAL_JOB_TIMEOUT_S", default_timeout))
                proc.communicate(payload, timeout=timeout)
                if proc.returncode == 75:
                    return {"deferred": True}
                if proc.returncode == SEMANTIC_MORE_EXIT and operation == "semantic":
                    return {"more": True}
                if proc.returncode:
                    raise RuntimeError(f"child exit={proc.returncode}; log={log.name}")
                # Completion is read from Registry, never guessed from stdout.
                from flex.registry import get_cell_metadata
                receipt = get_cell_metadata(name) or {}
                return {"pending": int(receipt.get("refresh_pending") or 0)} if operation != "semantic" else {"more": False}
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
                from flex.registry import mark_refresh_failed
                if operation != "semantic":
                    mark_refresh_failed(name, "lifecycle child timed out", pending=1)
                raise RuntimeError(f"child timed out; log={log.name}")
            finally:
                with self.process_lock:
                    self.processes.discard(proc)

    def refresh_watchers(self, cells):
        from flex.watch import WatcherSet, WatchRegistration, registrations_for_cells, load_watch_config
        if load_watch_config().disabled:
            return
        # Derived compilers already probe complete provider signatures. Watching
        # every repository they select duplicates the source topology and can
        # exhaust inotify before ordinary file/session authorities are online.
        source_cells = [c for c in cells if c.get("cell_type") in DOCUMENT_TYPES | SESSION_TYPES]
        registrations = registrations_for_cells(source_cells)
        # Explicit legacy enrollment, matching the scheduler rather than a
        # provider-specific daemon startup exception.
        for cell in local_cells(cells):
            if cell["name"] == "claude_code":
                from flex.modules.claude_code.compile.worker import CLAUDE_PROJECTS
                if CLAUDE_PROJECTS.is_dir() and not any(r.cell_name == "claude_code" for r in registrations):
                    registrations.append(WatchRegistration("claude_code", CLAUDE_PROJECTS, "**/*.jsonl"))
        signature = tuple(registrations)
        if signature == self.last_watch_signature:
            return
        if self.watcher:
            self.watcher.stop()
        self.last_watch_signature = signature
        self.watcher = WatcherSet(registrations, self.queue)
        self.watcher.start_background()
        print(f"[lifecycle] detector initializing {len(registrations)} roots; reconciliation independent", file=sys.stderr)

    def write_status(self):
        from flex.watch import load_watch_config
        data = {
            "owner": "local-lifecycle", "pid": os.getpid(), "updated_at": time.time(),
            "enabled": not load_watch_config().disabled,
            "healthy": bool(self.watcher and self.watcher.healthy and not self.watcher._starting),
            "backend": self.watcher.backend if self.watcher else None,
            "last_error": self.watcher.last_error if self.watcher else None,
            "initializing": bool(self.watcher and self.watcher._starting),
            "queue": self.queue.stats() if self.queue else {},
            "cells": {name: {
                "path": state.cell["path"], "operation": state.operation,
                "queued_paths": len(state.events), "retry_at": state.retry_at,
                "last_reconcile_ts": state.last_reconcile_ts,
            } for name, state in self.states.items()},
        }
        self.home.mkdir(parents=True, exist_ok=True)
        path = self.home / "lifecycle-status.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, sort_keys=True))
        tmp.replace(path)

    def run(self, interval=2):
        from flex.registry import list_cells
        cells = []
        try:
            while not self.stopping.is_set():
                if self.now() - self.last_registry >= 15:
                    cells = list_cells()
                    self.last_registry = self.now()
                    self.refresh_watchers(cells)
                self.step(cells)
                if self.watcher:
                    self.watcher.check_health()
                self.write_status()
                # One heartbeat per completed step: if the loop stalls for any
                # reason, systemd's WatchdogSec kills and restarts the worker.
                sd_notify("WATCHDOG=1")
                self.stopping.wait(interval)
        finally:
            self.close()

    def close(self):
        self.stopping.set()
        if self.watcher:
            self.watcher.stop()
        with self.process_lock:
            for proc in self.processes:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        self.pool.shutdown(wait=True, cancel_futures=True)
