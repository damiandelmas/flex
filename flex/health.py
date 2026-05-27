"""Operational health summaries for flex services and cells."""

from __future__ import annotations

from collections import Counter
import subprocess
from typing import Iterable

from flex.registry import classify_refresh_state, list_cells


def _fmt_age(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def refresh_problem(cell: dict, state: dict | None = None) -> dict | None:
    """Return a problem record for a refreshable cell, or None when healthy."""
    state = state or classify_refresh_state(cell)
    status = state["effective_refresh_status"]
    raw_status = cell.get("refresh_status") or ""
    age_s = state.get("refresh_age_s")

    if raw_status.startswith("error"):
        severity = "error"
        reason = raw_status
        next_step = f"python -m flex.refresh --cells {cell['name']}"
    elif status == "stale-running":
        severity = "error"
        reason = "running marker exceeded stale window"
        next_step = f"python -m flex.refresh --cells {cell['name']}"
    elif status == "never-run":
        severity = "warning"
        reason = "registered refresh has never completed"
        next_step = f"python -m flex.refresh --cells {cell['name']}"
    elif status == "overdue":
        severity = "warning"
        reason = "last refresh is older than interval"
        next_step = f"python -m flex.refresh --cells {cell['name']}"
    else:
        return None

    return {
        "cell": cell["name"],
        "cell_type": cell.get("cell_type"),
        "status": status,
        "severity": severity,
        "reason": reason,
        "next": next_step,
        "age_s": age_s,
        "age": _fmt_age(age_s),
        "last_refresh_at": cell.get("last_refresh_at"),
        "refresh_interval": cell.get("refresh_interval"),
        "refresh_status": cell.get("refresh_status"),
        "active": bool(cell.get("active", 1)),
        "unlisted": bool(cell.get("unlisted", 0)),
    }


def refresh_problems(
    cells: Iterable[dict] | None = None,
    *,
    include_unlisted: bool = False,
) -> list[dict]:
    """Return refresh lifecycle problems for registered cells."""
    selected = list(cells) if cells is not None else list_cells()
    if not include_unlisted:
        selected = [c for c in selected if not c.get("unlisted")]

    problems = []
    for cell in selected:
        if cell.get("lifecycle") != "refresh":
            continue
        problem = refresh_problem(cell)
        if problem:
            problems.append(problem)

    severity_rank = {"error": 0, "warning": 1}
    problems.sort(key=lambda p: (severity_rank.get(p["severity"], 9), p["cell"]))
    return problems


def refresh_summary(
    cells: Iterable[dict] | None = None,
    *,
    include_unlisted: bool = False,
) -> dict:
    """Return aggregate refresh health for status and HTTP health endpoints."""
    selected = list(cells) if cells is not None else list_cells()
    if not include_unlisted:
        selected = [c for c in selected if not c.get("unlisted")]

    states = []
    for cell in selected:
        if cell.get("lifecycle") == "refresh":
            states.append((cell, classify_refresh_state(cell)))

    problems = [
        problem for cell, state in states
        if (problem := refresh_problem(cell, state))
    ]
    counts = Counter(problem["status"] for problem in problems)
    severity = Counter(problem["severity"] for problem in problems)

    return {
        "status": "degraded" if problems else "ok",
        "refresh_cells": len(states),
        "problems": len(problems),
        "counts": dict(counts),
        "severity": dict(severity),
        "due": sum(1 for _, state in states if state["refresh_due"]),
        "overdue": sum(1 for _, state in states if state["refresh_overdue"]),
        "stale_running": sum(1 for _, state in states if state["refresh_stale"]),
        "never_run": sum(1 for _, state in states if state["refresh_never_run"]),
        "errors": sum(
            1 for cell, _ in states
            if (cell.get("refresh_status") or "").startswith("error")
        ),
        "problem_cells": [problem["cell"] for problem in problems],
    }


def local_worker_state() -> dict:
    """Best-effort local worker service state for watch-cell diagnostics."""
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "flex-worker.service"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        state = result.stdout.strip() or result.stderr.strip() or "unknown"
        return {
            "known": True,
            "active": result.returncode == 0 and state == "active",
            "state": state,
            "manager": "systemd",
            "service": "flex-worker.service",
            "next": "systemctl --user restart flex-worker.service",
        }
    except Exception:
        return {
            "known": False,
            "active": None,
            "state": "unknown",
            "manager": None,
            "service": "flex-worker.service",
            "next": "check flex-worker service",
        }


def watch_problem(cell: dict, worker: dict | None = None, state: dict | None = None) -> dict | None:
    """Return a stable watch-cell problem record, or None when healthy."""
    if cell.get("lifecycle") != "watch":
        return None
    state = state or classify_refresh_state(cell)
    worker = worker or local_worker_state()
    name = cell["name"]
    age_s = state.get("refresh_age_s")

    def record(problem: str, reason: str, next_step: str) -> dict:
        return {
            "cell": name,
            "cell_type": cell.get("cell_type"),
            "status": problem,
            "problem": problem,
            "severity": "error",
            "reason": reason,
            "next": next_step,
            "affected": [name],
            "age_s": age_s,
            "age": _fmt_age(age_s),
            "watch_path": cell.get("watch_path"),
            "refresh_status": cell.get("refresh_status"),
            "last_refresh_at": cell.get("last_refresh_at"),
            "active": bool(cell.get("active", 1)),
            "unlisted": bool(cell.get("unlisted", 0)),
        }

    if state["effective_refresh_status"] == "stale-running":
        return record(
            "watch-stale-running",
            "watch refresh running marker exceeded stale window",
            f"python -m flex.refresh --cells {name}",
        )
    if worker.get("known") and worker.get("active") is False:
        return record(
            "worker-dead",
            "local worker service is not active",
            worker.get("next") or "check flex-worker service",
        )
    raw_status = cell.get("refresh_status") or ""
    if raw_status.startswith("error"):
        return record(
            "watch-refresh-error",
            raw_status,
            f"python -m flex.refresh --cells {name}",
        )
    return None


def watch_problems(
    cells: Iterable[dict] | None = None,
    *,
    include_unlisted: bool = False,
    worker: dict | None = None,
    worker_state: dict | None = None,
) -> list[dict]:
    selected = list(cells) if cells is not None else list_cells()
    if not include_unlisted:
        selected = [c for c in selected if not c.get("unlisted")]
    worker_state = worker_state or worker or local_worker_state()
    problems = []
    for cell in selected:
        if cell.get("lifecycle") != "watch":
            continue
        problem = watch_problem(cell, worker_state)
        if problem:
            problems.append(problem)
    return problems


def watch_summary(
    cells: Iterable[dict] | None = None,
    *,
    include_unlisted: bool = False,
    worker: dict | None = None,
    worker_state: dict | None = None,
) -> dict:
    selected = list(cells) if cells is not None else list_cells()
    if not include_unlisted:
        selected = [c for c in selected if not c.get("unlisted")]
    watch_cells = [c for c in selected if c.get("lifecycle") == "watch"]
    worker_state = worker_state or worker or local_worker_state()
    problems = watch_problems(watch_cells, include_unlisted=True, worker=worker_state)
    counts = Counter(problem["problem"] for problem in problems)
    return {
        "status": "degraded" if problems else "ok",
        "watch_cells": len(watch_cells),
        "problems": len(problems),
        "counts": dict(counts),
        "stale_running": counts.get("watch-stale-running", 0),
        "worker_dead": counts.get("worker-dead", 0),
        "affected_by_worker": [
            problem["cell"] for problem in problems
            if problem["problem"] == "worker-dead"
        ],
        "worker": worker_state,
        "problem_cells": [problem["cell"] for problem in problems],
    }
