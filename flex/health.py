"""Operational health summaries for flex services and cells."""

from __future__ import annotations

from collections import Counter
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
