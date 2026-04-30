"""Local signature watcher for coding-agent cells."""

from __future__ import annotations

import sys
import time
from typing import Any


_running: set[str] = set()
_last_checked: dict[str, float] = {}


def _needs_resync(stats: Any) -> bool:
    """Interpret module dry-run stats without binding every module to one shape."""
    if not isinstance(stats, dict):
        return False
    return stats.get("needs_resync") is True or stats.get("changed") is True


def scan_coding_agent_cells(min_interval_s: float = 0) -> dict[str, int]:
    """Refresh local coding-agent watch cells when their source signatures drift.

    Registry shape:
      lifecycle='watch'
      refresh_module='flex.modules.<agent>.refresh'
      watch_path=<local source store>

    The refresh modules own the source-specific signature contract. This helper
    asks for a dry-run decision and triggers real refresh only when drift is
    reported.
    """
    from flex.registry import discover_watched
    from flex.refresh import refresh_cell

    now = time.monotonic()
    stats = {"checked": 0, "refreshed": 0, "skipped": 0, "errors": 0}

    for cell in discover_watched():
        name = cell.get("name")
        if not name or not cell.get("refresh_module"):
            continue
        if cell.get("cell_type") in {"markdown", "obsidian"}:
            continue
        if name in _running:
            stats["skipped"] += 1
            continue
        if min_interval_s > 0 and now - _last_checked.get(name, 0) < min_interval_s:
            stats["skipped"] += 1
            continue

        stats["checked"] += 1
        _last_checked[name] = now
        try:
            dry = refresh_cell(name, dry_run=True, quiet=True)
            if not _needs_resync(dry):
                stats["skipped"] += 1
                continue

            _running.add(name)
            try:
                result = refresh_cell(name)
            finally:
                _running.discard(name)

            if result is None:
                stats["errors"] += 1
            else:
                stats["refreshed"] += 1
        except Exception as e:
            stats["errors"] += 1
            print(f"[coding-agent-watch] {name}: {e}", file=sys.stderr)

    return stats
