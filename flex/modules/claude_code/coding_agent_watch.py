"""Local signature watcher for coding-agent cells."""

from __future__ import annotations

import time

from flex.modules.specs import module_spec_for, normalize_cell_type


_last_checked: dict[str, float] = {}


def _is_coding_agent_cell(cell_type: str | None) -> bool:
    """Return whether a cell belongs to the shared coding-agent substrate."""
    normalized = normalize_cell_type(cell_type)
    # Codex is already owned by the bounded event + reconciliation lanes in
    # engines.py. Sending it through the generic refresh module as well performs
    # a duplicate full transpile/embed/enrichment, defeating those bounds and
    # racing the live writer. External substrate modules use this lane; Codex
    # deliberately does not.
    if normalized == "codex":
        return False
    if normalized == "claude_code":
        return True
    spec = module_spec_for(normalized)
    return bool(spec and normalize_cell_type(spec.get("substrate")) == "claude_code")


def scan_coding_agent_cells(min_interval_s: float = 0) -> dict[str, int]:
    """Refresh local coding-agent watch cells when their source signatures drift.

    Registry shape:
      lifecycle='watch'
      refresh_module='flex.modules.<agent>.refresh'
      watch_path=<local source store>

    The Registry LifecycleCoordinator owns the dry-run and real materialization
    boundary. This compatibility entry point only scopes that coordinator to
    coding-agent cells; it never spawns ``flex.refresh --cells`` children.
    """
    from flex.lifecycle import coordinator
    from flex.registry import discover_watched
    from flex.refresh import refresh_cell

    now = time.monotonic()
    stats = {"checked": 0, "started": 0, "skipped": 0, "errors": 0}

    eligible: set[str] = set()
    for cell in discover_watched():
        name = cell.get("name")
        if not name or not cell.get("refresh_module"):
            continue
        if not _is_coding_agent_cell(cell.get("cell_type")):
            continue
        if min_interval_s > 0 and now - _last_checked.get(name, 0) < min_interval_s:
            stats["skipped"] += 1
            continue
        stats["checked"] += 1
        _last_checked[name] = now
        eligible.add(name)

    results = coordinator(refresh_cell).local_watch_pass(
        eligible=lambda cell: cell.get("name") in eligible,
    )
    for status in results.values():
        if status.startswith("error:"):
            stats["errors"] += 1
        elif status == "ok":
            stats["started"] += 1
    stats["skipped"] += stats["checked"] - stats["started"] - stats["errors"]

    return stats
