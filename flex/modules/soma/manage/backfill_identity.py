#!/usr/bin/env python3
"""
SOMA Identity Backfill — stamp `_edges_fs_identity` on existing cells
without a rebuild.

For each target cell (any registered cell with a `_raw_sources` table),
assign SOMA file UUIDs to every path in `_raw_sources` and INSERT OR
REPLACE them into `_edges_fs_identity` — creating that table first if the
cell predates it (e.g. docpac/markdown cells, which never had a SOMA
identity edge table at all; identity-seam G0-identity). Never
rebuilds/re-chunks/re-embeds — this only ever writes into the identity
edge table. Idempotent: a cell already at full coverage (before >= total)
is skipped.

Usage:
  python -m flex.modules.soma.manage.backfill_identity              # sweep all cells
  python -m flex.modules.soma.manage.backfill_identity --dry-run
  python -m flex.modules.soma.manage.backfill_identity --cell NAME

Also reachable as ``flex soma backfill-identity`` (see flex/cli.py).
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# Canonical identity edge table (identity-seam: _edges_fs_identity is the
# schema every engine converges on — the view-join target `instant` already
# writes, not docpac's `_raw_sources.file_uuid` column). Same DDL as
# flex/modules/instant/install.py::_FS_IDENTITY_DDL — kept identical so a
# cell backfilled here and one stamped by instant at compile time are
# indistinguishable.
_FS_IDENTITY_DDL = """
CREATE TABLE IF NOT EXISTS _edges_fs_identity (
    source_id TEXT PRIMARY KEY,
    file_uuid TEXT
);
"""


def _registered_cells() -> list[tuple[str, Path]]:
    """(name, path) for every registered cell.

    Registry enumeration (``flex.registry.list_cells``) first, so this
    targets cells by their real registered ``path``. Falls back to a glob
    of ``~/.flex/cells/labs/*.db`` only if the registry is unavailable.
    """
    try:
        from flex.registry import list_cells

        cells = list_cells()
        if cells:
            return [(c["name"], Path(c["path"])) for c in cells if c.get("path")]
    except Exception:
        pass

    labs = Path.home() / ".flex" / "cells" / "labs"
    return [(p.stem, p) for p in sorted(labs.glob("*.db"))]


def _source_paths(conn: sqlite3.Connection) -> tuple[list[str], dict[str, str] | None]:
    """Paths to assign identity for, plus an optional source_path->source_id map.

    Instant cells: ``source_id`` IS the file path — return it directly
    (map=None). docpac-shape cells: ``_raw_sources`` has a ``source_path``
    column and a non-path ``source_id`` — assign against ``source_path``,
    and return the reverse map so the caller inserts ``(source_id, uuid)``
    instead of ``(source_path, uuid)``.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(_raw_sources)").fetchall()}
    if "source_path" in cols and "source_id" in cols:
        row = conn.execute(
            "SELECT source_id, source_path FROM _raw_sources WHERE source_path IS NOT NULL LIMIT 1"
        ).fetchone()
        if row and row[1] and not str(row[0]).startswith("/"):
            rows = conn.execute(
                "SELECT source_id, source_path FROM _raw_sources WHERE source_path IS NOT NULL"
            ).fetchall()
            id_by_path = {path: sid for sid, path in rows}
            return list(id_by_path.keys()), id_by_path

    paths = [r[0] for r in conn.execute("SELECT source_id FROM _raw_sources").fetchall()]
    return paths, None


def backfill_cell(name: str, db_path: Path, dry_run: bool = False) -> dict:
    """Stamp ``_edges_fs_identity`` on one cell. Returns a result dict; never raises."""
    result: dict = {"name": name, "path": str(db_path)}
    if not db_path.exists():
        result.update(status="skip", reason="db not found")
        return result

    try:
        conn = sqlite3.connect(str(db_path), timeout=30)
    except Exception as e:
        result.update(status="skip", reason=f"open failed: {e}")
        return result

    try:
        has_sources = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_raw_sources'"
        ).fetchone()
        if not has_sources:
            result.update(status="skip", reason="no _raw_sources table (not a chunk-atom cell)")
            return result

        has_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_edges_fs_identity'"
        ).fetchone()
        if not has_table:
            # docpac/markdown-shaped cells never had this table (identity-seam
            # G0-identity gap) — create it rather than skip, so backfill can
            # be the honest closer for cells that predate the shared mint
            # step, not a no-op on them. INSERT-only downstream; never drops
            # or migrates an existing identity surface. Dry-run never
            # mutates the schema — report what WOULD happen instead.
            if dry_run:
                paths, _ = _source_paths(conn)
                result.update(before=0, total=len(paths), status="dry-run",
                              would_create_table=True)
                return result
            conn.executescript(_FS_IDENTITY_DDL)
            conn.commit()

        before = conn.execute("SELECT COUNT(*) FROM _edges_fs_identity").fetchone()[0]
        paths, id_by_path = _source_paths(conn)
        total = len(paths)
        result.update(before=before, total=total)

        if total and before >= total:
            result.update(status="ok", after=before, stamped=0, reason="already full")
            return result

        if dry_run:
            result.update(status="dry-run", after=before)
            return result

        from flex.modules.soma.lib.identity.file_identity import get_instance

        soma = get_instance()
        assigned = soma.assign_batch(paths)
        if id_by_path is not None:
            rows = [(id_by_path[p], u) for p, u in assigned.items() if u and p in id_by_path]
        else:
            rows = [(p, u) for p, u in assigned.items() if u]

        conn.executemany(
            "INSERT OR REPLACE INTO _edges_fs_identity (source_id, file_uuid) VALUES (?, ?)",
            rows,
        )
        conn.commit()

        after = conn.execute("SELECT COUNT(*) FROM _edges_fs_identity").fetchone()[0]
        result.update(status="stamped", after=after, stamped=after - before)
        return result
    except Exception as e:
        result.update(status="error", reason=str(e))
        return result
    finally:
        conn.close()


def run(cell_names: list[str] | None = None, dry_run: bool = False) -> list[dict]:
    """Sweep registered cells (or a subset) and stamp their identity edges.

    Returns one result dict per cell, in registry order.
    """
    cells = _registered_cells()
    if cell_names:
        wanted = set(cell_names)
        cells = [(n, p) for n, p in cells if n in wanted]
    return [backfill_cell(name, path, dry_run=dry_run) for name, path in cells]


def _format_line(r: dict) -> str:
    name = r["name"]
    if r["status"] in ("skip", "error"):
        return f"  {name:<28} skip ({r.get('reason', 'unknown')})"
    before = r.get("before", 0)
    after = r.get("after", before)
    total = r.get("total", 0)
    pct = (after / total * 100) if total else 0.0
    return f"  {name:<28} {before:>7,} -> {after:>7,}  ({pct:5.1f}% of {total:,})"


def report(results: list[dict], dry_run: bool = False) -> int:
    """Print the per-cell + total summary. Returns a process exit code."""
    if not results:
        print("No matching registered cells.")
        return 1

    print("SOMA identity backfill" + (" (dry run)" if dry_run else ""))
    print()
    total_before = total_after = total_total = 0
    swept = 0
    for r in results:
        print(_format_line(r))
        if r["status"] in ("stamped", "ok", "dry-run"):
            swept += 1
            total_before += r.get("before", 0)
            total_after += r.get("after", r.get("before", 0))
            total_total += r.get("total", 0)

    print()
    pct = (total_after / total_total * 100) if total_total else 0.0
    print(
        f"  {'TOTAL':<28} {total_before:>7,} -> {total_after:>7,}  "
        f"({pct:5.1f}% of {total_total:,})  [{swept} cell(s)]"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="SOMA identity backfill — stamp _edges_fs_identity without a rebuild"
    )
    parser.add_argument("--cell", default=None, help="Only this cell (default: all registered cells)")
    parser.add_argument("--dry-run", action="store_true", help="Report coverage only, write nothing")
    args = parser.parse_args(argv)

    results = run([args.cell] if args.cell else None, dry_run=args.dry_run)
    return report(results, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
