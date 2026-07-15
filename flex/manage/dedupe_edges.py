#!/usr/bin/env python3
"""
Edge-table dedupe migration — fixes existing cells whose `_edges_source` /
`_edges_delegations` rows were never constrained, so `INSERT OR IGNORE`
silently no-op'd on every re-ingest (see flex/compile/edges_schema.py).

`CREATE TABLE IF NOT EXISTS` never reaches an already-built table, so the
schema fix in edges_schema.py does nothing for cells that already exist —
this migration is the actual fix for disk. Per table it either:
  - creates the unique index directly (cheap) when there are no duplicates
    to remove, or
  - dedupes via create-clean-table-and-swap (not `DELETE ... WHERE rowid
    NOT IN (...)`, which is substantially slower on large tables), THEN
    creates the index.
Either way, every cell this touches ends up with the unique index — a
cell already free of duplicates is not "safe without one," it is one
re-ingest away from duplicating again, so index creation is never
conditional on `before != after`.

The rebuilt `_edges_source` reuses `edges_schema.edges_source_ddl()` text
verbatim (source-type DEFAULT included), so a migrated table and a
freshly-created one carry the same columns/defaults/indexes — not just the
same rows. (`ALTER TABLE ... RENAME` re-serializes the CREATE TABLE text
with the identifier quoted, so `sqlite_master.sql` won't be byte-identical
to a fresh table's — that's SQLite's own rename behavior, not something
this migration controls, and it has no schema/behavioral effect.)

`_edges_delegations` fork rows (chunk_id IS NULL) carry real lineage —
dedup must be lossless there. The fork-fact count (distinct
`parent_source_id, child_session_id` pairs among NULL-chunk_id rows) is
computed on the staged clean table BEFORE it is swapped in and committed;
a mismatch aborts the whole transaction (`ROLLBACK`), leaving the
original table untouched, rather than swapping in a bad table and merely
logging the problem afterward. This requires explicit transaction control
(`conn.isolation_level = None` + manual BEGIN/ROLLBACK) because Python's
default sqlite3 isolation mode implicitly commits before DDL — a plain
`conn.rollback()` does NOT undo a `CREATE TABLE` under that default,
verified against SQLite's transaction behavior.

Registered cells whose edge tables predate an owning schema builder are swept
too. Their protection against future duplication is the index this migration
installs.

Idempotent: a cell already at target row count + with the index present
is a fast no-op.

Usage:
  python -m flex.manage.dedupe_edges                    # sweep all cells
  python -m flex.manage.dedupe_edges --dry-run
  python -m flex.manage.dedupe_edges --cell CELL_NAME

Also reachable as ``flex edges dedupe`` (see flex/cli.py). Not run inside
a daemon tick — tables may contain millions of rows; run explicitly.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

from flex.compile.edges_schema import edges_source_ddl, EDGES_DELEGATIONS_DDL

_SOURCE_TYPE_DEFAULT_RE = re.compile(r"source_type\s+TEXT\s+DEFAULT\s+'([^']*)'")


def _registered_cells() -> list[tuple[str, Path]]:
    try:
        from flex.registry import list_cells

        cells = list_cells()
        if cells:
            return [(c["name"], Path(c["path"])) for c in cells if c.get("path")]
    except Exception:
        pass

    labs = Path.home() / ".flex" / "cells"
    return [(p.stem, p) for p in sorted(labs.glob("*.db"))]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _index_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (name,)
    ).fetchone() is not None


def _current_source_type_default(conn: sqlite3.Connection) -> str:
    """Extract the existing `source_type TEXT DEFAULT '...'` value from
    `_edges_source`'s current schema, so the rebuilt table keeps it instead
    of silently dropping it (a real cell always has one — every DDL site,
    including the 3 that predate the shared helper, declares this column
    with a per-module default)."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='_edges_source'"
    ).fetchone()
    if row and row[0]:
        m = _SOURCE_TYPE_DEFAULT_RE.search(row[0])
        if m:
            return m.group(1)
    return ""


def _swap_table(conn: sqlite3.Connection, old: str, new: str) -> None:
    """Drop `old`, rename `new` -> `old`. `new`'s indexes already carry the
    final names/definitions (built via the real DDL text against the `new`
    table name), so no separate re-create step is needed after the rename.

    Requires `PRAGMA legacy_alter_table=ON`. Without it, `ALTER TABLE ...
    RENAME` tries to rewrite dependent views' SQL text to point at the new
    name, but since the new table is being renamed BACK to the name the
    view already references, that rewrite pass corrupts the view mid-flight
    ("error in view chunks: no such table"). legacy_alter_table skips the
    rewrite entirely, which is exactly correct when old-name == new-name."""
    conn.execute(f"DROP TABLE {old}")
    conn.execute("PRAGMA legacy_alter_table=ON")
    conn.execute(f"ALTER TABLE {new} RENAME TO {old}")
    conn.execute("PRAGMA legacy_alter_table=OFF")


def dedupe_edges_source(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """Dedupe `_edges_source` by (chunk_id, source_id), keeping the
    lowest-rowid row per key. Always ends with the unique index in place
    (unless dry_run). Returns {"before", "after", "status", ...}."""
    if not _table_exists(conn, "_edges_source"):
        return {"status": "skip", "reason": "no _edges_source table"}

    before = conn.execute("SELECT COUNT(*) FROM _edges_source").fetchone()[0]
    after = conn.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM _edges_source GROUP BY chunk_id, source_id)"
    ).fetchone()[0]
    has_index = _index_exists(conn, "idx_es_unique")

    if before == after and has_index:
        return {"status": "ok", "before": before, "after": after, "removed": 0}
    if dry_run:
        return {"status": "dry-run", "before": before, "after": after,
                 "removed": before - after, "would_index": not has_index}

    if before == after:
        # No duplicates to remove — just install the index directly, no
        # table rebuild needed. Still the deliverable of this function:
        # a cell must never leave here without the unique index in place.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_es_unique ON _edges_source(chunk_id, source_id)")
        conn.commit()
        return {"status": "indexed", "before": before, "after": after, "removed": 0}

    source_type = _current_source_type_default(conn)
    # Reuse the real shared DDL text (DEFAULT clause included) against the
    # staging table name, so the rebuilt table's schema is byte-identical
    # to a freshly-created one, not hand-approximated here.
    new_ddl = edges_source_ddl(source_type).replace("_edges_source", "_edges_source_new")

    conn.execute("DROP TABLE IF EXISTS _edges_source_new")
    conn.executescript(new_ddl)
    conn.execute("""
        INSERT INTO _edges_source_new (chunk_id, source_id, source_type, position)
        SELECT chunk_id, source_id, source_type, position
        FROM _edges_source
        WHERE rowid IN (SELECT MIN(rowid) FROM _edges_source GROUP BY chunk_id, source_id)
    """)
    _swap_table(conn, "_edges_source", "_edges_source_new")
    conn.commit()

    actual_after = conn.execute("SELECT COUNT(*) FROM _edges_source").fetchone()[0]
    return {"status": "deduped", "before": before, "after": actual_after,
            "removed": before - actual_after}


def dedupe_edges_delegations(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """Dedupe `_edges_delegations` by the NULL-safe COALESCE key, keeping
    the lowest-id row per key. Always ends with the NULL-safe unique index
    in place (unless dry_run). The fork-fact count is checked on the
    staged clean table BEFORE swap/commit — a mismatch rolls back the
    whole transaction instead of swapping in a bad table."""
    if not _table_exists(conn, "_edges_delegations"):
        return {"status": "skip", "reason": "no _edges_delegations table"}

    before = conn.execute("SELECT COUNT(*) FROM _edges_delegations").fetchone()[0]
    after = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM _edges_delegations
            GROUP BY COALESCE(chunk_id,''), COALESCE(child_session_id,''),
                     COALESCE(agent_type,''), COALESCE(parent_source_id,'')
        )
    """).fetchone()[0]
    has_index = _index_exists(conn, "idx_deleg_unique")

    fork_facts_before = conn.execute("""
        SELECT COUNT(DISTINCT COALESCE(parent_source_id,'') || '||' || COALESCE(child_session_id,''))
        FROM _edges_delegations WHERE chunk_id IS NULL
    """).fetchone()[0]

    if before == after and has_index:
        return {"status": "ok", "before": before, "after": after, "removed": 0,
                "fork_facts": fork_facts_before}
    if dry_run:
        return {"status": "dry-run", "before": before, "after": after,
                "removed": before - after, "fork_facts": fork_facts_before,
                "would_index": not has_index}

    if before == after:
        # No duplicates — just install the NULL-safe index, dropping the
        # old non-NULL-safe one by name if it's still there.
        conn.executescript(EDGES_DELEGATIONS_DDL)
        conn.commit()
        return {"status": "indexed", "before": before, "after": after, "removed": 0,
                "fork_facts": fork_facts_before}

    # Explicit transaction control: Python's sqlite3 default isolation mode
    # implicitly commits before DDL, so a plain conn.rollback() would NOT
    # undo the CREATE TABLE below (verified empirically). isolation_level=None
    # + manual BEGIN/ROLLBACK is required to make this atomic.
    prior_isolation = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN")
        conn.execute("DROP TABLE IF EXISTS _edges_delegations_new")
        conn.execute("""
            CREATE TABLE _edges_delegations_new (
                id INTEGER PRIMARY KEY,
                chunk_id TEXT,
                child_session_id TEXT,
                agent_type TEXT,
                created_at INTEGER,
                parent_source_id TEXT
            )
        """)
        conn.execute("""
            INSERT INTO _edges_delegations_new (id, chunk_id, child_session_id, agent_type, created_at, parent_source_id)
            SELECT id, chunk_id, child_session_id, agent_type, created_at, parent_source_id
            FROM _edges_delegations
            WHERE id IN (
                SELECT MIN(id) FROM _edges_delegations
                GROUP BY COALESCE(chunk_id,''), COALESCE(child_session_id,''),
                         COALESCE(agent_type,''), COALESCE(parent_source_id,'')
            )
        """)

        # Guard BEFORE swap/commit: check the staged table, not the live one.
        fork_facts_staged = conn.execute("""
            SELECT COUNT(DISTINCT COALESCE(parent_source_id,'') || '||' || COALESCE(child_session_id,''))
            FROM _edges_delegations_new WHERE chunk_id IS NULL
        """).fetchone()[0]

        if fork_facts_staged != fork_facts_before:
            conn.execute("ROLLBACK")
            return {
                "status": "error", "before": before, "after": after,
                "fork_facts": fork_facts_before,
                "reason": (
                    f"fork-fact count would change: {fork_facts_before} -> {fork_facts_staged} "
                    "(dedup must be lossless — aborted, original table untouched)"
                ),
            }

        conn.execute("DROP INDEX IF EXISTS idx_deleg_chunk_child")
        conn.execute(f"DROP TABLE _edges_delegations")
        conn.execute("PRAGMA legacy_alter_table=ON")
        conn.execute("ALTER TABLE _edges_delegations_new RENAME TO _edges_delegations")
        conn.execute("PRAGMA legacy_alter_table=OFF")
        # executescript() implicitly commits any pending transaction before it
        # runs its own statements — so this both finalizes the drop/rename
        # above AND creates the index, in one commit. No separate COMMIT after
        # this: by the time executescript returns, nothing is left open (a
        # raw "COMMIT" here would fail with "cannot commit - no transaction
        # is active" — hit this empirically while building the guard above).
        conn.executescript(EDGES_DELEGATIONS_DDL)
    finally:
        conn.isolation_level = prior_isolation

    actual_after = conn.execute("SELECT COUNT(*) FROM _edges_delegations").fetchone()[0]
    fork_facts_after = conn.execute("""
        SELECT COUNT(DISTINCT COALESCE(parent_source_id,'') || '||' || COALESCE(child_session_id,''))
        FROM _edges_delegations WHERE chunk_id IS NULL
    """).fetchone()[0]

    return {"status": "deduped", "before": before, "after": actual_after,
            "removed": before - actual_after, "fork_facts": fork_facts_after}


def dedupe_cell(name: str, db_path: Path, dry_run: bool = False, vacuum: bool = False) -> dict:
    """Dedupe both edge tables on one cell. Returns a result dict; never raises."""
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
        t0 = time.time()
        result["_edges_source"] = dedupe_edges_source(conn, dry_run=dry_run)
        result["_edges_delegations"] = dedupe_edges_delegations(conn, dry_run=dry_run)
        result["elapsed_s"] = round(time.time() - t0, 2)

        did_write = any(
            result[k].get("status") in ("deduped", "indexed")
            for k in ("_edges_source", "_edges_delegations")
        )
        if did_write and vacuum and not dry_run:
            conn.execute("VACUUM")

        statuses = {result["_edges_source"]["status"], result["_edges_delegations"]["status"]}
        if "error" in statuses:
            result["status"] = "error"
        elif "deduped" in statuses:
            result["status"] = "deduped"
        elif "indexed" in statuses:
            result["status"] = "indexed"
        elif "dry-run" in statuses:
            result["status"] = "dry-run"
        else:
            result["status"] = "ok"
        return result
    except Exception as e:
        result.update(status="error", reason=str(e))
        return result
    finally:
        conn.close()


def run(cell_names: list[str] | None = None, dry_run: bool = False, vacuum: bool = False) -> list[dict]:
    cells = _registered_cells()
    if cell_names:
        wanted = set(cell_names)
        cells = [(n, p) for n, p in cells if n in wanted]
    return [dedupe_cell(name, path, dry_run=dry_run, vacuum=vacuum) for name, path in cells]


def _format_sub(label: str, r: dict) -> str:
    if r["status"] in ("skip", "error"):
        return f"      {label:<20} {r['status']} ({r.get('reason', 'unknown')})"
    before, after = r.get("before", 0), r.get("after", 0)
    extra = f"  fork_facts={r['fork_facts']:,}" if "fork_facts" in r else ""
    return f"      {label:<20} {before:>10,} -> {after:>10,}  (-{r.get('removed', 0):,}){extra}"


def report(results: list[dict], dry_run: bool = False) -> int:
    if not results:
        print("No matching registered cells.")
        return 1

    print("Edge-table dedupe" + (" (dry run)" if dry_run else ""))
    print()
    had_error = False
    for r in results:
        if r["status"] == "skip":
            print(f"  {r['name']:<24} skip ({r.get('reason', 'unknown')})")
            continue
        print(f"  {r['name']:<24} [{r['status']}]  {r.get('elapsed_s', 0)}s")
        if "_edges_source" in r:
            print(_format_sub("_edges_source", r["_edges_source"]))
        if "_edges_delegations" in r:
            print(_format_sub("_edges_delegations", r["_edges_delegations"]))
        if r["status"] == "error":
            had_error = True
    print()
    return 1 if had_error else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Dedupe _edges_source / _edges_delegations on existing cells"
    )
    parser.add_argument("--cell", default=None, help="Only this cell (default: all registered cells)")
    parser.add_argument("--dry-run", action="store_true", help="Report counts only, write nothing")
    parser.add_argument("--vacuum", action="store_true", help="VACUUM cells that were actually deduped")
    args = parser.parse_args(argv)

    results = run([args.cell] if args.cell else None, dry_run=args.dry_run, vacuum=args.vacuum)
    return report(results, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
