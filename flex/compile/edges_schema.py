"""
Shared edge-table DDL for all cell modules.

`_edges_source` and `_edges_delegations` were hand-copied into multiple module
workers with `CREATE TABLE IF NOT EXISTS` and no uniqueness constraint, so
`INSERT OR IGNORE` against them was a silent no-op — every re-ingest
appended another identical row. This module is the single source of truth
for both DDLs, now with the uniqueness constraint each table always needed.

`_edges_delegations` is NULL-safe: SQLite treats NULLs as distinct in a
plain UNIQUE index, but codex's fork-lineage rows legitimately carry a NULL
chunk_id (the delegation isn't anchored to one chunk). A COALESCE-based
expression unique index closes that gap — empirically verified that
`INSERT OR IGNORE` respects expression unique indexes on SQLite 3.45.

Usage:
    from flex.compile.edges_schema import ensure_edges_source, ensure_edges_delegations
    ensure_edges_source(conn, source_type='x')
    ensure_edges_delegations(conn)

This creates the tables AND the unique index for a fresh cell. It does
NOT fix an already-built table — `CREATE TABLE IF NOT EXISTS` is a no-op
against existing schema, and a plain `CREATE UNIQUE INDEX IF NOT EXISTS`
will fail outright if the table already carries duplicate rows. Existing
cells are fixed by the migration in `flex/manage/dedupe_edges.py`.
"""

from __future__ import annotations

import sqlite3


def edges_source_ddl(source_type: str) -> str:
    """DDL text for `_edges_source`, ready to interpolate into a larger
    executescript string. Exists (not just `ensure_edges_source` below)
    because several module SCHEMA_DDL constants are imported and executed
    directly by other code and tests (docpac/compile/init.py, install.py
    scripts, etc.) — those callers never get a chance to call a follow-up
    `ensure_edges_source()`, so the DDL has to be embedded inline for them
    too. Keep both call styles pointed at this one string."""
    return f"""
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT '{source_type}',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_es_unique ON _edges_source(chunk_id, source_id);
"""


# NULL-safe: COALESCE each nullable column so codex's fork rows (chunk_id
# IS NULL) dedupe against each other instead of each counting as distinct.
EDGES_DELEGATIONS_DDL = """
CREATE TABLE IF NOT EXISTS _edges_delegations (
    id INTEGER PRIMARY KEY,
    chunk_id TEXT,
    child_session_id TEXT,
    agent_type TEXT,
    created_at INTEGER,
    parent_source_id TEXT
);
DROP INDEX IF EXISTS idx_deleg_chunk_child;
CREATE UNIQUE INDEX IF NOT EXISTS idx_deleg_unique ON _edges_delegations(
    COALESCE(chunk_id, ''),
    COALESCE(child_session_id, ''),
    COALESCE(agent_type, ''),
    COALESCE(parent_source_id, '')
);
"""


def ensure_edges_source(conn: sqlite3.Connection, source_type: str) -> None:
    """Create `_edges_source` (if absent) with its per-module default
    `source_type` and the `UNIQUE(chunk_id, source_id)` index. Idempotent.
    For callers that build their own executescript string, prefer
    interpolating `edges_source_ddl(source_type)` into it directly instead —
    see that function's docstring for why."""
    conn.executescript(edges_source_ddl(source_type))


def ensure_edges_delegations(conn: sqlite3.Connection) -> None:
    """Create `_edges_delegations` (if absent) with the NULL-safe unique
    index. Idempotent. Drops the old non-NULL-safe `idx_deleg_chunk_child`
    index by name if present (harmless no-op on a fresh cell)."""
    conn.executescript(EDGES_DELEGATIONS_DDL)
