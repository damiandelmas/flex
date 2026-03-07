"""
Flex Core — cell loading, SQL execution, metadata access.

Infrastructure plumbing. Every domain depends on core. No domain logic here.

Functions:
- open_cell()        -> load .db, return conn
- run_sql()          -> execute SQL, return list[dict]
- get_meta/set_meta  -> _meta table access

View generation lives in views.py (same package).
"""

import json
import sqlite3
from typing import Optional

# Re-export for backward compatibility — callers can import from either module
from flex.views import regenerate_views  # noqa: F401


def open_cell(db_path: str) -> sqlite3.Connection:
    """Open a cell database with optimized settings."""
    db = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-20000")
    db.execute("PRAGMA temp_store=MEMORY")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA max_page_count=262144")  # 1GB ceiling (256K × 4KB pages)
    return db


def run_sql(db: sqlite3.Connection, query: str,
            params: tuple = ()) -> list[dict]:
    """Execute SQL, return list of dicts."""
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_meta(db: sqlite3.Connection, key: str) -> Optional[str]:
    """Read a single value from _meta table."""
    try:
        row = db.execute(
            "SELECT value FROM _meta WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def set_meta(db: sqlite3.Connection, key: str, value: str):
    """Write a key-value pair to _meta table."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)"
    )
    db.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (key, value)
    )
    db.commit()


def ensure_ops_table(db: sqlite3.Connection):
    """Create _ops table if it doesn't exist. Idempotent."""
    db.execute("""CREATE TABLE IF NOT EXISTS _ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER DEFAULT (strftime('%s','now')),
        operation TEXT,
        target TEXT,
        sql TEXT,
        params TEXT,
        rows_affected INTEGER,
        source TEXT
    )""")


def log_op(db: sqlite3.Connection, operation: str, target: str,
           params: dict = None, rows_affected: int = None,
           source: str = None, sql: str = None):
    """Log a cell mutation to _ops. Self-logging — callers capture their own params."""
    ensure_ops_table(db)
    db.execute(
        "INSERT INTO _ops (operation, target, sql, params, rows_affected, source) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (operation, target, sql,
         json.dumps(params) if params else None,
         rows_affected, source))


def validate_cell(db: sqlite3.Connection):
    """Post-COMPILE sanity checks. Call after population, before embed.

    Catches invariant violations at ingest time, not when a view query
    returns wrong counts 3 months later.
    """
    errors = []

    # Source edge 1:1 invariant — each chunk belongs to exactly one source
    dupes = db.execute("""
        SELECT chunk_id, COUNT(*) as n FROM _edges_source
        GROUP BY chunk_id HAVING n > 1
    """).fetchall()
    if dupes:
        errors.append(f"{len(dupes)} chunks have multiple sources")

    # Every chunk should have a source edge
    orphans = db.execute("""
        SELECT c.id FROM _raw_chunks c
        LEFT JOIN _edges_source e ON c.id = e.chunk_id
        WHERE e.chunk_id IS NULL
    """).fetchall()
    if orphans:
        errors.append(f"{len(orphans)} chunks have no source edge")

    if errors:
        raise ValueError("Cell validation failed: " + "; ".join(errors))
