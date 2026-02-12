"""
Flexsearch Core — cell loading, SQL execution, metadata access.

Infrastructure plumbing. Every domain depends on core. No domain logic here.

Functions:
- open_cell()        -> load .db, return conn
- run_sql()          -> execute SQL, return list[dict]
- get_meta/set_meta  -> _meta table access

View generation lives in views.py (same package).
"""

import sqlite3
from typing import Optional

# Re-export for backward compatibility — callers can import from either module
from flexsearch.views import regenerate_views  # noqa: F401


def open_cell(db_path: str) -> sqlite3.Connection:
    """Open a cell database with optimized settings."""
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-20000")
    db.execute("PRAGMA temp_store=MEMORY")
    db.execute("PRAGMA journal_mode=WAL")
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
