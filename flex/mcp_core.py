"""Copy-safe MCP query execution helpers.

This module intentionally avoids Flex registry, cell, engine, and module
imports. Flex owns discovery/presets/materialization wiring in mcp_server.py;
FlexVec can reuse this file with its own adapter scaffolding.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable


# SQLite authorizer action codes. Keep numeric constants here so the kernel can
# be copied without importing SQLite's private enum names from another package.
_SQLITE_OK, _SQLITE_DENY = 0, 1
_SQLITE_PRAGMA = 19
_SQLITE_INSERT = 18
_SQLITE_UPDATE = 23
_SQLITE_ATTACH = 24


_SEARCH_ALLOW = {
    20,  # SQLITE_READ
    21,  # SQLITE_SELECT
    29,  # SQLITE_CREATE_VTABLE - FTS5 vtable access
    31,  # SQLITE_FUNCTION
    33,  # SQLITE_RECURSIVE
}

_ALLOWED_PRAGMAS = frozenset(
    {
        "table_info",
        "table_xinfo",
        "index_list",
        "index_info",
        "index_xinfo",
        "foreign_key_list",
        "data_version",
        "page_count",
        "page_size",
    }
)


def search_authorizer(action, arg1, arg2, db_name, trigger_name):
    """SQLite authorizer for final user queries."""
    if action == _SQLITE_PRAGMA:
        return _SQLITE_OK if (arg1 or "").lower() in _ALLOWED_PRAGMAS else _SQLITE_DENY
    return _SQLITE_OK if action in _SEARCH_ALLOW else _SQLITE_DENY


_MATERIALIZE_ALLOW = _SEARCH_ALLOW | {
    3,   # SQLITE_CREATE_TEMP_INDEX
    4,   # SQLITE_CREATE_TEMP_TABLE
    6,   # SQLITE_CREATE_TEMP_VIEW
    12,  # SQLITE_DROP_TEMP_INDEX
    13,  # SQLITE_DROP_TEMP_TABLE
    15,  # SQLITE_DROP_TEMP_VIEW
    22,  # SQLITE_TRANSACTION
    30,  # SQLITE_DROP_VTABLE
}


def materialize_authorizer(action, arg1, arg2, db_name, trigger_name):
    """SQLite authorizer for materializer staging.

    Temp table DDL/DML is allowed so materializers can stage table-valued
    function results. Writes to the main schema, ATTACH, and broad PRAGMA use
    remain blocked.
    """
    if action == _SQLITE_PRAGMA:
        return _SQLITE_OK if (arg1 or "").lower() == "data_version" else _SQLITE_DENY
    if action == _SQLITE_INSERT:
        return _SQLITE_OK if db_name == "temp" else _SQLITE_DENY
    if action == _SQLITE_UPDATE:
        return _SQLITE_OK if db_name == "temp" else _SQLITE_DENY
    if action == _SQLITE_ATTACH:
        return _SQLITE_DENY
    return _SQLITE_OK if action in _MATERIALIZE_ALLOW else _SQLITE_DENY


def is_bare_text(query: str) -> bool:
    """Return True when a query is neither SQL nor a preset expression."""
    q = query.strip()
    if q.startswith("@"):
        return False
    upper = q.upper().lstrip()
    sql_starts = (
        "SELECT",
        "WITH",
        "PRAGMA",
        "EXPLAIN",
        "INSERT",
        "DELETE",
        "UPDATE",
        "DROP",
        "CREATE",
        "ALTER",
        "ATTACH",
    )
    return not any(upper.startswith(kw) for kw in sql_starts)


def bare_text_error(sql: str) -> str:
    """Return the standard helpful error for non-SQL query text."""
    escaped = sql.replace("'", "''")
    return json.dumps(
        {
            "error": f'Not valid SQL: "{sql}"',
            "hint": "Use vec_ops() for semantic search, FTS for keyword match, or @preset syntax.",
            "semantic": (
                f"SELECT v.id, v.score, c.content "
                f"FROM vec_ops('similar:{escaped}') v "
                f"JOIN chunks c ON v.id = c.id "
                f"ORDER BY v.score DESC LIMIT 10"
            ),
            "keyword": (
                f"SELECT k.id, k.rank, k.snippet, c.content "
                f"FROM keyword('{escaped}') k "
                f"JOIN chunks c ON k.id = c.id "
                f"ORDER BY k.rank DESC LIMIT 10"
            ),
        }
    )


def execute_query(
    db: sqlite3.Connection,
    query: str,
    *,
    preset_executor: Callable[[sqlite3.Connection, str], str] | None = None,
    materializer: Callable[[sqlite3.Connection, str], str] | None = None,
) -> str:
    """Execute a read-only SQL query and return JSON text.

    The optional callables are adapter hooks. Flex passes preset and
    materializer implementations; FlexVec can pass only the materializer it
    needs, or neither for plain read-only SQL.
    """
    sql = query.strip()

    if sql.startswith("@"):
        if preset_executor is None:
            return json.dumps({"error": "Presets unavailable."})
        return preset_executor(db, sql)

    if is_bare_text(sql):
        return bare_text_error(sql)

    if materializer is not None:
        try:
            db.set_authorizer(materialize_authorizer)
            sql = materializer(db, sql)
        except sqlite3.DatabaseError as e:
            err_str = str(e)
            if "not authorized" in err_str.lower():
                return json.dumps({"error": "Write operations not allowed"})
            return json.dumps({"error": err_str})
        finally:
            db.set_authorizer(None)
        if sql.startswith('{"error"'):
            return sql

    try:
        db.set_authorizer(search_authorizer)
        rows = db.execute(sql).fetchall()
        results = [dict(r) for r in rows]
        return json.dumps(results, indent=2, default=str)
    except sqlite3.DatabaseError as e:
        err_str = str(e)
        if "not authorized" in err_str.lower():
            return json.dumps({"error": "Write operations not allowed"})
        return json.dumps({"error": err_str})
    finally:
        db.set_authorizer(None)
