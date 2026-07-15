"""Standalone query execution — vec_ops + keyword without MCP.

Chains both materializers, then executes. Works from any context:
CLI, worker scripts, tests, notebooks, or MCP.

Usage:
    from flex.retrieve.execute import open_cell_for_query, execute

    db = open_cell_for_query('my_cell')
    rows = execute(db, '''
        SELECT v.id, v.score, c.content
        FROM vec_ops('similar:ai coding tools diverse') v
        JOIN chunks c ON v.id = c.id
        ORDER BY v.score DESC LIMIT 10
    ''')
    db.close()
"""

import json
import sqlite3
import sys
import threading
from pathlib import Path

from flex.retrieve.vec_ops import materialize_vec_ops
from flex.retrieve.keyword import materialize_keyword
from flex import engine as _engine

# Module-level cache — survives across calls within a process.
# {cell_name: {'caches': {table: VectorCache}, 'config': dict, 'mtime': float,
#              'model': str|None, 'serve_dim': int|None}}
_cache_state: dict = {}
_cache_lock = threading.Lock()

# Unified with engine.py: same resolver (_query_embedder_for), same cache-state
# shape (build_vec_state), same incremental-append policy (refresh_vec_state) —
# so this path serves a tagged cell identically to the MCP path.


def _build_cache(db, name: str, db_path: Path) -> dict | None:
    """Build VectorCache state for a cell — delegates to engine.build_vec_state
    so this path and the MCP path resolve the same way for a tagged cell."""
    mtime = db_path.stat().st_mtime if db_path.exists() else 0
    return _engine.build_vec_state(name, db, mtime)


def _register_udf(db: sqlite3.Connection, state: dict):
    """Register vec_ops UDF on a connection using cached VectorCache —
    delegates to engine.register_vec_udf (the shared tag-aware resolver)."""
    _engine.register_vec_udf(db, state)


def _try_append(state: dict, db) -> bool:
    """Incremental refresh of a cached state — thin bool-returning wrapper
    around engine.refresh_vec_state (the shared append/rebuild policy), kept
    for API compatibility with existing callers/tests. Returns True if all
    tables appended cleanly (successors swapped in), False if a full rebuild
    is needed."""
    return _engine.refresh_vec_state(state, db) == 'appended'


def open_cell_for_query(name: str, force_refresh: bool = False) -> sqlite3.Connection:
    """Open a cell connection with vec_ops registered and ready.

    Caches VectorCache across calls. Refreshes when cell mtime changes.

    Args:
        name: Cell name (resolved via registry)
        force_refresh: Force VectorCache rebuild

    Returns:
        sqlite3.Connection with vec_ops and keyword ready to use via execute()
    """
    from flex.registry import resolve_cell
    from flex.core import open_cell

    p = resolve_cell(name)
    if p is None:
        raise FileNotFoundError(f"Cell '{name}' not found in registry")

    db_path = Path(p) if not isinstance(p, Path) else p
    if not db_path.exists():
        raise FileNotFoundError(f"Cell file not found: {db_path}")

    db = open_cell(str(db_path))

    # Check cache freshness
    current_mtime = db_path.stat().st_mtime
    state = _cache_state.get(name)

    if state and state['mtime'] == current_mtime and not force_refresh:
        _register_udf(db, state)
    else:
        with _cache_lock:
            state = _cache_state.get(name)
            if state and state['mtime'] == current_mtime and not force_refresh:
                _register_udf(db, state)
            elif state and not force_refresh and _try_append(state, db):
                # Incremental: only new rows read; in-flight consumers keep
                # their old self-consistent cache objects.
                state['mtime'] = current_mtime
                _register_udf(db, state)
            else:
                new_state = _build_cache(db, name, db_path)
                if new_state:
                    _cache_state[name] = new_state
                    _register_udf(db, new_state)
                    print(f"[flex] VectorCache {'refreshed' if state else 'warmed'}: "
                          f"{name} ({list(new_state['caches'].keys())})", file=sys.stderr)

    return db


def execute(db: sqlite3.Connection, sql: str) -> list[dict] | dict:
    """Chain vec_ops and keyword materializers, then execute.

    Returns list of row dicts on success, or error dict on failure.

    Usage:
        rows = execute(db, "SELECT v.id, v.score FROM vec_ops('similar:auth') v LIMIT 10")
    """
    sql = sql.strip()

    # Preset dispatch
    if sql.startswith('@'):
        from flex.retrieve.presets import PresetLoader
        loader = PresetLoader()
        result = loader.execute(db, sql)
        if isinstance(result, str):
            return json.loads(result)
        return result

    # Materialize vec_ops → temp table
    sql = materialize_vec_ops(db, sql)
    if sql.startswith('{"error"'):
        return json.loads(sql)

    # Materialize keyword → temp table
    sql = materialize_keyword(db, sql)
    if sql.startswith('{"error"'):
        return json.loads(sql)

    try:
        from flex.modules.query import get_materializers
        for fn in get_materializers():
            sql = fn(db, sql)
            if sql.startswith('{"error"'):
                return json.loads(sql)
    except ImportError:
        pass

    # Execute
    try:
        rows = db.execute(sql).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.DatabaseError as e:
        return {"error": str(e)}
