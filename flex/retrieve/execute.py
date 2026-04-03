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

from flex.retrieve.vec_ops import VectorCache, register_vec_ops, materialize_vec_ops
from flex.retrieve.keyword import materialize_keyword

# Module-level cache — survives across calls within a process.
# {cell_name: {'caches': {table: VectorCache}, 'config': dict, 'mtime': float}}
_cache_state: dict = {}
_cache_lock = threading.Lock()

_embedder = None
_embedder_lock = threading.Lock()


def _get_embedder():
    """Lazy-load ONNX embedder singleton."""
    global _embedder
    if _embedder is not None:
        return _embedder
    with _embedder_lock:
        if _embedder is not None:
            return _embedder
        try:
            from flex.onnx import get_model
            _embedder = get_model()
            return _embedder
        except ImportError:
            return None


def _read_vec_config(db) -> dict:
    """Read vec:* keys from _meta."""
    config = {}
    try:
        rows = db.execute(
            "SELECT key, value FROM _meta WHERE key LIKE 'vec:%'"
        ).fetchall()
        for row in rows:
            config[row[0]] = row[1]
    except Exception:
        pass
    return config


def _build_cache(db, name: str, db_path: Path) -> dict | None:
    """Build VectorCache state for a cell."""
    caches = {}
    for table, id_col in [('_raw_chunks', 'id'), ('_raw_sources', 'source_id')]:
        try:
            cache = VectorCache()
            cache.load_from_db(db, table, 'embedding', id_col)
            if cache.size > 0:
                cache.load_columns(db, table, id_col)
                caches[table] = cache
        except Exception:
            pass

    if not caches:
        return None

    mtime = db_path.stat().st_mtime if db_path.exists() else 0
    return {
        'caches': caches,
        'config': _read_vec_config(db),
        'mtime': mtime,
    }


def _register_udf(db: sqlite3.Connection, state: dict):
    """Register vec_ops UDF on a connection using cached VectorCache."""
    embedder = _get_embedder()
    if embedder:
        embed_query = lambda text: embedder.encode(text, prefix='search_query: ')
        embed_doc = lambda text: embedder.encode(text, prefix='search_document: ')
        register_vec_ops(db, state['caches'], embed_query, state['config'],
                         embed_doc_fn=embed_doc)


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
