"""Engine facade — single import point for retrieve + manage internals."""

import json
import sqlite3
import sys
import threading
from pathlib import Path


# ============================================================
# Embedder (singleton)
# ============================================================

_embedder = None
_embedder_lock = threading.Lock()


def get_embedder():
    """Lazy-load ONNX embedder singleton (thread-safe)."""
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
            print("[flex-engine] Embedding not available", file=sys.stderr)
            return None


def warm_embedder():
    """Force ONNX session init by encoding a dummy string."""
    embedder = get_embedder()
    if embedder:
        embedder.encode("warmup")
        print("[flex-engine] ONNX embedder warmed", file=sys.stderr)
    return embedder


# ============================================================
# VectorCache state
# ============================================================

def _read_vec_config(db) -> dict:
    """Read vec:* keys from _meta for modulation config."""
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


def build_vec_state(name: str, db: sqlite3.Connection, mtime: float) -> dict | None:
    """Build VectorCache state for a cell. Returns state dict or None."""
    try:
        from flex.retrieve.vec_ops import VectorCache
    except ImportError:
        return None

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

    return {
        'caches': caches,
        'config': _read_vec_config(db),
        'mtime': mtime,
    }


# Force a full VectorCache reload this often even if appends succeed —
# bounds the lifetime of ghost rows from delete+insert sequences the
# count-drift detector cannot see (see VectorCache.append_from_db).
_VEC_FULL_REBUILD_INTERVAL_S = 3600


def refresh_vec_state(state: dict, db: sqlite3.Connection) -> str:
    """Try an incremental append on every cached table.

    Returns 'appended' on success (successor caches swapped in; zero-row
    appends count as success) or 'rebuild' if any table needs a full
    build_vec_state. Successors are applied only if ALL tables succeed,
    so the state never mixes appended and stale tables.
    """
    import time as _time

    caches = (state or {}).get('caches') or {}
    if not caches:
        return 'rebuild'

    updates = {}
    for table, id_col in [('_raw_chunks', 'id'), ('_raw_sources', 'source_id')]:
        cache = caches.get(table)
        if cache is None:
            continue
        if cache.loaded_at and (_time.time() - cache.loaded_at) > _VEC_FULL_REBUILD_INTERVAL_S:
            return 'rebuild'
        try:
            result = cache.append_from_db(db, table, 'embedding', id_col)
        except Exception:
            return 'rebuild'
        if result is None:
            return 'rebuild'
        if result != 0:
            updates[table] = result

    for table, succ in updates.items():
        caches[table] = succ  # single dict-key assignment — atomic swap

    return 'appended'


def register_vec_udf(db: sqlite3.Connection, state: dict):
    """Register vec_ops UDF on a connection using cached VectorCache."""
    try:
        from flex.retrieve.vec_ops import register_vec_ops
        embedder = get_embedder()
        if embedder:
            embed_query = lambda text: embedder.encode(text, prefix='search_query: ')
            embed_doc   = lambda text: embedder.encode(text, prefix='search_document: ')
            register_vec_ops(db, state['caches'], embed_query, state['config'],
                             embed_doc_fn=embed_doc)
    except ImportError:
        pass


# ============================================================
# Query execution
# ============================================================

def execute_preset(db: sqlite3.Connection, query: str) -> str:
    """Execute a @preset query from the cell's _presets table. Returns JSON string."""
    from flex.retrieve.presets import PresetLoader

    parts = query[1:].split()
    preset_name = parts[0]

    # Alias common guesses to orient
    if preset_name in ('help', 'info', 'about', 'introspect', 'orientation'):
        preset_name = 'orient'
    params = {}
    positional = []
    for p in parts[1:]:
        if '=' in p:
            k, v = p.split('=', 1)
            try:
                params[k] = int(v)
            except ValueError:
                params[k] = v
        else:
            positional.append(p)

    loader = PresetLoader(db)
    if preset_name not in loader.list_presets():
        available = loader.list_presets()
        return json.dumps({"error": f"Preset not found: {preset_name}",
                            "available": available})

    # Bind positional args to required params (in declaration order)
    if positional:
        preset = loader.load(preset_name)
        param_str = preset.get('params', '')
        if param_str:
            declared = [p.strip().split()[0] for p in param_str.split(',')]
            for name, value in zip(declared, positional):
                if name not in params:
                    try:
                        params[name] = int(value)
                    except ValueError:
                        params[name] = value

    results = loader.execute(db, preset_name, params)
    return json.dumps(results, indent=2, default=str)


def materialize(db: sqlite3.Connection, sql: str) -> str:
    """Run materializers. Returns transformed SQL or error JSON."""
    from flex.retrieve.doc_mounts import materialize_docs
    from flex.retrieve.vec_ops import materialize_vec_ops
    from flex.retrieve.keyword import materialize_keyword

    sql = materialize_docs(db, sql)
    if sql.startswith('{"error"'):
        return sql
    sql = materialize_vec_ops(db, sql)
    if sql.startswith('{"error"'):
        return sql
    sql = materialize_keyword(db, sql)
    if sql.startswith('{"error"'):
        return sql

    try:
        from flex.modules.query import get_materializers
        for fn in get_materializers():
            sql = fn(db, sql)
            if sql.startswith('{"error"'):
                return sql
    except ImportError:
        pass

    return sql


# ============================================================
# Background indexer
# ============================================================

def drain_primary_cell(cell_path: Path):
    """Run the primary claude_code stat-scan path once. Synchronous."""
    try:
        from flex.modules.engines import drain_primary_cell as _drain
        _drain(cell_path)
    except ImportError:
        pass


def drain_local_cells():
    """Drain local cell sources. Synchronous."""
    try:
        from flex.modules.engines import drain_local_cells as _drain
        _drain()
    except ImportError:
        pass


def run_enrichment(cell_path: Path):
    """Run background enrichment cycle. Synchronous."""
    try:
        from flex.modules.engines import run_enrichment as _enrich
        _enrich(cell_path)
    except ImportError:
        pass
