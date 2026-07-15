"""Engine facade — single import point for retrieve + manage internals."""

import json
import os
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
# Inline tag resolver (minilm | nomic-v1.5 | nomic-v1.5-fp32)
# ============================================================
#
# Three serving models use a small explicit switch: tag 'minilm'/absent ->
# the bundled default embedder; tag 'nomic-v1.5' -> the legacy int8 Nomic
# ONNX embedder; tag 'nomic-v1.5-fp32' -> the fp32 Nomic embedder. int8 is
# NOT reproducible cross-ISA (ORT's dynamic
# quantization kernels saturate differently by ISA -- rank correlation ~0
# cross-machine, measured) and has no CUDA kernels, so it cannot be GPU-
# accelerated. fp32 and fp16 are the SAME embedding space (cos 1.000000) --
# fp32 serves queries on CPU (no fp16 upcast overhead), fp16 embeds docs on
# GPU (tensor cores) -- but int8 vs float is cos ~0.96, a DIFFERENT space.
# An int8-tagged cell MUST keep the int8 embedder until it is explicitly
# re-embedded (flex.compile.reembed) and re-stamped `nomic-v1.5-fp32` --
# there is never a mixed-space window where a cell's stored (int8) vectors
# and its live query embedder disagree on space.
#
# Every branch is guarded by an explicit path-exists check (NOT a try/except
# around construction -- ONNXEmbedder.__init__ never touches disk; the model
# file is opened lazily in the `session` property at first `.encode()`). A
# missing model fails closed: falling back would silently query another vector
# space while appearing healthy.

_NOMIC_MODEL_PATH = (
    Path(os.environ.get("FLEX_HOME", str(Path.home() / ".flex")))
    / "models" / "nomic-v1.5" / "model.onnx"
)
_NOMIC_TOKENIZER_PATH = _NOMIC_MODEL_PATH.parent / "tokenizer.json"

# fp32 model dir uses basename `model.onnx` (NOT `model_fp16.onnx` -- that
# basename is specific to the fp16 dir); tokenizer is identical across all
# three Nomic dirs but we read fp32's own copy for locality.
_NOMIC_FP32_MODEL_PATH = (
    Path(os.environ.get("FLEX_HOME", str(Path.home() / ".flex")))
    / "models" / "nomic-v1.5-fp32" / "model.onnx"
)
_NOMIC_FP32_TOKENIZER_PATH = _NOMIC_FP32_MODEL_PATH.parent / "tokenizer.json"

# The pre-0.52 default MiniLM artifact remains a compatibility reader only.
# Upgrades retain it at ~/.flex/models; clean installs do not download it.
_LEGACY_MODEL_PATH = (
    Path(os.environ.get("FLEX_HOME", str(Path.home() / ".flex")))
    / "models" / "model.onnx"
)
_LEGACY_TOKENIZER_PATH = _LEGACY_MODEL_PATH.parent / "tokenizer.json"
if not _LEGACY_MODEL_PATH.exists():
    _LEGACY_MODEL_PATH = Path(__file__).parent / "onnx" / "model.onnx"
if not _LEGACY_TOKENIZER_PATH.exists():
    _LEGACY_TOKENIZER_PATH = Path(__file__).parent / "onnx" / "tokenizer.json"

_nomic_embedder = None
_nomic_embedder_lock = threading.Lock()

_nomic_fp32_embedder = None
_nomic_fp32_embedder_lock = threading.Lock()

_legacy_embedder = None
_legacy_embedder_lock = threading.Lock()


def _get_legacy_embedder():
    """Load the retained pre-0.52 MiniLM model for legacy cells only."""
    global _legacy_embedder
    if _legacy_embedder is not None:
        return _legacy_embedder
    with _legacy_embedder_lock:
        if _legacy_embedder is None:
            from flex.onnx.embed import ONNXEmbedder
            _legacy_embedder = ONNXEmbedder(
                model_path=_LEGACY_MODEL_PATH,
                tokenizer_path=_LEGACY_TOKENIZER_PATH,
            )
        return _legacy_embedder


def _get_nomic_embedder():
    """Lazy-load the Nomic (int8) ONNX embedder singleton (thread-safe).
    Caller MUST have already verified `_NOMIC_MODEL_PATH.exists()` -- this
    does not re-check, it only constructs+caches."""
    global _nomic_embedder
    if _nomic_embedder is not None:
        return _nomic_embedder
    with _nomic_embedder_lock:
        if _nomic_embedder is not None:
            return _nomic_embedder
        from flex.onnx.embed import ONNXEmbedder
        _nomic_embedder = ONNXEmbedder(
            model_path=_NOMIC_MODEL_PATH, tokenizer_path=_NOMIC_TOKENIZER_PATH)
        return _nomic_embedder


def _get_nomic_fp32_embedder():
    """Lazy-load the Nomic fp32 ONNX embedder singleton (thread-safe).
    Caller MUST have already verified `_NOMIC_FP32_MODEL_PATH.exists()` --
    this does not re-check, it only constructs+caches."""
    global _nomic_fp32_embedder
    if _nomic_fp32_embedder is not None:
        return _nomic_fp32_embedder
    with _nomic_fp32_embedder_lock:
        if _nomic_fp32_embedder is not None:
            return _nomic_fp32_embedder
        from flex.onnx.embed import ONNXEmbedder
        _nomic_fp32_embedder = ONNXEmbedder(
            model_path=_NOMIC_FP32_MODEL_PATH,
            tokenizer_path=_NOMIC_FP32_TOKENIZER_PATH)
        return _nomic_fp32_embedder


def _query_embedder_for(tag: str | None, serve_dim: int | None = None):
    """Resolve (embed_query_fn, embed_doc_fn) for a cell's `vec:model` tag.

    tag 'minilm' / None -> bundled default embedder, symmetric prefixes,
    dim 128 (byte-identical to the legacy untagged path).
    tag 'nomic-v1.5' -> legacy int8 Nomic embedder, asymmetric
    `search_query:`/`search_document:` prefixes, at `serve_dim` (defaults to
    128). Kept serving int8-tagged cells until they
    are explicitly re-embedded -- never silently upgraded to a different
    space.
    tag 'nomic-v1.5-fp32' -> fp32 Nomic embedder (same space as fp16, the
    GPU doc-embed precision -- cos 1.000000), same prefixes/dim contract.

    Explicit tags fail closed. A vector space is invisible in its bytes, so
    neither an unknown tag nor a missing tagged model may fall back to another
    embedder: doing so would query or ingest in the wrong space while appearing
    healthy.
    """
    from flex.onnx.embed import STORE_DIM

    dim = serve_dim or STORE_DIM

    if tag == 'nomic-v1.5-fp32':
        if not _NOMIC_FP32_MODEL_PATH.exists():
            raise RuntimeError(
                f"vec:model={tag!r} requires missing model {_NOMIC_FP32_MODEL_PATH}")
        emb = _get_nomic_fp32_embedder()
        embed_query = lambda text, **kw: emb.encode(
            text, prefix='search_query: ', matryoshka_dim=dim, **kw)
        embed_doc = lambda text, **kw: emb.encode(
            text, prefix='search_document: ', matryoshka_dim=dim, **kw)
        return embed_query, embed_doc

    elif tag == 'nomic-v1.5':
        if not _NOMIC_MODEL_PATH.exists():
            raise RuntimeError(
                f"vec:model={tag!r} requires missing model {_NOMIC_MODEL_PATH}")
        emb = _get_nomic_embedder()
        embed_query = lambda text, **kw: emb.encode(
            text, prefix='search_query: ', matryoshka_dim=dim, **kw)
        embed_doc = lambda text, **kw: emb.encode(
            text, prefix='search_document: ', matryoshka_dim=dim, **kw)
        return embed_query, embed_doc

    elif tag not in (None, 'minilm'):
        raise ValueError(f"unrecognized vec:model tag {tag!r}")

    # Only 'minilm' or absent use the retained pre-0.52 space. Never route
    # these bytes through the new fp32 default model.
    if not _LEGACY_MODEL_PATH.exists() or not _LEGACY_TOKENIZER_PATH.exists():
        raise RuntimeError(
            "legacy minilm cell requires its retained pre-0.52 model; "
            "the model is not installed"
        )
    embedder = _get_legacy_embedder()
    embed_query = lambda text, **kw: embedder.encode(
        text, prefix='search_query: ', matryoshka_dim=dim, **kw)
    embed_doc = lambda text, **kw: embedder.encode(
        text, prefix='search_document: ', matryoshka_dim=dim, **kw)
    return embed_query, embed_doc


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

    # Tag-driven, single-column serving: every cell — nomic or minilm —
    # serves from `_raw_chunks.embedding`/`_raw_sources.embedding` directly,
    # Matryoshka-sliced to serve_dim at load. No `_embeddings`-table gate: the
    # multi-model store was retired once serving moved to the column.
    # Each cell's metadata selects its bounded serving dimension.
    from flex.retrieve.embeddings import active_model
    try:
        model = active_model(db)
    except Exception:
        model = None
    # serve_dim comes from the cell's own `_meta vec:serve_dim` (written by
    # set_active_model at reembed/stamp time), bounded to sane Matryoshka slices.
    # minilm cells are stamped 128 (stored width — the slice is a no-op, byte-
    # identical to the legacy path); fp32 cells carry 128 or 256. A missing or
    # garbage value falls back to 128, never to "serve the raw stored width".
    try:
        from flex.retrieve.embeddings import _serve_dim
        serve_dim = int(_serve_dim(db) or 128)
    except Exception:
        serve_dim = 128
    if serve_dim not in (64, 128, 256, 512, 768):
        serve_dim = 128

    caches = {}
    for table, id_col in [('_raw_chunks', 'id'), ('_raw_sources', 'source_id')]:
        try:
            cache = VectorCache()
            cache.load_from_db(db, table, 'embedding', id_col, serve_dim=serve_dim)
            if cache.size > 0:
                cache.load_columns(db, table, id_col)   # timestamps by id — source-agnostic
                caches[table] = cache
        except Exception:
            pass

    if not caches:
        return None

    return {
        'caches': caches,
        'config': _read_vec_config(db),
        'mtime': mtime,
        'model': model,          # active vec:model (None = legacy _raw_chunks path)
        'serve_dim': serve_dim,  # Matryoshka slice the query must match
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
    """Register vec_ops UDF on a connection using cached VectorCache.

    Tag-driven via the inline `_query_embedder_for` resolver: when the state
    carries an active `vec:model` (e.g. 'nomic-v1.5'), the query is embedded
    with THAT tag's embedder at the cell's serve_dim — so the query vector
    lands in the same space as the column-sourced matrix. Untagged cells
    (model=None) use the bundled default embedder, unchanged."""
    try:
        from flex.retrieve.vec_ops import register_vec_ops
    except ImportError:
        return
    model = state.get('model')
    serve_dim = state.get('serve_dim')
    embed_query, embed_doc = _query_embedder_for(model, serve_dim)
    if embed_query:
        register_vec_ops(db, state['caches'], embed_query, state['config'],
                         embed_doc_fn=embed_doc)


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
