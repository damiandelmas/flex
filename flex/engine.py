"""Engine facade — single import point for retrieve + manage internals."""

import json
import gc
import hashlib
import os
import sqlite3
import sys
import threading
import time
import uuid
from pathlib import Path


# ============================================================
# Embedder (singleton)
# ============================================================

_embedder = None
_embedder_lock = threading.Lock()
_embedder_last_used: dict[str, float] = {}
_inference_lock = threading.Lock()


def _touch_embedder(name: str) -> None:
    _embedder_last_used[name] = time.monotonic()


def _encode_with_owner(name: str, embedder, text, **kwargs):
    """Serialize inference so one owner cannot multiply peak workspaces."""
    with _inference_lock:
        _touch_embedder(name)
        try:
            return embedder.encode(text, **kwargs)
        finally:
            _touch_embedder(name)


def get_embedder():
    """Lazy-load ONNX embedder singleton (thread-safe)."""
    global _embedder
    if _embedder is not None:
        _touch_embedder('default')
        return _embedder
    with _embedder_lock:
        if _embedder is not None:
            return _embedder
        try:
            from flex.onnx import get_model
            _embedder = get_model()
            _touch_embedder('default')
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
        _touch_embedder('legacy')
        return _legacy_embedder
    with _legacy_embedder_lock:
        if _legacy_embedder is None:
            from flex.onnx.embed import ONNXEmbedder
            _legacy_embedder = ONNXEmbedder(
                model_path=_LEGACY_MODEL_PATH,
                tokenizer_path=_LEGACY_TOKENIZER_PATH,
            )
        _touch_embedder('legacy')
        return _legacy_embedder


def _get_nomic_embedder():
    """Lazy-load the Nomic (int8) ONNX embedder singleton (thread-safe).
    Caller MUST have already verified `_NOMIC_MODEL_PATH.exists()` -- this
    does not re-check, it only constructs+caches."""
    global _nomic_embedder
    if _nomic_embedder is not None:
        _touch_embedder('nomic')
        return _nomic_embedder
    with _nomic_embedder_lock:
        if _nomic_embedder is not None:
            return _nomic_embedder
        from flex.onnx.embed import ONNXEmbedder
        _nomic_embedder = ONNXEmbedder(
            model_path=_NOMIC_MODEL_PATH, tokenizer_path=_NOMIC_TOKENIZER_PATH)
        _touch_embedder('nomic')
        return _nomic_embedder


def _get_nomic_fp32_embedder():
    """Lazy-load the Nomic fp32 ONNX embedder singleton (thread-safe).
    Caller MUST have already verified `_NOMIC_FP32_MODEL_PATH.exists()` --
    this does not re-check, it only constructs+caches."""
    global _nomic_fp32_embedder
    if _nomic_fp32_embedder is not None:
        _touch_embedder('nomic-fp32')
        return _nomic_fp32_embedder
    with _nomic_fp32_embedder_lock:
        if _nomic_fp32_embedder is not None:
            return _nomic_fp32_embedder
        from flex.onnx.embed import ONNXEmbedder
        _nomic_fp32_embedder = ONNXEmbedder(
            model_path=_NOMIC_FP32_MODEL_PATH,
            tokenizer_path=_NOMIC_FP32_TOKENIZER_PATH)
        _touch_embedder('nomic-fp32')
        return _nomic_fp32_embedder


def embedder_state() -> dict[str, object]:
    """Return process-local model residency without initializing a model."""
    return {
        'resident': [
            name for name, value in (
                ('default', _embedder),
                ('legacy', _legacy_embedder),
                ('nomic', _nomic_embedder),
                ('nomic-fp32', _nomic_fp32_embedder),
            ) if value is not None
        ],
        'last_used': dict(_embedder_last_used),
    }


def release_idle_embedders(idle_seconds: float | None = None) -> list[str]:
    """Release singleton ownership after inactivity.

    In-flight encode closures retain their own reference, so clearing the
    singleton cannot invalidate a running inference.  It only allows the ONNX
    session to be collected after that call completes.
    """
    global _embedder, _legacy_embedder, _nomic_embedder, _nomic_fp32_embedder
    try:
        idle = float(
            idle_seconds if idle_seconds is not None
            else os.environ.get('FLEX_EMBED_IDLE_SEC', '300')
        )
    except (TypeError, ValueError):
        idle = 300.0
    idle = max(0.0, idle)
    now = time.monotonic()
    released = []
    slots = (
        ('default', _embedder_lock, '_embedder'),
        ('legacy', _legacy_embedder_lock, '_legacy_embedder'),
        ('nomic', _nomic_embedder_lock, '_nomic_embedder'),
        ('nomic-fp32', _nomic_fp32_embedder_lock, '_nomic_fp32_embedder'),
    )
    namespace = globals()
    for name, lock, variable in slots:
        used = _embedder_last_used.get(name)
        if namespace[variable] is None or used is None or now - used < idle:
            continue
        with lock:
            used = _embedder_last_used.get(name)
            if namespace[variable] is not None and used is not None and now - used >= idle:
                namespace[variable] = None
                _embedder_last_used.pop(name, None)
                released.append(name)
    if released:
        gc.collect()
    return released


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
        embed_query = lambda text, **kw: _encode_with_owner(
            'nomic-fp32', emb, text,
            prefix='search_query: ', matryoshka_dim=dim, **kw)
        embed_doc = lambda text, **kw: _encode_with_owner(
            'nomic-fp32', emb, text,
            prefix='search_document: ', matryoshka_dim=dim, **kw)
        return embed_query, embed_doc

    elif tag == 'nomic-v1.5':
        if not _NOMIC_MODEL_PATH.exists():
            raise RuntimeError(
                f"vec:model={tag!r} requires missing model {_NOMIC_MODEL_PATH}")
        emb = _get_nomic_embedder()
        embed_query = lambda text, **kw: _encode_with_owner(
            'nomic', emb, text,
            prefix='search_query: ', matryoshka_dim=dim, **kw)
        embed_doc = lambda text, **kw: _encode_with_owner(
            'nomic', emb, text,
            prefix='search_document: ', matryoshka_dim=dim, **kw)
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
    embed_query = lambda text, **kw: _encode_with_owner(
        'legacy', embedder, text,
        prefix='search_query: ', matryoshka_dim=dim, **kw)
    embed_doc = lambda text, **kw: _encode_with_owner(
        'legacy', embedder, text,
        prefix='search_document: ', matryoshka_dim=dim, **kw)
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

    main_path = None
    try:
        main_row = next(
            row for row in db.execute("PRAGMA database_list").fetchall()
            if str(row[1]) == "main"
        )
        if main_row[2]:
            main_path = str(Path(str(main_row[2])).resolve())
    except (StopIteration, OSError, sqlite3.DatabaseError):
        pass

    return {
        'caches': caches,
        'vector_generations': {
            table: cache.source_generation for table, cache in caches.items()
        },
        'config': _read_vec_config(db),
        'mtime': mtime,
        'path': main_path,
        'model': model,          # active vec:model (None = legacy _raw_chunks path)
        'serve_dim': serve_dim,  # Matryoshka slice the query must match
    }


def vector_state_is_current(state: dict, db: sqlite3.Connection) -> bool | None:
    """Compare a cached state to transactional vector/config generations.

    ``None`` means the cell predates generation receipts and callers should use
    the legacy database-mtime fallback. ``False`` is authoritative staleness,
    including WAL-only commits that do not change the main database mtime.
    """
    recorded = (state or {}).get('vector_generations')
    if not isinstance(recorded, dict) or not recorded:
        return None
    try:
        from flex.retrieve.embeddings import active_model, _serve_dim
        from flex.retrieve.vector_generation import vector_generation

        for table, expected in recorded.items():
            if expected is None:
                return None
            if vector_generation(db, table) != expected:
                return False
        if _read_vec_config(db) != (state.get('config') or {}):
            return False
        if active_model(db) != state.get('model'):
            return False
        serve_dim = int(_serve_dim(db) or 128)
        if serve_dim not in (64, 128, 256, 512, 768):
            serve_dim = 128
        return serve_dim == int(state.get('serve_dim') or 0)
    except (ValueError, TypeError, sqlite3.DatabaseError):
        return False


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
    try:
        from flex.retrieve.embeddings import active_model, _serve_dim

        current_serve_dim = int(_serve_dim(db) or 128)
        if current_serve_dim not in (64, 128, 256, 512, 768):
            current_serve_dim = 128
        if (
            _read_vec_config(db) != (state.get('config') or {})
            or ('model' in state and active_model(db) != state.get('model'))
            or ('serve_dim' in state
                and current_serve_dim != int(state.get('serve_dim') or 0))
        ):
            return 'rebuild'
    except (ValueError, TypeError, sqlite3.DatabaseError):
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

    state['vector_generations'] = {
        table: cache.source_generation for table, cache in caches.items()
    }
    state['config'] = _read_vec_config(db)

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


def _quoted_schema(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _world_vec_metadata(db: sqlite3.Connection, alias: str) -> dict[str, str]:
    schema = _quoted_schema(alias)
    try:
        return {
            str(row[0]): str(row[1])
            for row in db.execute(
                f"SELECT key,value FROM {schema}._meta "
                "WHERE key LIKE 'vec:%' "
                "OR key IN ('embedding_model','embedding_dim')"
            ).fetchall()
            if row[1] is not None
        }
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(f"missing vector metadata: {exc}") from exc


def register_world_vec_udf(
    db: sqlite3.Connection, members, native_states
) -> dict[str, str]:
    """Register one exact semantic landscape over native member caches.

    The combined matrix is a derived, disk-backed artifact. Every member is
    validated against its live attached rows before composition; incomplete,
    stale, or incompatible state fails the unified operation rather than
    silently serving a subset.
    """
    import numpy as np

    from flex import registry
    from flex.meta import (
        RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS,
        RETRIEVAL_WORLD_OBJECTS,
    )
    from flex.retrieve.vec_ops import VectorCache, _open_npy_stream, register_vec_ops

    members = tuple(members)
    if not members:
        raise RuntimeError("retrieval world semantic unavailable: no members")

    matrices = []
    member_cache_versions = []
    world_ids: list[str] = []
    timestamps = []
    coordinates: dict[str, tuple[str, str]] = {}
    semantic_contract = None
    shared_config = None
    shared_model = None
    shared_serve_dim = None

    for member in members:
        registry_metadata = registry.get_cell_metadata(member.cell_name)
        if not registry_metadata or not registry_metadata.get("active", 1):
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} is no longer active"
            )
        if str(registry_metadata.get("id") or "") != member.cell_id:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} Registry identity changed after attachment"
            )
        registry_path = registry.resolve_cell(member.cell_name)
        if registry_path is None or Path(registry_path).resolve() != member.path.resolve():
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} Registry path changed after attachment"
            )
        state = (
            native_states.get(member.cell_name)
            or native_states.get(member.cell_id)
        )
        if not state:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} has no native vector state"
            )
        state_path = state.get('path')
        if not state_path or Path(str(state_path)).resolve() != member.path.resolve():
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} native vector state path mismatch"
            )
        try:
            current_mtime = member.path.stat().st_mtime
        except OSError as exc:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} path cannot be read: {exc}"
            ) from exc
        if state.get('mtime') != current_mtime:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} native vector state is stale"
            )

        cache = (state.get('caches') or {}).get('_raw_chunks')
        if cache is None or cache.matrix is None:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} has no chunk vector cache"
            )
        schema = _quoted_schema(member.schema_alias)
        total, embedded, widths, min_width, max_width = db.execute(
            f"SELECT count(*),count(embedding),count(DISTINCT length(embedding)),"
            f"min(length(embedding)),max(length(embedding)) "
            f"FROM {schema}._raw_chunks"
        ).fetchone()
        if total <= 0 or embedded != total or widths != 1 or min_width != max_width:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} embedding coverage is incomplete or mixed "
                f"({embedded}/{total})"
            )
        stored_dim = int(min_width) // 4
        if int(min_width) % 4 or cache.size != total:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} native cache coverage mismatch "
                f"({cache.size}/{total})"
            )
        if cache._stored_dim != stored_dim or cache.matrix.dtype != np.float32:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} vector width or dtype mismatch"
            )

        metadata = _world_vec_metadata(db, member.schema_alias)
        required_metadata = {
            'vec:model',
            'embedding_model',
            'embedding_dim',
            'vec:serve_dim',
            'vec:dtype',
            'vec:normalization',
            'vec:score',
        }
        missing_metadata = sorted(required_metadata - metadata.keys())
        if missing_metadata:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} lacks explicit vector metadata: "
                + ", ".join(missing_metadata)
            )
        model = metadata['vec:model']
        model_fingerprint = metadata['embedding_model']
        try:
            declared_storage_dim = int(metadata['embedding_dim'])
            serve_dim = int(metadata['vec:serve_dim'])
        except ValueError as exc:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} has invalid vector dimensions"
            ) from exc
        if (
            not model
            or not model_fingerprint
            or not declared_storage_dim
            or not serve_dim
            or declared_storage_dim != stored_dim
            or cache.dims != serve_dim
        ):
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} model/dimension metadata disagrees with data"
            )
        declared_dtype = metadata['vec:dtype'].lower()
        if declared_dtype != str(cache.matrix.dtype):
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} declared dtype disagrees with native cache"
            )
        if (
            metadata['vec:normalization'].lower() != 'l2'
            or metadata['vec:score'].lower() != 'cosine'
        ):
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} declares unsupported scoring semantics"
            )
        state_config = {
            str(key): str(value) for key, value in (state.get('config') or {}).items()
        }
        if state.get('model') != model or int(state.get('serve_dim') or 0) != serve_dim:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} native state disagrees with live metadata"
            )
        live_vec_config = {
            key: value for key, value in metadata.items() if key.startswith('vec:')
        }
        if state_config != live_vec_config:
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} scoring/config state is stale"
            )

        member_contract = (
            model, model_fingerprint, stored_dim, serve_dim,
            str(cache.matrix.dtype),
            tuple(sorted(metadata.items())),
        )
        if semantic_contract is None:
            semantic_contract = member_contract
            shared_config = metadata
            shared_model = model
            shared_serve_dim = serve_dim
        elif member_contract != semantic_contract:
            raise RuntimeError(
                "retrieval world semantic unavailable: member vector contracts "
                "are incompatible"
            )

        coordinate_rows = db.execute(
            f"SELECT id,native_id FROM temp.{RETRIEVAL_WORLD_OBJECTS} "
            "WHERE object_kind='chunk' AND cell_id=?",
            (member.cell_id,),
        ).fetchall()
        native_to_world = {str(row[1]): str(row[0]) for row in coordinate_rows}
        if len(native_to_world) != total or set(cache.ids) != set(native_to_world):
            raise RuntimeError(
                "retrieval world semantic unavailable: "
                f"{member.cell_name} coordinate coverage mismatch"
            )
        member_world_ids = [native_to_world[str(native_id)] for native_id in cache.ids]
        world_ids.extend(member_world_ids)
        coordinates.update({
            world_id: (member.cell_id, str(native_id))
            for world_id, native_id in zip(member_world_ids, cache.ids)
        })
        matrices.append(cache.matrix)
        member_cache_versions.append({
            'vector_generation': cache.source_generation,
            'fallback_mtime': (
                state.get('mtime') if cache.source_generation is None else None
            ),
        })
        timestamps.append(
            cache.timestamps
            if cache.timestamps is not None
            else np.zeros(cache.size, dtype=np.float64)
        )

    if len(world_ids) != len(set(world_ids)):
        raise RuntimeError(
            "retrieval world semantic unavailable: world vector IDs collide"
        )
    combined = VectorCache()
    combined.ids = world_ids
    combined._id_bytes = sum(sys.getsizeof(value) for value in world_ids)
    combined._id_to_idx = {value: index for index, value in enumerate(world_ids)}
    combined.dims = int(shared_serve_dim)
    combined._stored_dim = int(semantic_contract[2])
    combined.embedded_count = len(world_ids)

    # Preserve the exact single-landscape scoring contract without allocating
    # a query-local concatenate of every native matrix.  The world artifact is
    # derived from the validated member states and mapped read-only; repeated
    # queries with the same member generations reuse it.
    signature = hashlib.sha256(json.dumps({
        'members': [
            {
                'cell_id': member.cell_id,
                **version,
                'rows': int(matrix.shape[0]),
            }
            for member, matrix, version in zip(
                members, matrices, member_cache_versions,
            )
        ],
        'contract': semantic_contract,
    }, sort_keys=True, default=str).encode('utf-8')).hexdigest()[:24]
    artifact_dir = (
        Path(os.environ.get('FLEX_HOME', str(Path.home() / '.flex')))
        / 'cache' / 'vectors' / 'worlds'
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    matrix_path = artifact_dir / f'{signature}.matrix.npy'
    timestamp_path = artifact_dir / f'{signature}.timestamps.npy'
    total_rows = len(world_ids)

    if matrix_path.exists() and timestamp_path.exists():
        world_matrix = np.load(matrix_path, mmap_mode='r')
        world_timestamps = np.load(timestamp_path, mmap_mode='r')
        if (
            world_matrix.shape != (total_rows, int(shared_serve_dim))
            or world_timestamps.shape != (total_rows,)
            or world_matrix.dtype != np.float32
            or world_timestamps.dtype != np.float64
        ):
            world_matrix = None
    else:
        world_matrix = None

    if world_matrix is None:
        matrix_tmp = matrix_path.with_name(
            f'{matrix_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp'
        )
        timestamp_tmp = timestamp_path.with_name(
            f'{timestamp_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp'
        )
        matrix_out = _open_npy_stream(
            matrix_tmp, np.float32, (total_rows, int(shared_serve_dim)),
        )
        timestamp_out = _open_npy_stream(
            timestamp_tmp, np.float64, (total_rows,),
        )
        try:
            copy_batch = max(1, int(os.environ.get('FLEX_VEC_LOAD_BATCH', '2048')))
            for matrix, member_timestamps in zip(matrices, timestamps):
                for start in range(0, matrix.shape[0], copy_batch):
                    end = min(start + copy_batch, matrix.shape[0])
                    matrix_block = np.ascontiguousarray(
                        matrix[start:end], dtype=np.float32,
                    )
                    timestamp_block = np.ascontiguousarray(
                        member_timestamps[start:end], dtype=np.float64,
                    )
                    matrix_out.write(memoryview(matrix_block).cast('B'))
                    timestamp_out.write(memoryview(timestamp_block).cast('B'))
        finally:
            matrix_out.close()
            timestamp_out.close()
        os.replace(matrix_tmp, matrix_path)
        os.replace(timestamp_tmp, timestamp_path)
        world_matrix = np.load(matrix_path, mmap_mode='r')
        world_timestamps = np.load(timestamp_path, mmap_mode='r')

    combined.matrix = world_matrix
    combined.timestamps = world_timestamps

    embed_query, embed_doc = _query_embedder_for(shared_model, shared_serve_dim)
    register_vec_ops(
        db,
        {'_raw_chunks': combined},
        embed_query,
        shared_config,
        embed_doc_fn=embed_doc,
        result_coordinates=coordinates,
        token_resolver=None,
        reject_extra_tokens=True,
        default_pre_filter_sql=(
            f"SELECT o.id FROM temp.{RETRIEVAL_WORLD_OBJECTS} o "
            f"LEFT JOIN temp.{RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS} x "
            "ON x.object_kind=o.object_kind AND x.id=o.id "
            "WHERE o.object_kind='chunk' AND x.id IS NULL"
        ),
    )
    return {
        "model": str(shared_model),
        "store_dim": str(semantic_contract[2]),
        "serve_dim": str(shared_serve_dim),
    }


# ============================================================
# Query execution
# ============================================================

def execute_preset(
    db: sqlite3.Connection,
    query: str,
    *,
    materializer=None,
    cell_name: str | None = None,
) -> str:
    """Execute a SQL program from the selected cell. Returns JSON."""
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

    loader = PresetLoader(db, cell_name=cell_name)
    available_presets = loader.list_presets()
    if preset_name == "orient" and materializer is not None:
        if positional and str(positional[0]).lower() == "global":
            positional.pop(0)
        elif "self-orient" in available_presets:
            preset_name = "self-orient"
    if preset_name not in available_presets:
        available = available_presets
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

    results = loader.execute(
        db,
        preset_name,
        params,
        materializer=materializer,
    )
    return json.dumps(results, indent=2, default=str)


def materialize(db: sqlite3.Connection, sql: str, *, context=None) -> str:
    """Run materializers without making a read-only cell writable.

    MCP query connections use SQLite ``query_only`` so publication bytes stay
    immutable. Materializers still need query-local TEMP relations. SQLite
    blocks TEMP DDL while ``query_only`` is enabled, so briefly disable that
    connection-local guard around the trusted materializer chain; the staging
    authorizer below continues to deny every durable write and the guard is
    restored before user SQL executes.
    """
    was_query_only = False
    try:
        # MCP enters this function with the staging authorizer already set.
        # ``query_only`` is a connection-local guard, not user SQL; inspect it
        # through the trusted engine boundary before the materializer chain
        # installs its own authorizer.
        db.set_authorizer(None)
        row = db.execute("PRAGMA query_only").fetchone()
        was_query_only = bool(row and row[0])
        if was_query_only:
            db.execute("PRAGMA query_only=OFF")
        return _materialize(db, sql, context=context)
    finally:
        if was_query_only:
            # The materializer chain leaves its staging authorizer installed;
            # clear it for this connection-local PRAGMA, then the MCP wrapper
            # installs the final search authorizer before user SQL runs.
            db.set_authorizer(None)
            db.execute("PRAGMA query_only=ON")


def _materialize(db: sqlite3.Connection, sql: str, *, context=None) -> str:
    """Run the trusted materializer chain. ``materialize`` owns the guard."""
    from flex.mcp_core import materialize_authorizer
    from flex.meta import attach_registered_cells
    from flex.retrieve.doc_mounts import materialize_docs
    from flex.retrieve.vec_ops import materialize_vec_ops
    from flex.retrieve.keyword import materialize_keyword
    from flex.self import MaterializationContext, materialize_self

    context = context or MaterializationContext()

    # Meta is the same materializer primitive at database grain. Attachment is
    # trusted core work; restore the narrow staging authorizer before any
    # ordinary or plugin materializer runs.
    try:
        db.set_authorizer(None)
        sql, error = attach_registered_cells(
            db,
            sql,
            explicit_cells=context.explicit_cells,
            available_cells=context.available_cells,
        )
    finally:
        db.set_authorizer(materialize_authorizer)
    if error:
        return json.dumps({"error": error})

    sql = materialize_self(
        db,
        sql,
        context=context,
        restore_authorizer=materialize_authorizer,
    )
    if sql.startswith('{"error"'):
        return sql

    for fn in (materialize_docs, materialize_vec_ops, materialize_keyword):
        db.set_authorizer(materialize_authorizer)
        sql = fn(db, sql)
        if sql.startswith('{"error"'):
            return sql

    try:
        from flex.modules.query import get_materializers
        for fn in get_materializers():
            db.set_authorizer(materialize_authorizer)
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
