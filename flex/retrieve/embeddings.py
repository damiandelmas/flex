"""Active-model tag — thin reader/writer for `vec:model`/`vec:serve_dim`
keys that select which embedder serves a cell's queries.

The multi-model `_embeddings` store this module used to own is retired
(single named vector representation per (id, kind, model)). Serving now reads
`_raw_chunks.embedding`/`_raw_sources.embedding` directly, Matryoshka-sliced
to `vec:serve_dim` at load (see engine.build_vec_state /
flex.retrieve.vec_ops.VectorCache.load_from_db). This module only remains as
the tag: `active_model`/`set_active_model` flip which embedder a cell's
queries (and, via compile/embed.py, its new ingest) use — a metadata write,
not a data migration.
"""
from __future__ import annotations


def set_active_model(
    db, model: str, serve_dim: int | None = None, *, commit: bool = True,
) -> None:
    """Flip the serving/ingest tag (and optional Matryoshka serve dimension).

    Callers which are already inside a publication transaction may defer the
    commit so the model contract and the rows it governs become visible (or
    roll back) together.
    """
    # Avoid `executescript()` compatibility installation inside a caller's
    # savepoint. Fresh canonical cells already own `_metadata`; only legacy or
    # incomplete callers require the migrating helper.
    physical_metadata = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_metadata'"
    ).fetchone()
    if physical_metadata is None:
        from flex.envelope import ensure_metadata_surface
        ensure_metadata_surface(db)
    db.execute("INSERT INTO _metadata(key,value) VALUES('vec:model',?) "
               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (model,))
    if serve_dim is not None:
        db.execute("INSERT INTO _metadata(key,value) VALUES('vec:serve_dim',?) "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(serve_dim),))
    else:
        db.execute("DELETE FROM _metadata WHERE key='vec:serve_dim'")
    if commit:
        db.commit()


def active_model(db) -> str | None:
    from flex.core import get_meta

    return get_meta(db, "vec:model")


def _serve_dim(db) -> int | None:
    from flex.core import get_meta

    value = get_meta(db, "vec:serve_dim")
    return int(value) if value else None
