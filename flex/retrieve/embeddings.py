"""Active-model tag — thin reader/writer for the `_meta vec:model`/`vec:serve_dim`
keys that select which embedder serves a cell's queries.

The multi-model `_embeddings` store this module used to own is retired
(single named vector representation per (id, kind, model)). Serving now reads
`_raw_chunks.embedding`/`_raw_sources.embedding` directly, Matryoshka-sliced
to `vec:serve_dim` at load (see engine.build_vec_state /
flex.retrieve.vec_ops.VectorCache.load_from_db). This module only remains as
the tag: `active_model`/`set_active_model` flip which embedder a cell's
queries (and, via compile/embed.py, its new ingest) use — a `_meta` write,
not a data migration.
"""
from __future__ import annotations


def set_active_model(db, model: str, serve_dim: int | None = None) -> None:
    """Flip the serving/ingest tag (and optional Matryoshka serve_dim)."""
    db.execute("INSERT INTO _meta(key,value) VALUES('vec:model',?) "
               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (model,))
    if serve_dim is not None:
        db.execute("INSERT INTO _meta(key,value) VALUES('vec:serve_dim',?) "
                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(serve_dim),))
    else:
        db.execute("DELETE FROM _meta WHERE key='vec:serve_dim'")  # reset to model's full dim
    db.commit()


def active_model(db) -> str | None:
    r = db.execute("SELECT value FROM _meta WHERE key='vec:model'").fetchone()
    return r[0] if r else None


def _serve_dim(db) -> int | None:
    r = db.execute("SELECT value FROM _meta WHERE key='vec:serve_dim'").fetchone()
    return int(r[0]) if r and r[0] else None
