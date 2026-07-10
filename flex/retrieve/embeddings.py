"""Multi-model embeddings store — one table, many representations.

Every vector representation (a semantic model, a projection space like LDA, a
reranker space) is a named `model` in `_embeddings`. A cell can carry several
models at once; the active serving model is `_meta vec:model`, sliced to
`vec:serve_dim` at load (Matryoshka). Switching or A/B-ing a model is a `_meta`
flip — no re-embed needed to revert, and the dim is explicit data per model so
nothing assumes a column width.

This is additive: it does NOT touch `_raw_chunks.embedding` or any existing
reader. Integration (pointing VectorCache at the active model) is a later step.
"""
from __future__ import annotations

import numpy as np

_DDL = """
CREATE TABLE IF NOT EXISTS _embeddings (
    id      TEXT NOT NULL,
    kind    TEXT NOT NULL DEFAULT 'chunk',   -- 'chunk' | 'source'
    model   TEXT NOT NULL,                   -- '<family>-<variant>-<dim>'
    dim     INTEGER NOT NULL,                -- stored (full) dim of this row's vector
    vector  BLOB NOT NULL,                   -- float32, unit-norm, len = dim*4
    PRIMARY KEY (id, kind, model)
);
CREATE INDEX IF NOT EXISTS idx_emb_model ON _embeddings(model);
"""


def ensure_embeddings_table(db) -> None:
    db.executescript(_DDL)
    db.commit()


def set_active_model(db, model: str, serve_dim: int | None = None) -> None:
    """Flip the serving model (and optional Matryoshka serve_dim). Reversible — the
    other models' rows are untouched."""
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


def store_embeddings(db, items, model: str, kind: str = "chunk") -> int:
    """items: iterable of (id, vector). Vectors are stored at their full dim;
    re-storing a (id, kind, model) replaces it. New `model` = new rows, old models
    untouched (that's the A/B)."""
    rows = []
    for id_, v in items:
        v = np.asarray(v, dtype=np.float32).ravel()
        rows.append((id_, kind, model, int(v.shape[0]), v.tobytes()))
    db.executemany(
        "INSERT OR REPLACE INTO _embeddings(id,kind,model,dim,vector) VALUES(?,?,?,?,?)", rows)
    db.commit()
    return len(rows)


def list_models(db):
    """[(model, kind, n, dim)] — what representations this cell holds."""
    return db.execute(
        "SELECT model, kind, COUNT(*), MAX(dim) FROM _embeddings GROUP BY model, kind"
    ).fetchall()


def mean_pool_sources(db, model: str, kind_chunk: str = "chunk",
                      commit_every: int = 500) -> int:
    """Mean-pool each source's vector from its chunks' vectors in `model`'s
    space, and store into `_embeddings` (kind='source'). Mirrors the legacy
    `_raw_chunks.embedding` -> `_raw_sources.embedding` mean-pool, but reads
    and writes the additive `_embeddings` table only — `_raw_sources.embedding`
    is never touched. Only pools sources that have at least one chunk vector
    for this model and don't already have an up-to-date pooled vector.
    Returns the number of sources pooled."""
    rows = db.execute(
        "SELECT DISTINCT e.source_id FROM _edges_source e "
        "JOIN _raw_sources s ON e.source_id = s.source_id "
        "LEFT JOIN _embeddings se ON se.id = e.source_id AND se.kind='source' AND se.model=? "
        "WHERE se.id IS NULL",
        (model,),
    ).fetchall()

    pooled = 0
    for idx, (source_id,) in enumerate(rows):
        chunk_rows = db.execute(
            "SELECT ce.vector FROM _raw_chunks c "
            "JOIN _edges_source e ON c.id = e.chunk_id "
            "JOIN _embeddings ce ON ce.id = c.id AND ce.kind=? AND ce.model=? "
            "WHERE e.source_id = ?",
            (kind_chunk, model, source_id),
        ).fetchall()
        if not chunk_rows:
            continue

        vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunk_rows]
        mean_vec = np.mean(vecs, axis=0).astype(np.float32)
        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm

        store_embeddings(db, [(source_id, mean_vec)], model=model, kind="source")
        pooled += 1

        if (idx + 1) % commit_every == 0:
            db.commit()

    db.commit()
    return pooled


def load_matrix(db, model: str | None = None, kind: str = "chunk",
                serve_dim: int | None = None):
    """(ids, matrix) for a model, sliced+renormalized to serve_dim (Matryoshka).
    Defaults to the active model / serve_dim from `_meta`. The dim comes from the
    data — no width assumption anywhere."""
    model = model or active_model(db)
    if model is None:
        return [], None
    if serve_dim is None:
        serve_dim = _serve_dim(db)
    rows = db.execute("SELECT id, vector FROM _embeddings WHERE model=? AND kind=?",
                      (model, kind)).fetchall()
    if not rows:
        return [], None
    ids = [r[0] for r in rows]
    M = np.vstack([np.frombuffer(r[1], dtype=np.float32) for r in rows])
    if serve_dim and serve_dim < M.shape[1]:
        M = M[:, :serve_dim]
        n = np.linalg.norm(M, axis=1, keepdims=True)
        M = M / np.maximum(n, 1e-9)
    return ids, M
