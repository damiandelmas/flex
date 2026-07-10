"""
Shared embedding pipeline for all cell modules.

embed_new(db) embeds NULL chunks, then mean-pools source embeddings.
Tries Rust binary (flex-embed) first for speed, falls back to Python ONNX.
Handles orphan recovery and batched commits (WAL safety).

Usage:
    from flex.compile.embed import embed_new
    embedded = embed_new(db)
"""

import numpy as np
import os
import shutil
import subprocess
import sys


def embed_new(db, batch_size=128, commit_every=500, enrich_fn=None):
    """Embed all chunks missing embeddings, then mean-pool sources.

    Priority: Rust flex-embed (subprocess) → Python ONNX (in-process).
    Both produce identical STORE_DIM-d Nomic vectors (see flex.onnx.embed.STORE_DIM).

    Args:
        enrich_fn: Optional callable(str) -> str that transforms content
            before embedding. The stored content in _raw_chunks is NOT
            modified — enrichment is applied only to what the embedder sees.
            When provided, forces the Python path (Rust binary embeds raw).

    Returns the number of chunks embedded.

    Model-aware dual-write: if this cell has been converted (`flex reembed`)
    onto a richer model — it has an `_embeddings` table AND an active,
    registered `vec:model` — newly embedded chunks (and any source mean-pools
    that depend on them) are ALSO embedded into `_embeddings` for that model,
    on top of the legacy `_raw_chunks.embedding` write above. This keeps a
    converted, still-growing cell fully indexed on its active model instead of
    silently falling behind. Purely additive; `_raw_chunks.embedding` /
    `_raw_sources.embedding` are never touched by this path. Cells without an
    active model (the overwhelming majority) pay one cheap metadata check and
    are otherwise byte-for-byte unchanged.
    """
    # Check if there's work to do
    null_count = db.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NULL"
    ).fetchone()[0]

    if null_count == 0:
        _recover_orphaned_sources(db, commit_every)
        _sync_active_model_embeddings(db, batch_size, commit_every)
        return 0

    # enrich_fn requires Python path (Rust binary can't call Python)
    if enrich_fn is None:
        # Release WAL locks so Rust subprocess can write
        db.commit()
        rust_count = _try_rust_embed(db)
        if rust_count is not None:
            _sync_active_model_embeddings(db, batch_size, commit_every)
            return rust_count

    # Fallback: Python ONNX (or forced by enrich_fn)
    embedded = _python_embed(db, batch_size, commit_every, enrich_fn=enrich_fn)
    _sync_active_model_embeddings(db, batch_size, commit_every)
    return embedded


def _try_rust_embed(db):
    """Try flex-embed Rust binary via subprocess. Returns count or None."""
    db_path = _get_db_path(db)
    if not db_path:
        return None

    binary = _find_rust_binary()
    if not binary:
        return None

    flex_home = os.environ.get("FLEX_HOME", os.path.expanduser("~/.flex"))
    model_dir = os.path.join(flex_home, "models")

    # Find ORT shared library (from Python onnxruntime package)
    ort_lib_dir = _find_ort_lib()
    env = os.environ.copy()
    if ort_lib_dir:
        env["ORT_DYLIB_PATH"] = ort_lib_dir
        env["LD_LIBRARY_PATH"] = ort_lib_dir + ":" + env.get("LD_LIBRARY_PATH", "")

    try:
        result = subprocess.run(
            [binary, db_path, "--model-dir", model_dir],
            capture_output=True, text=True, timeout=600, env=env,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if line.startswith('embedded '):
                    count = int(line.split()[1])
                    print(f"[flex-embed] {count} chunks (Rust)", file=sys.stderr)
                    return count
            return 0
        else:
            print(f"[flex-embed] failed (exit {result.returncode}), falling back to Python",
                  file=sys.stderr)
            return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def _python_embed(db, batch_size=64, commit_every=500, enrich_fn=None):
    """Python ONNX fallback — streams in batches to bound memory."""
    from flex.onnx.embed import ONNXEmbedder, STORE_DIM

    total = db.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NULL"
    ).fetchone()[0]

    if total == 0:
        _recover_orphaned_sources(db, commit_every)
        return 0

    embedder = ONNXEmbedder()
    embedded = 0

    while True:
        rows = db.execute(
            "SELECT id, content FROM _raw_chunks WHERE embedding IS NULL "
            "ORDER BY LENGTH(content) LIMIT ?",
            (commit_every,)
        ).fetchall()

        if not rows:
            break

        chunk_ids = [r[0] for r in rows]
        texts = [r[1] for r in rows]

        # Optional enrichment — transform what the embedder sees, not what's stored
        if enrich_fn is not None:
            texts = [enrich_fn(t) for t in texts]

        embeddings = embedder.encode(texts, batch_size=batch_size, matryoshka_dim=STORE_DIM)

        for i, chunk_id in enumerate(chunk_ids):
            blob = embeddings[i].astype(np.float32).tobytes()
            db.execute("UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
                       (blob, chunk_id))

        db.commit()
        embedded += len(chunk_ids)

        if total > 1000:
            print(f"\r  {embedded}/{total} ({embedded*100//total}%)",
                  end="", flush=True, file=sys.stderr)

    if total > 1000:
        print(file=sys.stderr)

    sources = db.execute("""
        SELECT DISTINCT e.source_id FROM _edges_source e
        JOIN _raw_sources s ON e.source_id = s.source_id
        WHERE s.embedding IS NULL
    """).fetchall()

    _mean_pool_sources(db, sources, commit_every)
    return embedded


def _get_db_path(db):
    """Extract file path from an open sqlite3 connection."""
    try:
        row = db.execute("PRAGMA database_list").fetchone()
        path = row[2] if row else None
        return path if path and path != ':memory:' and path != '' else None
    except Exception:
        return None


def _find_rust_binary():
    """Find flex-embed binary: ~/.flex/bin/ → PATH → adjacent to package."""
    flex_home = os.environ.get("FLEX_HOME", os.path.expanduser("~/.flex"))
    candidates = [
        os.path.join(flex_home, "bin", "flex-embed"),
        shutil.which("flex-embed"),
    ]
    # Also check if built locally (dev mode) — go up from flex/compile/ to repo root
    repo_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    candidates.append(os.path.join(repo_dir, "flex-embed", "target", "release", "flex-embed"))

    for c in candidates:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return None


def _find_ort_lib():
    """Find libonnxruntime.so from the Python onnxruntime package."""
    try:
        import onnxruntime
        ort_dir = os.path.join(os.path.dirname(onnxruntime.__file__), "capi")
        if os.path.isdir(ort_dir):
            return ort_dir
    except ImportError:
        pass
    return None


def _recover_orphaned_sources(db, commit_every=500):
    """Find and mean-pool sources orphaned by a previous crash."""
    sources = db.execute("""
        SELECT DISTINCT e.source_id FROM _edges_source e
        JOIN _raw_sources s ON e.source_id = s.source_id
        WHERE s.embedding IS NULL
    """).fetchall()
    if sources:
        _mean_pool_sources(db, sources, commit_every)


def _mean_pool_sources(db, sources, commit_every=500):
    """Compute mean-pooled embeddings for sources from their chunks.

    Commits in batches to keep WAL size bounded.
    """
    for idx, (source_id,) in enumerate(sources):
        chunk_rows = db.execute("""
            SELECT c.embedding FROM _raw_chunks c
            JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.source_id = ? AND c.embedding IS NOT NULL
        """, (source_id,)).fetchall()

        if not chunk_rows:
            continue

        vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunk_rows]
        mean_vec = np.mean(vecs, axis=0).astype(np.float32)
        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm

        db.execute("UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                   (mean_vec.tobytes(), source_id))

        if (idx + 1) % commit_every == 0:
            db.commit()

    db.commit()


def _sync_active_model_embeddings(db, batch_size=128, commit_every=500):
    """Dual-write NEW chunks (and their derived source mean-pools) into the
    additive `_embeddings` table when this cell has an active, registered
    model (i.e. it was converted by `flex reembed`). No-op — one cheap
    metadata check — for the overwhelming majority of cells that have never
    been converted (no `_embeddings` table, or no `vec:model` set).

    NEVER writes `_raw_chunks.embedding` / `_raw_sources.embedding`; this only
    ever inserts rows into `_embeddings`, so legacy readers and the byte-exact
    128d MiniLM column are untouched.
    """
    try:
        from flex.retrieve import embeddings as _embstore
        from flex.retrieve import model_registry as _mr
    except ImportError:
        return

    try:
        has_table = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_embeddings'"
        ).fetchone()
    except Exception:
        return
    if not has_table:
        return

    model = _embstore.active_model(db)
    if not model or not _mr.is_registered(model):
        return

    # The full (stored) dim for this model: read from what's already stored so
    # we never guess a width; only reachable if the cell somehow has an active
    # model with zero stored vectors (shouldn't happen post-`flex reembed`).
    store_dim = None
    for m, kind, _n, dim in _embstore.list_models(db):
        if m == model and kind == 'chunk':
            store_dim = dim
            break
    if store_dim is None:
        store_dim = _mr._native_dim(model)

    embed_doc = _mr.doc_embedder(model, store_dim)
    target_bytes = store_dim * 4

    while True:
        rows = db.execute(
            "SELECT c.id, c.content FROM _raw_chunks c "
            "LEFT JOIN _embeddings e ON e.id = c.id AND e.kind='chunk' AND e.model=? "
            "WHERE c.content IS NOT NULL "
            "AND (e.id IS NULL OR length(e.vector) != ?) "
            "LIMIT ?",
            (model, target_bytes, commit_every),
        ).fetchall()
        if not rows:
            break

        ids = [r[0] for r in rows]
        texts = [r[1] or "" for r in rows]
        vecs = np.asarray(embed_doc(texts), dtype=np.float32)
        _embstore.store_embeddings(db, zip(ids, vecs), model=model, kind='chunk')

    _embstore.mean_pool_sources(db, model, commit_every=commit_every)
