"""`flex reembed` — upgrade a MiniLM or legacy Nomic cell to Nomic fp32.

Serving reads `_raw_chunks.embedding` and `_raw_sources.embedding` directly.
`reembed_cell` rewrites those columns with native 768-dimensional Nomic fp32
vectors on an independent copy, verifies the copy, then atomically replaces
the live database. The live cell is never rewritten in place.

Per-cell procedure:
  1. Resolve the cell; refuse if already `vec:model=nomic-v1.5-fp32` unless `force`.
     (A cell tagged plain `nomic-v1.5` is the legacy int8 space — running this
     tool on it converts it forward to fp32, never leaving a mixed-space window.)
  2. Freeze ingest: `registry.set_active(name, False)` + drain in-flight embed.
  3. `PRAGMA wal_checkpoint(TRUNCATE)` on the LIVE db (fold WAL into the main file).
  4. Backup the live db (reuses `_backup_path`).
  5. Copy live -> `<stem>.reembed.tmp` (same dir/filesystem, for atomic replace).
  6. Re-embed every chunk (Nomic-768, `search_document:` prefix) into the TMP's
     `_raw_chunks.embedding`; mean-pool `_raw_sources.embedding`. Batched +
     committed periodically; resumable via `length(embedding) != 768*4`.
  7. Verify the TMP: `PRAGMA quick_check == ok`; every chunk embedding is
     3072 bytes; count matches; a live `vec_ops(...)` probe (registered the
     same way `engine.build_vec_state`/`register_vec_udf` do) returns a sane
     self-similarity hit. The tag is stamped on the TMP just before this probe
     so the probe queries in the same space the swapped-in live file will
     serve — by the time `os.replace` runs, the file becoming live already
     carries `vec:model=nomic-v1.5-fp32`/`vec:serve_dim=256`.
  8. Checkpoint the TMP + drop its `-wal`/`-shm` (clean single file).
  9. `os.replace(tmp, live)` (atomic) + remove stale `<live>.vec_*.npy` memmaps.
  10. The stamp from step 7 is already live (no separate write needed post-swap).
  11. Restore the exact pre-run registry `active` state.

On ANY failure before step 9 (os.replace): the TMP is discarded, the live db
is untouched (still on its prior tag/space, still fully queryable) — never a
half-state.

The local migration is single-process and uses ONNX intra-op threads configured
by `FLEX_ONNX_THREADS`.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np

STORE_MODEL = "nomic-v1.5-fp32"
STORE_DIM = 768      # native Nomic dim — what gets stored in `_raw_chunks.embedding`
SERVE_DIM = 256      # Matryoshka slice served at query time (`vec:serve_dim`).
                     # 256 is the measured quality peak (recall@5 0.97 vs 0.95 @128,
                     # flat 256->768); engine.build_vec_state reads the per-cell
                     # `vec:serve_dim` and the query embedder matches it.

_DRAIN_SETTLE_S = 1.0   # best-effort settle window for an in-flight embed_new() commit


def _backup_path(db_path: Path) -> Path:
    flex_home = Path(os.environ.get("FLEX_HOME", str(Path.home() / ".flex")))
    backup_dir = flex_home / "backups" / "reembed-nomic"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%y%m%d-%H%M%S")
    return backup_dir / f"{db_path.stem}.{ts}.db"


def _resolve_cell(cell_name_or_path) -> tuple[str | None, Path | None]:
    """Resolve a cell argument to (name, db_path).

    Accepts a registered cell name (looked up via `flex.registry`) OR a raw
    filesystem path to a `.db` file (tests exercise synthetic cells that are
    never registered). Returns (None, None) if neither resolves."""
    p = Path(str(cell_name_or_path))
    if p.suffix == ".db" and p.exists():
        return p.stem, p

    from flex import registry
    resolved = registry.resolve_cell(str(cell_name_or_path))
    if resolved is not None:
        return str(cell_name_or_path), resolved

    return None, None


def _active_model(db_path: Path) -> str | None:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT value FROM _meta WHERE key='vec:model'").fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None  # no _meta table (shouldn't happen on a real cell)
    finally:
        conn.close()


def _chunk_counts(db_path: Path, full_reembed: bool = False) -> tuple[int, int]:
    """`pending` for dry-run reporting. Width-based counting is meaningless
    when `full_reembed` is True (same-width space conversion, e.g. int8 ->
    fp32) — every row is "pending" regardless of its current byte length, or
    a dry-run would misreport 0 pending on a cell about to be fully
    re-embedded (the same width-blindness class as the _reembed_chunks bug)."""
    conn = sqlite3.connect(str(db_path))
    try:
        total = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        if full_reembed:
            return total, total
        pending = conn.execute(
            "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NULL OR length(embedding) != ?",
            (STORE_DIM * 4,),
        ).fetchone()[0]
        return total, pending
    finally:
        conn.close()


def _migration_eligibility(db_path: Path) -> tuple[bool, str | None]:
    """Return whether a cell has an embedding surface that may be migrated."""
    conn = sqlite3.connect(str(db_path))
    try:
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        required = {'_raw_chunks', '_raw_sources'}
        if not required <= tables:
            return False, "no embedding tables"
        try:
            row = conn.execute(
                "SELECT value FROM _meta WHERE key='embed'"
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        if row and str(row[0]).strip().lower() in {'false', '0', 'off', 'no'}:
            return False, "cell is structural-only (embed=false)"
        return True, None
    finally:
        conn.close()


def _drain_ingest(_name: str | None) -> None:
    """Best-effort wait for an in-flight `embed_new()` cycle to drain after
    ingest has been frozen (`registry.set_active(name, False)`). There is no
    per-cell lock file to poll here — a fixed short settle window covers the
    common case (an in-process worker finishing its current commit) without
    risking an indefinite hang if something else is stuck."""
    time.sleep(_DRAIN_SETTLE_S)


def _freeze_active_state(name: str | None) -> bool | None:
    """Freeze a registered cell and return its exact prior active state.

    None means the name is absent/unregistered and therefore needs no restore.
    A parked cell returns False: migration may temporarily freeze it again, but
    must not activate it on exit.
    """
    if not name:
        return None
    from flex import registry
    meta = registry.get_cell_metadata(name)
    if meta is None:
        return None
    prior = bool(meta.get('active', 1))
    if not registry.set_active(name, False):
        return None
    return prior


def _restore_active_state(name: str | None, prior: bool | None) -> None:
    if name and prior is not None:
        from flex import registry
        registry.set_active(name, prior)


def _remove_wal_shm(db_path: Path) -> None:
    for suffix in ("-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


def _remove_stale_memmaps(db_path: Path) -> None:
    """Remove `turbovec` memmap sidecars (`<db>.vec_<table>_<dims>.npy`) left
    over from the pre-swap (MiniLM-dim) VectorCache — they'd otherwise be
    served stale until process restart, and a dims-matching leftover could
    silently outlive a cache rebuild."""
    for p in db_path.parent.glob(f"{db_path.name}.vec_*.npy"):
        try:
            p.unlink()
        except OSError:
            pass


def _get_nomic_embed_doc():
    """Resolve the Nomic fp32 document-embed function the same way engine.py's
    `_query_embedder_for` resolver does — reuses its lazy singleton + the
    explicit exists-guard, rather than constructing a second embedder or
    reaching into a separate model registry. This is the portable local CPU
    migration path used by the public CLI."""
    from flex import engine as _engine

    if not _engine._NOMIC_FP32_MODEL_PATH.exists():
        raise RuntimeError(
            f"Nomic fp32 model not found at {_engine._NOMIC_FP32_MODEL_PATH} — cannot reembed. "
            "Install the model or run on a box that has it.")
    emb = _engine._get_nomic_fp32_embedder()

    def embed_doc(texts, **kw):
        return emb.encode(texts, prefix="search_document: ", matryoshka_dim=STORE_DIM, **kw)

    return embed_doc


def _reembed_chunks(conn, embed_doc_fn, batch_size: int, commit_every: int,
                     progress: bool, full_reembed: bool = False) -> int:
    """Re-embed chunks into `_raw_chunks.embedding`.

    Two modes:
    - Width-resumable (full_reembed=False): selects only rows NOT already at
      STORE_DIM width (`embedding IS NULL OR length(embedding) != 3072`).
      Valid ONLY when the source and target widths actually differ (e.g. the
      128d MiniLM -> 768d Nomic upgrade) — a crash mid-loop just leaves more
      rows at the wrong width, picked back up by the same WHERE on the next
      attempt.
    - Full (full_reembed=True): selects EVERY row via a rowid cursor,
      regardless of current width. MANDATORY whenever source and target are
      BOTH already at STORE_DIM width — e.g. int8 (nomic-v1.5, native 768d/
      3072 bytes) -> fp32 (nomic-v1.5-fp32, native 768d/3072 bytes). Width
      cannot discriminate two same-width spaces: the old width-only WHERE
      selected ZERO rows on an already-768d int8 cell, then the caller
      unconditionally stamped `nomic-v1.5-fp32` on int8-valued vectors — a
      silent no-op conversion that left the cell in exactly the mixed-space
      state the tag exists to prevent. `reembed_cell` decides which mode
      applies from the cell's CURRENT tag, not from measuring width. Full
      mode is not resumable across separate `reembed_cell` invocations (the
      cursor is local to this call) but that costs nothing: it only ever
      runs against the TMP copy, and any failure before `os.replace` discards
      the TMP wholesale."""
    total = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    embedded = 0
    last_rowid = 0
    while True:
        if full_reembed:
            rows = conn.execute(
                "SELECT rowid, id, content FROM _raw_chunks "
                "WHERE rowid > ? ORDER BY rowid LIMIT ?",
                (last_rowid, commit_every),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT rowid, id, content FROM _raw_chunks "
                "WHERE embedding IS NULL OR length(embedding) != ? "
                "ORDER BY rowid LIMIT ?",
                (STORE_DIM * 4, commit_every),
            ).fetchall()
        if not rows:
            break
        last_rowid = rows[-1][0]

        ids = [r[1] for r in rows]
        texts = [r[2] or "" for r in rows]
        vectors = embed_doc_fn(texts, batch_size=batch_size)

        for i, chunk_id in enumerate(ids):
            blob = np.asarray(vectors[i], dtype=np.float32).tobytes()
            if len(blob) != STORE_DIM * 4:
                # Hard guard against an infinite loop: in width-resumable mode
                # the WHERE clause re-selects any row not at STORE_DIM width,
                # so an embedder that returns the wrong width would otherwise
                # re-select and re-embed the same rows forever without ever
                # converging. (Full mode advances the rowid cursor regardless,
                # so it can't loop forever either way — this guard still
                # applies uniformly so a bad embedder is caught immediately.)
                raise ValueError(
                    f"embedder returned {len(blob) // 4}d for chunk {chunk_id!r}, "
                    f"expected {STORE_DIM}d")
            conn.execute("UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
                         (blob, chunk_id))
        conn.commit()
        embedded += len(ids)

        if progress and total > 500:
            print(f"\r  chunks {embedded}/{total} ({embedded * 100 // total}%)",
                  end="", flush=True, file=sys.stderr)

    if progress and total > 500:
        print(file=sys.stderr)
    return embedded


def _reembed_sources(conn, commit_every: int) -> None:
    """Mean-pool every `_raw_sources.embedding` from its chunks' (now Nomic-768)
    vectors. Unlike `compile/embed.py::_mean_pool_sources` (which only fills
    NULL sources for newly-ingested chunks), this recomputes ALL sources —
    every chunk's vector changed dimension/space, so every pooled source is
    now stale."""
    sources = conn.execute("SELECT source_id FROM _raw_sources").fetchall()
    for idx, (source_id,) in enumerate(sources):
        chunk_rows = conn.execute("""
            SELECT c.embedding FROM _raw_chunks c
            JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.source_id = ? AND c.embedding IS NOT NULL
        """, (source_id,)).fetchall()
        if not chunk_rows:
            # A source embedding is derived from its chunks. Keeping an old
            # blob when no target-space chunks exist leaves an unverifiable
            # vector from the prior space in an otherwise converted cell.
            conn.execute(
                "UPDATE _raw_sources SET embedding = NULL WHERE source_id = ?",
                (source_id,),
            )
            continue

        vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunk_rows]
        dims = [v.shape[0] for v in vecs]
        if len(set(dims)) > 1:
            dominant = max(set(dims), key=dims.count)
            vecs = [v for v, d in zip(vecs, dims) if d == dominant]

        mean_vec = np.mean(vecs, axis=0).astype(np.float32)
        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm

        conn.execute("UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                     (mean_vec.tobytes(), source_id))
        if (idx + 1) % commit_every == 0:
            conn.commit()
    conn.commit()


SAMPLE_VERIFY_N = 20          # minimum sample size for the cosine structural guard
SAMPLE_VERIFY_MIN_COS = 0.999  # same-model re-embed of the same text must reproduce near-exactly;
                               # a leftover different-space vector (e.g. int8, cos ~0.96) fails hard


def _verify_tmp(conn, name: str | None, embed_doc_fn=None) -> tuple[bool, str]:
    """Verify the TMP before it's allowed anywhere near the live path.

    `embed_doc_fn`, when given, is the SAME target-space embedder the run
    just used to write `_raw_chunks.embedding` — required for the sample
    cosine structural guard (see `_verify_sample_cosine`). Callers that don't
    pass one (or whose embed_doc_fn is unavailable) skip that guard and rely
    on the weaker vec_ops probe alone — kept for backward compatibility, but
    every real caller in this repo now passes embed_doc_fn."""
    row = conn.execute("PRAGMA quick_check").fetchone()
    if not row or row[0] != "ok":
        return False, f"quick_check: {row}"

    bad = conn.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NULL OR length(embedding) != ?",
        (STORE_DIM * 4,),
    ).fetchone()[0]
    if bad:
        return False, f"{bad} chunk(s) not at {STORE_DIM * 4} bytes (expected {STORE_DIM}d)"

    total = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    if total == 0:
        return True, "ok (empty cell)"

    if embed_doc_fn is not None:
        ok, reason = _verify_sample_cosine(conn, embed_doc_fn)
        if not ok:
            return False, reason

    probe_row = conn.execute(
        "SELECT content FROM _raw_chunks WHERE content IS NOT NULL "
        "AND length(content) > 0 ORDER BY id LIMIT 1"
    ).fetchone()
    if not probe_row:
        return True, "ok (no probeable content)"

    result = _probe_vec_ops(conn, name, probe_row[0])
    # materialize_vec_ops populates a TEMP TABLE via executemany, which opens
    # an implicit write transaction (Python sqlite3's default isolation_level)
    # that is never otherwise committed on this connection. Left open, it
    # blocks the caller's subsequent `PRAGMA wal_checkpoint(TRUNCATE)` with
    # "database table is locked" — close it out here so verify never leaves
    # the connection mid-transaction regardless of the probe's outcome.
    try:
        conn.commit()
    except sqlite3.Error:
        pass
    return result


def _verify_sample_cosine(conn, embed_doc_fn, sample_n: int = SAMPLE_VERIFY_N) -> tuple[bool, str]:
    """THE structural guard against a silent no-op/wrong-space conversion
    (the class of bug that let an int8 cell get stamped `nomic-v1.5-fp32`
    with its int8 vectors untouched — the old vec_ops probe's 0.5 threshold
    is far too loose to catch that: int8-vs-fp32 cosine is ~0.96, comfortably
    above 0.5).

    Samples >= `sample_n` chunks (or all of them, if fewer), re-embeds their
    STORED content with the TARGET embedder (the same one the run just wrote
    with), and requires cosine >= SAMPLE_VERIFY_MIN_COS against the stored
    vector for EVERY sampled chunk. Re-embedding the exact same text with the
    exact same model is expected to reproduce near-exactly (ONNX inference is
    deterministic) — anything short of that means the stored vector did not
    actually come from this run's embedder, which is exactly the failure
    mode this guard exists to catch."""
    total = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    n = min(sample_n, total)
    rows = conn.execute(
        "SELECT id, content, embedding FROM _raw_chunks "
        "WHERE content IS NOT NULL AND length(content) > 0 AND embedding IS NOT NULL "
        "ORDER BY RANDOM() LIMIT ?", (n,)
    ).fetchall()
    if not rows:
        return True, "ok (no sampleable content for cosine guard)"

    ids = [r[0] for r in rows]
    texts = [r[1] for r in rows]
    stored = [np.frombuffer(r[2], dtype=np.float32) for r in rows]

    fresh = np.asarray(embed_doc_fn(texts, batch_size=len(texts)), dtype=np.float32)
    if fresh.ndim == 1:
        fresh = fresh.reshape(1, -1)

    min_cos = 1.0
    for i, chunk_id in enumerate(ids):
        s, f = stored[i], fresh[i]
        if s.shape != f.shape:
            return False, (f"sample verify: chunk {chunk_id!r} shape mismatch "
                            f"stored={s.shape} fresh={f.shape}")
        denom = (np.linalg.norm(s) * np.linalg.norm(f)) or 1e-9
        cos = float(np.dot(s, f) / denom)
        min_cos = min(min_cos, cos)
        if cos < SAMPLE_VERIFY_MIN_COS:
            return False, (f"sample verify: chunk {chunk_id!r} cos={cos:.6f} < "
                            f"{SAMPLE_VERIFY_MIN_COS} vs freshly-computed target-embedder vector "
                            "-- stored vector is likely a leftover from a different embedding space")
    return True, f"ok (sample cosine min={min_cos:.6f}, n={len(ids)})"


def _probe_vec_ops(conn, name: str | None, probe_text: str) -> tuple[bool, str]:
    """A live `vec_ops(...)` sanity check on the TMP, registered the same way
    `engine.build_vec_state`/`register_vec_udf` register it for real serving.
    Queries with an existing chunk's own content — a healthy Nomic column
    should return that same chunk as a strong (near-1.0) top hit; a corrupt
    or wrong-space column will not."""
    try:
        from flex import engine as _engine
        from flex.retrieve.execute import execute as _execute

        state = _engine.build_vec_state(name or "reembed-tmp", conn, time.time())
        if state is None:
            return False, "vec_ops probe: build_vec_state returned None"
        _engine.register_vec_udf(conn, state)

        # Legacy vec_ops form (table, raw query text) — no modifier-token DSL
        # parsing, so arbitrary chunk content is safe to embed verbatim.
        escaped = probe_text[:200].replace("'", "''")
        sql = ("SELECT v.id, v.score FROM vec_ops('_raw_chunks', '" + escaped + "') v "
               "ORDER BY v.score DESC LIMIT 3")
        rows = _execute(conn, sql)
        if isinstance(rows, dict) and rows.get("error"):
            return False, f"vec_ops probe error: {rows['error']}"
        if not rows:
            return False, "vec_ops probe returned 0 rows"

        top_score = rows[0].get("score")
        if top_score is None or top_score < 0.5:
            return False, f"vec_ops probe: top score too low ({top_score})"
        return True, "ok"
    except Exception as e:
        return False, f"vec_ops probe raised: {e}"


def reembed_cell(cell_name_or_path, dry_run: bool = False, force: bool = False,
                  batch_size: int = 64, commit_every: int = 256, progress: bool = True,
                  embed_doc_fn=None) -> dict:
    """Convert one cell's `_raw_chunks`/`_raw_sources` embedding column to
    Nomic-768, copy-then-atomic-swap, never in-place on the live db.

    Args:
        cell_name_or_path: a registered cell name, or a raw path to a `.db`
            file (the latter for tests against synthetic/unregistered cells).
        dry_run: report the plan (chunk counts, rough ETA) without mutating.
        force: proceed even if the cell is already `vec:model=nomic-v1.5`.
        embed_doc_fn: override the Nomic embed function — injection point for
            tests (deterministic fake embedder; avoids needing the real
            ONNX model on disk).

    Returns a summary dict: {"cell", "status", ...}. `status` is one of
    "converted", "skipped", "dry-run", "error".
    """
    name, db_path = _resolve_cell(cell_name_or_path)
    if db_path is None:
        return {"cell": str(cell_name_or_path), "status": "error",
                "reason": f"cell not found: {cell_name_or_path}"}
    if not db_path.exists():
        return {"cell": name, "status": "error", "reason": f"db file missing: {db_path}"}

    eligible, reason = _migration_eligibility(db_path)
    if not eligible:
        return {"cell": name, "status": "skipped", "reason": reason}

    current_model = _active_model(db_path)
    known_source_models = (None, 'minilm', 'nomic-v1.5', STORE_MODEL)
    if current_model not in known_source_models:
        return {"cell": name, "status": "error",
                "reason": f"unrecognized vec:model tag {current_model!r}; "
                          "source vector space cannot be inferred safely"}
    if current_model == STORE_MODEL and not force:
        return {"cell": name, "status": "skipped",
                "reason": f"already {STORE_MODEL} (use --force to re-run)"}

    # See the full_reembed docstring on _reembed_chunks: width can only tell
    # apart a minilm/None -> fp32 conversion (128d -> 768d). Any cell already
    # in a Nomic (768d) space needs a full pass regardless of measured width.
    full_reembed = current_model not in (None, 'minilm')
    total, pending = _chunk_counts(db_path, full_reembed=full_reembed)

    if dry_run:
        eta_s = round(pending * 0.02, 1)  # rough Python-ONNX-ballpark, informational only
        result = {"cell": name, "status": "dry-run", "chunks": total, "pending": pending,
                  "eta_seconds": eta_s}
        if total > 5000:
            result["note"] = (
                "large cell — reembed runs a while in the background; the cell keeps "
                f"serving {current_model or 'minilm'} until the swap completes")
        return result

    if total == 0:
        # Empty cell: no-op re-embed, just stamp the tag (trivial verify).
        conn = sqlite3.connect(str(db_path))
        try:
            from flex.retrieve.embeddings import set_active_model
            set_active_model(conn, STORE_MODEL, SERVE_DIM)
        finally:
            conn.close()
        return {"cell": name, "status": "converted", "chunks": 0, "backup": None}

    from flex import registry

    backup = _backup_path(db_path)
    tmp_path = db_path.parent / f"{db_path.stem}.reembed.tmp"
    if tmp_path.exists():
        tmp_path.unlink()
    _remove_wal_shm(tmp_path)

    prior_active = None
    backup_written = False
    try:
        # 2. Freeze ingest, drain any in-flight embed_new()
        if name:
            prior_active = _freeze_active_state(name)
        _drain_ingest(name)

        # 3. Checkpoint the LIVE db (fold WAL into the main file before copying)
        live_conn = sqlite3.connect(str(db_path))
        try:
            live_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            live_conn.close()

        # 4. Backup the live db
        shutil.copy2(db_path, backup)
        backup_written = True

        # 5. Copy live -> tmp (same dir/filesystem, for atomic os.replace later)
        shutil.copy2(db_path, tmp_path)

        # 6. Re-embed on the TMP (full_reembed computed above from the tag —
        # see the docstring on _reembed_chunks: width can't discriminate a
        # same-width space conversion, e.g. int8 -> fp32, so a Nomic-tagged
        # source forces a full unconditional pass rather than trusting width).
        embed_fn = embed_doc_fn or _get_nomic_embed_doc()
        tmp_conn = sqlite3.connect(str(tmp_path))
        tmp_conn.row_factory = sqlite3.Row  # matches core.open_cell — vec_ops/execute() assume Row
        try:
            tmp_conn.execute("PRAGMA journal_mode=WAL")
            embedded = _reembed_chunks(tmp_conn, embed_fn, batch_size, commit_every, progress,
                                        full_reembed=full_reembed)
            _reembed_sources(tmp_conn, commit_every)

            # Stamp the tag on the TMP now, ahead of the verify probe, so the
            # probe queries in the same space the swapped-in live file will
            # serve. By os.replace time the file becoming live already
            # carries the correct tag+serve_dim — nothing further to stamp
            # post-swap.
            from flex.retrieve.embeddings import set_active_model
            set_active_model(tmp_conn, STORE_MODEL, SERVE_DIM)

            # 7. Verify the TMP — embed_fn passed through so the sample
            # cosine guard checks against the SAME target embedder this run
            # just wrote with.
            ok, reason = _verify_tmp(tmp_conn, name, embed_fn)
            if not ok:
                raise RuntimeError(f"tmp verification failed: {reason}")

            # 8. Checkpoint the TMP, drop its WAL/SHM
            tmp_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            tmp_conn.close()
        _remove_wal_shm(tmp_path)

        # 9. Atomic swap
        os.replace(tmp_path, db_path)
        _remove_stale_memmaps(db_path)

        return {"cell": name, "status": "converted", "chunks": embedded,
                "backup": str(backup)}

    except Exception as e:
        # Discard the tmp; the live db is untouched (still MiniLM, still
        # fully queryable) — never a half-state.
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        _remove_wal_shm(tmp_path)
        return {"cell": name, "status": "error", "reason": str(e),
                "backup": str(backup) if backup_written else None}

    finally:
        # 11. Restore exact prior state (always — success or failure).
        _restore_active_state(name, prior_active)
