"""
Incremental re-embed: 384-dim → 128-dim (Nomic embed-text-v1.5, Matryoshka).

Resumable. Commits after each batch. Detects already-embedded chunks by
dimension (length(embedding)/4). Safe to re-run after crash — picks up
where it left off.

Parallel mode (--workers N) shards chunk IDs across N processes, each
with its own ONNX session. WAL mode handles concurrent writes. 4 workers
on 16 cores ≈ 4x speedup (~10 min for 138K chunks instead of 40).

Usage:
    python scripts/reembed_nomic.py                     # all cells
    python scripts/reembed_nomic.py claude_code          # single cell
    python scripts/reembed_nomic.py --workers 4          # parallel
    python scripts/reembed_nomic.py --batch 200          # smaller batches
"""
import argparse
import math
import multiprocessing as mp
import os
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import psutil

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flex.registry import list_cells, resolve_cell

TARGET_DIM = 128
EMBED_BYTES = TARGET_DIM * 4  # float32


def _throttle(max_cpu: int):
    """Sleep until system CPU usage drops below max_cpu percent.

    Samples at 0.5s intervals. A value of 70 means: if the machine is busier
    than 70%, pause before the next batch. Effectively yields to the rest of
    the system without stopping work entirely.
    """
    if max_cpu <= 0 or max_cpu >= 100:
        return
    while True:
        usage = psutil.cpu_percent(interval=0.5)
        if usage <= max_cpu:
            break
        time.sleep(0.5)


def count_remaining(db: sqlite3.Connection, table: str, id_col: str) -> int:
    """Count rows that need re-embedding (NULL or wrong dimension)."""
    return db.execute(f"""
        SELECT count(*) FROM {table}
        WHERE {id_col} IS NOT NULL
          AND (embedding IS NULL OR length(embedding) != ?)
    """, (EMBED_BYTES,)).fetchone()[0]


def _fetch_stale_ids(db_path: str) -> list:
    """Fetch all chunk IDs that need re-embedding."""
    db = sqlite3.connect(db_path, timeout=30)
    rows = db.execute(f"""
        SELECT id FROM _raw_chunks
        WHERE content IS NOT NULL
          AND (embedding IS NULL OR length(embedding) != ?)
        ORDER BY id
    """, (EMBED_BYTES,)).fetchall()
    db.close()
    return [r[0] for r in rows]


def _worker_fn(args: tuple):
    """Worker process: embed a shard of chunk IDs."""
    db_path, chunk_ids, batch_size, worker_id, num_workers, max_cpu = args

    # Each worker gets its own ONNX session
    # Limit threads per worker to avoid oversubscription
    os.environ['OMP_NUM_THREADS'] = str(max(1, (os.cpu_count() or 4) // num_workers))

    from flex.onnx import ONNXEmbedder
    embedder = ONNXEmbedder()
    embedder.encode("warmup")  # force session init

    db = sqlite3.connect(db_path, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=60000")

    total = len(chunk_ids)
    done = 0
    t0 = time.time()

    for i in range(0, total, batch_size):
        batch_ids = chunk_ids[i:i + batch_size]
        placeholders = ','.join('?' * len(batch_ids))
        rows = db.execute(f"""
            SELECT id, content FROM _raw_chunks
            WHERE id IN ({placeholders})
        """, batch_ids).fetchall()

        if not rows:
            continue

        texts = [r[1] for r in rows]
        embeddings = embedder.encode(texts, batch_size=batch_size)

        db.executemany(
            "UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
            [(emb.astype(np.float32).tobytes(), r[0])
             for emb, r in zip(embeddings, rows)]
        )
        db.commit()
        _throttle(max_cpu)

        done += len(rows)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        remaining = (total - done) / rate if rate > 0 else 0
        print(f"    [w{worker_id}] {done:,}/{total:,}  ({elapsed:.0f}s, ~{remaining:.0f}s left)")

    db.close()
    return done


def reembed_chunks_parallel(db_path: str, num_workers: int, batch_size: int, cell_name: str, max_cpu: int = 0):
    """Re-embed chunks using multiple worker processes."""
    all_ids = _fetch_stale_ids(db_path)
    total = len(all_ids)

    if total == 0:
        print(f"  [{cell_name}] chunks: all {TARGET_DIM}d already")
        return

    print(f"  [{cell_name}] chunks: {total:,} to re-embed across {num_workers} workers")

    # Shard IDs evenly
    shard_size = math.ceil(total / num_workers)
    shards = [all_ids[i:i + shard_size] for i in range(0, total, shard_size)]

    worker_args = [
        (db_path, shard, batch_size, i, num_workers, max_cpu)
        for i, shard in enumerate(shards)
    ]

    t0 = time.time()
    with mp.Pool(num_workers) as pool:
        results = pool.map(_worker_fn, worker_args)

    elapsed = time.time() - t0
    done = sum(results)
    print(f"  [{cell_name}] chunks: {done:,} re-embedded in {elapsed:.0f}s "
          f"({done/elapsed:.0f} chunks/s)")


def reembed_chunks(db: sqlite3.Connection, embedder, batch_size: int, cell_name: str, max_cpu: int = 0):
    """Re-embed _raw_chunks incrementally (single-process). Commits after each batch."""
    total = count_remaining(db, '_raw_chunks', 'id')
    if total == 0:
        print(f"  [{cell_name}] chunks: all {TARGET_DIM}d already")
        return

    print(f"  [{cell_name}] chunks: {total:,} to re-embed")
    done = 0
    t0 = time.time()

    while True:
        rows = db.execute(f"""
            SELECT id, content FROM _raw_chunks
            WHERE content IS NOT NULL
              AND (embedding IS NULL OR length(embedding) != ?)
            ORDER BY length(content), id
            LIMIT ?
        """, (EMBED_BYTES, batch_size)).fetchall()

        if not rows:
            break

        texts = [r[1] for r in rows]
        embeddings = embedder.encode(texts, batch_size=batch_size)

        db.executemany(
            "UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
            [(emb.astype(np.float32).tobytes(), r[0])
             for emb, r in zip(embeddings, rows)]
        )
        db.commit()
        _throttle(max_cpu)

        done += len(rows)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        remaining = (total - done) / rate if rate > 0 else 0
        print(f"    {done:,}/{total:,}  ({elapsed:.0f}s, ~{remaining:.0f}s left)")


def reembed_sources(db: sqlite3.Connection, cell_name: str):
    """Re-pool source embeddings from child chunks (mean of 768d chunks)."""
    total = count_remaining(db, '_raw_sources', 'source_id')
    if total == 0:
        print(f"  [{cell_name}] sources: all {TARGET_DIM}d already")
        return

    print(f"  [{cell_name}] sources: {total:,} to re-pool")
    sources = db.execute(f"""
        SELECT source_id FROM _raw_sources
        WHERE source_id IS NOT NULL
          AND (embedding IS NULL OR length(embedding) != ?)
    """, (EMBED_BYTES,)).fetchall()

    pooled = 0
    for (sid,) in sources:
        chunk_embs = db.execute("""
            SELECT c.embedding FROM _raw_chunks c
            JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.source_id = ? AND c.embedding IS NOT NULL
              AND length(c.embedding) = ?
        """, (sid, EMBED_BYTES)).fetchall()

        if chunk_embs:
            vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunk_embs]
            mean_vec = np.mean(vecs, axis=0).astype(np.float32)
            norm = np.linalg.norm(mean_vec)
            if norm > 1e-9:
                mean_vec /= norm
            db.execute(
                "UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                (mean_vec.tobytes(), sid)
            )
            pooled += 1

    db.commit()
    print(f"  [{cell_name}] sources: pooled {pooled:,}")


def reembed_cell(cell_name: str, cell_path: str, num_workers: int, batch_size: int, max_cpu: int = 0):
    """Re-embed a single cell (chunks + sources)."""
    print(f"\n{'='*60}")
    print(f"Cell: {cell_name} ({Path(cell_path).name})")
    print(f"{'='*60}")

    if num_workers > 1:
        reembed_chunks_parallel(cell_path, num_workers, batch_size, cell_name, max_cpu=max_cpu)
    else:
        from flex.onnx import get_model
        embedder = get_model()
        db = sqlite3.connect(cell_path, timeout=30)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=30000")
        reembed_chunks(db, embedder, batch_size, cell_name, max_cpu=max_cpu)
        db.close()

    # Source re-pooling is fast — single process
    db = sqlite3.connect(cell_path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    try:
        reembed_sources(db, cell_name)
        # Verify
        dim = db.execute(
            "SELECT length(embedding)/4 FROM _raw_chunks "
            "WHERE embedding IS NOT NULL LIMIT 1"
        ).fetchone()
        if dim:
            print(f"  [{cell_name}] verified: {dim[0]}d")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Re-embed cells 384→128d (Nomic Matryoshka)")
    parser.add_argument('cell', nargs='?', help="Cell name (default: all)")
    parser.add_argument('--batch', type=int, default=500,
                        help="Batch size for chunk re-embed (default: 500)")
    parser.add_argument('--workers', '-w', type=int, default=1,
                        help="Parallel workers (default: 1, try 4 for ~4x speedup)")
    parser.add_argument('--max-cpu', type=int, default=0, metavar='PCT',
                        help="Throttle: pause between batches if system CPU%% exceeds this "
                             "(0 = no throttle, e.g. 70 = yield when machine is busy)")
    args = parser.parse_args()

    if args.workers <= 1:
        # Single-process: warm the singleton embedder
        from flex.onnx import get_model
        embedder = get_model()
        embedder.encode("warmup")
        print(f"Embedder warm. Target: {TARGET_DIM}d. Batch: {args.batch}"
              + (f". CPU cap: {args.max_cpu}%" if args.max_cpu else ""))
    else:
        print(f"Parallel mode: {args.workers} workers. "
              f"Target: {TARGET_DIM}d. Batch: {args.batch}"
              + (f". CPU cap: {args.max_cpu}%" if args.max_cpu else ""))

    if args.cell:
        path = str(resolve_cell(args.cell))
        reembed_cell(args.cell, path, args.workers, args.batch, max_cpu=args.max_cpu)
    else:
        cells = list_cells()
        cells.sort(key=lambda c: {
            'docpac': 0, 'website-raw': 1, 'claude-chat': 2, 'claude-code': 3
        }.get(c.get('cell_type', ''), 9))

        for cell in cells:
            reembed_cell(cell['name'], cell['path'], args.workers, args.batch, max_cpu=args.max_cpu)

    print("\nDone.")


if __name__ == '__main__':
    main()
