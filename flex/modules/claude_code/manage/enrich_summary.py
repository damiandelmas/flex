"""Session Fingerprint Enrichment — zero ONNX, chunk-level HDBSCAN.

Single bulk SQL query loads all chunks for eligible sessions.
HDBSCAN runs on pre-computed _raw_chunks.embedding. Span selection
uses text entropy. No re-embedding. Total runtime: ~30s.

Output: _enrich_session_summary table (source_id PK -> auto-JOINs sessions view)
"""

import sys
import time
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flex.core import open_cell, log_op
from flex.views import regenerate_views

from flex.modules.claude_code.manage.noise import session_filter_sql
from flex.modules.claude_code.manage.fingerprint import (
    HDBSCAN_MIN_CHUNKS, build_fingerprint, build_short_fingerprint,
    _is_content_chunk,
)

from flex.registry import resolve_cell
CLAUDE_CODE_DB = resolve_cell('claude_code')

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_session_summary (
    source_id TEXT PRIMARY KEY,
    fingerprint_index TEXT
)
"""

COMMIT_INTERVAL = 100


# ---------------------------------------------------------------------------
# Incremental — worker-callable, processes only missing sessions
# ---------------------------------------------------------------------------

def run(db):
    """Incremental fingerprinting — only sessions missing from _enrich_session_summary.

    Called by the worker's 30min enrichment cycle. Returns count of new fingerprints.
    """
    db.execute(CREATE_TABLE)

    # Find eligible sessions not yet fingerprinted
    eligible = db.execute(session_filter_sql()).fetchall()
    eligible_ids = [r[0] for r in eligible]

    existing = {r[0] for r in db.execute(
        "SELECT source_id FROM _enrich_session_summary"
    ).fetchall()}

    missing_ids = [sid for sid in eligible_ids if sid not in existing]
    if not missing_ids:
        return 0

    # Bulk load chunks for missing sessions only
    db.execute("CREATE TEMP TABLE _fp_incr (source_id TEXT PRIMARY KEY)")
    db.executemany("INSERT INTO _fp_incr VALUES (?)",
                   [(sid,) for sid in missing_ids])

    rows = db.execute("""
        SELECT e.source_id, c.id, c.embedding, c.content,
               e.position as message_number,
               t.tool_name, t.target_file
        FROM _fp_incr el
        JOIN _edges_source e ON el.source_id = e.source_id
        JOIN _raw_chunks c ON e.chunk_id = c.id
        LEFT JOIN _edges_tool_ops t ON c.id = t.chunk_id
        WHERE c.embedding IS NOT NULL
        ORDER BY e.source_id, e.position
    """).fetchall()

    db.execute("DROP TABLE IF EXISTS _fp_incr")

    # Group by source_id (use index-based access — caller may not have row_factory)
    session_chunks = {}
    for r in rows:
        sid = r[0]  # source_id
        if sid not in session_chunks:
            session_chunks[sid] = []
        session_chunks[sid].append({
            'id': r[1],
            'embedding': np.frombuffer(r[2], dtype=np.float32),
            'content': r[3] or '',
            'tool_name': r[5],
            'target_file': r[6],
            'message_number': r[4] or 0,
        })

    del rows

    processed = 0
    hdbscan_count = 0
    short_count = 0

    for sid, chunks in session_chunks.items():
        n_content = sum(1 for ch in chunks
                        if _is_content_chunk(ch) and ch.get('content', '').strip()
                        and ch.get('embedding') is not None)

        if n_content >= HDBSCAN_MIN_CHUNKS:
            fingerprint = build_fingerprint(chunks)
            hdbscan_count += 1
        else:
            fingerprint = build_short_fingerprint(chunks)
            short_count += 1

        db.execute("""
            INSERT OR REPLACE INTO _enrich_session_summary
            (source_id, fingerprint_index) VALUES (?, ?)
        """, (sid, fingerprint))
        processed += 1

        if processed % COMMIT_INTERVAL == 0:
            db.commit()

    db.commit()

    if processed > 0:
        log_op(db, 'build_session_fingerprint', '_enrich_session_summary',
               params={'processed': processed, 'hdbscan': hdbscan_count,
                       'short': short_count, 'mode': 'incremental'},
               rows_affected=processed, source='enrich_summary.py')

    return processed


# ---------------------------------------------------------------------------
# Full rebuild — manual use (DROP + recreate)
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Session Fingerprint Enrichment (Chunk-Level HDBSCAN, Zero ONNX)")
    print("=" * 60)

    t_start = time.time()

    db = open_cell(str(CLAUDE_CODE_DB))
    print(f"\nOpened: {CLAUDE_CODE_DB}")

    # Eligible sessions
    eligible = db.execute(session_filter_sql()).fetchall()
    eligible_ids = [r['source_id'] for r in eligible]
    print(f"Eligible sessions: {len(eligible_ids)}")

    # Create table
    db.execute("DROP TABLE IF EXISTS _enrich_session_summary")
    db.execute(CREATE_TABLE)
    db.commit()

    # -----------------------------------------------------------------------
    # Phase 1: Bulk load all chunks for eligible sessions (one query)
    # -----------------------------------------------------------------------
    print("\nPhase 1: Bulk loading chunks...")
    t1 = time.time()

    db.execute("CREATE TEMP TABLE _fp_eligible (source_id TEXT PRIMARY KEY)")
    db.executemany("INSERT INTO _fp_eligible VALUES (?)",
                   [(sid,) for sid in eligible_ids])

    rows = db.execute("""
        SELECT e.source_id, c.id, c.embedding, c.content,
               e.position as message_number,
               t.tool_name, t.target_file
        FROM _fp_eligible el
        JOIN _edges_source e ON el.source_id = e.source_id
        JOIN _raw_chunks c ON e.chunk_id = c.id
        LEFT JOIN _edges_tool_ops t ON c.id = t.chunk_id
        WHERE c.embedding IS NOT NULL
        ORDER BY e.source_id, e.position
    """).fetchall()

    # Group by source_id
    session_chunks = {}
    for r in rows:
        sid = r['source_id']
        if sid not in session_chunks:
            session_chunks[sid] = []
        session_chunks[sid].append({
            'id': r['id'],
            'embedding': np.frombuffer(r['embedding'], dtype=np.float32),
            'content': r['content'] or '',
            'tool_name': r['tool_name'],
            'target_file': r['target_file'],
            'message_number': r['message_number'] or 0,
        })

    total_chunks = len(rows)
    del rows  # free memory
    skipped = len(eligible_ids) - len(session_chunks)

    print(f"  {len(session_chunks)} sessions, {total_chunks} chunks")
    print(f"  {skipped} skipped (no chunks)")
    print(f"  Phase 1: {time.time() - t1:.1f}s")

    # -----------------------------------------------------------------------
    # Phase 2: Per-session HDBSCAN on pre-computed embeddings (zero ONNX)
    # -----------------------------------------------------------------------
    print(f"\nPhase 2: Fingerprinting {len(session_chunks)} sessions...")
    t2 = time.time()

    processed = 0
    hdbscan_count = 0
    short_count = 0

    for i, (sid, chunks) in enumerate(session_chunks.items()):
        if i > 0 and i % 200 == 0:
            elapsed = time.time() - t_start
            print(f"  {i}/{len(session_chunks)}... ({elapsed:.0f}s)")

        # Count content chunks to determine path
        n_content = sum(1 for ch in chunks
                        if _is_content_chunk(ch) and ch.get('content', '').strip()
                        and ch.get('embedding') is not None)

        if n_content >= HDBSCAN_MIN_CHUNKS:
            fingerprint = build_fingerprint(chunks)
            hdbscan_count += 1
        else:
            fingerprint = build_short_fingerprint(chunks)
            short_count += 1

        db.execute("""
            INSERT OR REPLACE INTO _enrich_session_summary
            (source_id, fingerprint_index)
            VALUES (?, ?)
        """, (sid, fingerprint))
        processed += 1

        if processed % COMMIT_INTERVAL == 0:
            db.commit()

    db.commit()
    print(f"  Phase 2: {time.time() - t2:.1f}s")

    log_op(db, 'build_session_fingerprint', '_enrich_session_summary',
           params={'processed': processed, 'hdbscan': hdbscan_count,
                   'short': short_count, 'skipped': skipped,
                   'total_chunks': total_chunks},
           rows_affected=processed, source='enrich_summary.py')
    print(f"\n  Processed: {processed} ({hdbscan_count} HDBSCAN, {short_count} short)")
    print(f"  Skipped (no chunks): {skipped}")

    # Regenerate views
    print("\nRegenerating views...")
    regenerate_views(db, views={'messages': 'chunk', 'sessions': 'source'})
    print("  Done")

    # Verify
    print("\nVerification:")
    total = db.execute(
        "SELECT COUNT(*) FROM _enrich_session_summary"
    ).fetchone()[0]
    with_fingerprint = db.execute(
        "SELECT COUNT(*) FROM _enrich_session_summary WHERE fingerprint_index IS NOT NULL"
    ).fetchone()[0]
    print(f"  Total rows: {total}")
    print(f"  With fingerprint: {with_fingerprint}")

    # Sample
    print("\n  Top sessions by size:")
    samples = db.execute("""
        SELECT s.source_id, s.fingerprint_index, src.message_count
        FROM _enrich_session_summary s
        JOIN _raw_sources src ON s.source_id = src.source_id
        WHERE s.fingerprint_index IS NOT NULL
        ORDER BY src.message_count DESC
        LIMIT 5
    """).fetchall()
    for s in samples:
        sid_short = s['source_id'][:8]
        first_line = (s['fingerprint_index'] or '').split('\n')[0][:120]
        line_count = len((s['fingerprint_index'] or '').split('\n'))
        print(f"    [{sid_short}] ({s['message_count']:>4} msgs, {line_count} lines) {first_line}")

    # Check sessions view
    print("\n  Sessions view columns:")
    cols = db.execute("PRAGMA table_info(sessions)").fetchall()
    col_names = [c[1] for c in cols]
    has_fingerprint = 'fingerprint_index' in col_names
    has_old = any(c in col_names for c in ('topic_summary', 'topic_clusters', 'community_label'))
    print(f"    fingerprint_index: {has_fingerprint}")
    print(f"    old columns present: {has_old}")

    db.close()
    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == '__main__':
    main()
