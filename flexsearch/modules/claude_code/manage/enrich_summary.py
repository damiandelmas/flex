"""Session Summary Enrichment — Embedding-relative topic extraction.

For each meaningful session (filtered by module noise config):
  - HDBSCAN on chunk embeddings -> topic clusters with percentages
  - Labels from centroid-adjacent chunks (files > actions > kinds > content)
  - Community label from hub sessions in same community

Output: _enrich_session_summary table (source_id PK -> auto-JOINs sessions view)
"""

import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import hdbscan
import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# scripts/ -> claude_code/ -> modules/ -> flexsearch/ -> main/
FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flexsearch.core import open_cell, log_op
from flexsearch.views import regenerate_views

# Module imports — all claude-code-specific config lives here
from flexsearch.modules.claude_code.manage.noise import session_filter_sql
from flexsearch.modules.claude_code.manage.summary import (
    HDBSCAN_MIN_CHUNKS, HDBSCAN_MIN_CLUSTER_SIZE, HDBSCAN_MIN_SAMPLES,
    HDBSCAN_METRIC, label_cluster, short_session_label,
)

from flexsearch.registry import resolve_cell
CLAUDE_CODE_DB = resolve_cell('claude_code')

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_session_summary (
    source_id TEXT PRIMARY KEY,
    topic_clusters TEXT,
    community_label TEXT,
    topic_summary TEXT
)
"""


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_session_metadata(db):
    """Load session-level metadata: title, community_id, is_hub, embedding."""
    rows = db.execute("""
        SELECT src.source_id, src.title, src.message_count, src.embedding,
               g.community_id, g.is_hub, g.centrality
        FROM _raw_sources src
        LEFT JOIN _enrich_source_graph g ON src.source_id = g.source_id
    """).fetchall()
    meta = {}
    for r in rows:
        emb = np.frombuffer(r['embedding'], dtype=np.float32) if r['embedding'] else None
        meta[r['source_id']] = {
            'title': r['title'],
            'message_count': r['message_count'],
            'embedding': emb,
            'community_id': r['community_id'],
            'is_hub': bool(r['is_hub']) if r['is_hub'] is not None else False,
            'centrality': r['centrality'] or 0.0,
        }
    return meta


def load_session_chunks(db, source_id):
    """Load chunks for a single session with embeddings + metadata."""
    rows = db.execute("""
        SELECT c.id, c.embedding, c.content,
               t.tool_name, t.target_file,
               et.semantic_role
        FROM _raw_chunks c
        JOIN _edges_source e ON c.id = e.chunk_id
        LEFT JOIN _edges_tool_ops t ON c.id = t.chunk_id
        LEFT JOIN _enrich_types et ON c.id = et.chunk_id
        WHERE e.source_id = ?
          AND c.embedding IS NOT NULL
        ORDER BY e.position
    """, (source_id,)).fetchall()
    chunks = []
    for r in rows:
        chunks.append({
            'id': r['id'],
            'embedding': np.frombuffer(r['embedding'], dtype=np.float32),
            'content': r['content'] or '',
            'action': r['tool_name'],
            'target_file': r['target_file'],
            'kind': r['semantic_role'],
        })
    return chunks


# ---------------------------------------------------------------------------
# Topic extraction
# ---------------------------------------------------------------------------

def _merge_topics(topics):
    """Merge topics with the same label, summing counts."""
    merged = {}
    for t in topics:
        key = t['label']
        if key in merged:
            merged[key]['count'] += t['count']
            merged[key]['pct'] += t['pct']
        else:
            merged[key] = dict(t)
    result = list(merged.values())
    for t in result:
        t['pct'] = round(t['pct'], 1)
    result.sort(key=lambda t: t['pct'], reverse=True)
    return result


def intra_session_topics(chunks):
    """HDBSCAN clustering on session chunks -> topic list with labels + pct."""
    if len(chunks) < HDBSCAN_MIN_CHUNKS:
        return None

    vecs = np.array([ch['embedding'] for ch in chunks])

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=HDBSCAN_MIN_CLUSTER_SIZE,
        min_samples=HDBSCAN_MIN_SAMPLES,
        metric=HDBSCAN_METRIC,
    )
    labels = clusterer.fit_predict(vecs)

    unique_labels = set(labels)
    unique_labels.discard(-1)

    if not unique_labels:
        return None

    total = len(chunks)
    noise_indices = [i for i, l in enumerate(labels) if l == -1]
    topics = []

    for lbl in sorted(unique_labels):
        indices = [i for i, l in enumerate(labels) if l == lbl]
        count = len(indices)
        pct = round(100.0 * count / total, 1)
        label_str = label_cluster(chunks, indices)
        topics.append({
            'label': label_str,
            'pct': pct,
            'count': count,
        })

    if noise_indices:
        pct = round(100.0 * len(noise_indices) / total, 1)
        if pct >= 5.0:
            noise_label = label_cluster(chunks, noise_indices)
            topics.append({
                'label': noise_label,
                'pct': pct,
                'count': len(noise_indices),
            })

    topics = _merge_topics(topics)
    return topics


# ---------------------------------------------------------------------------
# Community label
# ---------------------------------------------------------------------------

def build_community_labels(db):
    """Label communities by dominant project distribution."""
    rows = db.execute("""
        SELECT g.community_id, src.project, COUNT(*) as cnt
        FROM _enrich_source_graph g
        JOIN _raw_sources src ON g.source_id = src.source_id
        WHERE src.project IS NOT NULL
        GROUP BY g.community_id, src.project
        ORDER BY g.community_id, cnt DESC
    """).fetchall()

    comms = defaultdict(list)
    for r in rows:
        comms[r['community_id']].append((r['cnt'], r['project']))

    labels = {}
    for cid, projs in comms.items():
        total = sum(c for c, _ in projs)
        if total == 0:
            continue
        top = projs[0]
        pct1 = round(100 * top[0] / total)
        label = f"{top[1]} ({pct1}%)"
        if len(projs) > 1:
            pct2 = round(100 * projs[1][0] / total)
            if pct2 >= 20:
                label += f" + {projs[1][1]} ({pct2}%)"
        labels[cid] = label

    return labels


# ---------------------------------------------------------------------------
# Summary composition
# ---------------------------------------------------------------------------

def compose_summary(topics, comm_label):
    """Build one-liner: 'topic1 (65%) + topic2 (25%) | community: ...'"""
    parts = []

    if topics:
        topic_strs = []
        for t in topics[:3]:
            topic_strs.append(f"{t['label']} ({t['pct']}%)")
        parts.append(' + '.join(topic_strs))

    if comm_label:
        parts.append(f"community: {comm_label}")

    summary = ' | '.join(parts)
    if len(summary) > 250:
        summary = summary[:247] + '...'

    return summary or None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Session Summary Enrichment (Embedding-Relative)")
    print("=" * 60)

    t_start = time.time()

    db = open_cell(str(CLAUDE_CODE_DB))
    print(f"\nOpened: {CLAUDE_CODE_DB}")

    # Eligible sessions — filter from module config
    eligible = db.execute(session_filter_sql()).fetchall()
    eligible_ids = [r['source_id'] for r in eligible]
    print(f"Eligible sessions: {len(eligible_ids)}")

    # Create table
    db.execute("DROP TABLE IF EXISTS _enrich_session_summary")
    db.execute(CREATE_TABLE)
    db.commit()
    print("Created _enrich_session_summary table")

    # Load metadata
    print("\nLoading metadata...")
    meta = load_session_metadata(db)
    comm_labels = build_community_labels(db)
    print(f"  {len(meta)} sources, {len(comm_labels)} community labels")

    # Process sessions
    print("\nProcessing sessions...")
    processed = 0
    hdbscan_count = 0
    short_count = 0
    skipped = 0

    for i, sid in enumerate(eligible_ids):
        if i > 0 and i % 200 == 0:
            print(f"  {i}/{len(eligible_ids)}...")

        chunks = load_session_chunks(db, sid)
        if not chunks:
            skipped += 1
            continue

        # Topic extraction — uses module HDBSCAN config
        topics = intra_session_topics(chunks)
        if topics is not None:
            hdbscan_count += 1
        else:
            topics = short_session_label(chunks)
            short_count += 1

        # Community label
        m = meta.get(sid, {})
        comm = comm_labels.get(m.get('community_id'))

        # Compose summary
        summary = compose_summary(topics, comm)

        # Insert
        db.execute("""
            INSERT OR REPLACE INTO _enrich_session_summary
            (source_id, topic_clusters, community_label, topic_summary)
            VALUES (?, ?, ?, ?)
        """, (
            sid,
            json.dumps(topics),
            comm,
            summary,
        ))
        processed += 1

    db.commit()
    log_op(db, 'build_session_summary', '_enrich_session_summary',
           params={'processed': processed, 'hdbscan': hdbscan_count,
                   'short': short_count, 'skipped': skipped},
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
    with_summary = db.execute(
        "SELECT COUNT(*) FROM _enrich_session_summary WHERE topic_summary IS NOT NULL"
    ).fetchone()[0]
    unknown = db.execute(
        "SELECT COUNT(*) FROM _enrich_session_summary WHERE topic_summary LIKE '%mixed%'"
    ).fetchone()[0]
    print(f"  Total rows: {total}")
    print(f"  With summary: {with_summary}")
    print(f"  With 'mixed' fallback: {unknown} ({100*unknown/total:.1f}%)")

    # Sample
    print("\n  Top sessions by size:")
    samples = db.execute("""
        SELECT s.source_id, s.topic_summary, src.message_count
        FROM _enrich_session_summary s
        JOIN _raw_sources src ON s.source_id = src.source_id
        ORDER BY src.message_count DESC
        LIMIT 10
    """).fetchall()
    for s in samples:
        sid_short = s['source_id'][:8]
        print(f"    [{sid_short}] ({s['message_count']:>4} msgs) {s['topic_summary'][:100]}")

    # Check sessions view
    print("\n  Sessions view columns:")
    cols = db.execute("PRAGMA table_info(sessions)").fetchall()
    col_names = [c[1] for c in cols]
    new_cols = [c for c in col_names if c in ('topic_clusters', 'community_label', 'topic_summary')]
    print(f"    New columns present: {new_cols}")

    db.close()
    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed:.1f}s")
    print("Restart flexsearch-mcp to pick up changes.")


if __name__ == '__main__':
    main()
