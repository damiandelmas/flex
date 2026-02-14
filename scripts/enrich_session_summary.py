"""Session Summary Enrichment — Embedding-relative topic extraction.

For each meaningful session (message_count >= 5, not agent, not warmup):
  - HDBSCAN on chunk embeddings → topic clusters with percentages
  - Labels from centroid-adjacent chunks (files > actions > kinds > content)
  - Community label from hub sessions in same community

Output: _enrich_session_summary table (source_id PK → auto-JOINs sessions view)
"""

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import hdbscan
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FLEX_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flexsearch.core import open_cell
from flexsearch.views import regenerate_views

CELLS_ROOT = Path.home() / '.qmem' / 'cells' / 'projects'
THREAD_DB = CELLS_ROOT / 'thread' / 'main.db'

SESSION_FILTER = """
    SELECT source_id FROM _raw_sources
    WHERE message_count >= 5
      AND source_id NOT LIKE 'agent-%'
      AND (title IS NULL OR title != 'Warmup')
"""

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_session_summary (
    source_id TEXT PRIMARY KEY,
    topic_clusters TEXT,
    community_label TEXT,
    topic_summary TEXT
)
"""

# Full action map covering all tool_name values
ACTION_MAP = {
    'Bash': 'shell',
    'Read': 'reading',
    'Write': 'writing',
    'Edit': 'editing',
    'Grep': 'search',
    'Glob': 'search',
    'Task': 'delegation',
    'TodoWrite': 'planning',
    'TaskCreate': 'planning',
    'TaskUpdate': 'planning',
    'TaskOutput': 'delegation',
    'BashOutput': 'shell',
    'WebFetch': 'web research',
    'WebSearch': 'web research',
    'Skill': 'skill invocation',
    'ExitPlanMode': 'planning',
    'UserPrompt': 'conversation',
}

# kind (semantic_role) → human label
KIND_MAP = {
    'prompt': 'conversation',
    'response': 'conversation',
    'command': 'shell',
    'read': 'reading',
    'file_operation': 'file ops',
    'search': 'search',
    'delegation': 'delegation',
    'message': 'conversation',
}


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
# Cluster labeling
# ---------------------------------------------------------------------------

def label_cluster(chunks, cluster_indices):
    """Label a cluster from its centroid-adjacent chunks.

    Priority: file basenames > action pattern > kind pattern > content snippet.
    """
    if not cluster_indices:
        return "mixed"

    # Compute centroid
    vecs = np.array([chunks[i]['embedding'] for i in cluster_indices])
    centroid = vecs.mean(axis=0)
    centroid /= (np.linalg.norm(centroid) + 1e-10)

    # Find 5 nearest to centroid
    sims = cosine_similarity(centroid.reshape(1, -1), vecs)[0]
    top_k = min(5, len(sims))
    nearest_idx = np.argsort(sims)[-top_k:][::-1]
    nearest_chunks = [chunks[cluster_indices[i]] for i in nearest_idx]

    # Strategy 1: file basenames (deduplicated)
    files = []
    for ch in nearest_chunks:
        if ch['target_file']:
            basename = os.path.basename(ch['target_file'])
            if basename and basename not in files:
                files.append(basename)
    if files:
        return ' + '.join(files[:3])

    # Strategy 2: dominant action (mapped to human label)
    actions = [ch['action'] for ch in nearest_chunks if ch['action']]
    if actions:
        counts = Counter(actions)
        top_action = counts.most_common(1)[0][0]
        # MCP tools: extract the last segment
        if top_action.startswith('mcp__'):
            parts = top_action.split('__')
            return parts[-1] if len(parts) > 2 else 'MCP tool'
        return ACTION_MAP.get(top_action, top_action)

    # Strategy 3: dominant kind from ALL cluster chunks (not just centroid)
    all_cluster_chunks = [chunks[i] for i in cluster_indices]
    kinds = [ch['kind'] for ch in all_cluster_chunks if ch['kind']]
    if kinds:
        counts = Counter(kinds)
        top_kind = counts.most_common(1)[0][0]
        return KIND_MAP.get(top_kind, top_kind)

    return "mixed"


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
    # Round pcts
    for t in result:
        t['pct'] = round(t['pct'], 1)
    result.sort(key=lambda t: t['pct'], reverse=True)
    return result


def intra_session_topics(chunks):
    """HDBSCAN clustering on session chunks → topic list with labels + pct."""
    if len(chunks) < 20:
        return None  # too few for HDBSCAN

    vecs = np.array([ch['embedding'] for ch in chunks])

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=5,
        min_samples=3,
        metric='euclidean',
    )
    labels = clusterer.fit_predict(vecs)

    unique_labels = set(labels)
    unique_labels.discard(-1)  # noise

    if not unique_labels:
        return None  # no clusters found

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

    # Label noise too (instead of generic "other")
    if noise_indices:
        pct = round(100.0 * len(noise_indices) / total, 1)
        if pct >= 5.0:
            noise_label = label_cluster(chunks, noise_indices)
            topics.append({
                'label': noise_label,
                'pct': pct,
                'count': len(noise_indices),
            })

    # Merge duplicate labels (e.g. multiple "shell" clusters)
    topics = _merge_topics(topics)
    return topics


def short_session_label(chunks):
    """For sessions with <20 chunks, build a simple label from file/action/kind distribution."""
    files = []
    actions = []
    kinds = []
    for ch in chunks:
        if ch['target_file']:
            basename = os.path.basename(ch['target_file'])
            if basename:
                files.append(basename)
        if ch['action']:
            actions.append(ch['action'])
        if ch['kind']:
            kinds.append(ch['kind'])

    if files:
        counts = Counter(files)
        top_files = [f for f, _ in counts.most_common(3)]
        label = ' + '.join(top_files)
    elif actions:
        counts = Counter(actions)
        top_action = counts.most_common(1)[0][0]
        if top_action.startswith('mcp__'):
            parts = top_action.split('__')
            label = parts[-1] if len(parts) > 2 else 'MCP tool'
        else:
            label = ACTION_MAP.get(top_action, top_action)
    elif kinds:
        counts = Counter(kinds)
        top_kind = counts.most_common(1)[0][0]
        label = KIND_MAP.get(top_kind, top_kind)
    else:
        label = "mixed"

    return [{'label': label, 'pct': 100.0, 'count': len(chunks)}]


# ---------------------------------------------------------------------------
# Community label
# ---------------------------------------------------------------------------

def build_community_labels(db):
    """Label communities by dominant project distribution.

    Returns dict: community_id → label string like 'axpmarket (96%)'
    """
    rows = db.execute("""
        SELECT g.community_id, src.project, COUNT(*) as cnt
        FROM _enrich_source_graph g
        JOIN _raw_sources src ON g.source_id = src.source_id
        WHERE src.project IS NOT NULL
        GROUP BY g.community_id, src.project
        ORDER BY g.community_id, cnt DESC
    """).fetchall()

    from collections import defaultdict
    comms = defaultdict(list)
    for r in rows:
        comms[r['community_id']].append((r['cnt'], r['project']))

    labels = {}
    for cid, projs in comms.items():
        total = sum(c for c, _ in projs)
        if total == 0:
            continue
        # Top 2 projects if second is >= 20%
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
    """Build one-liner: 'topic1 (65%) + topic2 (25%) [community: ...]'"""
    parts = []

    if topics:
        topic_strs = []
        for t in topics[:3]:  # max 3 topics
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

    db = open_cell(str(THREAD_DB))
    print(f"\nOpened: {THREAD_DB}")

    # Eligible sessions
    eligible = db.execute(SESSION_FILTER).fetchall()
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

        # Topic extraction
        topics = intra_session_topics(chunks)
        if topics is not None:
            hdbscan_count += 1
        else:
            topics = short_session_label(chunks)
            short_count += 1

        # Community label (from project distribution)
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
    print(f"\n  Processed: {processed} ({hdbscan_count} HDBSCAN, {short_count} short)")
    print(f"  Skipped (no chunks): {skipped}")

    # Regenerate views
    print("\nRegenerating views...")
    regenerate_views(db)
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

    # Sample — largest sessions
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
