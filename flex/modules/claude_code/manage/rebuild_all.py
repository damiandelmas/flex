#!/usr/bin/env python
"""Rebuild all enrichments on claude_code cell with noise filters.

Runs: source_graph -> session_summary -> file_graph + delegation_graph
Uses module configs from flex.modules.claude_code.manage.*
"""

import sys
import time
from pathlib import Path

import numpy as np

FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flex.core import open_cell, log_op
from flex.views import regenerate_views
from flex.manage.meditate import build_similarity_graph, compute_scores, persist
from flex.modules.claude_code.manage.noise import graph_filter_sql

from flex.registry import resolve_cell
CLAUDE_CODE_DB = resolve_cell('claude_code')

# Plan 10: selective pooling + mean centering shifted median pairwise similarity
# from 0.61 to -0.006. Old threshold 0.65 produced 168 communities but 135
# singletons — fractured PageRank. 0.55 gives 15.2% density, 48 communities,
# 40 singletons. Connected enough for PageRank to differentiate.
GRAPH_THRESHOLD = 0.55


def rebuild_warmup_types(db):
    """Build _types_source_warmup — structural warmup detection.

    Warmup: < 50 tool-op chunks AND no Write/Edit/Task/MultiEdit.
    Replaces brittle title = 'Warmup' filter.
    """
    print("=" * 60)
    print("Step 0: Warmup Detection")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()
    db.execute("""
        CREATE TABLE IF NOT EXISTS _types_source_warmup (
            source_id TEXT PRIMARY KEY,
            is_warmup_only INTEGER DEFAULT 0
        )
    """)
    db.execute("DELETE FROM _types_source_warmup")
    db.execute("""
        INSERT INTO _types_source_warmup (source_id, is_warmup_only)
        SELECT es.source_id, 1
        FROM _edges_source es
        JOIN _edges_tool_ops t ON es.chunk_id = t.chunk_id
        GROUP BY es.source_id
        HAVING COUNT(*) < 50
           AND SUM(CASE WHEN t.tool_name IN ('Write', 'Edit', 'Task', 'MultiEdit')
                        THEN 1 ELSE 0 END) = 0
    """)
    db.commit()

    warmup_count = db.execute(
        "SELECT COUNT(*) FROM _types_source_warmup WHERE is_warmup_only = 1"
    ).fetchone()[0]
    log_op(db, 'rebuild_warmup_types', '_types_source_warmup',
           params={'rule': '<50 tool ops AND no Write/Edit/Task/MultiEdit'},
           rows_affected=warmup_count, source='rebuild_all.py')
    print(f'  {warmup_count} warmup sessions detected in {time.time()-t0:.1f}s\n')
    sys.stdout.flush()


def reembed_sources(db):
    """Re-embed _raw_sources from prompt + response chunks only.

    Plan 10 selective pooling: mean-pool of ALL chunks converges every session
    toward 'generic Claude Code session' (median pairwise sim 0.61). Filtering
    to user_prompt + assistant captures human intent and agent reasoning —  the
    actual topical content. Tool calls, Read results, Edit diffs are structural
    and already captured in edge tables.

    IDF-weighted pooling: each chunk is weighted by (1 - cosine(chunk, corpus_centroid)).
    Chunks near the corpus centroid (boilerplate preambles, nexus injections, generic
    greetings) get near-zero weight. Distinctive chunks dominate the pool. The corpus
    centroid is computed once from existing source embeddings before the loop — it
    approximates the average session direction in embedding space.
    """
    print("=" * 60)
    print("Step 0.5: Selective Source Pooling (prompt + response only)")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()

    # Null out any chunk embeddings with wrong dimension (e.g. legacy 384d MiniLM).
    # The worker re-embeds NULL chunks at 128d on its next cycle.
    # length(embedding) / 4 = dims (float32). Expected: 128d = 512 bytes.
    nulled = db.execute("""
        UPDATE _raw_chunks SET embedding = NULL
        WHERE embedding IS NOT NULL AND length(embedding) != 512
    """).rowcount
    if nulled:
        db.commit()
        print(f"  Nulled {nulled} chunks with wrong embedding dimension (will re-embed at 128d)")
        sys.stdout.flush()

    # Compute corpus centroid from existing source embeddings.
    # Used to down-weight chunks that are close to the corpus mean (boilerplate).
    centroid = None
    src_rows = db.execute(
        "SELECT embedding FROM _raw_sources WHERE embedding IS NOT NULL"
    ).fetchall()
    if src_rows:
        src_vecs = np.array([np.frombuffer(r[0], dtype=np.float32) for r in src_rows])
        centroid = src_vecs.mean(axis=0)
        c_norm = np.linalg.norm(centroid)
        if c_norm > 0:
            centroid = centroid / c_norm
    print(f"  Corpus centroid from {len(src_rows)} source embeddings")
    sys.stdout.flush()

    sources = db.execute("SELECT source_id FROM _raw_sources").fetchall()
    updated = 0
    skipped = 0

    for (source_id,) in sources:
        rows = db.execute("""
            SELECT c.embedding
            FROM _raw_chunks c
            JOIN _edges_source es ON c.id = es.chunk_id
            JOIN _types_message tm ON c.id = tm.chunk_id
            WHERE es.source_id = ?
              AND tm.type IN ('user_prompt', 'assistant')
              AND c.embedding IS NOT NULL
        """, (source_id,)).fetchall()

        if not rows:
            skipped += 1
            continue

        vecs = np.array([np.frombuffer(r[0], dtype=np.float32) for r in rows])

        if centroid is not None and len(vecs) > 1:
            # Weight = 1 - cosine(chunk, corpus_centroid).
            # Chunks near the centroid (common boilerplate) → weight ≈ 0.
            # Distinctive chunks → weight ≈ 2. Falls on a concave curve.
            # Clip to [0.05, 2.0] so no chunk is completely silenced.
            cosines = vecs @ centroid          # (N,) — already normalized vecs
            weights = np.clip(1.0 - cosines, 0.05, 2.0)
            new_emb = (vecs * weights[:, None]).sum(axis=0) / weights.sum()
        else:
            new_emb = vecs.mean(axis=0)

        norm = np.linalg.norm(new_emb)
        if norm > 0:
            new_emb = new_emb / norm

        db.execute("UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                   (new_emb.tobytes(), source_id))
        updated += 1

    db.commit()
    elapsed = time.time() - t0
    print(f"  Re-embedded {updated} sources ({skipped} skipped, no prompt/response chunks)")
    print(f"  Done in {elapsed:.1f}s\n")
    sys.stdout.flush()


def rebuild_source_graph(db):
    """Rebuild source graph with noise filter + tuned threshold."""
    print("=" * 60)
    print("Step 1: Source Graph (with noise filter + mean centering)")
    print("=" * 60)

    old_cnt = db.execute('SELECT COUNT(*) FROM _enrich_source_graph').fetchone()[0]
    old_hubs = db.execute('SELECT COUNT(*) FROM _enrich_source_graph WHERE is_hub = 1').fetchone()[0]
    print(f'BEFORE: {old_cnt} rows, {old_hubs} hubs')
    sys.stdout.flush()

    where = graph_filter_sql()
    print(f'Noise filter: min_chunks >= 20, no warmups')
    print(f'Threshold: {GRAPH_THRESHOLD} (was 0.5)')
    sys.stdout.flush()

    t0 = time.time()

    cnt = db.execute(f"""
        SELECT COUNT(*) FROM _raw_sources
        WHERE embedding IS NOT NULL AND ({where})
    """).fetchone()[0]
    print(f'Filtered sources: {cnt}')
    sys.stdout.flush()

    G, edges = build_similarity_graph(db, table='_raw_sources', id_col='source_id',
                                       threshold=GRAPH_THRESHOLD, where=where,
                                       center=True)
    t1 = time.time()
    print(f'Graph built in {t1-t0:.1f}s — {edges} edges')
    sys.stdout.flush()

    scores = compute_scores(G)
    t2 = time.time()
    print(f'Scored in {t2-t1:.1f}s')
    print(f'  {len(scores["communities"])} communities, {len(scores["hubs"])} hubs, {len(scores["bridges"])} bridges')
    sys.stdout.flush()

    persist(db, scores, table='_enrich_source_graph', id_col='source_id')
    t3 = time.time()
    log_op(db, 'build_similarity_graph', '_enrich_source_graph',
           params={'threshold': GRAPH_THRESHOLD, 'where': where,
                   'nodes': G.number_of_nodes() if G else 0, 'edges': edges,
                   'communities': len(scores['communities']),
                   'hubs': len(scores['hubs']),
                   'bridges': len(scores['bridges'])},
           rows_affected=G.number_of_nodes() if G else 0,
           source='rebuild_all.py')
    print(f'Persisted in {t3-t2:.1f}s')

    new_cnt = db.execute('SELECT COUNT(*) FROM _enrich_source_graph').fetchone()[0]
    new_hubs = db.execute('SELECT COUNT(*) FROM _enrich_source_graph WHERE is_hub = 1').fetchone()[0]
    comms = db.execute('SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph WHERE community_id IS NOT NULL').fetchone()[0]
    print(f'\nAFTER: {new_cnt} rows, {new_hubs} hubs, {comms} communities')
    print(f'Source graph total: {t3-t0:.1f}s\n')
    sys.stdout.flush()


_LABEL_STOPWORDS = {
    # common English
    'the', 'and', 'for', 'with', 'that', 'this', 'from', 'have', 'are',
    'was', 'were', 'been', 'will', 'would', 'could', 'should', 'into',
    'also', 'just', 'back', 'next', 'step', 'done', 'good', 'here',
    'then', 'when', 'what', 'which', 'they', 'their', 'some', 'all',
    'not', 'now', 'can', 'get', 'let', 'use', 'make', 'run', 'add',
    'each', 'only', 'want', 'need', 'keep', 'like', 'used', 'more',
    'than', 'both', 'well', 'most', 'over', 'after', 'before', 'very',
    'these', 'where', 'your', 'there', 'here', 'been', 'using',
    # tool names (structural fingerprint noise)
    'read', 'edit', 'bash', 'write', 'task', 'grep', 'glob', 'search',
    'fetch', 'create', 'delete', 'update', 'select', 'insert',
    # universal flex-domain words (appear in every session, carry no signal)
    'flex', 'flexsearch', 'query', 'session', 'sessions',
    'chunk', 'chunks', 'source', 'sources', 'view', 'views',
    'column', 'columns', 'cell', 'cells', 'agent', 'agents',
    'claude', 'data', 'code', 'file', 'files', 'table', 'tables',
    'result', 'results', 'model', 'value', 'index', 'build',

    # SQL / Python keywords
    'null', 'true', 'false', 'none', 'self', 'return', 'print',
    'list', 'dict', 'import', 'from', 'class', 'pass', 'else',
}


def _extract_community_keywords(texts, n=4, extra_stopwords=None):
    """Top N keywords from a list of fingerprint texts via token frequency.

    Targets quoted content in fingerprints (the high-signal lines) rather
    than structural lines like "> [2-8] 4x op:Read".

    extra_stopwords: additional tokens to suppress (e.g. path components,
                     dominant project name to avoid duplication).
    """
    import re
    from collections import Counter

    stopwords = _LABEL_STOPWORDS | (extra_stopwords or set())
    counter = Counter()
    for text in texts:
        if not text:
            continue
        # Quoted strings in fingerprints are the content-rich lines.
        # Skip quoted strings that are file paths (start with /).
        quoted = [q for q in re.findall(r'"([^"]{10,300})"', text)
                  if not q.strip().startswith('/')]
        source = ' '.join(quoted) if quoted else text
        # 4+ char lowercase words only
        for tok in re.findall(r'\b[a-z]{4,}\b', source.lower()):
            if tok not in stopwords:
                counter[tok] += 1
    return [w for w, _ in counter.most_common(n)]


def rebuild_community_labels(db):
    """Label each community by keyword extraction from its hub fingerprints.

    Reads the top 5 hub session fingerprints per community, extracts dominant
    keywords, and writes a human-readable label to _enrich_source_graph.community_label.
    Labels survive as long as community membership is stable — they're overwritten
    on every rebuild_all run, so they always reflect the current graph topology.
    """
    print("=" * 60)
    print("Step 1.5: Community Labeling")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()

    # Add column if this is an existing cell that predates community_label
    try:
        db.execute("ALTER TABLE _enrich_source_graph ADD COLUMN community_label TEXT")
        db.commit()
    except Exception:
        pass  # column already exists

    from collections import defaultdict

    # Dominant project per community — top non-dot project among hub sessions.
    # Dot-projects (.nexus, .claude) are structural infrastructure that appears
    # in almost every session; they carry no topical signal.
    project_rows = db.execute("""
        SELECT g.community_id, rs.project, COUNT(*) as n
        FROM _enrich_source_graph g
        JOIN _raw_sources rs ON g.source_id = rs.source_id
        WHERE g.community_id IS NOT NULL
          AND g.is_hub = 1
          AND rs.project IS NOT NULL
          AND rs.project NOT LIKE '.%'
        GROUP BY g.community_id, rs.project
        ORDER BY g.community_id, n DESC
    """).fetchall()

    dominant_project = {}
    for community_id, project, _ in project_rows:
        if community_id not in dominant_project:
            dominant_project[community_id] = project

    # Top 5 hub fingerprints per community for keyword extraction
    fp_rows = db.execute("""
        SELECT g.community_id, ess.fingerprint_index
        FROM _enrich_source_graph g
        JOIN _enrich_session_summary ess ON g.source_id = ess.source_id
        WHERE g.community_id IS NOT NULL
          AND g.is_hub = 1
          AND ess.fingerprint_index IS NOT NULL
        ORDER BY g.community_id, g.centrality DESC
    """).fetchall()

    community_texts = defaultdict(list)
    for community_id, fingerprint in fp_rows:
        if len(community_texts[community_id]) < 5:
            community_texts[community_id].append(fingerprint)

    # Derive path-component noise dynamically from repo paths in this cell.
    # Avoids hardcoding user-specific tokens (username, dir names).
    path_stopwords = set()
    try:
        repo_rows = db.execute(
            "SELECT repo_path FROM _enrich_repo_identity WHERE repo_path IS NOT NULL"
        ).fetchall()
        for (path,) in repo_rows:
            for part in path.strip('/').split('/'):
                if len(part) >= 3:
                    path_stopwords.add(part.lower())
    except Exception:
        pass

    all_communities = set(dominant_project) | set(community_texts)

    labeled = 0
    for community_id in all_communities:
        parts = []
        proj = dominant_project.get(community_id)
        if proj:
            parts.append(proj)

        # Suppress path noise + project name from keywords to avoid duplication
        extra_stop = path_stopwords | ({proj.lower()} if proj else set())
        keywords = _extract_community_keywords(
            community_texts.get(community_id, []),
            n=3,
            extra_stopwords=extra_stop,
        )
        parts.extend(keywords)
        if parts:
            label = ' · '.join(parts[:4])
            db.execute(
                "UPDATE _enrich_source_graph SET community_label = ? WHERE community_id = ?",
                (label, community_id)
            )
            labeled += 1

    db.commit()
    log_op(db, 'rebuild_community_labels', '_enrich_source_graph',
           rows_affected=labeled, source='rebuild_all.py')
    print(f'  {labeled} communities labeled in {time.time()-t0:.1f}s\n')
    sys.stdout.flush()


def rebuild_file_graph(db):
    """Rebuild file co-edit graph."""
    from flex.modules.claude_code.manage.file_graph import (
        CREATE_TABLE, build_file_graph, analyze_file_graph,
    )
    from collections import defaultdict

    print("=" * 60)
    print("Step 2: File Co-Edit Graph")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()
    G, session_files = build_file_graph(db)
    if G.number_of_nodes() == 0:
        print("No file identity data. Skipping.")
        return

    partition, pr, hubs = analyze_file_graph(G)
    communities = defaultdict(int)
    for comm_id in partition.values():
        communities[comm_id] += 1
    print(f'  {len(communities)} communities, {len(hubs)} hubs')

    db.execute("DROP TABLE IF EXISTS _enrich_file_graph")
    db.execute(CREATE_TABLE)
    for node in G.nodes():
        db.execute(
            "INSERT INTO _enrich_file_graph VALUES (?,?,?,?,?)",
            (node, partition.get(node), pr.get(node, 0.0),
             1 if node in hubs else 0, len(session_files.get(node, set())))
        )
    db.commit()
    row_count = G.number_of_nodes()
    log_op(db, 'build_file_graph', '_enrich_file_graph',
           params={'nodes': row_count, 'edges': G.number_of_edges(),
                   'communities': len(communities), 'hubs': len(hubs)},
           rows_affected=row_count, source='rebuild_all.py')
    t1 = time.time()
    print(f'Done in {t1-t0:.1f}s — {row_count} rows\n')
    sys.stdout.flush()


def rebuild_delegation_graph(db):
    """Rebuild delegation graph."""
    from flex.modules.claude_code.manage.delegation_graph import (
        CREATE_TABLE, build_delegation_graph, compute_delegation_metrics,
    )

    print("=" * 60)
    print("Step 3: Delegation Graph")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()
    G = build_delegation_graph(db)
    if G.number_of_nodes() == 0:
        print("No delegation data. Skipping.")
        return

    metrics = compute_delegation_metrics(G)
    orchestrators = sum(1 for m in metrics.values() if m['is_orchestrator'])
    print(f'  {len(metrics)} sessions, {orchestrators} orchestrators')

    db.execute("DROP TABLE IF EXISTS _enrich_delegation_graph")
    db.execute(CREATE_TABLE)
    for node, m in metrics.items():
        db.execute(
            "INSERT INTO _enrich_delegation_graph VALUES (?,?,?,?,?)",
            (node, m['agents_spawned'], m['is_orchestrator'],
             m['delegation_depth'], m['parent_session'])
        )
    db.commit()
    row_count = len(metrics)
    log_op(db, 'build_delegation_graph', '_enrich_delegation_graph',
           params={'sessions': row_count, 'orchestrators': orchestrators},
           rows_affected=row_count, source='rebuild_all.py')
    t1 = time.time()
    print(f'Done in {t1-t0:.1f}s — {row_count} rows\n')
    sys.stdout.flush()


def rebuild_fingerprints(db):
    """Rebuild session fingerprints (incremental — only missing sessions)."""
    from flex.modules.claude_code.manage.enrich_summary import run as run_fingerprints

    print("=" * 60)
    print("Step 3.5: Session Fingerprints (incremental, zero ONNX)")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()
    processed = run_fingerprints(db)
    elapsed = time.time() - t0
    print(f'  {processed} sessions fingerprinted in {elapsed:.1f}s\n')
    sys.stdout.flush()


def rebuild_repo_project(db):
    """Rebuild repo project attribution (incremental — NULL project only)."""
    from flex.modules.claude_code.manage.enrich_repo_project import run as run_repo_project
    from flex.modules.claude_code.manage.noise import INFRA_REPO_PATH_PATTERNS

    print("=" * 60)
    print("Step 4: Repo Project Attribution (SOMA-first)")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()

    # Reset sessions misattributed to infrastructure repos so they get
    # re-evaluated by the full tier stack. These were typically old sessions
    # where .nexus/.claude reads won the SOMA vote over the real project.
    infra_like = ' OR '.join(
        f"git_root LIKE '%{p}%'" for p in INFRA_REPO_PATH_PATTERNS
    )
    reset_count = db.execute(f"""
        UPDATE _raw_sources SET project = NULL, git_root = NULL
        WHERE ({infra_like})
    """).rowcount
    if reset_count:
        db.commit()
        print(f'  Reset {reset_count} infra-attributed sessions for re-attribution')
        sys.stdout.flush()

    updated = run_repo_project(db)
    elapsed = time.time() - t0
    print(f'  {updated} sources attributed in {elapsed:.1f}s\n')
    sys.stdout.flush()


def main():
    t_total = time.time()
    db = open_cell(str(CLAUDE_CODE_DB))
    print(f'Opened: {CLAUDE_CODE_DB}\n')
    sys.stdout.flush()

    rebuild_warmup_types(db)
    reembed_sources(db)
    rebuild_source_graph(db)
    rebuild_file_graph(db)
    rebuild_delegation_graph(db)
    rebuild_fingerprints(db)
    rebuild_repo_project(db)
    rebuild_community_labels(db)

    # Install curated views + presets before regenerating auto-generated views.
    # Ensures schema changes in .sql source files are always applied — whether
    # this script is run directly or via `flex sync --full`.
    from pathlib import Path
    from flex.views import install_views
    from flex.manage.install_presets import install_cell as install_presets_cell

    # User library takes precedence; stock library ships with module
    view_dir = Path.home() / '.flex' / 'views' / 'claude_code'
    if not view_dir.exists():
        view_dir = Path(__file__).resolve().parent.parent / 'stock' / 'views'
    if view_dir.exists():
        print("Installing curated views...")
        install_views(db, view_dir)

    print("Installing presets...")
    install_presets_cell('claude_code')

    print("Regenerating auto-generated views...")
    regenerate_views(db, views={'messages': 'chunk', 'sessions': 'source'})

    # Final stats
    cols = db.execute("PRAGMA table_info(sessions)").fetchall()
    print(f'Sessions view: {len(cols)} columns')
    mcols = db.execute("PRAGMA table_info(messages)").fetchall()
    print(f'Messages view: {len(mcols)} columns')

    db.close()

    elapsed = time.time() - t_total
    print(f'\n{"=" * 60}')
    print(f'All enrichments rebuilt in {elapsed:.1f}s')


if __name__ == '__main__':
    main()
