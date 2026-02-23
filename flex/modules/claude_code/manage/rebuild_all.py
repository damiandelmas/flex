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
    """
    print("=" * 60)
    print("Step 0.5: Selective Source Pooling (prompt + response only)")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()

    # Backup current embeddings
    db.execute("DROP TABLE IF EXISTS _backup_source_embeddings_meanpool")
    db.execute("""
        CREATE TABLE _backup_source_embeddings_meanpool AS
        SELECT source_id, embedding FROM _raw_sources
        WHERE embedding IS NOT NULL
    """)
    backup_cnt = db.execute(
        "SELECT COUNT(*) FROM _backup_source_embeddings_meanpool"
    ).fetchone()[0]
    print(f"  Backed up {backup_cnt} source embeddings")
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

    print("=" * 60)
    print("Step 4: Repo Project Attribution (SOMA-first)")
    print("=" * 60)
    sys.stdout.flush()

    t0 = time.time()
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

    # Install curated views + presets before regenerating auto-generated views.
    # Ensures schema changes in .sql source files are always applied — whether
    # this script is run directly or via `flex sync --full`.
    from pathlib import Path
    from flex.views import install_views
    from flex.utils.install_presets import install_cell as install_presets_cell

    view_dir = Path(__file__).resolve().parent.parent.parent / 'views' / 'claude_code'
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
