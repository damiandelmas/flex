#!/usr/bin/env python
"""Rebuild all enrichments on thread cell with noise filters.

Runs: source_graph -> session_summary -> file_graph + delegation_graph
Uses module configs from flexsearch.modules.claude_code.manage.*
"""

import sys
import time
from pathlib import Path

FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flexsearch.core import open_cell
from flexsearch.views import regenerate_views
from flexsearch.manage.meditate import build_similarity_graph, compute_scores, persist
from flexsearch.modules.claude_code.manage.noise import graph_filter_sql

from flexsearch.registry import CELLS_ROOT, resolve_cell
THREAD_DB = resolve_cell('thread') or (CELLS_ROOT / 'thread' / 'main.db')

# Thread corpus: median pairwise similarity is 0.61 at threshold 0.5 -> 78% density.
# 0.65 gives 39% density — meaningful topology without near-complete graph.
GRAPH_THRESHOLD = 0.65


def rebuild_source_graph(db):
    """Rebuild source graph with noise filter + tuned threshold."""
    print("=" * 60)
    print("Step 1: Source Graph (with noise filter)")
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
                                       threshold=GRAPH_THRESHOLD, where=where)
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
    print(f'Persisted in {t3-t2:.1f}s')

    new_cnt = db.execute('SELECT COUNT(*) FROM _enrich_source_graph').fetchone()[0]
    new_hubs = db.execute('SELECT COUNT(*) FROM _enrich_source_graph WHERE is_hub = 1').fetchone()[0]
    comms = db.execute('SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph WHERE community_id IS NOT NULL').fetchone()[0]
    print(f'\nAFTER: {new_cnt} rows, {new_hubs} hubs, {comms} communities')
    print(f'Source graph total: {t3-t0:.1f}s\n')
    sys.stdout.flush()


def rebuild_file_graph(db):
    """Rebuild file co-edit graph."""
    from flexsearch.modules.claude_code.manage.file_graph import (
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
    t1 = time.time()
    print(f'Done in {t1-t0:.1f}s — {G.number_of_nodes()} rows\n')
    sys.stdout.flush()


def rebuild_delegation_graph(db):
    """Rebuild delegation graph."""
    from flexsearch.modules.claude_code.manage.delegation_graph import (
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
    t1 = time.time()
    print(f'Done in {t1-t0:.1f}s — {len(metrics)} rows\n')
    sys.stdout.flush()


def main():
    t_total = time.time()
    db = open_cell(str(THREAD_DB))
    print(f'Opened: {THREAD_DB}\n')
    sys.stdout.flush()

    rebuild_source_graph(db)
    rebuild_file_graph(db)
    rebuild_delegation_graph(db)

    print("Regenerating views...")
    regenerate_views(db)

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
