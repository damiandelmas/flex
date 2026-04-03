"""File Co-Edit Graph Enrichment — thin runner.

Imports graph logic from flex.modules.claude_code.manage.file_graph.
Only applicable to claude-code cells with _edges_file_identity.

Output: _enrich_file_graph table (source_id PK -> auto-JOINs sessions view)
"""

import sys
import time
from collections import defaultdict
from pathlib import Path

FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flex.core import open_cell, log_op
from flex.views import regenerate_views
from flex.modules.claude_code.manage.file_graph import (
    CREATE_TABLE, build_file_graph, analyze_file_graph,
)

from flex.registry import resolve_cell
CLAUDE_CODE_DB = resolve_cell('claude_code')


def main():
    print("=" * 60)
    print("File Co-Edit Graph Enrichment")
    print("=" * 60)

    t_start = time.time()
    db = open_cell(str(CLAUDE_CODE_DB))
    print(f"\nOpened: {CLAUDE_CODE_DB}")

    # Build graph
    print("\nBuilding file co-edit graph...")
    G, session_files = build_file_graph(db)

    if G.number_of_nodes() == 0:
        print("No file identity data. Skipping.")
        db.close()
        return

    # Analyze
    print("\nRunning Louvain + PageRank...")
    partition, pr, hubs = analyze_file_graph(G)

    communities = defaultdict(int)
    for comm_id in partition.values():
        communities[comm_id] += 1
    print(f"  {len(communities)} communities, {len(hubs)} hubs")

    # Persist
    print("\nPersisting to _enrich_file_graph...")
    db.execute("DROP TABLE IF EXISTS _enrich_file_graph")
    db.execute(CREATE_TABLE)

    for node in G.nodes():
        db.execute(
            "INSERT INTO _enrich_file_graph VALUES (?,?,?,?,?)",
            (
                node,
                partition.get(node),
                pr.get(node, 0.0),
                1 if node in hubs else 0,
                len(session_files.get(node, set())),
            )
        )

    db.commit()
    row_count = G.number_of_nodes()
    log_op(db, 'build_file_graph', '_enrich_file_graph',
           params={'nodes': row_count, 'communities': len(communities),
                   'hubs': len(hubs)},
           rows_affected=row_count, source='enrich_file_graph.py')
    print(f"  Persisted {row_count} rows")

    # Regenerate views
    print("\nRegenerating views...")
    regenerate_views(db, views={'messages': 'chunk', 'sessions': 'source'})

    # Verify
    print("\nVerification:")
    total = db.execute("SELECT COUNT(*) FROM _enrich_file_graph").fetchone()[0]
    hub_count = db.execute("SELECT COUNT(*) FROM _enrich_file_graph WHERE file_is_hub = 1").fetchone()[0]
    print(f"  Total rows: {total}")
    print(f"  Hubs: {hub_count}")

    # Community distribution
    print("\n  Top communities:")
    comms = db.execute("""
        SELECT file_community_id, COUNT(*) as cnt
        FROM _enrich_file_graph
        GROUP BY file_community_id
        ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    for c in comms:
        print(f"    community {c['file_community_id']}: {c['cnt']} sessions")

    # Top hubs
    print("\n  Top file hubs:")
    top_hubs = db.execute("""
        SELECT f.source_id, f.file_centrality, f.shared_file_count, f.file_community_id
        FROM _enrich_file_graph f
        WHERE f.file_is_hub = 1
        ORDER BY f.file_centrality DESC
        LIMIT 10
    """).fetchall()
    for h in top_hubs:
        print(f"    {h['source_id'][:12]}  cent={h['file_centrality']:.4f}  files={h['shared_file_count']}  comm={h['file_community_id']}")

    # Check sessions view
    cols = db.execute("PRAGMA table_info(sessions)").fetchall()
    col_names = [c[1] for c in cols]
    new_cols = [c for c in col_names if c.startswith('file_') or c == 'shared_file_count']
    print(f"\n  Sessions view new columns: {new_cols}")

    db.close()
    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == '__main__':
    main()
