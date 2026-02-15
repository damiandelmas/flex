"""Delegation Graph Enrichment — thin runner.

Imports graph logic from flexsearch.modules.claude_code.manage.delegation_graph.
Only applicable to claude-code cells with _edges_delegations.

Output: _enrich_delegation_graph table (source_id PK -> auto-JOINs sessions view)
"""

import sys
import time
from pathlib import Path

FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flexsearch.core import open_cell
from flexsearch.views import regenerate_views
from flexsearch.modules.claude_code.manage.delegation_graph import (
    CREATE_TABLE, build_delegation_graph, compute_delegation_metrics,
)

CELLS_ROOT = Path.home() / '.qmem' / 'cells' / 'projects'
THREAD_DB = CELLS_ROOT / 'thread' / 'main.db'


def main():
    print("=" * 60)
    print("Delegation Graph Enrichment")
    print("=" * 60)

    t_start = time.time()
    db = open_cell(str(THREAD_DB))
    print(f"\nOpened: {THREAD_DB}")

    # Build graph
    print("\nBuilding delegation graph...")
    G = build_delegation_graph(db)

    if G.number_of_nodes() == 0:
        print("No delegation data. Skipping.")
        db.close()
        return

    # Compute metrics
    print("\nComputing metrics...")
    metrics = compute_delegation_metrics(G)

    orchestrators = sum(1 for m in metrics.values() if m['is_orchestrator'])
    max_depth = max(m['delegation_depth'] for m in metrics.values())
    max_spawns = max(m['agents_spawned'] for m in metrics.values())
    print(f"  {len(metrics)} sessions in delegation graph")
    print(f"  {orchestrators} orchestrators (>5 spawns)")
    print(f"  Max depth: {max_depth}, max spawns: {max_spawns}")

    # Persist
    print("\nPersisting to _enrich_delegation_graph...")
    db.execute("DROP TABLE IF EXISTS _enrich_delegation_graph")
    db.execute(CREATE_TABLE)

    for node, m in metrics.items():
        db.execute(
            "INSERT INTO _enrich_delegation_graph VALUES (?,?,?,?,?)",
            (
                node,
                m['agents_spawned'],
                m['is_orchestrator'],
                m['delegation_depth'],
                m['parent_session'],
            )
        )

    db.commit()
    print(f"  Persisted {len(metrics)} rows")

    # Regenerate views
    print("\nRegenerating views...")
    regenerate_views(db)

    # Verify
    print("\nVerification:")
    total = db.execute("SELECT COUNT(*) FROM _enrich_delegation_graph").fetchone()[0]
    orch = db.execute("SELECT COUNT(*) FROM _enrich_delegation_graph WHERE is_orchestrator = 1").fetchone()[0]
    with_parent = db.execute("SELECT COUNT(*) FROM _enrich_delegation_graph WHERE parent_session IS NOT NULL").fetchone()[0]
    print(f"  Total rows: {total}")
    print(f"  Orchestrators: {orch}")
    print(f"  With parent: {with_parent}")

    # Top orchestrators
    print("\n  Top orchestrators:")
    top = db.execute("""
        SELECT source_id, agents_spawned, delegation_depth
        FROM _enrich_delegation_graph
        WHERE is_orchestrator = 1
        ORDER BY agents_spawned DESC
        LIMIT 10
    """).fetchall()
    for t in top:
        print(f"    {t['source_id'][:12]}  spawned={t['agents_spawned']}  depth={t['delegation_depth']}")

    # Depth distribution
    print("\n  Depth distribution:")
    depths = db.execute("""
        SELECT delegation_depth, COUNT(*) as cnt
        FROM _enrich_delegation_graph
        GROUP BY delegation_depth
        ORDER BY delegation_depth
    """).fetchall()
    for d in depths:
        print(f"    depth {d['delegation_depth']}: {d['cnt']} sessions")

    # Check sessions view
    cols = db.execute("PRAGMA table_info(sessions)").fetchall()
    col_names = [c[1] for c in cols]
    new_cols = [c for c in col_names if c in ('agents_spawned', 'is_orchestrator', 'delegation_depth', 'parent_session')]
    print(f"\n  Sessions view new columns: {new_cols}")

    db.close()
    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == '__main__':
    main()
