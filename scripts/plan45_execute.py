"""Plan 4.5: Foundation Cleanup — One-shot execution script.

Steps:
  A. Backfill 427 missing source embeddings (thread)
  B. Fix message_count on ~425 sessions (thread)
  C. Rebuild thread graph with noise filter
  D. Drop flat tables + VACUUM (thread, claude)
  E. Reinstall presets on all 5 cells
"""

import sys
import time
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
FLEX_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flexsearch.core import open_cell
from flexsearch.views import regenerate_views
from flexsearch.manage.meditate import build_similarity_graph, compute_scores, persist
from flexsearch.retrieve.presets import install_presets

from flexsearch.registry import CELLS_ROOT
PRESET_GENERAL = FLEX_ROOT / 'flexsearch' / 'retrieve' / 'presets' / 'general'
PRESET_THREAD = FLEX_ROOT / 'flexsearch' / 'retrieve' / 'presets' / 'thread'

CELL_PRESET_MAP = {
    'thread':          [PRESET_GENERAL, PRESET_THREAD],
    'claude':          [PRESET_GENERAL, PRESET_THREAD],
    'qmem':            [PRESET_GENERAL],
    'inventory':       [PRESET_GENERAL],
    'thread-codebase': [PRESET_GENERAL],
}

NOISE_WHERE = """
    source_id IN (
        SELECT source_id FROM _edges_source
        GROUP BY source_id HAVING COUNT(*) >= 20
    ) AND source_id NOT IN (
        SELECT source_id FROM _raw_sources WHERE title = 'Warmup'
    )
"""

THREAD_FLAT_TABLES = [
    'chunks', 'docs', 'docs_fts',
    '_qmem_meta', '_qmem_indexed_files', '_qmem_clusters',
    '_qmem_concepts', '_qmem_timeline',
]

CLAUDE_FLAT_TABLES = [
    '_flat_messages', '_flat_conversations',
]


def table_exists(db, name):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def step_a_backfill_embeddings(db):
    """Backfill missing source embeddings via mean-pooling."""
    missing = db.execute("""
        SELECT src.source_id FROM _raw_sources src
        WHERE src.embedding IS NULL
        AND EXISTS (SELECT 1 FROM _edges_source e
                    JOIN _raw_chunks c ON e.chunk_id = c.id
                    WHERE e.source_id = src.source_id AND c.embedding IS NOT NULL)
    """).fetchall()
    print(f"  A. Found {len(missing)} sources with missing embeddings")
    if not missing:
        return

    filled = 0
    for (sid,) in missing:
        chunks = db.execute("""
            SELECT c.embedding FROM _raw_chunks c
            JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.source_id = ? AND c.embedding IS NOT NULL
        """, (sid,)).fetchall()
        if chunks:
            vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunks]
            mean = np.mean(vecs, axis=0)
            norm = np.linalg.norm(mean)
            if norm > 1e-10:
                mean = mean / norm
            db.execute("UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                       (mean.tobytes(), sid))
            filled += 1
    db.commit()
    print(f"     Filled {filled} embeddings")


def step_b_fix_message_count(db):
    """Backfill message_count from actual chunk count."""
    before = db.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE message_count = 0 OR message_count IS NULL"
    ).fetchone()[0]
    print(f"  B. Found {before} sources with zero/null message_count")
    if before == 0:
        return

    db.execute("""
        UPDATE _raw_sources SET message_count = (
            SELECT COUNT(*) FROM _edges_source e
            WHERE e.source_id = _raw_sources.source_id
        ) WHERE message_count = 0 OR message_count IS NULL
    """)
    db.commit()
    after = db.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE message_count = 0 OR message_count IS NULL"
    ).fetchone()[0]
    print(f"     Fixed. Remaining zero/null: {after}")


def step_c_rebuild_graph(db):
    """Rebuild thread graph with noise filter."""
    print("  C. Rebuilding thread graph with noise filter...")
    t0 = time.time()
    G, edges = build_similarity_graph(
        db, table='_raw_sources', id_col='source_id',
        threshold=0.5, where=NOISE_WHERE
    )
    scores = compute_scores(G)
    persist(db, scores)
    elapsed = time.time() - t0

    hub_count = db.execute(
        "SELECT COUNT(*) FROM _enrich_source_graph WHERE is_hub = 1"
    ).fetchone()[0]
    comm_count = db.execute(
        "SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph"
    ).fetchone()[0]
    node_count = db.execute(
        "SELECT COUNT(*) FROM _enrich_source_graph"
    ).fetchone()[0]
    print(f"     Done in {elapsed:.1f}s: {node_count} nodes, {hub_count} hubs, {comm_count} communities")


def step_d_drop_flat_tables(db, flat_tables, cell_name):
    """Drop legacy flat tables and VACUUM."""
    dropped = []
    for tbl in flat_tables:
        if table_exists(db, tbl):
            db.execute(f"DROP TABLE [{tbl}]")
            dropped.append(tbl)
    if dropped:
        db.commit()
        db.execute("VACUUM")
        print(f"  D. [{cell_name}] Dropped {len(dropped)} flat tables: {', '.join(dropped)}")
    else:
        print(f"  D. [{cell_name}] No flat tables to drop")


def step_e_reinstall_presets(cell_name, db, preset_dirs):
    """Reinstall presets from source .sql files."""
    total = 0
    for d in preset_dirs:
        if d.exists():
            install_presets(db, d)
            total += len(list(d.glob('*.sql')))
    print(f"  E. [{cell_name}] Installed {total} presets")


def verify(db, cell_name):
    """Run verification checks."""
    print(f"\n  Verification [{cell_name}]:")
    null_emb = db.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE embedding IS NULL"
    ).fetchone()[0]
    print(f"    NULL embeddings: {null_emb}")

    null_mc = db.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE message_count = 0 OR message_count IS NULL"
    ).fetchone()[0]
    print(f"    Zero/null message_count: {null_mc}")

    # Hub quality
    hubs = db.execute("""
        SELECT src.title,
               ROUND(g.centrality, 4) as cent,
               (SELECT COUNT(*) FROM _edges_source e WHERE e.source_id = src.source_id) as chunks
        FROM _enrich_source_graph g
        JOIN _raw_sources src ON g.source_id = src.source_id
        WHERE g.is_hub = 1
        ORDER BY g.centrality DESC LIMIT 5
    """).fetchall()
    print(f"    Top hubs:")
    for h in hubs:
        print(f"      {h[1]}  {h[2]:>5} chunks  {h[0][:60] if h[0] else '(untitled)'}")

    comm = db.execute(
        "SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph"
    ).fetchone()[0]
    print(f"    Communities: {comm}")

    flat = db.execute(
        "SELECT name FROM sqlite_master WHERE name IN ('chunks','docs')"
    ).fetchall()
    print(f"    Flat tables remaining: {[r[0] for r in flat] if flat else 'none'}")


def main():
    print("=" * 60)
    print("Plan 4.5: Foundation Cleanup")
    print("=" * 60)

    # --- Thread cell (steps A-D) ---
    thread_path = CELLS_ROOT / 'thread' / 'main.db'
    print(f"\n--- Thread cell: {thread_path} ---")
    db = open_cell(str(thread_path))

    step_a_backfill_embeddings(db)
    step_b_fix_message_count(db)
    step_c_rebuild_graph(db)
    step_d_drop_flat_tables(db, THREAD_FLAT_TABLES, 'thread')
    step_e_reinstall_presets('thread', db, CELL_PRESET_MAP['thread'])
    verify(db, 'thread')
    db.close()

    # --- Claude cell (step D only) ---
    claude_path = CELLS_ROOT / 'claude' / 'main.db'
    print(f"\n--- Claude cell: {claude_path} ---")
    db = open_cell(str(claude_path))
    step_d_drop_flat_tables(db, CLAUDE_FLAT_TABLES, 'claude')
    step_e_reinstall_presets('claude', db, CELL_PRESET_MAP['claude'])
    db.close()

    # --- Doc-pac cells (step E only) ---
    for name in ['qmem', 'inventory', 'thread-codebase']:
        cell_path = CELLS_ROOT / name / 'main.db'
        print(f"\n--- {name} cell: {cell_path} ---")
        db = open_cell(str(cell_path))
        step_e_reinstall_presets(name, db, CELL_PRESET_MAP[name])
        db.close()

    print("\n" + "=" * 60)
    print("Done. Restart flexsearch-mcp to pick up changes.")
    print("=" * 60)


if __name__ == '__main__':
    main()
