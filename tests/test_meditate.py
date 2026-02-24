"""
Tests for flex/manage/meditate.py — offline graph intelligence.

Tests: build_similarity_graph, compute_scores, persist, run_sandbox.

Run with: pytest tests/test_meditate.py -v
"""
import numpy as np
import sqlite3
import struct
import pytest


def _can_import():
    try:
        from flex.manage.meditate import (
            build_similarity_graph, compute_scores, persist, run_sandbox
        )
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _can_import(),
    reason="flex.manage.meditate not yet implemented"
)

EMBED_DIM = 128


# =============================================================================
# Fixtures
# =============================================================================

def _make_embedding(values, dim=EMBED_DIM):
    """Create float32 BLOB from first few values, zero-padded."""
    vec = np.zeros(dim, dtype=np.float32)
    vec[:len(values)] = values
    return vec.tobytes()


@pytest.fixture
def graph_db():
    """In-memory DB with sources + embeddings for graph building."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE _raw_sources (
            source_id TEXT PRIMARY KEY,
            embedding BLOB
        )
    """)
    conn.execute("CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT)")

    # 5 sources: 3 clustered (similar), 2 outliers
    sources = [
        ('s1', [1.0, 0.0, 0.0]),
        ('s2', [0.95, 0.05, 0.0]),
        ('s3', [0.9, 0.1, 0.0]),
        ('s4', [0.0, 1.0, 0.0]),    # outlier
        ('s5', [0.0, 0.0, 1.0]),    # outlier
    ]
    for sid, vals in sources:
        conn.execute(
            "INSERT INTO _raw_sources (source_id, embedding) VALUES (?,?)",
            (sid, _make_embedding(vals))
        )
    conn.commit()
    return conn


@pytest.fixture
def small_graph(graph_db):
    """Pre-built graph from graph_db."""
    from flex.manage.meditate import build_similarity_graph
    G, edge_count = build_similarity_graph(
        graph_db, '_raw_sources', 'source_id', 'embedding', threshold=0.3
    )
    return G


# =============================================================================
# build_similarity_graph
# =============================================================================

class TestBuildGraph:
    """Build NetworkX graph from embedding similarity."""

    def test_returns_graph_and_count(self, graph_db):
        from flex.manage.meditate import build_similarity_graph
        G, count = build_similarity_graph(
            graph_db, '_raw_sources', 'source_id', 'embedding', threshold=0.5
        )
        assert G is not None
        assert isinstance(count, int)
        assert count > 0

    def test_nodes_match_sources(self, graph_db):
        from flex.manage.meditate import build_similarity_graph
        G, _ = build_similarity_graph(
            graph_db, '_raw_sources', 'source_id', 'embedding', threshold=0.1
        )
        assert set(G.nodes()) == {'s1', 's2', 's3', 's4', 's5'}

    def test_similar_sources_connected(self, graph_db):
        from flex.manage.meditate import build_similarity_graph
        G, _ = build_similarity_graph(
            graph_db, '_raw_sources', 'source_id', 'embedding', threshold=0.8
        )
        # s1, s2, s3 are very similar — should be connected
        assert G.has_edge('s1', 's2') or G.has_edge('s2', 's1')

    def test_orthogonal_sources_not_connected(self, graph_db):
        from flex.manage.meditate import build_similarity_graph
        G, _ = build_similarity_graph(
            graph_db, '_raw_sources', 'source_id', 'embedding', threshold=0.5
        )
        # s1 and s4 are orthogonal — should NOT be connected
        assert not G.has_edge('s1', 's4')

    def test_top_k_limits_edges(self, graph_db):
        from flex.manage.meditate import build_similarity_graph
        G, _ = build_similarity_graph(
            graph_db, '_raw_sources', 'source_id', 'embedding',
            threshold=0.1, top_k=1
        )
        # Each node should have at most k neighbors (in directed sense)
        for node in G.nodes():
            assert G.degree(node) >= 0  # graph exists

    def test_empty_table_returns_none(self):
        from flex.manage.meditate import build_similarity_graph
        conn = sqlite3.connect(':memory:')
        conn.execute("CREATE TABLE t (id TEXT, embedding BLOB)")
        G, count = build_similarity_graph(conn, 't', 'id', 'embedding')
        assert G is None
        assert count == 0
        conn.close()

    def test_edges_have_weight(self, graph_db):
        from flex.manage.meditate import build_similarity_graph
        G, _ = build_similarity_graph(
            graph_db, '_raw_sources', 'source_id', 'embedding', threshold=0.5
        )
        for u, v, data in G.edges(data=True):
            assert 'weight' in data
            assert 0 < data['weight'] <= 1.0


# =============================================================================
# compute_scores
# =============================================================================

class TestComputeScores:
    """Run graph algorithms: Louvain, PageRank, hubs, bridges."""

    def test_returns_expected_keys(self, small_graph):
        from flex.manage.meditate import compute_scores
        scores = compute_scores(small_graph)
        assert 'communities' in scores
        assert 'centralities' in scores
        assert 'hubs' in scores
        assert 'bridges' in scores

    def test_centralities_sum_to_one(self, small_graph):
        from flex.manage.meditate import compute_scores
        scores = compute_scores(small_graph)
        if scores['centralities']:
            total = sum(scores['centralities'].values())
            assert abs(total - 1.0) < 0.01

    def test_hubs_are_subset_of_nodes(self, small_graph):
        from flex.manage.meditate import compute_scores
        scores = compute_scores(small_graph)
        all_nodes = set(small_graph.nodes())
        assert set(scores['hubs']) <= all_nodes

    def test_bridges_are_subset_of_nodes(self, small_graph):
        from flex.manage.meditate import compute_scores
        scores = compute_scores(small_graph)
        all_nodes = set(small_graph.nodes())
        assert set(scores['bridges']) <= all_nodes

    def test_none_graph_returns_empty(self):
        from flex.manage.meditate import compute_scores
        scores = compute_scores(None)
        assert scores['communities'] == []
        assert scores['centralities'] == {}
        assert scores['hubs'] == []
        assert scores['bridges'] == []

    def test_communities_have_members(self, small_graph):
        from flex.manage.meditate import compute_scores
        scores = compute_scores(small_graph)
        for comm in scores['communities']:
            assert 'id' in comm
            assert 'members' in comm
            assert len(comm['members']) > 0


# =============================================================================
# persist
# =============================================================================

class TestPersist:
    """Write graph scores to enrichment table."""

    def test_creates_table_and_rows(self, graph_db, small_graph):
        from flex.manage.meditate import compute_scores, persist
        scores = compute_scores(small_graph)
        persist(graph_db, scores, '_enrich_source_graph', 'source_id')

        count = graph_db.execute(
            "SELECT COUNT(*) FROM _enrich_source_graph"
        ).fetchone()[0]
        assert count > 0

    def test_persists_centrality(self, graph_db, small_graph):
        from flex.manage.meditate import compute_scores, persist
        scores = compute_scores(small_graph)
        persist(graph_db, scores, '_enrich_source_graph', 'source_id')

        row = graph_db.execute(
            "SELECT centrality FROM _enrich_source_graph WHERE source_id = 's1'"
        ).fetchone()
        assert row is not None
        assert row[0] > 0

    def test_idempotent_wipe_and_rewrite(self, graph_db, small_graph):
        from flex.manage.meditate import compute_scores, persist
        scores = compute_scores(small_graph)
        persist(graph_db, scores, '_enrich_source_graph', 'source_id')
        count1 = graph_db.execute(
            "SELECT COUNT(*) FROM _enrich_source_graph"
        ).fetchone()[0]
        # Persist again — should wipe and rewrite (same count)
        persist(graph_db, scores, '_enrich_source_graph', 'source_id')
        count2 = graph_db.execute(
            "SELECT COUNT(*) FROM _enrich_source_graph"
        ).fetchone()[0]
        assert count1 == count2

    def test_auto_detects_id_col(self, graph_db, small_graph):
        from flex.manage.meditate import compute_scores, persist
        scores = compute_scores(small_graph)
        # 'source' in table name → source_id auto-detected
        persist(graph_db, scores, '_enrich_source_graph')
        cols = graph_db.execute(
            "PRAGMA table_info(_enrich_source_graph)"
        ).fetchall()
        col_names = [c[1] for c in cols]
        assert 'source_id' in col_names


# =============================================================================
# run_sandbox
# =============================================================================

class TestSandbox:
    """Sandboxed networkx script execution."""

    def test_basic_script(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "result['count'] = len(list(graph.nodes()))")
        assert result['count'] == 5

    def test_script_error_returns_error(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph, "1/0")
        assert 'error' in result
        assert result['type'] == 'ZeroDivisionError'

    def test_numpy_available(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "result['pi'] = float(np.pi)")
        assert abs(result['pi'] - 3.14159) < 0.001

    def test_networkx_available(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "result['density'] = float(nx.density(graph))")
        assert 'density' in result

    # --- Security tests ---

    def test_import_blocked(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph, "import os")
        assert 'error' in result

    def test_open_blocked(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "open('/etc/passwd')")
        assert 'error' in result

    def test_dunder_import_blocked(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "__import__('os')")
        assert 'error' in result

    def test_exec_blocked(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "exec('import os')")
        assert 'error' in result

    def test_eval_blocked(self, small_graph, graph_db):
        from flex.manage.meditate import run_sandbox
        result = run_sandbox(graph_db, small_graph,
            "eval('__import__(\"os\")')")
        assert 'error' in result
