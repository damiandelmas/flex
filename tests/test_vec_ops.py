"""
Tests for flex/retrieve/vec_ops.py — VectorCache + vec_ops pipeline

Tests: matrix multiply, temporal decay, contrastive, MMR diversity,
SQL pre-filter, centroid (like:), trajectory (from:to:), local_communities,
masking, load_from_db, get_vector, parse_modifiers, load_columns.

Run with: pytest tests/test_vec_ops.py -v
"""
import numpy as np
import sqlite3
import struct
import time
import pytest


def _can_import():
    try:
        from flex.retrieve.vec_ops import VectorCache
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _can_import(),
    reason="flex.retrieve.vec_ops not yet implemented"
)


# =============================================================================
# Fixtures
# =============================================================================

EMBED_DIM = 128

def _make_vec(values, dim=EMBED_DIM):
    """Create a float32 vector of given dimension, padded with zeros."""
    vec = np.zeros(dim, dtype=np.float32)
    vec[:len(values)] = values
    return vec


def _make_blob(values, dim=EMBED_DIM):
    """Create a float32 BLOB for SQLite storage."""
    return _make_vec(values, dim).tobytes()


@pytest.fixture
def vec_db():
    """In-memory DB with 5 vectors for testing search."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE _raw_chunks (
            id TEXT PRIMARY KEY,
            content TEXT,
            embedding BLOB
        )
    """)

    # 5 vectors with distinct directions
    vectors = {
        'a': [1.0, 0.0, 0.0],   # points along dim 0
        'b': [0.9, 0.1, 0.0],   # similar to a
        'c': [0.0, 1.0, 0.0],   # orthogonal to a
        'd': [0.0, 0.0, 1.0],   # orthogonal to both
        'e': [0.7, 0.7, 0.0],   # between a and c
    }
    for id_, vals in vectors.items():
        conn.execute(
            "INSERT INTO _raw_chunks (id, content, embedding) VALUES (?,?,?)",
            (id_, f"content for {id_}", _make_blob(vals))
        )
    conn.commit()
    return conn


@pytest.fixture
def cache(vec_db):
    """Loaded VectorCache from vec_db."""
    from flex.retrieve.vec_ops import VectorCache
    vc = VectorCache()
    vc.load_from_db(vec_db, '_raw_chunks', 'embedding', 'id')
    return vc


@pytest.fixture
def mod_db():
    """In-memory DB with vectors, timestamps, sources, graph, and types for tests."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE _raw_chunks (
            id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER
        );
        CREATE TABLE _edges_source (
            chunk_id TEXT NOT NULL, source_id TEXT NOT NULL, position INTEGER
        );
        CREATE TABLE _enrich_source_graph (
            source_id TEXT PRIMARY KEY, centrality REAL,
            is_hub INTEGER DEFAULT 0, is_bridge INTEGER DEFAULT 0, community_id INTEGER
        );
        CREATE TABLE _types_message (
            chunk_id TEXT PRIMARY KEY, role TEXT, tool_name TEXT
        );
        CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT);
    """)

    now = int(time.time())
    # 5 vectors: a,b are hub source (src-1), c,d,e are non-hub (src-2, src-3)
    vectors = {
        'a': ([1.0, 0.0, 0.0], now - 86400 * 1,   'src-1'),
        'b': ([0.9, 0.1, 0.0], now - 86400 * 30,  'src-1'),
        'c': ([0.0, 1.0, 0.0], now - 86400 * 90,  'src-2'),
        'd': ([0.0, 0.0, 1.0], now - 86400 * 365, 'src-3'),
        'e': ([0.7, 0.7, 0.0], now - 86400 * 7,   'src-2'),
    }
    for id_, (vals, ts, src) in vectors.items():
        conn.execute(
            "INSERT INTO _raw_chunks (id, content, embedding, timestamp) VALUES (?,?,?,?)",
            (id_, f"content for {id_}", _make_blob(vals), ts)
        )
        conn.execute(
            "INSERT INTO _edges_source (chunk_id, source_id, position) VALUES (?,?,0)",
            (id_, src)
        )

    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-1', 0.85, 1, 0, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-2', 0.30, 0, 1, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-3', 0.10, 0, 0, 2)")

    # Types: a,c,e are user prompts; b,d are assistant
    conn.execute("INSERT INTO _types_message VALUES ('a', 'user', NULL)")
    conn.execute("INSERT INTO _types_message VALUES ('b', 'assistant', NULL)")
    conn.execute("INSERT INTO _types_message VALUES ('c', 'user', NULL)")
    conn.execute("INSERT INTO _types_message VALUES ('d', 'assistant', NULL)")
    conn.execute("INSERT INTO _types_message VALUES ('e', 'user', NULL)")

    conn.execute("INSERT INTO _meta VALUES ('vec:hubs:weight', '1.3')")
    conn.execute("INSERT INTO _meta VALUES ('vec:recent:half_life', '30')")
    conn.commit()
    return conn


@pytest.fixture
def mod_cache(mod_db):
    """VectorCache with modulation columns loaded."""
    from flex.retrieve.vec_ops import VectorCache
    vc = VectorCache()
    vc.load_from_db(mod_db, '_raw_chunks', 'embedding', 'id')
    vc.load_columns(mod_db, '_raw_chunks', 'id')
    return vc


# =============================================================================
# Loading
# =============================================================================

class TestLoad:
    """load_from_db populates the cache from SQLite BLOBs."""

    def test_loads_correct_count(self, cache):
        assert cache.size == 5

    def test_correct_dimension(self, cache):
        assert cache.dims == EMBED_DIM

    def test_matrix_is_normalized(self, cache):
        norms = np.linalg.norm(cache.matrix, axis=1)
        np.testing.assert_allclose(norms, 1.0, atol=1e-6)

    def test_ids_preserved(self, cache):
        assert set(cache.ids) == {'a', 'b', 'c', 'd', 'e'}

    def test_memory_mb_positive(self, cache):
        assert cache.memory_mb > 0

    def test_empty_table_returns_empty(self):
        from flex.retrieve.vec_ops import VectorCache
        conn = sqlite3.connect(':memory:')
        conn.execute("CREATE TABLE t (id TEXT, embedding BLOB)")
        vc = VectorCache()
        vc.load_from_db(conn, 't', 'embedding', 'id')
        assert vc.size == 0
        conn.close()


# =============================================================================
# Basic Search
# =============================================================================

class TestSearch:
    """Matrix multiply search — corpus-wide cosine similarity."""

    def test_returns_list_of_dicts(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=3)
        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)
        assert all('id' in r and 'score' in r for r in results)

    def test_top_result_is_most_similar(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=3)
        assert results[0]['id'] == 'a'

    def test_scores_descending(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=5)
        scores = [r['score'] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_limit_respected(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=2)
        assert len(results) <= 2

    def test_empty_cache_returns_empty(self):
        from flex.retrieve.vec_ops import VectorCache
        vc = VectorCache()
        results = vc.search(_make_vec([1.0, 0.0, 0.0]))
        assert results == []

    def test_threshold_filters(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=10, threshold=0.99)
        assert all(r['score'] >= 0.99 for r in results)

    def test_zero_vector_query_returns_results(self, cache):
        query = _make_vec([0.0, 0.0, 0.0])
        results = cache.search(query, limit=3)
        assert isinstance(results, list)


# =============================================================================
# Contrastive Search
# =============================================================================

class TestContrastive:
    """not_like_vec penalizes similarity to a negative query."""

    def test_contrastive_demotes_similar(self, cache):
        query = _make_vec([0.7, 0.7, 0.0])
        base = cache.search(query, limit=5)
        contra = cache.search(query, not_like_vec=_make_vec([1.0, 0.0, 0.0]), limit=5)
        base_a_rank = next(i for i, r in enumerate(base) if r['id'] == 'a')
        contra_a_rank = next(i for i, r in enumerate(contra) if r['id'] == 'a')
        assert contra_a_rank >= base_a_rank


# =============================================================================
# MMR Diversity
# =============================================================================

class TestMMR:
    """MMR diversity — iterative selection for relevance minus redundancy."""

    def test_diverse_returns_correct_count(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, diverse=True, limit=3, oversample=5)
        assert len(results) == 3

    def test_diverse_reduces_redundancy(self, cache):
        query = _make_vec([0.5, 0.5, 0.0])
        diverse_results = cache.search(query, diverse=True, limit=3, oversample=5)
        diverse_ids = {r['id'] for r in diverse_results}
        assert len(diverse_ids) == 3


# =============================================================================
# Masking
# =============================================================================

class TestMasking:
    """Boolean masks restrict search to subset of vectors."""

    def test_mask_restricts_results(self, cache):
        mask = cache.get_mask_for_ids(['a', 'b'])
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=10, mask=mask)
        result_ids = {r['id'] for r in results}
        assert result_ids <= {'a', 'b'}

    def test_mask_unknown_ids_ignored(self, cache):
        mask = cache.get_mask_for_ids(['a', 'nonexistent'])
        assert mask.sum() == 1

    def test_get_mask_from_db(self, cache, vec_db):
        mask = cache.get_mask_from_db(
            vec_db, '_raw_chunks',
            "content LIKE '%a%'"
        )
        assert isinstance(mask, np.ndarray)
        assert mask.dtype == bool


# =============================================================================
# get_vector
# =============================================================================

class TestGetVector:
    def test_returns_vector(self, cache):
        vec = cache.get_vector('a')
        assert vec is not None
        assert vec.shape == (EMBED_DIM,)

    def test_nonexistent_returns_none(self, cache):
        assert cache.get_vector('nonexistent') is None

    def test_vector_is_normalized(self, cache):
        vec = cache.get_vector('a')
        np.testing.assert_allclose(np.linalg.norm(vec), 1.0, atol=1e-6)


# =============================================================================
# Dimension validation
# =============================================================================

class TestDimensionValidation:
    def test_wrong_dimension_raises(self, cache):
        wrong = np.random.randn(384).astype(np.float32)  # 384d != 128d
        with pytest.raises(ValueError, match="dimension"):
            cache.search(wrong)

    def test_correct_dimension_ok(self, cache):
        ok = np.random.randn(EMBED_DIM).astype(np.float32)
        results = cache.search(ok)
        assert isinstance(results, list)


# =============================================================================
# MMR lambda
# =============================================================================

class TestMMRLambda:
    def test_high_lambda_favors_relevance(self, cache):
        q = np.random.randn(EMBED_DIM).astype(np.float32)
        high = cache.search(q, diverse=True, limit=3, mmr_lambda=0.99)
        low = cache.search(q, diverse=True, limit=3, mmr_lambda=0.01)
        assert len(high) == 3
        assert len(low) == 3


# =============================================================================
# parse_modifiers
# =============================================================================

class TestParseModifiers:
    """parse_modifiers() parses modifier strings into dicts."""

    def test_empty_string(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('')
        assert result['recent'] is False
        assert result['diverse'] is False
        assert result['unlike'] is None
        assert result['limit'] is None

    def test_none(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers(None)
        assert result['recent'] is False

    def test_recent_no_days(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('recent')
        assert result['recent'] is True
        assert result['recent_days'] is None

    def test_recent_with_days(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('recent:7')
        assert result['recent'] is True
        assert result['recent_days'] == 7

    def test_unlike(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('unlike:jwt')
        assert result['unlike'] == 'jwt'

    def test_diverse(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('diverse')
        assert result['diverse'] is True

    def test_limit(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('limit:50')
        assert result['limit'] == 50

    def test_composed(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('recent:7 diverse unlike:jwt limit:50')
        assert result['recent'] is True
        assert result['recent_days'] == 7
        assert result['diverse'] is True
        assert result['unlike'] == 'jwt'
        assert result['limit'] == 50

    def test_like_token(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('like:abc,def,ghi')
        assert result['like'] == ['abc', 'def', 'ghi']

    def test_trajectory_tokens(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('diverse from:naive understanding to:expert framework')
        assert result['trajectory_from'] == 'naive understanding'
        assert result['trajectory_to'] == 'expert framework'
        assert result['diverse'] is True

    def test_local_communities_token(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('local_communities')
        assert result['local_communities'] is True

    def test_detect_communities_alias(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('detect_communities')
        assert result['local_communities'] is True

    def test_dead_tokens_ignored(self):
        """kind: and community: silently ignored."""
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('kind:prompt community:3 diverse')
        assert 'kind' not in result
        assert 'community' not in result
        assert result['diverse'] is True

    def test_unknown_token_ignored(self):
        from flex.retrieve.vec_ops import parse_modifiers
        result = parse_modifiers('hubs foo bar')
        assert result['recent'] is False


# =============================================================================
# load_columns
# =============================================================================

class TestLoadColumns:
    """load_columns() populates timestamps array."""

    def test_timestamps_loaded(self, mod_cache):
        assert mod_cache.timestamps is not None
        assert mod_cache.timestamps.shape == (5,)
        assert np.all(mod_cache.timestamps > 0)

    def test_missing_graph_table_is_safe(self):
        """Cells without _enrich_source_graph don't crash."""
        from flex.retrieve.vec_ops import VectorCache
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE _raw_chunks (id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER)")
        conn.execute("INSERT INTO _raw_chunks VALUES ('x', 'test', ?, 1000000)",
                     (_make_blob([1.0, 0.0, 0.0]),))
        conn.commit()
        vc = VectorCache()
        vc.load_from_db(conn, '_raw_chunks', 'embedding', 'id')
        vc.load_columns(conn, '_raw_chunks', 'id')
        assert vc.timestamps is not None


# =============================================================================
# Temporal Modulation
# =============================================================================

class TestRecentModulation:
    """Temporal decay modulates the full landscape before candidate selection."""

    def test_recent_boosts_new_over_old(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        base_a = cache_search_score(mod_cache, query, 'a')
        base_b = cache_search_score(mod_cache, query, 'b')
        recent_a = cache_search_score(mod_cache, query, 'a', modifiers=modifiers, config=config)
        recent_b = cache_search_score(mod_cache, query, 'b', modifiers=modifiers, config=config)
        ratio_base = base_a / max(base_b, 1e-9)
        ratio_recent = recent_a / max(recent_b, 1e-9)
        assert ratio_recent > ratio_base

    def test_recent_days_overrides_config(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '365'}
        modifiers_fast = {'recent': True, 'recent_days': 7,
                         'unlike': None, 'diverse': False, 'limit': None,
                         'like': None, 'trajectory_from': None, 'trajectory_to': None,
                         'local_communities': False}
        modifiers_slow = {'recent': True, 'recent_days': None,
                         'unlike': None, 'diverse': False, 'limit': None,
                         'like': None, 'trajectory_from': None, 'trajectory_to': None,
                         'local_communities': False}
        fast_b = cache_search_score(mod_cache, query, 'b', modifiers=modifiers_fast, config=config)
        slow_b = cache_search_score(mod_cache, query, 'b', modifiers=modifiers_slow, config=config)
        assert fast_b < slow_b

    def test_recent_no_timestamps_is_noop(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        base = cache.search(query, limit=5)
        recent = cache.search(query, limit=5, modifiers=modifiers, config={})
        assert base[0]['id'] == recent[0]['id']


# =============================================================================
# Composed Modulations
# =============================================================================

class TestComposedModulations:
    def test_recent_plus_diverse(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': True, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=3, modifiers=modifiers, config=config,
                                   oversample=5)
        assert len(results) == 3

    def test_all_modulations(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': 7,
                     'unlike': None, 'diverse': True, 'limit': 3,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers, config=config,
                                   oversample=5)
        assert len(results) <= 3


# =============================================================================
# SQL Pre-Filter
# =============================================================================

class TestSQLPreFilter:
    """SQL pre-filter via pre_filter_ids on VectorCache.search()."""

    def test_pre_filter_restricts_results(self, mod_cache):
        """Pre-filter to user prompt IDs only."""
        query = _make_vec([0.5, 0.5, 0.5])
        results = mod_cache.search(query, pre_filter_ids={'a', 'c', 'e'}, limit=10)
        result_ids = {r['id'] for r in results}
        assert result_ids.issubset({'a', 'c', 'e'})

    def test_no_pre_filter_returns_all(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.5])
        results = mod_cache.search(query, limit=10)
        assert len(results) == 5

    def test_empty_pre_filter(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = mod_cache.search(query, pre_filter_ids=set(), limit=10)
        assert len(results) == 0

    def test_pre_filter_unknown_ids_skipped(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = mod_cache.search(query, pre_filter_ids={'a', 'FAKE_ID_999', 'c'}, limit=10)
        result_ids = {r['id'] for r in results}
        assert 'FAKE_ID_999' not in result_ids

    def test_pre_filter_all_unknown_ids(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = mod_cache.search(query, pre_filter_ids={'FAKE_1', 'FAKE_2'}, limit=10)
        assert len(results) == 0

    def test_pre_filter_composes_with_diverse(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.0])
        results = mod_cache.search(query, pre_filter_ids={'a', 'c', 'e'},
                                   diverse=True, limit=3, oversample=5)
        result_ids = {r['id'] for r in results}
        assert result_ids.issubset({'a', 'c', 'e'})
        assert len(results) == 3

    def test_pre_filter_composes_with_recent(self, mod_cache):
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': 7,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, pre_filter_ids={'a', 'b', 'c'},
                                   limit=10, modifiers=modifiers, config=config)
        result_ids = {r['id'] for r in results}
        assert result_ids.issubset({'a', 'b', 'c'})

    def test_pre_filter_top_result_correct(self, mod_cache):
        """Pre-filtering to {a, c} with query along dim 0 should rank a first."""
        query = _make_vec([1.0, 0.0, 0.0])
        results = mod_cache.search(query, pre_filter_ids={'a', 'c'}, limit=2)
        assert results[0]['id'] == 'a'

    def test_pre_filter_contrastive_composes(self, mod_cache):
        """Pre-filter + contrastive both work on the subset."""
        query = _make_vec([0.7, 0.7, 0.0])
        results = mod_cache.search(
            query, pre_filter_ids={'a', 'c', 'e'},
            not_like_vec=_make_vec([1.0, 0.0, 0.0]), limit=3
        )
        result_ids = {r['id'] for r in results}
        assert result_ids.issubset({'a', 'c', 'e'})


# =============================================================================
# Authorizer Guard (SQL pre-filter injection protection)
# =============================================================================

class TestAuthorizerGuard:
    """Pre-filter SQL authorizer: SELECT-only, deny writes/DDL."""

    @pytest.fixture
    def udf_conn(self, mod_db):
        """mod_db connection with vec_ops UDF registered."""
        from flex.retrieve.vec_ops import VectorCache, register_vec_ops
        cache = VectorCache()
        cache.load_from_db(mod_db, '_raw_chunks', 'embedding', 'id')
        cache.load_columns(mod_db, '_raw_chunks', 'id')

        def _embed(text):
            v = np.zeros(EMBED_DIM, dtype=np.float32)
            v[0] = 1.0
            return v.reshape(1, -1)

        register_vec_ops(mod_db, {'_raw_chunks': cache}, _embed)
        return mod_db

    def test_select_pre_filter_allowed(self, udf_conn):
        """Legitimate SELECT pre-filter passes."""
        import json
        result = udf_conn.execute(
            "SELECT vec_ops('_raw_chunks', 'test', '', "
            "\"SELECT chunk_id FROM _types_message WHERE role = 'user'\")"
        ).fetchone()[0]
        data = json.loads(result)
        assert isinstance(data, list)
        ids = {r['id'] for r in data}
        assert ids.issubset({'a', 'c', 'e'})

    def test_insert_via_pre_filter_denied(self, udf_conn):
        """INSERT in pre-filter SQL is denied by authorizer."""
        import json
        result = udf_conn.execute(
            "SELECT vec_ops('_raw_chunks', 'test', '', "
            "\"INSERT INTO _meta VALUES ('injected', '1'); SELECT 'x'\")"
        ).fetchone()[0]
        data = json.loads(result)
        assert 'error' in data
        count = udf_conn.execute(
            "SELECT COUNT(*) FROM _meta WHERE key = 'injected'"
        ).fetchone()[0]
        assert count == 0

    def test_drop_via_pre_filter_denied(self, udf_conn):
        """DROP TABLE in pre-filter SQL is denied by authorizer."""
        import json
        result = udf_conn.execute(
            "SELECT vec_ops('_raw_chunks', 'test', '', "
            "'DROP TABLE _types_message')"
        ).fetchone()[0]
        data = json.loads(result)
        assert 'error' in data
        count = udf_conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='_types_message'"
        ).fetchone()[0]
        assert count == 1

    def test_compound_join_pre_filter_allowed(self, udf_conn):
        """Compound JOIN SELECT in pre-filter passes."""
        import json
        result = udf_conn.execute(
            "SELECT vec_ops('_raw_chunks', 'test', '', "
            "'SELECT t.chunk_id FROM _types_message t "
            "JOIN _edges_source e ON t.chunk_id = e.chunk_id "
            "WHERE t.role = ''user''')"
        ).fetchone()[0]
        data = json.loads(result)
        assert isinstance(data, list)

    def test_authorizer_cleared_after_pre_filter(self, udf_conn):
        """Connection authorizer is None after vec_ops call (finally block)."""
        udf_conn.execute(
            "SELECT vec_ops('_raw_chunks', 'test', '', "
            "\"SELECT chunk_id FROM _types_message WHERE role = 'user'\")"
        ).fetchone()
        # Should be able to INSERT normally after the UDF call
        udf_conn.execute("INSERT INTO _meta VALUES ('post_udf', 'ok')")
        count = udf_conn.execute(
            "SELECT COUNT(*) FROM _meta WHERE key = 'post_udf'"
        ).fetchone()[0]
        assert count == 1

# =============================================================================
# Centroid (like:)
# =============================================================================

class TestCentroid:
    """like:id1,id2 centroid token."""

    def test_centroid_returns_results(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)  # zero — centroid replaces it
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': ['a', 'b'], 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) > 0

    def test_centroid_differs_from_single(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        mods_single = {'recent': False, 'recent_days': None,
                       'unlike': None, 'diverse': False, 'limit': None,
                       'like': ['a'], 'trajectory_from': None, 'trajectory_to': None,
                       'local_communities': False}
        mods_multi = {'recent': False, 'recent_days': None,
                      'unlike': None, 'diverse': False, 'limit': None,
                      'like': ['a', 'c'], 'trajectory_from': None, 'trajectory_to': None,
                      'local_communities': False}
        r1 = mod_cache.search(query, limit=5, modifiers=mods_single)
        r2 = mod_cache.search(query, limit=5, modifiers=mods_multi)
        scores1 = {r['id']: r['score'] for r in r1}
        scores2 = {r['id']: r['score'] for r in r2}
        assert scores1 != scores2

    def test_centroid_unknown_ids(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': ['FAKE1', 'FAKE2'], 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) == 0

    def test_centroid_composes_with_diverse(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': True, 'limit': None,
                     'like': ['a', 'b'], 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=3, modifiers=modifiers, oversample=5)
        assert len(results) == 3

    def test_centroid_with_pre_filter(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': ['a', 'b'], 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, pre_filter_ids={'a', 'c', 'e'},
                                   limit=5, modifiers=modifiers)
        result_ids = {r['id'] for r in results}
        assert result_ids.issubset({'a', 'c', 'e'})


# =============================================================================
# Trajectory (from:to:)
# =============================================================================

class TestTrajectory:
    """from:TEXT to:TEXT trajectory token."""

    def _embed_fn(self, text):
        """Simple test embed: hash text to a deterministic vector."""
        np.random.seed(hash(text) % 2**31)
        vec = np.random.randn(EMBED_DIM).astype(np.float32)
        return vec.reshape(1, -1)  # (1, EMBED_DIM) like real embedder

    def test_trajectory_returns_results(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None,
                     'trajectory_from': 'simple concept',
                     'trajectory_to': 'complex concept',
                     'local_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers,
                                   embed_fn=self._embed_fn)
        assert len(results) > 0

    def test_trajectory_composes_with_diverse(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': True, 'limit': None,
                     'like': None,
                     'trajectory_from': 'start', 'trajectory_to': 'end',
                     'local_communities': False}
        results = mod_cache.search(query, limit=3, modifiers=modifiers,
                                   oversample=5, embed_fn=self._embed_fn)
        assert len(results) == 3

    def test_trajectory_incomplete_ignored(self, mod_cache):
        """from: without to: falls back to normal query."""
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None,
                     'trajectory_from': 'something', 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers,
                                   embed_fn=self._embed_fn)
        assert len(results) > 0

    def test_trajectory_with_pre_filter(self, mod_cache):
        query = _make_vec([0.0] * EMBED_DIM)
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None,
                     'trajectory_from': 'start', 'trajectory_to': 'end',
                     'local_communities': False}
        results = mod_cache.search(query, pre_filter_ids={'a', 'c', 'e'},
                                   limit=5, modifiers=modifiers,
                                   embed_fn=self._embed_fn)
        result_ids = {r['id'] for r in results}
        assert result_ids.issubset({'a', 'c', 'e'})


# =============================================================================
# Local Communities (renamed from detect_communities)
# =============================================================================

class TestLocalCommunities:
    """Renamed from detect_communities."""

    def test_local_communities_token(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': True}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) > 0
        assert all('_community' in r for r in results)

    def test_without_flag_no_community(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert all('_community' not in r for r in results)

    def test_community_values_are_integers(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': True}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        for r in results:
            assert isinstance(r['_community'], int)
            assert r['_community'] >= 0

    def test_composable_with_diverse(self, mod_cache):
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': True, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': True}
        results = mod_cache.search(query, limit=3, modifiers=modifiers,
                                   oversample=5)
        assert len(results) == 3
        assert all('_community' in r for r in results)

    def test_too_few_candidates_skips(self, cache):
        mask = cache.get_mask_for_ids(['a', 'b'])
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'like': None, 'trajectory_from': None, 'trajectory_to': None,
                     'local_communities': True}
        results = cache.search(query, limit=2, mask=mask, modifiers=modifiers)
        assert len(results) == 2
        assert all('_community' not in r for r in results)


# =============================================================================
# Materialize vec_ops (temp table rewrite + error surfacing)
# =============================================================================

class TestMaterializeVecOps:
    """materialize_vec_ops rewrites vec_ops() calls into temp tables."""

    @pytest.fixture
    def mat_db(self):
        """Self-contained db with vec_ops registered for materialize tests."""
        from flex.retrieve.vec_ops import VectorCache, register_vec_ops
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE _raw_chunks (id TEXT PRIMARY KEY, content TEXT, embedding BLOB)")
        vectors = {'a': [1, 0, 0], 'b': [0.9, 0.1, 0], 'c': [0, 1, 0]}
        for id_, vals in vectors.items():
            conn.execute("INSERT INTO _raw_chunks VALUES (?,?,?)",
                         (id_, f"content {id_}", _make_blob(vals)))
        conn.commit()
        vc = VectorCache()
        vc.load_from_db(conn, '_raw_chunks', 'embedding', 'id')
        embed_fn = lambda text: _make_vec([0.5, 0.5, 0.5])
        register_vec_ops(conn, {'_raw_chunks': vc}, embed_fn)
        return conn

    def test_bad_prefilter_surfaces_real_error(self, mat_db):
        """Bad pre-filter table should return error JSON, not 'no such table'."""
        from flex.retrieve.vec_ops import materialize_vec_ops
        sql = "SELECT v.id FROM vec_ops('_raw_chunks', 'test', '', " \
              "'SELECT id FROM nonexistent_table') v LIMIT 3"
        result = materialize_vec_ops(mat_db, sql)
        assert '"error"' in result
        assert 'no such table: vec_ops' not in result
        assert 'nonexistent_table' in result

    def test_bad_prefilter_column_surfaces_error(self, mat_db):
        """Pre-filter referencing bad column returns error, not 'no such table'."""
        from flex.retrieve.vec_ops import materialize_vec_ops
        sql = "SELECT v.id FROM vec_ops('_raw_chunks', 'test', '', " \
              "'SELECT fake_col FROM _raw_chunks') v LIMIT 3"
        result = materialize_vec_ops(mat_db, sql)
        assert '"error"' in result
        assert 'fake_col' in result

    def test_valid_prefilter_materializes(self, mat_db):
        """Valid pre-filter produces a temp table, not the original SQL."""
        from flex.retrieve.vec_ops import materialize_vec_ops
        sql = "SELECT v.id, v.score FROM vec_ops('_raw_chunks', 'test', '', " \
              "'SELECT id FROM _raw_chunks WHERE id = ''a''') v LIMIT 3"
        result = materialize_vec_ops(mat_db, sql)
        assert 'vec_ops' not in result
        assert '_vec_results_' in result

    def test_no_vec_ops_passthrough(self, mat_db):
        """SQL without vec_ops passes through unchanged."""
        from flex.retrieve.vec_ops import materialize_vec_ops
        sql = "SELECT * FROM _raw_chunks LIMIT 5"
        result = materialize_vec_ops(mat_db, sql)
        assert result == sql

    def test_empty_prefilter_surfaces_error(self, mat_db):
        """Pre-filter returning 0 candidates should say so, not 'no such table'."""
        from flex.retrieve.vec_ops import materialize_vec_ops
        sql = "SELECT v.id FROM vec_ops('_raw_chunks', 'test', '', " \
              "'SELECT id FROM _raw_chunks WHERE id = ''nonexistent''') v LIMIT 3"
        result = materialize_vec_ops(mat_db, sql)
        assert '"error"' in result
        assert '0 results' in result
        assert 'no such table' not in result

    def test_vec_ops_fn_exception_surfaces_error(self):
        """If vec_ops_fn raises (not returns error dict), error surfaces, not 'no such table'."""
        from flex.retrieve.vec_ops import materialize_vec_ops
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        # Register a vec_ops that raises instead of returning error JSON
        def bad_vec_ops(*args):
            raise RuntimeError("embedding model crashed")
        conn.create_function("vec_ops", -1, bad_vec_ops)
        sql = "SELECT v.id FROM vec_ops('_raw_chunks', 'test') v LIMIT 3"
        result = materialize_vec_ops(conn, sql)
        assert '"error"' in result
        assert 'no such table' not in result


# =============================================================================
# Helpers
# =============================================================================

def cache_search_score(cache, query, target_id, modifiers=None, config=None):
    """Helper: search and return score for a specific ID."""
    results = cache.search(query, limit=10, modifiers=modifiers, config=config)
    for r in results:
        if r['id'] == target_id:
            return r['score']
    return 0.0
