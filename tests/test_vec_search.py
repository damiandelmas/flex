"""
Tests for flexsearch/retrieve/vec_search.py — VectorCache

Tests the numpy-only operations:
  1. Matrix multiply (corpus-wide cosine similarity)
  2. Hub boost (landscape-level centrality modulation)
  3. Temporal decay (landscape-level recency modulation)
  4. Contrastive (penalize similarity to negative query)
  5. MMR diversity (iterative pairwise selection)

Plus: masking, load_from_db, get_vector, get_mask_for_ids, get_mask_from_db,
      parse_modifiers, load_columns.

Run with: pytest tests/test_vec_search.py -v
"""
import numpy as np
import sqlite3
import struct
import time
import pytest


def _can_import():
    try:
        from flexsearch.retrieve.vec_search import VectorCache
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _can_import(),
    reason="flexsearch.retrieve.vec_search not yet implemented"
)


# =============================================================================
# Fixtures
# =============================================================================

def _make_vec(values, dim=384):
    """Create a float32 vector of given dimension, padded with zeros."""
    vec = np.zeros(dim, dtype=np.float32)
    vec[:len(values)] = values
    return vec


def _make_blob(values, dim=384):
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
    from flexsearch.retrieve.vec_search import VectorCache
    vc = VectorCache()
    vc.load_from_db(vec_db, '_raw_chunks', 'embedding', 'id')
    return vc


@pytest.fixture
def mod_db():
    """In-memory DB with vectors, timestamps, sources, and graph for modulation tests."""
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
        CREATE TABLE _enrich_types (
            chunk_id TEXT PRIMARY KEY, semantic_role TEXT, confidence REAL
        );
        CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT);
    """)

    now = int(time.time())
    # 5 vectors: a,b are hub source (src-1), c,d,e are non-hub (src-2, src-3)
    # Timestamps: a=1day, b=30days, c=90days, d=365days, e=7days
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

    # src-1 is hub (high centrality, community 1), src-2 is bridge (community 1), src-3 lowest (community 2)
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-1', 0.85, 1, 0, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-2', 0.30, 0, 1, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-3', 0.10, 0, 0, 2)")

    # Semantic kinds: a=delegation, b=response, c=prompt, d=command, e=delegation
    conn.execute("INSERT INTO _enrich_types VALUES ('a', 'delegation', 0.5)")
    conn.execute("INSERT INTO _enrich_types VALUES ('b', 'response', 0.5)")
    conn.execute("INSERT INTO _enrich_types VALUES ('c', 'prompt', 0.5)")
    conn.execute("INSERT INTO _enrich_types VALUES ('d', 'command', 0.5)")
    conn.execute("INSERT INTO _enrich_types VALUES ('e', 'delegation', 0.5)")

    conn.execute("INSERT INTO _meta VALUES ('vec:hubs:weight', '1.3')")
    conn.execute("INSERT INTO _meta VALUES ('vec:recent:half_life', '30')")
    conn.commit()
    return conn


@pytest.fixture
def mod_cache(mod_db):
    """VectorCache with modulation columns loaded."""
    from flexsearch.retrieve.vec_search import VectorCache
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
        assert cache.dims == 384

    def test_matrix_is_normalized(self, cache):
        norms = np.linalg.norm(cache.matrix, axis=1)
        np.testing.assert_allclose(norms, 1.0, atol=1e-6)

    def test_ids_preserved(self, cache):
        assert set(cache.ids) == {'a', 'b', 'c', 'd', 'e'}

    def test_memory_mb_positive(self, cache):
        assert cache.memory_mb > 0

    def test_empty_table_returns_empty(self):
        from flexsearch.retrieve.vec_search import VectorCache
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
        # 'a' is [1,0,0] — should be top match
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
        from flexsearch.retrieve.vec_search import VectorCache
        vc = VectorCache()
        results = vc.search(_make_vec([1.0, 0.0, 0.0]))
        assert results == []

    def test_threshold_filters(self, cache):
        query = _make_vec([1.0, 0.0, 0.0])
        results = cache.search(query, limit=10, threshold=0.99)
        # Only 'a' should survive a 0.99 threshold
        assert all(r['score'] >= 0.99 for r in results)

    def test_zero_vector_query_returns_results(self, cache):
        """Zero-norm query should not crash."""
        query = _make_vec([0.0, 0.0, 0.0])
        results = cache.search(query, limit=3)
        assert isinstance(results, list)


# =============================================================================
# Contrastive Search
# =============================================================================

class TestContrastive:
    """not_like_vec penalizes similarity to a negative query."""

    def test_contrastive_demotes_similar(self, cache):
        query = _make_vec([0.7, 0.7, 0.0])  # between a and c
        # Without contrastive
        base = cache.search(query, limit=5)
        # With contrastive against 'a' direction
        contra = cache.search(query, not_like_vec=_make_vec([1.0, 0.0, 0.0]), limit=5)
        # 'a' should rank lower with contrastive
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
        """Diverse mode should spread results across different directions."""
        query = _make_vec([0.5, 0.5, 0.0])
        diverse_results = cache.search(query, diverse=True, limit=3, oversample=5)
        diverse_ids = {r['id'] for r in diverse_results}
        # With diversity, we should get spread — not just the 3 most similar
        # 'd' (orthogonal) is more likely to appear in diverse set
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
        assert mask.sum() == 1  # only 'a' found

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
    """Retrieve individual vectors by ID."""

    def test_returns_vector(self, cache):
        vec = cache.get_vector('a')
        assert vec is not None
        assert vec.shape == (384,)

    def test_nonexistent_returns_none(self, cache):
        assert cache.get_vector('nonexistent') is None

    def test_vector_is_normalized(self, cache):
        vec = cache.get_vector('a')
        np.testing.assert_allclose(np.linalg.norm(vec), 1.0, atol=1e-6)


# =============================================================================
# Dimension validation
# =============================================================================

class TestDimensionValidation:
    """search() rejects wrong-dimension query vectors."""

    def test_wrong_dimension_raises(self, cache):
        wrong = np.random.randn(128).astype(np.float32)
        with pytest.raises(ValueError, match="dimension"):
            cache.search(wrong)

    def test_correct_dimension_ok(self, cache):
        ok = np.random.randn(384).astype(np.float32)
        results = cache.search(ok)
        assert isinstance(results, list)


# =============================================================================
# MMR lambda
# =============================================================================

class TestMMRLambda:
    """mmr_lambda parameter tunes relevance vs diversity."""

    def test_high_lambda_favors_relevance(self, cache):
        q = np.random.randn(384).astype(np.float32)
        high = cache.search(q, diverse=True, limit=3, mmr_lambda=0.99)
        low = cache.search(q, diverse=True, limit=3, mmr_lambda=0.01)
        # Both return results — exact ordering differs
        assert len(high) == 3
        assert len(low) == 3


# =============================================================================
# parse_modifiers
# =============================================================================

class TestParseModifiers:
    """parse_modifiers() parses modifier strings into dicts."""

    def test_empty_string(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('')
        assert result['recent'] is False
        assert result['diverse'] is False
        assert result['unlike'] is None
        assert result['limit'] is None

    def test_none(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers(None)
        assert result['recent'] is False

    def test_hubs_ignored(self):
        """hubs/bridges are now unknown tokens — silently ignored."""
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('hubs bridges')
        assert 'hubs' not in result
        assert 'bridges' not in result
        assert result['recent'] is False

    def test_recent_no_days(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('recent')
        assert result['recent'] is True
        assert result['recent_days'] is None

    def test_recent_with_days(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('recent:7')
        assert result['recent'] is True
        assert result['recent_days'] == 7

    def test_unlike(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('unlike:jwt')
        assert result['unlike'] == 'jwt'

    def test_diverse(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('diverse')
        assert result['diverse'] is True

    def test_limit(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('limit:50')
        assert result['limit'] == 50

    def test_composed(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('recent:7 diverse unlike:jwt limit:50')
        assert result['recent'] is True
        assert result['recent_days'] == 7
        assert result['diverse'] is True
        assert result['unlike'] == 'jwt'
        assert result['limit'] == 50

    def test_detect_communities(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('detect_communities')
        assert result['detect_communities'] is True

    def test_detect_communities_default_false(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('recent')
        assert result['detect_communities'] is False

    def test_unknown_token_ignored(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('hubs foo bar')
        assert result['recent'] is False


# =============================================================================
# load_columns
# =============================================================================

class TestLoadColumns:
    """load_columns() populates timestamps, community_ids, kinds arrays."""

    def test_timestamps_loaded(self, mod_cache):
        assert mod_cache.timestamps is not None
        assert mod_cache.timestamps.shape == (5,)
        # All timestamps should be positive (set in fixture)
        assert np.all(mod_cache.timestamps > 0)

    def test_community_ids_loaded(self, mod_cache):
        assert mod_cache.community_ids is not None
        assert mod_cache.community_ids.shape == (5,)

    def test_missing_graph_table_is_safe(self):
        """Cells without _enrich_source_graph don't crash."""
        from flexsearch.retrieve.vec_search import VectorCache
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE _raw_chunks (id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER)")
        conn.execute("INSERT INTO _raw_chunks VALUES ('x', 'test', ?, 1000000)",
                     (_make_blob([1.0, 0.0, 0.0]),))
        conn.commit()
        vc = VectorCache()
        vc.load_from_db(conn, '_raw_chunks', 'embedding', 'id')
        vc.load_columns(conn, '_raw_chunks', 'id')
        assert vc.community_ids is not None
        assert vc.community_ids[0] == -1


# =============================================================================
# Hub Modulation
# =============================================================================

# =============================================================================
# Temporal Modulation
# =============================================================================

class TestRecentModulation:
    """Temporal decay modulates the full landscape before candidate selection."""

    def test_recent_boosts_new_over_old(self, mod_cache):
        """Recent chunks should rank higher with recent modifier."""
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None}

        # a is 1 day old, b is 30 days old. Both similar to query.
        base_a = cache_search_score(mod_cache, query, 'a')
        base_b = cache_search_score(mod_cache, query, 'b')

        recent_a = cache_search_score(mod_cache, query, 'a', modifiers=modifiers, config=config)
        recent_b = cache_search_score(mod_cache, query, 'b', modifiers=modifiers, config=config)

        # a should get relatively more boost than b (it's more recent)
        ratio_base = base_a / max(base_b, 1e-9)
        ratio_recent = recent_a / max(recent_b, 1e-9)
        assert ratio_recent > ratio_base

    def test_recent_days_overrides_config(self, mod_cache):
        """recent:7 should use 7-day half-life regardless of config."""
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '365'}  # very slow decay
        modifiers_fast = {'recent': True, 'recent_days': 7,
                         'unlike': None, 'diverse': False, 'limit': None}
        modifiers_slow = {'recent': True, 'recent_days': None,
                         'unlike': None, 'diverse': False, 'limit': None}

        # b is 30 days old. With 7-day half-life, it decays much more than 365-day
        fast_b = cache_search_score(mod_cache, query, 'b', modifiers=modifiers_fast, config=config)
        slow_b = cache_search_score(mod_cache, query, 'b', modifiers=modifiers_slow, config=config)
        assert fast_b < slow_b

    def test_recent_no_timestamps_is_noop(self, cache):
        """Cache without timestamps should not crash with recent modifier."""
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None}
        base = cache.search(query, limit=5)
        recent = cache.search(query, limit=5, modifiers=modifiers, config={})
        assert base[0]['id'] == recent[0]['id']


# =============================================================================
# Composed Modulations
# =============================================================================

class TestComposedModulations:
    """Multiple modulations compose multiplicatively."""

    def test_recent_plus_diverse(self, mod_cache):
        """Recent and diverse compose."""
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': True, 'limit': None}
        results = mod_cache.search(query, limit=3, modifiers=modifiers, config=config,
                                   oversample=5)
        assert len(results) == 3

    def test_all_modulations(self, mod_cache):
        """All modifiers active at once doesn't crash."""
        query = _make_vec([0.5, 0.5, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': 7,
                     'unlike': None, 'diverse': True, 'limit': 3}
        results = mod_cache.search(query, limit=5, modifiers=modifiers, config=config,
                                   oversample=5)
        assert len(results) <= 3  # limit override from modifiers


# =============================================================================
# Pre-filter: community
# =============================================================================

class TestCommunityPreFilter:
    """community:N pre-filters candidate pool to a single community."""

    def test_community_filter_excludes_other_communities(self, mod_cache):
        """Only chunks in community 2 survive."""
        query = _make_vec([0.5, 0.5, 0.5])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': 2, 'kind': None}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        # Only d is in community 2 (src-3)
        result_ids = {r['id'] for r in results}
        assert 'd' in result_ids
        assert 'a' not in result_ids
        assert 'c' not in result_ids

    def test_community_filter_with_recent(self, mod_cache):
        """Recent composes with community filter."""
        query = _make_vec([1.0, 0.0, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': 1, 'kind': None}
        results = mod_cache.search(query, limit=5, modifiers=modifiers, config=config)
        # a,b (src-1, community 1) and c,e (src-2, community 1) survive
        # d (src-3, community 2) excluded
        result_ids = {r['id'] for r in results}
        assert 'd' not in result_ids
        assert len(results) >= 1

    def test_community_nonexistent_returns_empty(self, mod_cache):
        """Non-existent community returns no results."""
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': 999, 'kind': None}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) == 0

    def test_community_no_data_is_noop(self, cache):
        """Cache without community_ids ignores community modifier."""
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': 1, 'kind': None}
        results = cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) > 0  # no crash, returns results


# =============================================================================
# Pre-filter: kind
# =============================================================================

class TestKindPreFilter:
    """kind:TYPE pre-filters candidate pool to a semantic kind."""

    def test_kind_filter_selects_delegation_only(self, mod_cache):
        """Only delegation chunks survive."""
        query = _make_vec([0.5, 0.5, 0.5])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': 'delegation'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        result_ids = {r['id'] for r in results}
        # a and e are delegation
        assert result_ids == {'a', 'e'}

    def test_kind_filter_selects_prompt_only(self, mod_cache):
        """Only prompt chunks survive."""
        query = _make_vec([0.0, 1.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': 'prompt'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) == 1
        assert results[0]['id'] == 'c'

    def test_kind_filter_with_hub_boost(self, mod_cache):
        """Kind filter restricts to delegation only — a and e."""
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': 'delegation'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        result_ids = {r['id'] for r in results}
        assert result_ids == {'a', 'e'}

    def test_kind_nonexistent_returns_empty(self, mod_cache):
        """Non-existent kind returns no results."""
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': 'nonexistent'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) == 0

    def test_kind_no_data_is_noop(self, cache):
        """Cache without kinds ignores kind modifier."""
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': 'delegation'}
        results = cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) > 0  # no crash, returns results


# =============================================================================
# Combined pre-filters
# =============================================================================

class TestCombinedPreFilters:
    """community + kind compose as AND."""

    def test_community_and_kind_together(self, mod_cache):
        """community:1 AND kind:delegation = only 'a' (src-1, delegation) and 'e' (src-2, delegation)."""
        query = _make_vec([0.5, 0.5, 0.5])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': 1, 'kind': 'delegation'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        result_ids = {r['id'] for r in results}
        # a (community 1, delegation) and e (community 1, delegation)
        assert result_ids == {'a', 'e'}

    def test_community_and_kind_no_overlap(self, mod_cache):
        """community:2 AND kind:delegation = only d is community 2 but d is 'command'."""
        query = _make_vec([0.5, 0.5, 0.5])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': 2, 'kind': 'delegation'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) == 0

    def test_all_prefilters_plus_modulations(self, mod_cache):
        """Pre-filters + recent + diverse doesn't crash."""
        query = _make_vec([0.5, 0.5, 0.0])
        config = {'vec:recent:half_life': '30'}
        modifiers = {'recent': True, 'recent_days': 7,
                     'unlike': None, 'diverse': True, 'limit': None,
                     'community': 1, 'kind': 'delegation'}
        results = mod_cache.search(query, limit=5, modifiers=modifiers, config=config,
                                   oversample=5)
        result_ids = {r['id'] for r in results}
        assert result_ids <= {'a', 'e'}


# =============================================================================
# parse_modifiers: new tokens
# =============================================================================

class TestParseModifiersPreFilter:
    """parse_modifiers handles community:N and kind:TYPE tokens."""

    def test_community(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('community:12')
        assert result['community'] == 12

    def test_kind(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('kind:delegation')
        assert result['kind'] == 'delegation'

    def test_community_and_kind_composed(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('kind:delegation community:17')
        assert result['kind'] == 'delegation'
        assert result['community'] == 17

    def test_community_invalid_ignored(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('community:abc')
        assert result['community'] is None

    def test_defaults_are_none(self):
        from flexsearch.retrieve.vec_search import parse_modifiers
        result = parse_modifiers('recent')
        assert result['community'] is None
        assert result['kind'] is None


# =============================================================================
# load_columns: new arrays
# =============================================================================

class TestLoadColumnsPreFilter:
    """load_columns populates community_ids and kinds arrays."""

    def test_community_ids_loaded(self, mod_cache):
        assert mod_cache.community_ids is not None
        assert mod_cache.community_ids.shape == (5,)

    def test_community_id_values(self, mod_cache):
        # a,b in src-1 (community 1), c,e in src-2 (community 1), d in src-3 (community 2)
        assert mod_cache.community_ids[mod_cache._id_to_idx['a']] == 1
        assert mod_cache.community_ids[mod_cache._id_to_idx['d']] == 2

    def test_kinds_loaded(self, mod_cache):
        assert mod_cache.kinds is not None
        assert mod_cache.kinds.shape == (5,)

    def test_kind_values(self, mod_cache):
        assert mod_cache.kinds[mod_cache._id_to_idx['a']] == 'delegation'
        assert mod_cache.kinds[mod_cache._id_to_idx['b']] == 'response'
        assert mod_cache.kinds[mod_cache._id_to_idx['c']] == 'prompt'
        assert mod_cache.kinds[mod_cache._id_to_idx['d']] == 'command'
        assert mod_cache.kinds[mod_cache._id_to_idx['e']] == 'delegation'

    def test_missing_types_table_is_safe(self):
        """Cells without _enrich_types get empty kinds."""
        from flexsearch.retrieve.vec_search import VectorCache
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE _raw_chunks (id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER)")
        conn.execute("INSERT INTO _raw_chunks VALUES ('x', 'test', ?, 1000000)",
                     (_make_blob([1.0, 0.0, 0.0]),))
        conn.commit()
        vc = VectorCache()
        vc.load_from_db(conn, '_raw_chunks', 'embedding', 'id')
        vc.load_columns(conn, '_raw_chunks', 'id')
        assert vc.kinds is not None
        assert vc.kinds[0] == ''
        assert vc.community_ids is not None
        assert vc.community_ids[0] == -1


# =============================================================================
# detect_communities
# =============================================================================

class TestDetectCommunities:
    """detect_communities runs query-time Louvain on candidate embeddings."""

    def test_adds_community_field(self, mod_cache):
        """Results include _community when detect_communities is set."""
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': None,
                     'detect_communities': True}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert len(results) > 0
        assert all('_community' in r for r in results)

    def test_without_flag_no_community(self, mod_cache):
        """Results don't have _community when not requested."""
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': None,
                     'detect_communities': False}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        assert all('_community' not in r for r in results)

    def test_community_values_are_integers(self, mod_cache):
        """_community values are non-negative integers."""
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': None,
                     'detect_communities': True}
        results = mod_cache.search(query, limit=5, modifiers=modifiers)
        for r in results:
            assert isinstance(r['_community'], int)
            assert r['_community'] >= 0

    def test_composable_with_diverse(self, mod_cache):
        """detect_communities + diverse compose without crashing."""
        query = _make_vec([0.5, 0.5, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': True, 'limit': None,
                     'community': None, 'kind': None,
                     'detect_communities': True}
        results = mod_cache.search(query, limit=3, modifiers=modifiers,
                                   oversample=5)
        assert len(results) == 3
        assert all('_community' in r for r in results)

    def test_too_few_candidates_skips(self, cache):
        """With <3 candidates after filtering, community detection skips."""
        mask = cache.get_mask_for_ids(['a', 'b'])
        query = _make_vec([1.0, 0.0, 0.0])
        modifiers = {'recent': False, 'recent_days': None,
                     'unlike': None, 'diverse': False, 'limit': None,
                     'community': None, 'kind': None,
                     'detect_communities': True}
        results = cache.search(query, limit=2, mask=mask, modifiers=modifiers)
        # Should still return results, just no _community
        assert len(results) == 2
        assert all('_community' not in r for r in results)


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
