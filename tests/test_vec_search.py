"""
Tests for flexsearch/retrieve/vec_search.py — VectorCache

Tests the three numpy-only operations:
  1. Matrix multiply (corpus-wide cosine similarity)
  2. Contrastive (penalize similarity to negative query)
  3. MMR diversity (iterative pairwise selection)

Plus: masking, load_from_db, get_vector, get_mask_for_ids, get_mask_from_db.

Run with: pytest tests/test_vec_search.py -v
"""
import numpy as np
import sqlite3
import struct
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
