"""
TDD Tests for flexsearch/presets.py — Plan 1

Tests PresetLoader: read .sql files, parse annotations, interpolate params, execute.

Actual API (adapted from spec):
  PresetLoader(preset_dir)
  .execute(db, name, params={}) -> list[dict] | list[{query, results}]
  ._parse(text, name) -> dict

Run with: pytest tests/test_presets.py -v
"""
import sqlite3
import pytest
from pathlib import Path


def _can_import():
    try:
        from flexsearch.retrieve.presets import PresetLoader
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _can_import(),
    reason="flexsearch.presets not yet implemented (Plan 1)"
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def preset_dir(tmp_path):
    """Create a temp directory with sample .sql preset files."""
    d = tmp_path / "presets"
    d.mkdir()

    # Simple single-query preset
    (d / "hub-sources.sql").write_text("""\
-- @param: min_centrality
SELECT source_id, centrality
FROM _enrich_source_graph
WHERE centrality >= :min_centrality
ORDER BY centrality DESC
""")

    # Multi-query preset
    (d / "overview.sql").write_text("""\
-- @multi: true

-- @query: counts
SELECT COUNT(*) as n FROM _raw_chunks;

-- @query: sources
SELECT source_id, doc_type FROM _raw_sources ORDER BY file_date DESC;
""")

    # No-param preset
    (d / "all-chunks.sql").write_text("""\
SELECT id, content, timestamp FROM _raw_chunks ORDER BY timestamp
""")

    return d


@pytest.fixture
def loader(preset_dir):
    from flexsearch.retrieve.presets import PresetLoader
    return PresetLoader(preset_dir)


# =============================================================================
# Parsing
# =============================================================================

class TestParsing:
    """Annotation parsing from .sql files."""

    def test_parse_single_query(self, loader):
        preset = loader.load('hub-sources')
        assert len(preset['queries']) == 1
        assert not preset['multi']

    def test_parse_multi_query(self, loader):
        preset = loader.load('overview')
        assert preset['multi'] is True
        assert len(preset['queries']) == 2

    def test_parse_query_names(self, loader):
        preset = loader.load('overview')
        names = [q['name'] for q in preset['queries']]
        assert 'counts' in names
        assert 'sources' in names

    def test_list_presets(self, loader):
        names = loader.list_presets()
        assert 'hub-sources' in names
        assert 'overview' in names
        assert 'all-chunks' in names


# =============================================================================
# Execution
# =============================================================================

class TestExecution:
    """Execute presets against a live cell."""

    def test_single_query_returns_list(self, loader, qmem_cell):
        results = loader.execute(qmem_cell, 'all-chunks')
        assert isinstance(results, list)
        assert len(results) == 9  # qmem_cell has 9 chunks

    def test_param_interpolation(self, loader, qmem_cell):
        results = loader.execute(qmem_cell, 'hub-sources', {'min_centrality': 0.5})
        assert isinstance(results, list)
        # Only src-arch has centrality >= 0.5 (0.85)
        assert len(results) >= 1
        assert results[0]['source_id'] == 'src-arch'

    def test_multi_query_returns_list_of_query_results(self, loader, qmem_cell):
        results = loader.execute(qmem_cell, 'overview')
        assert isinstance(results, list)
        # Each entry has 'query' name and 'results' list
        names = [r['query'] for r in results]
        assert 'counts' in names
        assert 'sources' in names
        counts_entry = next(r for r in results if r['query'] == 'counts')
        assert isinstance(counts_entry['results'], list)

    def test_missing_preset_raises(self, loader, qmem_cell):
        with pytest.raises(FileNotFoundError):
            loader.execute(qmem_cell, 'nonexistent-preset')

    def test_result_is_dicts(self, loader, qmem_cell):
        results = loader.execute(qmem_cell, 'all-chunks')
        assert isinstance(results[0], dict)
        assert 'id' in results[0]
        assert 'content' in results[0]
