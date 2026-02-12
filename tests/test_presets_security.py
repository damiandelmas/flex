"""
Security and edge case tests for PresetLoader.

Covers: SQL injection via _interpolate, empty presets, caching, error handling.

Run with: pytest tests/test_presets_security.py -v
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
    reason="flexsearch.retrieve.presets not yet implemented"
)


@pytest.fixture
def secure_db():
    """DB with a table to test injection against."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE _raw_chunks (id TEXT PRIMARY KEY, content TEXT)")
    conn.execute("INSERT INTO _raw_chunks VALUES ('c1', 'hello world')")
    conn.execute("INSERT INTO _raw_chunks VALUES ('c2', 'foo bar')")
    conn.commit()
    return conn


@pytest.fixture
def injection_preset(tmp_path):
    """Preset with a :param placeholder susceptible to injection."""
    d = tmp_path / "presets"
    d.mkdir()
    (d / "search.sql").write_text("""\
SELECT id, content FROM _raw_chunks WHERE content LIKE :term
""")
    return d


class TestSQLInjection:
    """Verify _interpolate escapes dangerous param values."""

    def test_single_quote_escaped(self, injection_preset, secure_db):
        from flexsearch.retrieve.presets import PresetLoader
        loader = PresetLoader(injection_preset)
        # This should NOT cause an error — quotes should be escaped
        results = loader.execute(secure_db, 'search', {'term': "it's"})
        assert isinstance(results, list)

    def test_injection_attempt_does_not_destroy_data(self, injection_preset, secure_db):
        from flexsearch.retrieve.presets import PresetLoader
        loader = PresetLoader(injection_preset)
        # Classic injection attempt
        try:
            loader.execute(secure_db, 'search',
                           {'term': "'; DROP TABLE _raw_chunks; --"})
        except sqlite3.OperationalError:
            pass  # Expected — the SQL is malformed after injection
        # Table must still exist with data
        count = secure_db.execute(
            "SELECT COUNT(*) FROM _raw_chunks"
        ).fetchone()[0]
        assert count == 2, "Data should survive injection attempt"


class TestPresetEdgeCases:
    """Edge cases: empty files, caching, missing dir."""

    def test_empty_sql_file(self, tmp_path, secure_db):
        from flexsearch.retrieve.presets import PresetLoader
        d = tmp_path / "presets"
        d.mkdir()
        (d / "empty.sql").write_text("")
        loader = PresetLoader(d)
        preset = loader.load('empty')
        assert preset['queries'] == []

    def test_cache_returns_same_object(self, injection_preset):
        from flexsearch.retrieve.presets import PresetLoader
        loader = PresetLoader(injection_preset)
        p1 = loader.load('search')
        p2 = loader.load('search')
        assert p1 is p2  # same cached object

    def test_list_presets_missing_dir(self, tmp_path):
        from flexsearch.retrieve.presets import PresetLoader
        loader = PresetLoader(tmp_path / "nonexistent")
        assert loader.list_presets() == []

    def test_multi_query_error_captured(self, tmp_path, secure_db):
        from flexsearch.retrieve.presets import PresetLoader
        d = tmp_path / "presets"
        d.mkdir()
        (d / "bad-multi.sql").write_text("""\
-- @multi: true
-- @query: good
SELECT COUNT(*) as n FROM _raw_chunks;
-- @query: bad
SELECT * FROM nonexistent_table;
""")
        loader = PresetLoader(d)
        results = loader.execute(secure_db, 'bad-multi')
        good = next(r for r in results if r['query'] == 'good')
        bad = next(r for r in results if r['query'] == 'bad')
        assert 'results' in good
        assert 'error' in bad
