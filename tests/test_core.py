"""
TDD Tests for flexsearch core — Plan 1 + Plan 7

Tests:
  - open_cell(path) -> sqlite3.Connection
  - run_sql(db, sql, params) -> list[dict]
  - regenerate_views(db, views) -> None (raw passthrough, no renames)
  - install_views(db, view_dir) -> None (curated views)

Run with: pytest tests/test_core.py -v
"""
import sqlite3
import pytest
from pathlib import Path

def _can_import_flexsearch():
    try:
        from flexsearch.core import open_cell, run_sql, regenerate_views
        return True
    except ImportError:
        return False


# These imports will fail until Plan 1 creates the module
pytestmark = pytest.mark.skipif(
    not _can_import_flexsearch(),
    reason="flexsearch.core not yet implemented (Plan 1)"
)


# =============================================================================
# open_cell
# =============================================================================

class TestOpenCell:
    """open_cell(path) loads a .db, registers UDFs, returns connection."""

    def test_returns_connection(self, tmp_path):
        from flexsearch.core import open_cell
        db_path = tmp_path / "test.db"
        db_path.touch()
        conn = open_cell(str(db_path))
        assert isinstance(conn, sqlite3.Connection)
        conn.close()

    def test_row_factory_is_row(self, tmp_path):
        from flexsearch.core import open_cell
        db_path = tmp_path / "test.db"
        db_path.touch()
        conn = open_cell(str(db_path))
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_set_meta_creates_table(self, tmp_path):
        """set_meta() creates _meta on demand if missing."""
        from flexsearch.core import open_cell, set_meta, get_meta
        db_path = tmp_path / "test.db"
        bare = sqlite3.connect(str(db_path))
        bare.close()
        conn = open_cell(str(db_path))
        set_meta(conn, 'description', 'Test cell')
        assert get_meta(conn, 'description') == 'Test cell'
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_meta'"
        ).fetchall()
        assert len(tables) == 1
        conn.close()


# =============================================================================
# run_sql
# =============================================================================

class TestRunSQL:
    """run_sql(db, sql, params) executes SQL and returns list[dict]."""

    def test_returns_list_of_dicts(self, qmem_cell):
        from flexsearch.core import run_sql
        results = run_sql(qmem_cell, "SELECT * FROM _raw_chunks LIMIT 2")
        assert isinstance(results, list)
        assert len(results) == 2
        assert isinstance(results[0], dict)

    def test_dict_has_column_names(self, qmem_cell):
        from flexsearch.core import run_sql
        results = run_sql(qmem_cell, "SELECT id, content FROM _raw_chunks LIMIT 1")
        assert 'id' in results[0]
        assert 'content' in results[0]

    def test_params_interpolation(self, qmem_cell):
        from flexsearch.core import run_sql
        results = run_sql(
            qmem_cell,
            "SELECT * FROM _raw_sources WHERE doc_type = :dtype",
            {'dtype': 'architecture'}
        )
        assert len(results) == 1
        assert results[0]['source_id'] == 'src-arch'

    def test_empty_result(self, qmem_cell):
        from flexsearch.core import run_sql
        results = run_sql(
            qmem_cell,
            "SELECT * FROM _raw_chunks WHERE id = 'nonexistent'"
        )
        assert results == []

    def test_count_query(self, qmem_cell):
        from flexsearch.core import run_sql
        results = run_sql(qmem_cell, "SELECT COUNT(*) as n FROM _raw_chunks")
        assert results[0]['n'] == 9


# =============================================================================
# regenerate_views — raw passthrough (Plan 7)
# =============================================================================

class TestRegenerateViews:
    """regenerate_views(db, views) discovers tables, emits raw CREATE VIEW."""

    def test_creates_views_with_explicit_param(self, qmem_cell):
        from flexsearch.views import regenerate_views
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        views = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        names = {v[0] for v in views}
        assert 'sections' in names
        assert 'documents' in names

    def test_raw_column_names_no_renames(self, claude_code_cell):
        """Auto-generated views have raw column names — no renames."""
        from flexsearch.views import regenerate_views
        regenerate_views(claude_code_cell, views={'messages': 'chunk', 'sessions': 'source'})
        cols = claude_code_cell.execute("PRAGMA table_info(messages)").fetchall()
        col_names = {c[1] for c in cols}
        # Raw column names — no renames applied
        assert 'tool_name' in col_names, "Should have raw 'tool_name', not 'action'"
        assert 'action' not in col_names, "Should NOT have renamed 'action'"
        assert 'kind' not in col_names, "Should NOT have renamed 'kind'"

    def test_view_has_one_row_per_chunk(self, qmem_cell):
        """Views must not multiply rows (1:1 PK rule)."""
        from flexsearch.views import regenerate_views
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        chunk_count = qmem_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        views = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        ).fetchall()
        for (view_name,) in views:
            view_count = qmem_cell.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            assert view_count <= chunk_count, \
                f"View '{view_name}' has {view_count} rows but only {chunk_count} chunks"

    def test_idempotent(self, qmem_cell):
        """Calling regenerate_views twice should produce same result."""
        from flexsearch.views import regenerate_views
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        views_first = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        views_second = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        assert views_first == views_second

    def test_detect_existing_views(self, qmem_cell):
        """When views=None, re-creates existing views from sqlite_master."""
        from flexsearch.views import regenerate_views
        # First call creates views
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        # Second call with no param should detect and re-create
        regenerate_views(qmem_cell)
        views = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        names = {v[0] for v in views}
        assert 'sections' in names
        assert 'documents' in names

    def test_enrichment_wipe_safe(self, qmem_cell):
        """Views still work after enrichment tables are wiped."""
        from flexsearch.views import regenerate_views
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        qmem_cell.execute("DELETE FROM _enrich_source_graph")
        qmem_cell.execute("DELETE FROM _enrich_types")
        views = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        ).fetchall()
        for (view_name,) in views:
            rows = qmem_cell.execute(f"SELECT * FROM {view_name}").fetchall()
            assert len(rows) > 0, f"View '{view_name}' should still have rows after enrich wipe"

    def test_no_meta_view_keys_needed(self, qmem_cell):
        """Views work without any view:* keys in _meta."""
        from flexsearch.views import regenerate_views
        # Verify no view keys exist
        count = qmem_cell.execute(
            "SELECT COUNT(*) FROM _meta WHERE key LIKE 'view:%'"
        ).fetchone()[0]
        assert count == 0, "Fixture should not have view:* meta keys"
        # Views still generate fine with explicit params
        regenerate_views(qmem_cell, views={'sections': 'chunk'})
        views = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        ).fetchall()
        assert len(views) > 0


# =============================================================================
# install_views — curated view layer (Plan 7)
# =============================================================================

class TestInstallViews:
    """install_views(db, view_dir) installs curated .sql views into _views table."""

    def test_installs_curated_view(self, qmem_cell, tmp_path):
        from flexsearch.views import install_views
        # Write a simple curated view
        view_file = tmp_path / "test_view.sql"
        view_file.write_text(
            "-- @name: test_chunks\n"
            "-- @description: Simple test view\n\n"
            "DROP VIEW IF EXISTS test_chunks;\n"
            "CREATE VIEW test_chunks AS SELECT id, content FROM _raw_chunks;\n"
        )
        install_views(qmem_cell, tmp_path)
        # View exists
        rows = qmem_cell.execute("SELECT * FROM test_chunks LIMIT 1").fetchall()
        assert len(rows) > 0
        # _views table populated
        meta = qmem_cell.execute(
            "SELECT name, description FROM _views WHERE name = 'test_chunks'"
        ).fetchone()
        assert meta is not None
        assert meta[0] == 'test_chunks'
        assert meta[1] == 'Simple test view'

    def test_views_table_created(self, qmem_cell, tmp_path):
        from flexsearch.views import install_views, _has_table
        assert not _has_table(qmem_cell, '_views')
        install_views(qmem_cell, tmp_path)  # empty dir, but creates table
        assert _has_table(qmem_cell, '_views')

    def test_curated_view_discoverable(self, qmem_cell, tmp_path):
        from flexsearch.views import install_views
        view_file = tmp_path / "hub_docs.sql"
        view_file.write_text(
            "-- @name: hub_docs\n"
            "-- @description: Hub documents with high centrality\n\n"
            "DROP VIEW IF EXISTS hub_docs;\n"
            "CREATE VIEW hub_docs AS\n"
            "SELECT src.source_id, src.title, g.centrality\n"
            "FROM _raw_sources src\n"
            "LEFT JOIN _enrich_source_graph g ON src.source_id = g.source_id\n"
            "WHERE g.is_hub = 1;\n"
        )
        install_views(qmem_cell, tmp_path)
        # Discoverable via _views
        names = [r[0] for r in qmem_cell.execute("SELECT name FROM _views").fetchall()]
        assert 'hub_docs' in names


class TestCuratedPrecedence:
    """Curated views in _views table take precedence over auto-generated."""

    def test_curated_survives_regenerate(self, qmem_cell, tmp_path):
        from flexsearch.views import install_views, regenerate_views
        # Install a curated 'sections' view
        view_file = tmp_path / "sections.sql"
        view_file.write_text(
            "-- @name: sections\n"
            "-- @description: Curated sections view\n\n"
            "DROP VIEW IF EXISTS sections;\n"
            "CREATE VIEW sections AS\n"
            "SELECT id, content, timestamp FROM _raw_chunks;\n"
        )
        install_views(qmem_cell, tmp_path)
        # Curated sections has 3 columns
        cols_before = qmem_cell.execute("PRAGMA table_info(sections)").fetchall()
        assert len(cols_before) == 3

        # regenerate_views should NOT overwrite the curated view
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        cols_after = qmem_cell.execute("PRAGMA table_info(sections)").fetchall()
        assert len(cols_after) == 3, "Curated view should survive regenerate_views()"

    def test_auto_generated_still_created_for_non_curated(self, qmem_cell, tmp_path):
        from flexsearch.views import install_views, regenerate_views
        # Install curated 'sections' only
        view_file = tmp_path / "sections.sql"
        view_file.write_text(
            "-- @name: sections\n"
            "-- @description: Curated sections\n\n"
            "DROP VIEW IF EXISTS sections;\n"
            "CREATE VIEW sections AS SELECT id FROM _raw_chunks;\n"
        )
        install_views(qmem_cell, tmp_path)
        # regenerate_views creates 'documents' (not curated) but skips 'sections'
        regenerate_views(qmem_cell, views={'sections': 'chunk', 'documents': 'source'})
        views = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        names = {v[0] for v in views}
        assert 'sections' in names  # curated
        assert 'documents' in names  # auto-generated


# =============================================================================
# _ops table + log_op (Plan 12)
# =============================================================================

class TestOpsTable:
    """_ops table: cell mutation provenance."""

    def test_ensure_ops_table_creates_table(self, empty_cell):
        from flexsearch.core import ensure_ops_table
        ensure_ops_table(empty_cell)
        tables = empty_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_ops'"
        ).fetchall()
        assert len(tables) == 1

    def test_ensure_ops_table_idempotent(self, empty_cell):
        from flexsearch.core import ensure_ops_table
        ensure_ops_table(empty_cell)
        ensure_ops_table(empty_cell)  # should not raise
        count = empty_cell.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_ops'"
        ).fetchone()[0]
        assert count == 1

    def test_log_op_basic(self, empty_cell):
        from flexsearch.core import log_op
        log_op(empty_cell, 'build_similarity_graph', '_enrich_source_graph',
               rows_affected=100, source='meditate.py')
        rows = empty_cell.execute("SELECT * FROM _ops").fetchall()
        assert len(rows) == 1
        row = dict(rows[0])
        assert row['operation'] == 'build_similarity_graph'
        assert row['target'] == '_enrich_source_graph'
        assert row['rows_affected'] == 100
        assert row['source'] == 'meditate.py'
        assert row['timestamp'] is not None

    def test_log_op_with_params(self, empty_cell):
        import json
        from flexsearch.core import log_op
        log_op(empty_cell, 'build_similarity_graph', '_enrich_source_graph',
               params={'threshold': 0.55, 'where': 'chunks>=20'},
               rows_affected=1037, source='rebuild_all.py')
        row = dict(empty_cell.execute("SELECT * FROM _ops").fetchone())
        params = json.loads(row['params'])
        assert params['threshold'] == 0.55
        assert params['where'] == 'chunks>=20'

    def test_log_op_with_sql(self, empty_cell):
        from flexsearch.core import log_op
        log_op(empty_cell, 'run_sandbox', '_enrich_*',
               sql='result["test"] = 42', source='meditate.py')
        row = dict(empty_cell.execute("SELECT * FROM _ops").fetchone())
        assert row['sql'] == 'result["test"] = 42'

    def test_log_op_params_none(self, empty_cell):
        from flexsearch.core import log_op
        log_op(empty_cell, 'install_views', '_views', source='views.py')
        row = dict(empty_cell.execute("SELECT * FROM _ops").fetchone())
        assert row['params'] is None

    def test_multiple_ops_ordered(self, empty_cell):
        from flexsearch.core import log_op
        log_op(empty_cell, 'op1', 'target1', source='a.py')
        log_op(empty_cell, 'op2', 'target2', source='b.py')
        log_op(empty_cell, 'op3', 'target3', source='c.py')
        rows = empty_cell.execute(
            "SELECT operation FROM _ops ORDER BY id"
        ).fetchall()
        assert [r[0] for r in rows] == ['op1', 'op2', 'op3']

    def test_ops_queryable_by_operation(self, empty_cell):
        from flexsearch.core import log_op
        log_op(empty_cell, 'build_similarity_graph', '_enrich_source_graph',
               params={'threshold': 0.5}, source='test')
        log_op(empty_cell, 'build_similarity_graph', '_enrich_source_graph',
               params={'threshold': 0.55}, source='test')
        log_op(empty_cell, 'install_views', '_views', source='test')
        rows = empty_cell.execute(
            "SELECT json_extract(params, '$.threshold') as threshold "
            "FROM _ops WHERE operation = 'build_similarity_graph' "
            "ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == 0.5
        assert rows[1][0] == 0.55

    def test_ops_stays_small(self, empty_cell):
        """_ops logs mutations, not queries — stays at tens of entries."""
        from flexsearch.core import log_op
        # Simulate a typical meditate run
        log_op(empty_cell, 'rebuild_warmup_types', '_types_source_warmup',
               rows_affected=1196, source='rebuild_all.py')
        log_op(empty_cell, 'build_similarity_graph', '_enrich_source_graph',
               rows_affected=1037, source='rebuild_all.py')
        log_op(empty_cell, 'persist_graph_scores', '_enrich_source_graph',
               rows_affected=1037, source='meditate.py')
        log_op(empty_cell, 'build_file_graph', '_enrich_file_graph',
               rows_affected=2409, source='rebuild_all.py')
        log_op(empty_cell, 'build_delegation_graph', '_enrich_delegation_graph',
               rows_affected=1732, source='rebuild_all.py')
        count = empty_cell.execute("SELECT COUNT(*) FROM _ops").fetchone()[0]
        assert count == 5


class TestValidateView:
    """_validate_view checks 1:1 invariant."""

    def test_valid_view_passes(self, qmem_cell):
        from flexsearch.views import regenerate_views, _validate_view
        regenerate_views(qmem_cell, views={'sections': 'chunk'})
        assert _validate_view(qmem_cell, 'sections') is True

    def test_multiplied_view_raises(self, qmem_cell):
        from flexsearch.views import _validate_view
        # Create a view that multiplies rows via 1:N join
        qmem_cell.execute("""
            CREATE VIEW bad_view AS
            SELECT r.id, r.content, e.source_id
            FROM _raw_chunks r, _edges_source e
        """)
        with pytest.raises(ValueError, match="multiplies rows"):
            _validate_view(qmem_cell, 'bad_view')
