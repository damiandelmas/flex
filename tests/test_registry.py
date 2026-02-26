"""Tests for flex.registry — cell catalog with UUID identity."""

import sqlite3
import pytest
from pathlib import Path
from unittest.mock import patch

# Guard: skip if registry not importable
try:
    from flex.registry import (
        register_cell, unregister_cell, resolve_cell,
        resolve_cell_for_path, list_cells, discover_cells,
        _open_registry, FLEX_HOME, REGISTRY_DB,
    )
    _can_import = True
except ImportError:
    _can_import = False

pytestmark = pytest.mark.skipif(not _can_import, reason="flex not importable")


@pytest.fixture
def tmp_registry(tmp_path, monkeypatch):
    """Redirect registry to tmp dir so tests don't touch real ~/.flex/."""
    reg_home = tmp_path / ".flex"
    reg_db = reg_home / "registry.db"
    monkeypatch.setattr("flex.registry.FLEX_HOME", reg_home)
    monkeypatch.setattr("flex.registry.REGISTRY_DB", reg_db)
    monkeypatch.setattr("flex.registry.CELLS_DIR", tmp_path / "cells")
    return tmp_path


def _make_cell(tmp_path, name="test-cell"):
    """Create a minimal chunk-atom cell .db for testing."""
    db_path = tmp_path / f"{name}.db"
    db = sqlite3.connect(str(db_path))
    db.execute("CREATE TABLE _raw_chunks (id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER)")
    db.execute("CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO _meta VALUES ('description', 'Test cell for unit tests')")
    db.execute("CREATE TABLE _types_docpac (chunk_id TEXT)")
    db.commit()
    db.close()
    return db_path


class TestRegisterAndResolve:

    def test_round_trip(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        register_cell("mycel", db_path)
        result = resolve_cell("mycel")
        assert result == db_path

    def test_uuid_assigned(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        cell_id = register_cell("mycel", db_path)
        assert cell_id is not None
        assert len(cell_id) == 36  # uuid4 format
        # Verify it's in the DB
        cells = list_cells()
        assert cells[0]['id'] == cell_id

    def test_uuid_preserved_on_update(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        id1 = register_cell("mycel", db_path)
        id2 = register_cell("mycel", db_path, description="updated")
        assert id1 == id2

    def test_auto_detect_type(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        register_cell("mycel", db_path)
        cells = list_cells()
        assert cells[0]['cell_type'] == 'docpac'

    def test_auto_detect_description(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        register_cell("mycel", db_path)
        cells = list_cells()
        assert cells[0]['description'] == 'Test cell for unit tests'

    def test_resolve_unknown(self, tmp_registry):
        assert resolve_cell("nonexistent") is None

    def test_corpus_path_stored(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        corpus = str(tmp_registry / "project" / "context")
        register_cell("mycel", db_path, corpus_path=corpus)
        cells = list_cells()
        from pathlib import Path
        assert cells[0]['corpus_path'] == str(Path(corpus).resolve())


class TestResolveForPath:

    def test_longest_match(self, tmp_registry):
        db1 = _make_cell(tmp_registry, "cell-a")
        db2 = _make_cell(tmp_registry, "cell-b")
        register_cell("broad", db1, corpus_path=str(tmp_registry / "projects"))
        register_cell("narrow", db2, corpus_path=str(tmp_registry / "projects" / "foo" / "context"))

        # File in narrow corpus should resolve to narrow cell
        file_path = tmp_registry / "projects" / "foo" / "context" / "changes" / "test.md"
        result = resolve_cell_for_path(str(file_path))
        assert result is not None
        assert result[0] == "narrow"

    def test_no_match(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        register_cell("mycel", db_path, corpus_path="/home/user/project/context")
        result = resolve_cell_for_path("/completely/different/path/file.md")
        assert result is None

    def test_null_corpus_path_skipped(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        register_cell("mycel", db_path)  # no corpus_path
        result = resolve_cell_for_path(str(db_path))
        assert result is None


class TestListAndDiscover:

    def test_list_cells(self, tmp_registry):
        db1 = _make_cell(tmp_registry, "alpha")
        db2 = _make_cell(tmp_registry, "beta")
        register_cell("alpha", db1)
        register_cell("beta", db2)
        cells = list_cells()
        assert len(cells) == 2
        assert cells[0]['name'] == 'alpha'
        assert cells[1]['name'] == 'beta'

    def test_discover_returns_registered(self, tmp_registry):
        db1 = _make_cell(tmp_registry, "registered")
        register_cell("registered", db1)

        names = discover_cells()
        assert "registered" in names


class TestUnregister:

    def test_unregister_removes(self, tmp_registry):
        db_path = _make_cell(tmp_registry)
        register_cell("mycel", db_path)
        assert resolve_cell("mycel") is not None
        result = unregister_cell("mycel")
        assert result is True
        assert resolve_cell("mycel") is None

    def test_unregister_nonexistent(self, tmp_registry):
        result = unregister_cell("ghost")
        assert result is False


class TestRegistryOnly:

    def test_unregistered_returns_none(self, tmp_registry):
        """No filesystem fallback — unregistered cells return None."""
        result = resolve_cell("not-in-registry")
        assert result is None
