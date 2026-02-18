"""
Tests for flexsearch.modules.docpac.compile.worker

Covers index_file() and process_queue() — the incremental indexing pipeline.
"""
import hashlib
import os
import sqlite3
import struct
import tempfile

import pytest
import numpy as np

try:
    from flexsearch.modules.docpac.compile.worker import (
        index_file, process_queue, make_source_id, make_chunk_id,
        _find_context_root, _graph_stale, GRAPH_REFRESH_THRESHOLD,
    )
    HAS_WORKER = True
except ImportError:
    HAS_WORKER = False

pytestmark = pytest.mark.skipif(not HAS_WORKER, reason="docpac worker not importable")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_embedding(dim=384):
    return struct.pack(f'{dim}f', *([0.1] * dim))


def _mock_embed_fn(texts):
    """Deterministic 384-dim embeddings."""
    vecs = np.array([[0.1] * 384 for _ in texts], dtype=np.float32)
    return vecs


def _mock_embed_fn_failing(texts):
    raise RuntimeError("embed failed")


DOCPAC_DDL = """
CREATE TABLE _raw_chunks (
    id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER
);
CREATE TABLE _raw_sources (
    source_id TEXT PRIMARY KEY, file_date TEXT, temporal TEXT, doc_type TEXT,
    title TEXT, source_path TEXT, type TEXT, status TEXT, keywords TEXT,
    embedding BLOB
);
CREATE TABLE _edges_source (
    chunk_id TEXT NOT NULL, source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'markdown', position INTEGER
);
CREATE INDEX idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX idx_es_source ON _edges_source(source_id);
CREATE TABLE _types_docpac (
    chunk_id TEXT PRIMARY KEY, temporal TEXT, doc_type TEXT, facet TEXT,
    section_title TEXT, yaml_type TEXT, yaml_status TEXT
);
CREATE TABLE _meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE _presets (name TEXT PRIMARY KEY, description TEXT, params TEXT DEFAULT '', sql TEXT);

CREATE VIRTUAL TABLE chunks_fts USING fts5(content, content='_raw_chunks', content_rowid='rowid');
CREATE TRIGGER raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
END;
CREATE TRIGGER raw_chunks_au AFTER UPDATE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
    INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
END;
"""


@pytest.fixture
def docpac_cell():
    """In-memory docpac cell."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript(DOCPAC_DDL)
    yield conn
    conn.close()


@pytest.fixture
def context_tree(tmp_path):
    """Temp directory structure: project/context/changes/code/file.md"""
    context = tmp_path / "project" / "context"
    code_dir = context / "changes" / "code"
    code_dir.mkdir(parents=True)
    design_dir = context / "intended" / "design"
    design_dir.mkdir(parents=True)
    buffer_dir = context / "buffer"
    buffer_dir.mkdir(parents=True)
    return context


def _write_md(path, content="# Title\n\n## Section 1\n\nHello world.\n\n## Section 2\n\nGoodbye.",
              frontmatter=None):
    """Write a markdown file with optional frontmatter."""
    fm = ""
    if frontmatter:
        lines = ["---"]
        for k, v in frontmatter.items():
            lines.append(f"{k}: {v}")
        lines.append("---\n")
        fm = "\n".join(lines)
    path.write_text(fm + content, encoding='utf-8')
    return path


# ===========================================================================
# TestIndexFile
# ===========================================================================

class TestIndexFile:
    """Tests for index_file()."""

    def test_new_file(self, docpac_cell, context_tree):
        """New file: parses, splits, embeds, inserts into all 4 tables."""
        f = _write_md(context_tree / "changes" / "code" / "260218-test.md")
        result = index_file(docpac_cell, str(f), _mock_embed_fn)

        assert result is True
        source_id = make_source_id(str(f))

        # _raw_sources
        src = docpac_cell.execute("SELECT * FROM _raw_sources WHERE source_id = ?",
                                  (source_id,)).fetchone()
        assert src is not None
        assert src['title'] is not None

        # _raw_chunks (preamble + 2 sections = 3)
        chunks = docpac_cell.execute("SELECT * FROM _raw_chunks").fetchall()
        assert len(chunks) == 3

        # _edges_source
        edges = docpac_cell.execute("SELECT * FROM _edges_source WHERE source_id = ?",
                                     (source_id,)).fetchall()
        assert len(edges) == 3
        assert edges[0]['source_type'] == 'markdown'

        # _types_docpac
        types = docpac_cell.execute("SELECT * FROM _types_docpac").fetchall()
        assert len(types) == 3

        # embeddings present
        for c in chunks:
            assert c['embedding'] is not None

        # source embedding (mean-pooled)
        assert src['embedding'] is not None

    def test_updated_file(self, docpac_cell, context_tree):
        """Updated file: old chunks deleted, new chunks inserted."""
        f = context_tree / "changes" / "code" / "260218-update.md"
        _write_md(f, "# V1\n\n## A\n\nFirst.\n\n## B\n\nSecond.")
        index_file(docpac_cell, str(f), _mock_embed_fn)
        docpac_cell.commit()

        assert docpac_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0] == 3  # preamble + 2

        # Update with 3 sections (preamble + 3 = 4)
        _write_md(f, "# V2\n\n## A\n\nFirst.\n\n## B\n\nSecond.\n\n## C\n\nThird.")
        index_file(docpac_cell, str(f), _mock_embed_fn)
        docpac_cell.commit()

        assert docpac_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0] == 4
        assert docpac_cell.execute("SELECT COUNT(*) FROM _edges_source").fetchone()[0] == 4
        assert docpac_cell.execute("SELECT COUNT(*) FROM _types_docpac").fetchone()[0] == 4

    def test_nonexistent_file(self, docpac_cell):
        """Nonexistent file: returns False."""
        result = index_file(docpac_cell, "/nonexistent/path.md", _mock_embed_fn)
        assert result is False

    def test_file_outside_context(self, docpac_cell, tmp_path):
        """File outside any context/ directory: returns False."""
        f = tmp_path / "random.md"
        _write_md(f)
        result = index_file(docpac_cell, str(f), _mock_embed_fn)
        assert result is False

    def test_file_in_skip_folder(self, docpac_cell, context_tree):
        """File in buffer/ skip folder: returns False."""
        f = context_tree / "buffer" / "old.md"
        _write_md(f)
        result = index_file(docpac_cell, str(f), _mock_embed_fn)
        assert result is False

    def test_embed_failure(self, docpac_cell, context_tree):
        """Embedding failure: chunks inserted with NULL embedding, no crash."""
        f = _write_md(context_tree / "changes" / "code" / "260218-embfail.md")
        result = index_file(docpac_cell, str(f), _mock_embed_fn_failing)

        assert result is True
        chunks = docpac_cell.execute("SELECT * FROM _raw_chunks").fetchall()
        assert len(chunks) == 3  # preamble + 2 sections
        # Embeddings are NULL on failure
        for c in chunks:
            assert c['embedding'] is None

    def test_ops_logged(self, docpac_cell, context_tree):
        """_ops entry created after indexing."""
        f = _write_md(context_tree / "changes" / "code" / "260218-ops.md")
        index_file(docpac_cell, str(f), _mock_embed_fn)
        docpac_cell.commit()

        ops = docpac_cell.execute(
            "SELECT * FROM _ops WHERE operation = 'docpac_incremental_index'"
        ).fetchall()
        assert len(ops) == 1

    def test_frontmatter_metadata(self, docpac_cell, context_tree):
        """Frontmatter fields flow through to types table."""
        f = context_tree / "changes" / "code" / "260218-fm.md"
        _write_md(f, "# Title\n\n## Sec\n\nBody.",
                  frontmatter={'type': 'implementation.test', 'status': 'active'})
        index_file(docpac_cell, str(f), _mock_embed_fn)
        docpac_cell.commit()

        row = docpac_cell.execute("SELECT yaml_type, yaml_status FROM _types_docpac").fetchone()
        assert row['yaml_type'] == 'implementation.test'
        assert row['yaml_status'] == 'active'

    def test_single_section_file(self, docpac_cell, context_tree):
        """File with no ## headers becomes single chunk."""
        f = context_tree / "changes" / "code" / "260218-single.md"
        _write_md(f, "Just plain text, no headers at all.")
        index_file(docpac_cell, str(f), _mock_embed_fn)
        docpac_cell.commit()

        assert docpac_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0] == 1


# ===========================================================================
# TestProcessQueue
# ===========================================================================

class TestProcessQueue:
    """Tests for process_queue(). Uses monkeypatching for registry + queue."""

    def test_no_queue_db(self, monkeypatch):
        """Missing queue.db: returns clean stats."""
        import flexsearch.modules.docpac.compile.worker as w
        monkeypatch.setattr(w, 'QUEUE_DB', pytest.importorskip('pathlib').Path('/nonexistent/queue.db'))
        stats = process_queue(_mock_embed_fn)
        assert stats == {'processed': 0, 'indexed': 0, 'skipped': 0}

    def test_empty_queue(self, tmp_path, monkeypatch):
        """Empty pending table: returns clean stats."""
        import flexsearch.modules.docpac.compile.worker as w
        qdb = tmp_path / "queue.db"
        qconn = sqlite3.connect(str(qdb))
        qconn.execute("CREATE TABLE pending (path TEXT PRIMARY KEY, ts INTEGER)")
        qconn.commit()
        qconn.close()

        monkeypatch.setattr(w, 'QUEUE_DB', qdb)
        stats = process_queue(_mock_embed_fn)
        assert stats['indexed'] == 0

    def test_no_pending_table(self, tmp_path, monkeypatch):
        """Queue DB exists but no pending table: returns clean stats."""
        import flexsearch.modules.docpac.compile.worker as w
        qdb = tmp_path / "queue.db"
        sqlite3.connect(str(qdb)).close()

        monkeypatch.setattr(w, 'QUEUE_DB', qdb)
        stats = process_queue(_mock_embed_fn)
        assert stats == {'processed': 0, 'indexed': 0, 'skipped': 0}

    def test_file_with_no_cell(self, tmp_path, monkeypatch):
        """File that doesn't resolve to any cell: cleared from queue."""
        import flexsearch.modules.docpac.compile.worker as w

        qdb = tmp_path / "queue.db"
        qconn = sqlite3.connect(str(qdb))
        qconn.execute("CREATE TABLE pending (path TEXT PRIMARY KEY, ts INTEGER)")
        qconn.execute("INSERT INTO pending VALUES ('/no/match.md', 1000)")
        qconn.commit()
        qconn.close()

        monkeypatch.setattr(w, 'QUEUE_DB', qdb)
        monkeypatch.setattr(w, 'resolve_cell_for_path', lambda p: None)

        stats = process_queue(_mock_embed_fn)
        assert stats['indexed'] == 0

        # Verify cleared from queue
        qconn = sqlite3.connect(str(qdb))
        remaining = qconn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
        qconn.close()
        assert remaining == 0


# ===========================================================================
# TestGraphStale
# ===========================================================================

class TestGraphStale:
    """Tests for _graph_stale()."""

    def test_no_ops_table(self, docpac_cell):
        """No _ops table: returns False (can't determine staleness)."""
        assert _graph_stale(docpac_cell) is False

    def test_never_built(self, docpac_cell):
        """_ops exists but no graph build: returns True."""
        from flexsearch.core import ensure_ops_table
        ensure_ops_table(docpac_cell)
        assert _graph_stale(docpac_cell) is True

    def test_below_threshold(self, docpac_cell):
        """Few sources since last graph: returns False."""
        from flexsearch.core import ensure_ops_table
        ensure_ops_table(docpac_cell)

        # Graph build at t=1000
        docpac_cell.execute(
            "INSERT INTO _ops (operation, target, timestamp) VALUES (?, ?, ?)",
            ('build_similarity_graph', '_enrich_source_graph', 1000))

        # Add a few index ops after graph (below threshold)
        for i in range(GRAPH_REFRESH_THRESHOLD - 1):
            docpac_cell.execute(
                "INSERT INTO _ops (operation, target, timestamp) VALUES (?, ?, ?)",
                ('docpac_incremental_index', '_raw_chunks', 1001 + i))
        docpac_cell.commit()

        assert _graph_stale(docpac_cell) is False

    def test_above_threshold(self, docpac_cell):
        """Enough sources since last graph: returns True."""
        from flexsearch.core import ensure_ops_table
        ensure_ops_table(docpac_cell)

        # Graph build at t=1000
        docpac_cell.execute(
            "INSERT INTO _ops (operation, target, timestamp) VALUES (?, ?, ?)",
            ('build_similarity_graph', '_enrich_source_graph', 1000))

        for i in range(GRAPH_REFRESH_THRESHOLD):
            docpac_cell.execute(
                "INSERT INTO _ops (operation, target, timestamp) VALUES (?, ?, ?)",
                ('docpac_incremental_index', '_raw_chunks', 1001 + i))
        docpac_cell.commit()

        assert _graph_stale(docpac_cell) is True


# ===========================================================================
# TestHelpers
# ===========================================================================

class TestHelpers:
    """Tests for helper functions."""

    def test_make_source_id(self):
        assert len(make_source_id("/some/path.md")) == 16
        # Deterministic
        assert make_source_id("/a/b.md") == make_source_id("/a/b.md")
        # Different paths → different ids
        assert make_source_id("/a/b.md") != make_source_id("/a/c.md")

    def test_make_chunk_id(self):
        assert make_chunk_id("abc123", 0) == "abc123:0"
        assert make_chunk_id("abc123", 5) == "abc123:5"

    def test_find_context_root(self):
        assert _find_context_root("/home/user/projects/foo/context/changes/code/bar.md") is not None
        assert str(_find_context_root("/home/user/projects/foo/context/changes/code/bar.md")).endswith("context")
        assert _find_context_root("/home/user/projects/foo/src/bar.md") is None
