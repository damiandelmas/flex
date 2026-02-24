"""
Schema Contract Tests — Chunk-Atom Architectural Invariants

Validates the seven principles without importing flex.
Pure SQL against in-memory fixtures. These are acceptance criteria
for Plans 1-4.

Run with: pytest tests/test_schema_contract.py -v
"""
import sqlite3
import struct

EMBED_DIM = 128


# =============================================================================
# Prefix Rule Tests
# =============================================================================

class TestPrefixRule:
    """Table prefix declares lifecycle and mutability."""

    VALID_PREFIXES = ('_raw_', '_edges_', '_types_', '_enrich_', '_meta', '_presets')
    SYSTEM_TABLES = ('chunks_fts', 'chunks_fts_data', 'chunks_fts_idx',
                     'chunks_fts_content', 'chunks_fts_docsize', 'chunks_fts_config')

    def _user_tables(self, conn):
        """All non-system, non-view tables."""
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return [r[0] for r in rows]

    def test_all_tables_have_valid_prefix(self, empty_cell):
        """Every table must start with a valid prefix or be a system table."""
        for table in self._user_tables(empty_cell):
            if table in self.SYSTEM_TABLES:
                continue
            assert any(table.startswith(p) for p in self.VALID_PREFIXES), \
                f"Table '{table}' has no valid prefix. Expected one of {self.VALID_PREFIXES}"

    def test_raw_tables_exist(self, empty_cell):
        tables = self._user_tables(empty_cell)
        assert '_raw_chunks' in tables
        assert '_raw_sources' in tables

    def test_meta_table_exists(self, empty_cell):
        tables = self._user_tables(empty_cell)
        assert '_meta' in tables

    def test_meta_has_pk_on_key(self, empty_cell):
        info = empty_cell.execute("PRAGMA table_info(_meta)").fetchall()
        pk_cols = [row[1] for row in info if row[5] == 1]
        assert pk_cols == ['key']

    def test_claude_code_module_tables_have_valid_prefix(self, claude_code_cell):
        for table in self._user_tables(claude_code_cell):
            if table in self.SYSTEM_TABLES:
                continue
            assert any(table.startswith(p) for p in self.VALID_PREFIXES), \
                f"claude_code module table '{table}' has no valid prefix"


# =============================================================================
# PK Rule Tests (1:1 vs 1:N)
# =============================================================================

class TestPKRule:
    """Tables with PK on chunk_id are 1:1 (view-safe).
       Tables without PK on chunk_id are 1:N (query directly)."""

    # Tables that MUST have PK on chunk_id (1:1, in view)
    MUST_HAVE_PK = ['_types_docpac', '_enrich_types']
    CLAUDE_CODE_MUST_HAVE_PK = ['_edges_tool_ops', '_types_message']

    # Tables that MUST NOT have PK on chunk_id (1:N, not in view)
    MUST_NOT_HAVE_PK = ['_edges_source']
    CLAUDE_CODE_MUST_NOT_HAVE_PK = [
        '_edges_file_identity', '_edges_repo_identity',
        '_edges_content_identity', '_edges_url_identity',
        '_edges_delegations', '_edges_soft_ops',
    ]

    def _has_pk_on_chunk_id(self, conn, table):
        """Check if chunk_id is the PRIMARY KEY of this table."""
        info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        for row in info:
            col_name, pk_flag = row[1], row[5]
            if col_name == 'chunk_id' and pk_flag == 1:
                return True
        return False

    def test_1_1_tables_have_pk(self, qmem_cell):
        for table in self.MUST_HAVE_PK:
            assert self._has_pk_on_chunk_id(qmem_cell, table), \
                f"Table '{table}' should have PK on chunk_id (1:1 rule)"

    def test_1_n_tables_no_pk(self, qmem_cell):
        for table in self.MUST_NOT_HAVE_PK:
            assert not self._has_pk_on_chunk_id(qmem_cell, table), \
                f"Table '{table}' should NOT have PK on chunk_id (1:N)"

    def test_claude_code_1_1_tables(self, claude_code_cell):
        for table in self.CLAUDE_CODE_MUST_HAVE_PK:
            assert self._has_pk_on_chunk_id(claude_code_cell, table), \
                f"claude_code table '{table}' should have PK on chunk_id"

    def test_claude_code_1_n_tables(self, claude_code_cell):
        for table in self.CLAUDE_CODE_MUST_NOT_HAVE_PK:
            assert not self._has_pk_on_chunk_id(claude_code_cell, table), \
                f"claude_code table '{table}' should NOT have PK on chunk_id"


# =============================================================================
# Source Invariant Tests
# =============================================================================

class TestSourceInvariant:
    """Each chunk belongs to exactly one source. No orphans, no duplicates."""

    def test_no_duplicate_source_edges(self, qmem_cell):
        """A chunk must not have multiple source edges."""
        dupes = qmem_cell.execute("""
            SELECT chunk_id, COUNT(*) as n FROM _edges_source
            GROUP BY chunk_id HAVING n > 1
        """).fetchall()
        assert len(dupes) == 0, \
            f"{len(dupes)} chunks have multiple sources"

    def test_no_orphan_chunks(self, qmem_cell):
        """Every chunk must have a source edge."""
        orphans = qmem_cell.execute("""
            SELECT c.id FROM _raw_chunks c
            LEFT JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.chunk_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, \
            f"{len(orphans)} chunks have no source edge"

    def test_validate_cell_passes(self, qmem_cell):
        """validate_cell() should pass on a well-formed cell."""
        from flex.core import validate_cell
        validate_cell(qmem_cell)  # should not raise


# =============================================================================
# Two Lifecycles Tests
# =============================================================================

class TestTwoLifecycles:
    """Content is immutable (COMPILE). Labels are mutable (meditate).
       DELETE FROM _enrich_* is always safe."""

    def test_enrich_wipe_preserves_raw(self, qmem_cell):
        """Wiping all _enrich_* tables must not affect _raw_* counts."""
        raw_chunks_before = qmem_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        raw_sources_before = qmem_cell.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]

        # Wipe all enrichments
        tables = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_enrich_%'"
        ).fetchall()
        for (table,) in tables:
            qmem_cell.execute(f"DELETE FROM {table}")

        raw_chunks_after = qmem_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        raw_sources_after = qmem_cell.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]

        assert raw_chunks_before == raw_chunks_after
        assert raw_sources_before == raw_sources_after

    def test_enrich_wipe_preserves_types(self, qmem_cell):
        """Wiping _enrich_* must not affect _types_* counts."""
        types_before = qmem_cell.execute("SELECT COUNT(*) FROM _types_docpac").fetchone()[0]

        tables = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_enrich_%'"
        ).fetchall()
        for (table,) in tables:
            qmem_cell.execute(f"DELETE FROM {table}")

        types_after = qmem_cell.execute("SELECT COUNT(*) FROM _types_docpac").fetchone()[0]
        assert types_before == types_after

    def test_enrich_wipe_preserves_edges(self, qmem_cell):
        """Wiping _enrich_* must not affect _edges_* counts."""
        edges_before = qmem_cell.execute("SELECT COUNT(*) FROM _edges_source").fetchone()[0]

        tables = qmem_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_enrich_%'"
        ).fetchall()
        for (table,) in tables:
            qmem_cell.execute(f"DELETE FROM {table}")

        edges_after = qmem_cell.execute("SELECT COUNT(*) FROM _edges_source").fetchone()[0]
        assert edges_before == edges_after

    def test_enrich_tables_are_idempotent_replaceable(self, qmem_cell):
        """DELETE + re-INSERT into _enrich_* should produce same result."""
        original = qmem_cell.execute(
            "SELECT * FROM _enrich_source_graph ORDER BY source_id"
        ).fetchall()

        # Wipe and re-insert
        qmem_cell.execute("DELETE FROM _enrich_source_graph")
        assert qmem_cell.execute("SELECT COUNT(*) FROM _enrich_source_graph").fetchone()[0] == 0

        for row in original:
            qmem_cell.execute(
                "INSERT INTO _enrich_source_graph VALUES (?,?,?,?,?)",
                tuple(row)
            )

        restored = qmem_cell.execute(
            "SELECT * FROM _enrich_source_graph ORDER BY source_id"
        ).fetchall()
        assert [tuple(r) for r in original] == [tuple(r) for r in restored]


# =============================================================================
# Chunk Is the Atom Tests
# =============================================================================

class TestChunkAtom:
    """Every relationship points TO a chunk. Chunk is the universal node."""

    def test_raw_chunks_has_id_pk(self, empty_cell):
        info = empty_cell.execute("PRAGMA table_info(_raw_chunks)").fetchall()
        pk_cols = [row[1] for row in info if row[5] == 1]
        assert pk_cols == ['id']

    def test_raw_chunks_has_required_columns(self, empty_cell):
        info = empty_cell.execute("PRAGMA table_info(_raw_chunks)").fetchall()
        cols = {row[1] for row in info}
        assert {'id', 'content', 'embedding', 'timestamp'} <= cols

    def test_raw_sources_has_source_id_pk(self, empty_cell):
        info = empty_cell.execute("PRAGMA table_info(_raw_sources)").fetchall()
        pk_cols = [row[1] for row in info if row[5] == 1]
        assert pk_cols == ['source_id']

    def test_edges_connect_chunks_to_sources(self, qmem_cell):
        """Every chunk in _edges_source references a valid chunk and source."""
        orphan_chunks = qmem_cell.execute("""
            SELECT es.chunk_id FROM _edges_source es
            LEFT JOIN _raw_chunks rc ON es.chunk_id = rc.id
            WHERE rc.id IS NULL
        """).fetchall()
        assert len(orphan_chunks) == 0, f"Orphan chunk refs: {orphan_chunks}"

        orphan_sources = qmem_cell.execute("""
            SELECT es.source_id FROM _edges_source es
            LEFT JOIN _raw_sources rs ON es.source_id = rs.source_id
            WHERE rs.source_id IS NULL
        """).fetchall()
        assert len(orphan_sources) == 0, f"Orphan source refs: {orphan_sources}"

    def test_every_chunk_has_a_source(self, qmem_cell):
        """No orphan chunks — every chunk belongs to at least one source."""
        orphans = qmem_cell.execute("""
            SELECT rc.id FROM _raw_chunks rc
            LEFT JOIN _edges_source es ON rc.id = es.chunk_id
            WHERE es.chunk_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, f"Chunks without source: {orphans}"

    def test_chunk_count_matches_edges(self, qmem_cell):
        """Number of distinct chunk_ids in _edges_source = chunks in _raw_chunks."""
        chunk_count = qmem_cell.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        edge_distinct = qmem_cell.execute(
            "SELECT COUNT(DISTINCT chunk_id) FROM _edges_source"
        ).fetchone()[0]
        assert chunk_count == edge_distinct


# =============================================================================
# Meta Table Tests
# =============================================================================

class TestMeta:
    """_meta table is the self-describing manifest."""

    def test_meta_populated(self, qmem_cell):
        desc = qmem_cell.execute(
            "SELECT value FROM _meta WHERE key = 'description'"
        ).fetchone()
        assert desc is not None
        assert len(desc[0]) > 20, "Description should be meaningful (>20 chars)"

    def test_meta_has_version(self, qmem_cell):
        ver = qmem_cell.execute(
            "SELECT value FROM _meta WHERE key = 'version'"
        ).fetchone()
        assert ver is not None

    def test_meta_has_schema(self, qmem_cell):
        schema = qmem_cell.execute(
            "SELECT value FROM _meta WHERE key = 'schema'"
        ).fetchone()
        assert schema is not None
        assert schema[0] in ('chunk-atom', 'flat-tables')

    def test_no_view_meta_keys(self, claude_code_cell):
        """Plan 7: _meta carries no view config. Views passed as code params."""
        view_keys = claude_code_cell.execute(
            "SELECT key, value FROM _meta WHERE key LIKE 'view:%'"
        ).fetchall()
        assert len(view_keys) == 0, \
            "_meta should have zero view:* keys (Plan 7: view config is code, not _meta)"


# =============================================================================
# FTS Tests
# =============================================================================

class TestFTS:
    """Full-text search on chunk content, synced via triggers."""

    def test_fts_table_exists(self, empty_cell):
        tables = empty_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='chunks_fts'"
        ).fetchall()
        assert len(tables) == 1

    def test_fts_synced_on_insert(self, qmem_cell):
        """FTS should contain all chunk content after inserts."""
        fts_count = qmem_cell.execute(
            "SELECT COUNT(*) FROM chunks_fts"
        ).fetchone()[0]
        chunk_count = qmem_cell.execute(
            "SELECT COUNT(*) FROM _raw_chunks"
        ).fetchone()[0]
        assert fts_count == chunk_count

    def test_fts_search_returns_results(self, qmem_cell):
        """BM25 search should find content."""
        results = qmem_cell.execute(
            "SELECT COUNT(*) FROM chunks_fts WHERE chunks_fts MATCH 'Content'"
        ).fetchone()[0]
        assert results > 0

    def test_fts_triggers_exist(self, empty_cell):
        triggers = empty_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name LIKE 'raw_chunks_%'"
        ).fetchall()
        trigger_names = {r[0] for r in triggers}
        assert 'raw_chunks_ai' in trigger_names, "Missing AFTER INSERT trigger"
        assert 'raw_chunks_ad' in trigger_names, "Missing AFTER DELETE trigger"
        assert 'raw_chunks_au' in trigger_names, "Missing AFTER UPDATE trigger"

    def test_fts_synced_on_delete(self, qmem_cell):
        """Deleting a chunk should remove it from FTS."""
        before = qmem_cell.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
        qmem_cell.execute("DELETE FROM _raw_chunks WHERE id = 'src-arch:0'")
        after = qmem_cell.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
        assert after == before - 1

    def test_fts_synced_on_update(self, qmem_cell):
        """Updating chunk content should update FTS."""
        qmem_cell.execute(
            "UPDATE _raw_chunks SET content = 'UNIQUE_CANARY_TEXT' WHERE id = 'src-arch:0'"
        )
        result = qmem_cell.execute(
            "SELECT COUNT(*) FROM chunks_fts WHERE chunks_fts MATCH 'UNIQUE_CANARY_TEXT'"
        ).fetchone()[0]
        assert result == 1


# =============================================================================
# Embedding Storage Tests
# =============================================================================

class TestEmbeddings:
    """Embeddings stored as float32 BLOBs in _raw_chunks."""

    def test_embedding_is_blob(self, qmem_cell):
        row = qmem_cell.execute(
            "SELECT typeof(embedding) FROM _raw_chunks LIMIT 1"
        ).fetchone()
        assert row[0] == 'blob'

    def test_embedding_dimension(self, qmem_cell):
        row = qmem_cell.execute(
            "SELECT embedding FROM _raw_chunks LIMIT 1"
        ).fetchone()
        blob = row[0]
        dim = len(blob) // 4  # float32 = 4 bytes
        assert dim == EMBED_DIM, f"Expected {EMBED_DIM}-dim embedding, got {dim}"

    def test_embedding_decodable(self, qmem_cell):
        row = qmem_cell.execute(
            "SELECT embedding FROM _raw_chunks LIMIT 1"
        ).fetchone()
        blob = row[0]
        values = struct.unpack(f'{EMBED_DIM}f', blob)
        assert len(values) == EMBED_DIM
        assert all(isinstance(v, float) for v in values)


# =============================================================================
# Schema Self-Description Tests
# =============================================================================

class TestSelfDescription:
    """An AI reading sqlite_master + PRAGMA table_info learns the system."""

    def test_sqlite_master_lists_all_tables(self, claude_code_cell):
        """sqlite_master should list every table for AI discovery."""
        tables = claude_code_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'chunks_fts%' "
            "ORDER BY name"
        ).fetchall()
        table_names = [r[0] for r in tables]
        # Core tables must be discoverable
        assert '_raw_chunks' in table_names
        assert '_raw_sources' in table_names
        assert '_meta' in table_names
        # claude_code module tables must be discoverable
        assert '_edges_tool_ops' in table_names
        assert '_types_message' in table_names

    def test_pragma_reveals_pk_constraints(self, claude_code_cell):
        """PRAGMA table_info should reveal which tables are 1:1 vs 1:N."""
        # 1:1 table — AI sees pk=1 on chunk_id
        info = claude_code_cell.execute("PRAGMA table_info(_edges_tool_ops)").fetchall()
        pk_col = [(r[1], r[5]) for r in info if r[5] == 1]
        assert pk_col == [('chunk_id', 1)]

        # 1:N table — AI sees no pk on chunk_id
        info = claude_code_cell.execute("PRAGMA table_info(_edges_file_identity)").fetchall()
        pk_col = [(r[1], r[5]) for r in info if r[5] == 1]
        assert pk_col == [], "1:N table should have no PK"

    def test_prefix_reveals_lifecycle(self, claude_code_cell):
        """Table names should clearly indicate lifecycle via prefix."""
        tables = claude_code_cell.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'chunks_fts%'"
        ).fetchall()
        for (name,) in tables:
            if name in ('_meta', '_presets'):
                continue
            if name.startswith('_raw_'):
                pass  # immutable fact
            elif name.startswith('_edges_'):
                pass  # relationships
            elif name.startswith('_types_'):
                pass  # classification
            elif name.startswith('_enrich_'):
                pass  # mutable scores
            else:
                raise AssertionError(f"Table '{name}' has unclear lifecycle prefix")
