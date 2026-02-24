"""
Flex Test Fixtures

Provides in-memory chunk-atom databases for testing schema contracts,
view generation, and migration correctness. No flex imports needed.

Run with: pytest tests/ -v
"""
import pytest
import sqlite3
import struct


# =============================================================================
# SCHEMA DDL — canonical chunk-atom tables
# =============================================================================

CHUNK_ATOM_DDL = """
-- RAW LAYER (immutable, COMPILE writes here)
CREATE TABLE _raw_chunks (
    id TEXT PRIMARY KEY,
    content TEXT,
    embedding BLOB,
    timestamp INTEGER
);

CREATE TABLE _raw_sources (
    source_id TEXT PRIMARY KEY,
    file_date TEXT,
    temporal TEXT,
    doc_type TEXT,
    title TEXT,
    summary TEXT,
    source_path TEXT,
    type TEXT,
    status TEXT,
    keywords TEXT,
    embedding BLOB
);

-- EDGE LAYER (append-only relationships)
CREATE TABLE _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'markdown',
    position INTEGER
);
CREATE INDEX idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX idx_es_source ON _edges_source(source_id);

-- TYPES LAYER (immutable COMPILE classification — pipeline signature)
-- Doc-pac pipeline: _types_docpac. Claude Code pipeline: _types_message.
CREATE TABLE _types_docpac (
    chunk_id TEXT PRIMARY KEY,
    temporal TEXT,
    doc_type TEXT,
    facet TEXT,
    section_title TEXT,
    yaml_type TEXT,
    yaml_status TEXT
);

-- ENRICHMENT LAYER (mutable, meditate writes here)
CREATE TABLE _enrich_source_graph (
    source_id TEXT PRIMARY KEY,
    centrality REAL,
    is_hub INTEGER DEFAULT 0,
    is_bridge INTEGER DEFAULT 0,
    community_id INTEGER
);

CREATE TABLE _enrich_types (
    chunk_id TEXT PRIMARY KEY,
    semantic_role TEXT,
    confidence REAL DEFAULT 1.0
);

-- METADATA
CREATE TABLE _meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- PRESETS (SQL skills — discoverable via SELECT name, description, params FROM _presets)
CREATE TABLE _presets (
    name TEXT PRIMARY KEY,
    description TEXT,
    params TEXT DEFAULT '',
    sql TEXT
);

-- FTS (content-synced)
CREATE VIRTUAL TABLE chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);

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

# Claude Code-specific tables (module additions)
CLAUDE_CODE_MODULE_DDL = """
-- Tool operations (1:1, PK on chunk_id — safe for view)
CREATE TABLE _edges_tool_ops (
    chunk_id TEXT PRIMARY KEY,
    tool_name TEXT,
    target_file TEXT,
    success INTEGER,
    cwd TEXT,
    git_branch TEXT
);

-- TYPES: COMPILE-written message classification (immutable)
CREATE TABLE _types_message (
    chunk_id TEXT PRIMARY KEY,
    type TEXT,
    role TEXT,
    chunk_number INTEGER,
    parent_uuid TEXT,
    is_sidechain INTEGER,
    entry_uuid TEXT
);

-- Edge tables (renamed from flat schema)
CREATE TABLE _edges_delegations (
    id INTEGER PRIMARY KEY,
    chunk_id TEXT,
    child_doc_id TEXT,
    agent_type TEXT,
    created_at INTEGER
);

CREATE TABLE _edges_soft_ops (
    id INTEGER PRIMARY KEY,
    chunk_id TEXT,
    file_path TEXT,
    file_uuid TEXT,
    inferred_op TEXT,
    confidence TEXT
);

CREATE TABLE _edges_git_state (
    id INTEGER PRIMARY KEY,
    session_id TEXT,
    repo_path TEXT,
    phase TEXT,
    ts INTEGER,
    head_commit TEXT,
    branch TEXT
);
"""

# SOMA identity edge tables (1:N, NO PK on chunk_id — NOT in view)
SOMA_MODULE_DDL = """
CREATE TABLE IF NOT EXISTS _edges_file_identity (
    chunk_id TEXT NOT NULL,
    file_uuid TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_efi_chunk ON _edges_file_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_efi_uuid ON _edges_file_identity(file_uuid);

CREATE TABLE IF NOT EXISTS _edges_repo_identity (
    chunk_id TEXT NOT NULL,
    repo_root TEXT NOT NULL,
    is_tracked INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_eri_chunk ON _edges_repo_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eri_root ON _edges_repo_identity(repo_root);

CREATE TABLE IF NOT EXISTS _edges_content_identity (
    chunk_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    blob_hash TEXT,
    old_blob_hash TEXT
);
CREATE INDEX IF NOT EXISTS idx_eci_chunk ON _edges_content_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eci_hash ON _edges_content_identity(content_hash);
CREATE INDEX IF NOT EXISTS idx_eci_blob ON _edges_content_identity(blob_hash);

CREATE TABLE IF NOT EXISTS _edges_url_identity (
    chunk_id TEXT NOT NULL,
    url_uuid TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eui_chunk ON _edges_url_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eui_uuid ON _edges_url_identity(url_uuid);
"""


EMBED_DIM = 128

def _make_embedding(dim=EMBED_DIM):
    """Create a fake float32 embedding BLOB."""
    return struct.pack(f'{dim}f', *([0.1] * dim))


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def empty_cell():
    """In-memory chunk-atom cell with schema but no data."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript(CHUNK_ATOM_DDL)
    yield conn
    conn.close()


@pytest.fixture
def qmem_cell():
    """In-memory cell populated with qmem-style doc data (small corpus)."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript(CHUNK_ATOM_DDL)

    # Populate _meta
    conn.execute("INSERT INTO _meta VALUES ('description', 'Test cell for schema validation. Small doc corpus.')")
    conn.execute("INSERT INTO _meta VALUES ('version', '2.0.0')")
    conn.execute("INSERT INTO _meta VALUES ('schema', 'chunk-atom')")
    # No view config in _meta — views passed as params to regenerate_views()

    # 3 sources, 9 chunks (3 per source)
    sources = [
        ('src-arch', '260201', 'present', 'architecture', 'Architecture Overview', '/context/current/architecture.md'),
        ('src-log1', '260210', 'past', 'changelog', 'SQL-First Refactor', '/context/changes/code/260210.md'),
        ('src-plan', '260211', 'future', 'plan', 'Migration Plan', '/context/intended/proximate/plan.md'),
    ]
    for sid, fdate, temporal, dtype, title, path in sources:
        conn.execute(
            "INSERT INTO _raw_sources (source_id, file_date, temporal, doc_type, title, source_path) VALUES (?,?,?,?,?,?)",
            (sid, fdate, temporal, dtype, title, path)
        )

    chunk_id = 0
    for sid, _, temporal, dtype, _, _ in sources:
        for i in range(3):
            cid = f"{sid}:{i}"
            conn.execute(
                "INSERT INTO _raw_chunks (id, content, embedding, timestamp) VALUES (?,?,?,?)",
                (cid, f"Content for {sid} section {i}", _make_embedding(), 1707000000 + chunk_id)
            )
            conn.execute(
                "INSERT INTO _edges_source (chunk_id, source_id, source_type, position) VALUES (?,?,?,?)",
                (cid, sid, 'markdown', i)
            )
            conn.execute(
                "INSERT INTO _types_docpac (chunk_id, temporal, doc_type) VALUES (?,?,?)",
                (cid, temporal, dtype)
            )
            chunk_id += 1

    # Graph enrichment for sources
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-arch', 0.85, 1, 0, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-log1', 0.30, 0, 0, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('src-plan', 0.45, 0, 1, 2)")

    # Presets
    conn.execute("INSERT INTO _presets (name, description, params, sql) VALUES (?, ?, ?, ?)", (
        'hub-sources', 'Find high-centrality sources', 'min_centrality (required)',
        "-- @name: hub-sources\n-- @params: min_centrality (required)\nSELECT source_id, centrality\nFROM _enrich_source_graph\nWHERE centrality >= :min_centrality\nORDER BY centrality DESC"))
    conn.execute("INSERT INTO _presets (name, description, params, sql) VALUES (?, ?, ?, ?)", (
        'overview', 'Cell overview counts and sources', '',
        "-- @name: overview\n-- @multi: true\n\n-- @query: counts\nSELECT COUNT(*) as n FROM _raw_chunks;\n\n-- @query: sources\nSELECT source_id, doc_type FROM _raw_sources ORDER BY file_date DESC;"))
    conn.execute("INSERT INTO _presets (name, description, params, sql) VALUES (?, ?, ?, ?)", (
        'all-chunks', 'All chunks ordered by time', '',
        "SELECT id, content, timestamp FROM _raw_chunks ORDER BY timestamp"))

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def claude_code_cell():
    """In-memory cell with claude_code-specific module tables (full schema)."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript(CHUNK_ATOM_DDL)
    conn.executescript(CLAUDE_CODE_MODULE_DDL)
    conn.executescript(SOMA_MODULE_DDL)

    # Populate _meta with view renames
    meta = [
        ('description', 'Session provenance for Claude Code. Test fixture.'),
        ('version', '2.0.0'),
        ('schema', 'chunk-atom'),
        # No view config in _meta — views passed as params to regenerate_views()
    ]
    conn.executemany("INSERT INTO _meta VALUES (?,?)", meta)

    # 2 sessions, 6 chunks
    for sid in ['session-aaa', 'session-bbb']:
        conn.execute(
            "INSERT INTO _raw_sources (source_id, file_date, temporal, doc_type, title) VALUES (?,?,?,?,?)",
            (sid, '260211', 'past', 'session', f'Test session {sid}')
        )
        for i in range(3):
            cid = f"{sid}:{i}"
            tool = ['Read', 'Edit', 'Bash'][i]
            role = 'assistant'
            conn.execute(
                "INSERT INTO _raw_chunks (id, content, embedding, timestamp) VALUES (?,?,?,?)",
                (cid, f"{tool} operation in {sid}", _make_embedding(), 1707000000 + i)
            )
            conn.execute(
                "INSERT INTO _edges_source (chunk_id, source_id, source_type, position) VALUES (?,?,?,?)",
                (cid, sid, 'claude-code', i)
            )
            conn.execute(
                "INSERT INTO _edges_tool_ops (chunk_id, tool_name, target_file, success) VALUES (?,?,?,?)",
                (cid, tool, f'/path/to/file{i}.py', 1)
            )
            conn.execute(
                "INSERT INTO _types_message (chunk_id, type, role, chunk_number) VALUES (?,?,?,?)",
                (cid, 'tool_call', role, i)
            )

    # SOMA identity (1:N)
    conn.execute("INSERT INTO _edges_file_identity VALUES ('session-aaa:1', 'uuid-file1')")
    conn.execute("INSERT INTO _edges_file_identity VALUES ('session-aaa:1', 'uuid-file2')")  # 1:N
    conn.execute("INSERT INTO _edges_repo_identity VALUES ('session-aaa:1', 'root-abc', 1)")

    # Graph enrichment
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('session-aaa', 0.72, 1, 0, 1)")
    conn.execute("INSERT INTO _enrich_source_graph VALUES ('session-bbb', 0.15, 0, 0, 2)")

    conn.commit()
    yield conn
    conn.close()


