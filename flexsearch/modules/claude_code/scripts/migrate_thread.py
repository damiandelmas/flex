#!/usr/bin/env python3
"""
Migrate thread-test cell from flat-table schema to chunk-atom schema.

Operates on a COPY of the thread cell. The real thread cell is never touched.
Single SQL transaction for table creation + data extraction. Views generated
programmatically via regenerate_views() after commit.

Usage:
    python flexsearch/modules/claude_code/scripts/migrate_thread.py
"""

import sqlite3
import sys
import time
from pathlib import Path

# Add flexsearch to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent.parent))

from flexsearch.views import regenerate_views

DB_PATH = Path.home() / ".qmem/cells/projects/thread-test/main.db"


def table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def safe_rename(db: sqlite3.Connection, old: str, new: str):
    """Rename table if source exists and target doesn't."""
    if table_exists(db, old) and not table_exists(db, new):
        db.execute(f"ALTER TABLE [{old}] RENAME TO [{new}]")
        print(f"  Renamed {old} -> {new}")
    elif table_exists(db, new):
        print(f"  Skipped {old} -> {new} (target already exists)")
    else:
        print(f"  Skipped {old} -> {new} (source doesn't exist)")


def migrate(db_path: Path):
    if not db_path.exists():
        print(f"ERROR: {db_path} not found")
        print("Run: cp -r ~/.qmem/cells/projects/thread/ ~/.qmem/cells/projects/thread-test/")
        sys.exit(1)

    db = sqlite3.connect(str(db_path))
    db.execute("PRAGMA journal_mode=WAL")

    t0 = time.time()
    print(f"Migrating {db_path} ...")

    # ── Pre-flight counts ──
    flat_chunks = db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    flat_docs = db.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
    print(f"  Flat schema: {flat_chunks:,} chunks, {flat_docs:,} docs")

    # ── 1. CREATE TABLES + EXTRACT DATA (single transaction) ──
    print("  Creating tables and extracting data...")
    db.executescript("""
        BEGIN;

        -- ═══ 1. CREATE NEW TABLES ═══

        CREATE TABLE IF NOT EXISTS _raw_chunks (
            id TEXT PRIMARY KEY,
            content TEXT,
            embedding BLOB,
            timestamp INTEGER
        );

        CREATE TABLE IF NOT EXISTS _raw_sources (
            source_id TEXT PRIMARY KEY,
            project TEXT,
            title TEXT,
            summary TEXT,
            source TEXT,
            file_date TEXT,
            start_time INTEGER,
            end_time INTEGER,
            duration_minutes INTEGER,
            message_count INTEGER,
            episode_count INTEGER,
            primary_cwd TEXT,
            model TEXT,
            embedding BLOB
        );

        CREATE TABLE IF NOT EXISTS _edges_source (
            chunk_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_type TEXT DEFAULT 'claude-code',
            position INTEGER
        );

        CREATE TABLE IF NOT EXISTS _enrich_source_graph (
            source_id TEXT PRIMARY KEY,
            centrality REAL,
            is_hub INTEGER DEFAULT 0,
            is_bridge INTEGER DEFAULT 0,
            community_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS _types_message (
            chunk_id TEXT PRIMARY KEY,
            type TEXT,
            role TEXT,
            chunk_number INTEGER
        );

        CREATE TABLE IF NOT EXISTS _enrich_types (
            chunk_id TEXT PRIMARY KEY,
            semantic_role TEXT,
            confidence REAL
        );

        CREATE TABLE IF NOT EXISTS _edges_file_identity (
            chunk_id TEXT NOT NULL,
            file_uuid TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _edges_repo_identity (
            chunk_id TEXT NOT NULL,
            repo_root TEXT NOT NULL,
            is_tracked INTEGER
        );

        CREATE TABLE IF NOT EXISTS _edges_content_identity (
            chunk_id TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            blob_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS _edges_url_identity (
            chunk_id TEXT NOT NULL,
            url_uuid TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _edges_tool_ops (
            chunk_id TEXT PRIMARY KEY,
            tool_name TEXT,
            target_file TEXT,
            success INTEGER,
            cwd TEXT,
            git_branch TEXT
        );

        CREATE TABLE IF NOT EXISTS _meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        -- ═══ 2. EXTRACT FROM FLAT TABLES ═══

        INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
        SELECT id, content, embedding, timestamp FROM chunks;

        INSERT OR IGNORE INTO _raw_sources (source_id, project, title, summary, source,
            file_date, start_time, end_time, duration_minutes, message_count, episode_count,
            primary_cwd, model, embedding)
        SELECT id, facet, title, content, source,
            file_date, start_time, end_time, duration_minutes, message_count, episode_count,
            primary_cwd, model, embedding
        FROM docs;

        INSERT OR IGNORE INTO _edges_source (chunk_id, source_id, source_type, position)
        SELECT id, doc_id, 'claude-code',
            COALESCE(chunk_number, rowid) FROM chunks WHERE doc_id IS NOT NULL;

        INSERT OR IGNORE INTO _enrich_source_graph (source_id, centrality, is_hub, is_bridge, community_id)
        SELECT id, centrality, is_hub, is_bridge, community_id
        FROM docs WHERE centrality IS NOT NULL;

        INSERT OR IGNORE INTO _types_message (chunk_id, type, role, chunk_number)
        SELECT id,
            CASE WHEN type = 'episode' THEN 'tool_call' ELSE type END,
            role,
            chunk_number
        FROM chunks WHERE type IS NOT NULL AND type != '';

        INSERT OR IGNORE INTO _edges_tool_ops (chunk_id, tool_name, target_file, success, cwd, git_branch)
        SELECT id, tool_name, target_file, success, cwd, git_branch
        FROM chunks WHERE tool_name IS NOT NULL;

        INSERT OR IGNORE INTO _edges_file_identity (chunk_id, file_uuid)
        SELECT id, file_uuid FROM chunks WHERE file_uuid IS NOT NULL AND file_uuid != '';

        INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked)
        SELECT id, repo_root, is_tracked FROM chunks WHERE repo_root IS NOT NULL AND repo_root != '';

        INSERT OR IGNORE INTO _edges_content_identity (chunk_id, content_hash, blob_hash)
        SELECT id, content_hash, blob_hash FROM chunks
        WHERE content_hash IS NOT NULL AND content_hash != '';

        INSERT OR IGNORE INTO _edges_url_identity (chunk_id, url_uuid)
        SELECT id, url_uuid FROM chunks WHERE url_uuid IS NOT NULL AND url_uuid != '';

        -- ═══ 3. METADATA ═══

        INSERT OR REPLACE INTO _meta VALUES ('description',
            'Session provenance for Claude Code. Each doc is a session, each chunk is a tool call/prompt/response. Views: messages (chunk-level), sessions (source-level). ~375K chunks, ~5.5K sessions, 29 projects.');
        INSERT OR REPLACE INTO _meta VALUES ('version', '2.0.0');
        INSERT OR REPLACE INTO _meta VALUES ('schema', 'chunk-atom');
        INSERT OR REPLACE INTO _meta VALUES ('migrated_at', datetime('now'));

        INSERT OR REPLACE INTO _meta VALUES ('view:messages:level', 'chunk');
        INSERT OR REPLACE INTO _meta VALUES ('view:sessions:level', 'source');

        INSERT OR REPLACE INTO _meta VALUES ('view:messages:rename:tool_name', 'action');
        INSERT OR REPLACE INTO _meta VALUES ('view:messages:rename:semantic_role', 'kind');

        -- ═══ 4. FTS + INDEXES ═══

        DROP TABLE IF EXISTS chunks_fts;
        CREATE VIRTUAL TABLE chunks_fts USING fts5(
            content,
            content='_raw_chunks',
            content_rowid='rowid'
        );
        INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild');

        CREATE TRIGGER IF NOT EXISTS raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
            INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
        END;
        CREATE TRIGGER IF NOT EXISTS raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
            INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
        END;
        CREATE TRIGGER IF NOT EXISTS raw_chunks_au AFTER UPDATE ON _raw_chunks BEGIN
            INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
            INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
        END;

        CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);
        CREATE INDEX IF NOT EXISTS idx_eto_chunk ON _edges_tool_ops(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_efi_chunk ON _edges_file_identity(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_eri_chunk ON _edges_repo_identity(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_eci_chunk ON _edges_content_identity(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_eui_chunk ON _edges_url_identity(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_tm_chunk ON _types_message(chunk_id);

        COMMIT;
    """)

    t1 = time.time()
    print(f"  Transaction committed in {t1 - t0:.1f}s")

    # ── 2. RENAME EXISTING EDGE TABLES (outside transaction — ALTER can't be in executescript transaction) ──
    print("  Renaming edge tables...")
    safe_rename(db, 'spawned_agents', '_edges_delegations')
    safe_rename(db, 'soft_file_ops', '_edges_soft_ops')
    safe_rename(db, 'git_states', '_edges_git_state')
    db.commit()

    # ── 3. GENERATE VIEWS (programmatic, single source of truth) ──
    print("  Generating views via regenerate_views()...")
    regenerate_views(db)

    t2 = time.time()
    print(f"  Views generated in {t2 - t1:.1f}s")

    # ── 4. COUNT VALIDATION ──
    print("\n  === COUNT VALIDATION ===")
    counts = db.execute("""
        SELECT 'raw_chunks' as t, COUNT(*) FROM _raw_chunks
        UNION ALL SELECT 'raw_sources', COUNT(*) FROM _raw_sources
        UNION ALL SELECT 'edges_source', COUNT(*) FROM _edges_source
        UNION ALL SELECT 'tool_ops', COUNT(*) FROM _edges_tool_ops
        UNION ALL SELECT 'file_identity', COUNT(*) FROM _edges_file_identity
        UNION ALL SELECT 'repo_identity', COUNT(*) FROM _edges_repo_identity
        UNION ALL SELECT 'content_identity', COUNT(*) FROM _edges_content_identity
        UNION ALL SELECT 'source_graph', COUNT(*) FROM _enrich_source_graph
        UNION ALL SELECT 'types_message', COUNT(*) FROM _types_message
        UNION ALL SELECT 'url_identity', COUNT(*) FROM _edges_url_identity
        UNION ALL SELECT 'delegations', COUNT(*) FROM _edges_delegations
        UNION ALL SELECT 'soft_ops', COUNT(*) FROM _edges_soft_ops
    """).fetchall()

    for name, count in counts:
        print(f"    {name:25s} {count:>10,}")

    new_chunks = dict(counts)['raw_chunks']
    new_sources = dict(counts)['raw_sources']
    print(f"\n  Chunk match: {new_chunks:,} vs {flat_chunks:,} {'OK' if new_chunks == flat_chunks else 'MISMATCH!'}")
    print(f"  Source match: {new_sources:,} vs {flat_docs:,} {'OK' if new_sources == flat_docs else 'MISMATCH!'}")

    # ── 5. VIEW VALIDATION ──
    print("\n  === VIEW VALIDATION ===")
    views = db.execute(
        "SELECT name FROM sqlite_master WHERE type='view'"
    ).fetchall()
    print(f"  Views: {[v[0] for v in views]}")

    for view_name in [v[0] for v in views]:
        cols = db.execute(f"PRAGMA table_info([{view_name}])").fetchall()
        print(f"  {view_name}: {len(cols)} columns — {', '.join(c[1] for c in cols)}")

    # ── 6. PERFORMANCE VALIDATION ──
    print("\n  === PERFORMANCE VALIDATION ===")
    for query, label in [
        ("SELECT COUNT(*) FROM messages", "COUNT(*) messages"),
        ("SELECT COUNT(*) FROM sessions", "COUNT(*) sessions"),
        ("SELECT * FROM messages LIMIT 5", "SELECT * messages LIMIT 5"),
        ("SELECT * FROM sessions ORDER BY centrality DESC LIMIT 10", "sessions ORDER BY centrality"),
    ]:
        t_start = time.time()
        db.execute(query).fetchall()
        elapsed = (time.time() - t_start) * 1000
        status = "OK" if elapsed < 500 else "SLOW"
        print(f"  {label:45s} {elapsed:8.1f}ms  {status}")

    total = time.time() - t0
    print(f"\n  Total migration time: {total:.1f}s")

    db.close()


if __name__ == "__main__":
    migrate(DB_PATH)
