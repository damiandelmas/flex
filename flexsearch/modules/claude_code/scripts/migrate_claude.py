#!/usr/bin/env python3
"""
Migrate claude cell from flat-table schema to chunk-atom schema.

Claude cell is claude.ai conversations (not Claude Code sessions).
Simpler than thread: no SOMA tables, no tool_ops, no edge tables to rename.
Two parallel data models (docs+chunks and conversations+messages) — we migrate
from docs+chunks (docpac-style IDs, has embeddings on both levels).

Usage:
    python flexsearch/modules/claude_code/scripts/migrate_claude.py
"""

import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent.parent))

from flexsearch.views import regenerate_views

DB_PATH = Path.home() / ".qmem/cells/projects/claude/main.db"


def table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def migrate(db_path: Path):
    if not db_path.exists():
        print(f"ERROR: {db_path} not found")
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
            title TEXT,
            source TEXT,
            file_date TEXT,
            temporal TEXT,
            doc_type TEXT,
            model TEXT,
            message_count INTEGER,
            embedding BLOB
        );

        CREATE TABLE IF NOT EXISTS _edges_source (
            chunk_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_type TEXT DEFAULT 'claude-ai',
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

        CREATE TABLE IF NOT EXISTS _meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        -- ═══ 2. EXTRACT FROM FLAT TABLES ═══

        INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
        SELECT id, content, embedding, NULL FROM chunks;

        INSERT OR IGNORE INTO _raw_sources (source_id, title, source, file_date,
            temporal, doc_type, model, message_count, embedding)
        SELECT id, title, source, file_date,
            temporal, doc_type, model, message_count, embedding
        FROM docs;

        INSERT OR IGNORE INTO _edges_source (chunk_id, source_id, source_type, position)
        SELECT id, doc_id, 'claude-ai', idx
        FROM chunks WHERE doc_id IS NOT NULL;

        INSERT OR IGNORE INTO _enrich_source_graph (source_id, centrality, is_hub, is_bridge, community_id)
        SELECT id, centrality, is_hub, is_bridge, community_id
        FROM docs WHERE centrality IS NOT NULL;

        INSERT OR IGNORE INTO _types_message (chunk_id, type, role, chunk_number)
        SELECT id, type, type, idx
        FROM chunks WHERE type IS NOT NULL AND type != '';

        -- ═══ 3. HEURISTIC ENRICHMENT ═══

        INSERT OR IGNORE INTO _enrich_types (chunk_id, semantic_role, confidence)
        SELECT c.id,
            CASE WHEN tm.role = 'user' THEN 'prompt'
                 WHEN tm.role = 'assistant' THEN 'response'
                 ELSE 'message'
            END, 0.5
        FROM _raw_chunks c
        LEFT JOIN _types_message tm ON c.id = tm.chunk_id;

        -- ═══ 4. METADATA ═══

        INSERT OR REPLACE INTO _meta VALUES ('description',
            'Claude.ai conversation archive. Each source is a conversation, each chunk is a message (user prompt or assistant response). Temporal dimension from folder structure. ~32K chunks, ~2K conversations.');
        INSERT OR REPLACE INTO _meta VALUES ('version', '2.0.0');
        INSERT OR REPLACE INTO _meta VALUES ('schema', 'chunk-atom');
        INSERT OR REPLACE INTO _meta VALUES ('migrated_at', datetime('now'));

        INSERT OR REPLACE INTO _meta VALUES ('view:messages:level', 'chunk');
        INSERT OR REPLACE INTO _meta VALUES ('view:conversations:level', 'source');

        INSERT OR REPLACE INTO _meta VALUES ('view:messages:rename:semantic_role', 'kind');

        -- ═══ 5. FTS + INDEXES ═══

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
        CREATE INDEX IF NOT EXISTS idx_tm_chunk ON _types_message(chunk_id);

        COMMIT;
    """)

    t1 = time.time()
    print(f"  Transaction committed in {t1 - t0:.1f}s")

    # ── 2. RENAME CONFLICTING FLAT TABLES ──
    # Old `messages` and `conversations` tables conflict with view names.
    # Rename them to _flat_* so regenerate_views() can create views.
    print("  Renaming conflicting flat tables...")
    for old, new in [('messages', '_flat_messages'), ('conversations', '_flat_conversations')]:
        if table_exists(db, old) and not table_exists(db, new):
            db.execute(f"ALTER TABLE [{old}] RENAME TO [{new}]")
            print(f"    {old} -> {new}")
    db.commit()

    # Also drop old FTS tables that reference flat tables
    for fts in ['messages_fts', 'docs_fts']:
        if table_exists(db, fts):
            db.execute(f"DROP TABLE IF EXISTS [{fts}]")
            print(f"    Dropped {fts}")
    db.commit()

    # ── 3. GENERATE VIEWS ──
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
        UNION ALL SELECT 'source_graph', COUNT(*) FROM _enrich_source_graph
        UNION ALL SELECT 'types_message', COUNT(*) FROM _types_message
        UNION ALL SELECT 'enrich_types', COUNT(*) FROM _enrich_types
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
        ("SELECT COUNT(*) FROM conversations", "COUNT(*) conversations"),
        ("SELECT * FROM messages LIMIT 5", "SELECT * messages LIMIT 5"),
        ("SELECT * FROM conversations ORDER BY centrality DESC LIMIT 10", "conversations ORDER BY centrality"),
    ]:
        t_start = time.time()
        db.execute(query).fetchall()
        elapsed = (time.time() - t_start) * 1000
        status = "OK" if elapsed < 500 else "SLOW"
        print(f"  {label:45s} {elapsed:8.1f}ms  {status}")

    # ── 7. SAMPLE ──
    print("\n  === SAMPLE ===")
    for r in db.execute("SELECT * FROM messages LIMIT 2").fetchall():
        print(f"    {dict(r) if hasattr(r, 'keys') else r}")

    total = time.time() - t0
    print(f"\n  Total migration time: {total:.1f}s")

    db.close()


if __name__ == "__main__":
    migrate(DB_PATH)
