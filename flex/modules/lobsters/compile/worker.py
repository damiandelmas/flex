"""
Lobsters cell compiler — ingests Lobsters data into a Flex cell.

Source = story (one top-level story + its comments).
Chunk = individual story or comment.

Entry point:
    python -m flex.modules.lobsters.compile.worker \
        --cell lobsters \
        --tags ai,ml,python \
        --graph
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from flex.core import open_cell, set_meta, validate_cell, log_op


# =================================================
# SCHEMA DDL
# =================================================

SCHEMA_DDL = """
-- RAW LAYER
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
    author TEXT,
    score INTEGER DEFAULT 0,
    num_comments INTEGER DEFAULT 0,
    url TEXT,
    tags TEXT,
    story_url TEXT,
    embedding BLOB
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'lobsters',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

-- TYPES LAYER (lobsters-specific metadata per chunk)
CREATE TABLE IF NOT EXISTS _types_lobsters (
    chunk_id TEXT PRIMARY KEY,
    item_type TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    url TEXT,
    tags TEXT,
    depth INTEGER DEFAULT 0,
    parent_id TEXT
);

-- ENRICHMENT LAYER
CREATE TABLE IF NOT EXISTS _enrich_source_graph (
    source_id TEXT PRIMARY KEY,
    centrality REAL,
    is_hub INTEGER DEFAULT 0,
    is_bridge INTEGER DEFAULT 0,
    community_id INTEGER
);

-- PRESETS
CREATE TABLE IF NOT EXISTS _presets (
    name TEXT PRIMARY KEY,
    description TEXT,
    params TEXT DEFAULT '',
    sql TEXT
);

-- METADATA + FTS
CREATE TABLE IF NOT EXISTS _meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);

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
"""


# =================================================
# Grouping
# =================================================

def group_into_threads(stories):
    """Stories already come with comments attached from the API client.

    Returns list of (story_dict, [comment_dicts]) sorted by created_utc.
    """
    threads = []
    for story in stories:
        comments = story.get("comments", [])
        threads.append((story, comments))

    threads.sort(key=lambda t: t[0].get("created_utc", 0))
    return threads


# =================================================
# Ingest
# =================================================

def ingest(threads, db):
    """INSERT threads into chunk-atom tables.

    Each story -> 1 source + 1 chunk.
    Each comment -> 1 chunk linked to the story's source.
    """
    total_sources = 0
    total_chunks = 0

    for story, comments in threads:
        short_id = story.get("id", "")
        source_id = f"lob_{short_id}"
        title = story.get("title", "")
        author = story.get("author", "")
        score = story.get("score", 0)
        num_comments = story.get("num_comments", 0)
        url = story.get("url", "")
        story_url = story.get("story_url", "")
        created_utc = story.get("created_utc", 0)
        tags = json.dumps(story.get("tags", []))
        file_date = ""
        if created_utc:
            dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
            file_date = dt.strftime("%y%m%d")

        # INSERT source
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, author,
             score, num_comments, url, tags, story_url, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (source_id, title, url, file_date, author,
              score, num_comments, url, tags, story_url))

        # INSERT story as chunk 0
        content = story.get("content", "") or title
        chunk_id = f"{source_id}:0"

        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, content, created_utc))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'lobsters', 0)
        """, (chunk_id, source_id))

        db.execute("""
            INSERT OR IGNORE INTO _types_lobsters
            (chunk_id, item_type, author, score, url, tags, depth, parent_id)
            VALUES (?, 'story', ?, ?, ?, ?, 0, NULL)
        """, (chunk_id, author, score, url, tags))

        total_chunks += 1

        # INSERT comments as chunks 1..N
        for i, comment in enumerate(comments, 1):
            c_chunk_id = f"{source_id}:{i}"
            c_body = comment.get("content", "") or comment.get("body", "") or ""
            c_author = comment.get("author", "")
            c_score = comment.get("score", 0)
            c_created = comment.get("created_utc", 0)
            c_depth = comment.get("depth", 0)
            c_parent = comment.get("parent_id", "")
            c_url = comment.get("url", "")

            if not c_body or c_body in ("[deleted]", "[removed]"):
                continue

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
                VALUES (?, ?, NULL, ?)
            """, (c_chunk_id, c_body, c_created))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'lobsters', ?)
            """, (c_chunk_id, source_id, i))

            db.execute("""
                INSERT OR IGNORE INTO _types_lobsters
                (chunk_id, item_type, author, score, url, tags, depth, parent_id)
                VALUES (?, 'comment', ?, ?, ?, NULL, ?, ?)
            """, (c_chunk_id, c_author, c_score, c_url, c_depth, c_parent))

            total_chunks += 1

        db.commit()
        total_sources += 1

    return total_sources, total_chunks


from flex.compile.embed import embed_new  # noqa: F401 — shared pipeline


# =================================================
# CLI
# =================================================

def main():
    parser = argparse.ArgumentParser(
        description='Index Lobsters data into a Flex cell')
    parser.add_argument('--cell', default='lobsters',
                        help='Cell name or path (default: lobsters)')
    parser.add_argument('--tags', default=None,
                        help='Comma-separated tags to pull (default: built-in list)')
    parser.add_argument('--pages', default=3, type=int,
                        help='Number of newest pages to pull (default: 3)')
    parser.add_argument('--since', default='30d',
                        help='How far back to pull (default: 30d)')
    parser.add_argument('--limit', default=None, type=int,
                        help='Maximum number of unique stories to ingest')
    parser.add_argument('--graph', action='store_true',
                        help='Build similarity graph after ingest')
    parser.add_argument('--append', action='store_true',
                        help='Append to existing cell')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show stats without indexing')
    parser.add_argument('--description', default=None,
                        help='Cell description')
    args = parser.parse_args()

    from flex.modules.lobsters.compile.lobsters_api import (
        pull_stories_with_comments, DEFAULT_TAGS,
    )

    # Parse --since
    since_days = int(args.since.strip().lower().rstrip('d'))
    after_ts = int(time.time()) - (since_days * 86400)

    # Parse --tags
    tags = args.tags.split(',') if args.tags else DEFAULT_TAGS

    print(f"Pulling Lobsters stories...")
    print(f"  Tags: {', '.join(tags)}")
    print(f"  Since: {datetime.fromtimestamp(after_ts, tz=timezone.utc).date()}")
    print()

    stories = pull_stories_with_comments(
        tags=tags, pages=args.pages, after=after_ts, limit=args.limit)
    threads = group_into_threads(stories)

    if args.dry_run:
        total_chunks = sum(1 + len(cs) for _, cs in threads)
        print(f"  Would create: {len(threads)} sources, ~{total_chunks} chunks")
        return

    # Resolve / create cell
    cell_path = args.cell
    if not cell_path.endswith('.db'):
        from flex.registry import CELLS_DIR
        CELLS_DIR.mkdir(parents=True, exist_ok=True)
        cell_path = str(CELLS_DIR / f"{args.cell}.db")

    if not args.append and os.path.exists(cell_path):
        os.remove(cell_path)
        print(f"  Removed old cell: {cell_path}")

    db = open_cell(cell_path)

    # Create schema
    if not args.append:
        db.executescript(SCHEMA_DDL)
        print("  Schema created.")

    t0 = time.time()

    # Ingest
    sources, chunks = ingest(threads, db)
    print(f"  Ingested: {sources} sources, {chunks} chunks")

    # Validate
    validate_cell(db)
    print("  Validation passed.")

    # Embed
    print("  Embedding...")
    embedded = embed_new(db)
    print(f"  Embedded: {embedded} chunks")

    # Log op
    log_op(db, 'lobsters_ingest', '_raw_chunks',
           params={'tags': tags, 'sources': sources,
                   'chunks': chunks, 'embedded': embedded},
           rows_affected=chunks,
           source='lobsters/compile/worker.py')
    db.commit()

    # Graph (optional — runs as subprocess to avoid engine import coupling)
    if args.graph:
        import subprocess
        print("  Building similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Install views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        from flex.views import install_views
        install_views(db, views_dir)
        print("  Curated views installed.")

    # Regenerate auto views
    from flex.views import regenerate_views
    regenerate_views(db)
    print("  Views regenerated.")

    # Install presets (general + lobsters-specific)
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / 'retrieve' / 'presets' / 'general'
    if preset_dir.exists():
        install_presets(db, preset_dir)
    lobsters_preset_dir = Path(__file__).parent.parent / 'stock' / 'presets'
    if lobsters_preset_dir.exists():
        install_presets(db, lobsters_preset_dir)
    print("  Presets installed.")

    # Set metadata
    set_meta(db, 'cell_type', 'lobsters')
    set_meta(db, 'description', args.description or 'Lobsters stories and comments')
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'tags', json.dumps(tags))

    # Register
    from flex.registry import register_cell
    cell_name = args.cell if not args.cell.endswith('.db') else Path(args.cell).stem
    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type='lobsters',
        description=args.description or 'Lobsters stories and comments',
        lifecycle='refresh',
        refresh_interval=6 * 60 * 60,
        refresh_module='flex.modules.lobsters.compile.refresh',
        active=True,
        unlisted=False,
    )
    print(f"  Registered as '{cell_name}'")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — {cell_path}")
    db.close()


if __name__ == '__main__':
    main()
