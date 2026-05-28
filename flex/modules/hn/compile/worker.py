"""
Hacker News cell compiler -- ingests HN data into a Flex cell.

Source = story (one HN submission).
Chunk 0 = story (title + story_text).
Chunk 1..N = comments on that story.

Entry point:
    python -m flex.modules.hn.compile.worker \
        --cell hn \
        --queries "claude code,semantic search" \
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


# =====================================================
# SCHEMA DDL
# =====================================================

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
    hn_url TEXT,
    embedding BLOB
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'hn',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

-- TYPES LAYER (HN-specific metadata per chunk)
CREATE TABLE IF NOT EXISTS _types_hn (
    chunk_id TEXT PRIMARY KEY,
    item_type TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    url TEXT,
    story_id TEXT,
    parent_id TEXT,
    hn_url TEXT
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


DEFAULT_QUERIES = [
    "claude code",
    "AI coding agent",
    "semantic search sqlite",
    "MCP server",
    "vector search local",
    "embedding search",
    "agentic coding",
    "conversation history search",
    "AI developer tools cli",
    "local first search",
    "sqlite vector",
]


# =====================================================
# Grouping
# =====================================================

def group_into_threads(stories, comments):
    """Group stories and comments into threads.

    Each thread = one story (source) + its comments (chunks).
    Returns list of (story_dict, [comment_dicts]) sorted by created_utc.

    Args:
        stories: list of normalized story dicts
        comments: list of normalized comment dicts
    """
    # Index comments by story_id
    comment_map = {}
    for c in comments:
        sid = str(c.get('story_id', ''))
        if sid:
            comment_map.setdefault(sid, []).append(c)

    threads = []
    for story in stories:
        story_id = str(story.get('id', ''))
        story_comments = comment_map.get(story_id, [])
        # Sort comments by created_utc
        story_comments.sort(key=lambda c: c.get('created_utc', 0))
        threads.append((story, story_comments))

    # Sort threads by created_utc
    threads.sort(key=lambda t: t[0].get('created_utc', 0))
    return threads


# =====================================================
# Ingest
# =====================================================

def ingest(threads, db):
    """INSERT threads into chunk-atom tables.

    Each story -> 1 source + 1 chunk (chunk 0).
    Each comment -> 1 chunk linked to the story's source.
    """
    total_sources = 0
    total_chunks = 0

    for story, comments in threads:
        item_id = story.get('id', '')
        source_id = f"hn_{item_id}"
        title = story.get('title', '')
        author = story.get('author', '')
        score = story.get('score', 0)
        num_comments = story.get('num_comments', 0)
        url = story.get('url', '')
        hn_url = story.get('hn_url', '')
        created_utc = story.get('created_utc', 0)
        file_date = ''
        if created_utc:
            dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
            file_date = dt.strftime('%y%m%d')

        # INSERT source (thread)
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, author,
             score, num_comments, url, hn_url, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (source_id, title, url, file_date, author,
              score, num_comments, url, hn_url))

        # INSERT story as chunk 0
        content = story.get('content', '') or story.get('body', '') or title
        chunk_id = f"{source_id}:0"

        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, content, created_utc))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'hn', 0)
        """, (chunk_id, source_id))

        db.execute("""
            INSERT OR IGNORE INTO _types_hn
            (chunk_id, item_type, author, score, url, story_id, parent_id, hn_url)
            VALUES (?, 'story', ?, ?, ?, ?, NULL, ?)
        """, (chunk_id, author, score, url, item_id, hn_url))

        total_chunks += 1

        # INSERT comments as chunks 1..N
        for i, comment in enumerate(comments, 1):
            c_chunk_id = f"{source_id}:{i}"
            c_content = comment.get('content', '') or comment.get('body', '') or ''
            c_author = comment.get('author', '')
            c_score = comment.get('score', 0)
            c_created = comment.get('created_utc', 0)
            c_url = comment.get('url', '')
            c_story_id = comment.get('story_id', '')
            c_parent_id = comment.get('parent_id', '')

            if not c_content or c_content in ('[deleted]', '[removed]', '[flagged]'):
                continue

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
                VALUES (?, ?, NULL, ?)
            """, (c_chunk_id, c_content, c_created))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'hn', ?)
            """, (c_chunk_id, source_id, i))

            db.execute("""
                INSERT OR IGNORE INTO _types_hn
                (chunk_id, item_type, author, score, url, story_id, parent_id, hn_url)
                VALUES (?, 'comment', ?, ?, ?, ?, ?, NULL)
            """, (c_chunk_id, c_author, c_score, c_url,
                  str(c_story_id), str(c_parent_id)))

            total_chunks += 1

        db.commit()
        total_sources += 1

    return total_sources, total_chunks


from flex.compile.embed import embed_new  # noqa: F401 — shared pipeline


# =====================================================
# CLI
# =====================================================

def main():
    parser = argparse.ArgumentParser(
        description='Index Hacker News data into a Flex cell')
    parser.add_argument('--cell', default='hn',
                        help='Cell name or path (default: hn)')
    parser.add_argument('--queries', default=None,
                        help='Comma-separated search queries')
    parser.add_argument('--since', default='30d',
                        help='How far back to pull (default: 30d)')
    parser.add_argument('--graph', action='store_true',
                        help='Build similarity graph after ingest')
    parser.add_argument('--append', action='store_true',
                        help='Append to existing cell')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show stats without indexing')
    parser.add_argument('--description', default=None,
                        help='Cell description')
    parser.add_argument('--authors', default=None,
                        help='Comma-separated HN usernames to keep in @me')
    parser.add_argument('--max-stories', type=int, default=None,
                        help='Maximum stories to ingest across all queries')
    parser.add_argument('--max-comments-per-story', type=int, default=None,
                        help='Maximum comments to ingest per story')
    parser.add_argument('--no-comments', action='store_true',
                        help='Only ingest story chunks')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Maximum Algolia pages per API call')
    parser.add_argument('--hits-per-page', type=int, default=None,
                        help='Algolia hitsPerPage for each API call')
    args = parser.parse_args()

    # Parse --since
    since_days = int(args.since.strip().lower().rstrip('d'))
    after_ts = int(time.time()) - (since_days * 86400)

    queries = args.queries.split(',') if args.queries else DEFAULT_QUERIES

    from flex.modules.hn.compile.algolia import pull_stories, pull_comments_for_story

    # Pull stories
    all_stories = []
    all_comments = []
    for q in queries:
        if args.max_stories is not None and len(all_stories) >= args.max_stories:
            break
        print(f"Query: {q}")
        stories = pull_stories(
            q, after_ts=after_ts, max_pages=args.max_pages,
            hits_per_page=args.hits_per_page)
        if args.max_stories is not None:
            remaining = max(0, args.max_stories - len(all_stories))
            stories = stories[:remaining]
        all_stories.extend(stories)

        # Pull comments for each story
        if not args.no_comments:
            for story in stories:
                sid = story.get('id', '')
                if sid:
                    comments = pull_comments_for_story(
                        sid, after_ts=after_ts, quiet=True,
                        max_pages=args.max_pages,
                        hits_per_page=args.hits_per_page)
                    if args.max_comments_per_story is not None:
                        comments = comments[:max(0, args.max_comments_per_story)]
                    all_comments.extend(comments)

    # Deduplicate stories by id
    seen_stories = {}
    for s in all_stories:
        seen_stories.setdefault(s['id'], s)
    all_stories = list(seen_stories.values())

    # Deduplicate comments by id
    seen_comments = {}
    for c in all_comments:
        seen_comments.setdefault(c['id'], c)
    all_comments = list(seen_comments.values())

    print(f"\nTotal: {len(all_stories)} unique stories, {len(all_comments)} unique comments")

    threads = group_into_threads(all_stories, all_comments)

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

    db = open_cell(cell_path)
    if not args.append:
        db.executescript(SCHEMA_DDL)

    t0 = time.time()

    # Ingest
    sources, chunks = ingest(threads, db)
    print(f"  Ingested: {sources} sources, {chunks} chunks")

    validate_cell(db)

    # Embed
    print("  Embedding...")
    embedded = embed_new(db)
    print(f"  Embedded: {embedded} chunks")

    # Log
    log_op(db, 'hn_ingest', '_raw_chunks',
           params={'sources': sources, 'chunks': chunks, 'embedded': embedded},
           rows_affected=chunks,
           source='hn/compile/worker.py')
    db.commit()

    # Graph (runs as subprocess to avoid engine import coupling)
    if args.graph:
        import subprocess
        print("  Building similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        from flex.views import install_views
        install_views(db, views_dir)
    from flex.views import regenerate_views
    regenerate_views(db)

    # Presets
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / 'retrieve' / 'presets' / 'general'
    if preset_dir.exists():
        install_presets(db, preset_dir)
    hn_preset_dir = Path(__file__).parent.parent / 'stock' / 'presets'
    if hn_preset_dir.exists():
        install_presets(db, hn_preset_dir)

    # Metadata
    set_meta(db, 'cell_type', 'hn')
    set_meta(db, 'description', args.description or 'Hacker News content')
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'queries', json.dumps(queries))
    if args.authors:
        authors = [a.strip() for a in args.authors.split(',') if a.strip()]
        set_meta(db, 'authors', json.dumps(authors))
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())

    # Register
    from flex.registry import register_cell
    cell_name = args.cell if not args.cell.endswith('.db') else Path(args.cell).stem
    register_cell(
        name=cell_name, path=cell_path, cell_type='hn',
        description=args.description or 'Hacker News content',
        lifecycle='refresh',
        refresh_interval=21600,
        refresh_module='flex.modules.hn.compile.refresh',
    )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s -- {cell_path}")
    db.close()


if __name__ == '__main__':
    main()
