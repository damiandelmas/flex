"""
Dev.to cell compiler — ingests Dev.to articles into a Flex cell.

Source = article (one top-level article + its flattened comments).
Chunk = individual article body OR comment.

Entry point:
    python -m flex.modules.devto.compile.worker \
        --cell devto \
        --tags "claude,ai,mcp" \
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


DEFAULT_SINCE_DAYS = 30
DEFAULT_TAG_LIMIT = 10
DEFAULT_COMMENT_LIMIT = 20
DEFAULT_REFRESH_INTERVAL = 6 * 60 * 60


def parse_csv(value: str | None) -> list[str]:
    """Parse a comma-separated CLI/config value into lowercase-ish tokens."""
    if not value:
        return []
    return [part.strip() for part in value.split(',') if part.strip()]


def parse_days(value: str | None, default: int = DEFAULT_SINCE_DAYS) -> int:
    """Parse strings like 7d or 30 into a positive day count."""
    if not value:
        return default
    raw = value.strip().lower().rstrip('d')
    days = int(raw)
    if days < 0:
        raise ValueError("day count must be non-negative")
    return days


def build_scope_meta(tags: list[str], authors: list[str], since_days: int,
                     tag_limit: int | None, comment_limit: int | None,
                     include_comments: bool) -> dict:
    """Build the public scope contract stored in _meta."""
    return {
        "tags": tags,
        "authors": authors,
        "since_days": since_days,
        "tag_limit": tag_limit,
        "comment_limit": comment_limit,
        "include_comments": include_comments,
    }


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
    tags TEXT,
    reading_time INTEGER,
    embedding BLOB
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'devto',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

-- TYPES LAYER (devto-specific metadata per chunk)
CREATE TABLE IF NOT EXISTS _types_devto (
    chunk_id TEXT PRIMARY KEY,
    item_type TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    url TEXT,
    tags TEXT,
    reading_time INTEGER
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


# =====================================================
# Grouping
# =====================================================

def group_into_threads(articles, comments_map):
    """Group articles with their flattened comments.

    articles: list of normalized article dicts
    comments_map: dict of article_id -> [flattened comment dicts]

    Returns list of (article_dict, [comment_dicts]) sorted by created_utc.
    """
    threads = []
    for article in articles:
        article_id = article.get('id', '')
        article_comments = comments_map.get(article_id, [])
        # Sort comments by created_utc
        article_comments.sort(key=lambda c: c.get('created_utc', 0))
        threads.append((article, article_comments))

    threads.sort(key=lambda t: t[0].get('created_utc', 0))
    return threads


# =====================================================
# Ingest
# =====================================================

def ingest(threads, db):
    """INSERT threads into chunk-atom tables.

    Each article -> 1 source + 1 chunk (the article body).
    Each comment -> 1 chunk linked to the article's source.
    """
    total_sources = 0
    total_chunks = 0

    for article, comments in threads:
        article_id = article.get('id', '')
        source_id = f"devto_{article_id}"
        title = article.get('title', '')
        author = article.get('author', '')
        score = article.get('score', 0)
        num_comments = article.get('num_comments', 0)
        url = article.get('url', '')
        created_utc = article.get('created_utc', 0)
        tags = article.get('tags', '[]')
        reading_time = article.get('reading_time', 0)
        file_date = ''
        if created_utc:
            dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
            file_date = dt.strftime('%y%m%d')

        # INSERT source (article)
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, author,
             score, num_comments, url, tags, reading_time, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (source_id, title, url, file_date, author,
              score, num_comments, url, tags, reading_time))

        # INSERT article body as chunk 0
        content = article.get('content', '') or title
        chunk_id = f"{source_id}:0"

        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, content, created_utc))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'devto', 0)
        """, (chunk_id, source_id))

        db.execute("""
            INSERT OR IGNORE INTO _types_devto
            (chunk_id, item_type, author, score, url, tags, reading_time)
            VALUES (?, 'article', ?, ?, ?, ?, ?)
        """, (chunk_id, author, score, url, tags, reading_time))

        total_chunks += 1

        # INSERT comments as chunks 1..N
        for i, comment in enumerate(comments, 1):
            c_chunk_id = f"{source_id}:{i}"
            c_content = comment.get('content', '') or comment.get('body', '') or ''
            c_author = comment.get('author', '')
            c_score = comment.get('score', 0)
            c_created = comment.get('created_utc', 0)
            c_url = comment.get('url', '')

            if not c_content or c_content in ('[deleted]', '[removed]'):
                continue

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
                VALUES (?, ?, NULL, ?)
            """, (c_chunk_id, c_content, c_created))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'devto', ?)
            """, (c_chunk_id, source_id, i))

            db.execute("""
                INSERT OR IGNORE INTO _types_devto
                (chunk_id, item_type, author, score, url, tags, reading_time)
                VALUES (?, 'comment', ?, ?, ?, '[]', 0)
            """, (c_chunk_id, c_author, c_score, c_url))

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
        description='Index Dev.to articles into a Flex cell')
    parser.add_argument('--cell', default='devto',
                        help='Cell name or path (default: devto)')
    parser.add_argument('--tags', default=None,
                        help='Comma-separated tags to pull (default: built-in list)')
    parser.add_argument('--authors', default=None,
                        help='Comma-separated Dev.to usernames to pull in addition to tags')
    parser.add_argument('--since', default='30d',
                        help='How far back to pull (default: 30d)')
    parser.add_argument('--limit', type=int, default=DEFAULT_TAG_LIMIT,
                        help='Max articles per tag or author (default: 10)')
    parser.add_argument('--comment-limit', type=int, default=DEFAULT_COMMENT_LIMIT,
                        help='Max comments per article; use 0 to skip comments (default: 20)')
    parser.add_argument('--no-comments', action='store_true',
                        help='Do not fetch article comments')
    parser.add_argument('--refresh-interval', type=int,
                        default=DEFAULT_REFRESH_INTERVAL,
                        help='Registry refresh interval in seconds (default: 21600)')
    parser.add_argument('--graph', action='store_true',
                        help='Build similarity graph after ingest')
    parser.add_argument('--append', action='store_true',
                        help='Append to existing cell')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show stats without indexing')
    parser.add_argument('--description', default=None,
                        help='Cell description')
    args = parser.parse_args()

    from flex.modules.devto.compile.forem_api import (
        pull_articles, pull_articles_by_author, pull_comments, DEFAULT_TAGS,
    )

    # Parse --since
    since_days = parse_days(args.since)
    after_ts = int(time.time()) - (since_days * 86400)

    # Resolve tags
    tags = parse_csv(args.tags) if args.tags else DEFAULT_TAGS
    authors = parse_csv(args.authors)
    include_comments = not args.no_comments and args.comment_limit != 0
    comment_limit = None if args.comment_limit is None else max(args.comment_limit, 0)
    article_limit = None if args.limit is None or args.limit < 1 else args.limit

    # Pull articles per tag (deduplicate by article ID)
    seen_ids = set()
    all_articles = []
    comments_map = {}

    for tag in tags:
        print(f"Pulling tag: {tag}")
        articles = pull_articles(tag, after_ts=after_ts, limit=article_limit)
        for article in articles:
            aid = article['id']
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            all_articles.append(article)

            # Pull comments for this article
            if include_comments and article.get('num_comments', 0) > 0:
                article_comments = pull_comments(aid, limit=comment_limit)
                if article_comments:
                    comments_map[aid] = article_comments

    for author in authors:
        print(f"Pulling author: {author}")
        articles = pull_articles_by_author(
            author, after_ts=after_ts, limit=article_limit)
        for article in articles:
            aid = article['id']
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            all_articles.append(article)

            if include_comments and article.get('num_comments', 0) > 0:
                article_comments = pull_comments(aid, limit=comment_limit)
                if article_comments:
                    comments_map[aid] = article_comments

    print(f"\nTotal: {len(all_articles)} unique articles, "
          f"{sum(len(v) for v in comments_map.values())} comments")

    threads = group_into_threads(all_articles, comments_map)

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

    db.executescript(SCHEMA_DDL)
    print("  Schema ready.")

    t0 = time.time()

    # Ingest
    sources, chunks = ingest(threads, db)
    print(f"  Ingested: {sources} sources, {chunks} chunks")

    validate_cell(db)
    print("  Validation passed.")

    # Embed
    print("  Embedding...")
    embedded = embed_new(db)
    print(f"  Embedded: {embedded} chunks")

    # Log op
    log_op(db, 'devto_ingest', '_raw_chunks',
           params={'tags': tags, 'sources': sources,
                   'chunks': chunks, 'embedded': embedded},
           rows_affected=chunks,
           source='devto/compile/worker.py')
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

    # Install presets (general + devto-specific)
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / 'retrieve' / 'presets' / 'general'
    if preset_dir.exists():
        install_presets(db, preset_dir)
    devto_preset_dir = Path(__file__).parent.parent / 'stock' / 'presets'
    if devto_preset_dir.exists():
        install_presets(db, devto_preset_dir)
    print("  Presets installed.")

    # Set metadata
    set_meta(db, 'cell_type', 'devto')
    set_meta(db, 'description', args.description or 'Dev.to articles and comments')
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())

    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())

    # Track tags in cell
    set_meta(db, 'tags', json.dumps(tags))
    set_meta(db, 'authors', json.dumps(authors))
    set_meta(db, 'tag_limit', str(article_limit or ''))
    set_meta(db, 'comment_limit', str(comment_limit if include_comments else 0))
    set_meta(db, 'include_comments', '1' if include_comments else '0')
    set_meta(db, 'since_days', str(since_days))
    set_meta(db, 'scope', json.dumps(build_scope_meta(
        tags, authors, since_days, article_limit, comment_limit,
        include_comments,
    )))

    # Register
    from flex.registry import register_cell
    cell_name = args.cell if not args.cell.endswith('.db') else Path(args.cell).stem
    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type='devto',
        description=args.description or 'Dev.to articles and comments',
        lifecycle='refresh',
        refresh_interval=args.refresh_interval,
        refresh_module='flex.modules.devto.compile.refresh',
        active=True,
        unlisted=False,
    )
    print(f"  Registered as '{cell_name}'")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — {cell_path}")
    db.close()


if __name__ == '__main__':
    main()
