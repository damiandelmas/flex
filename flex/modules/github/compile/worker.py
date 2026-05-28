"""
GitHub Issues cell compiler -- ingests GitHub issues into a Flex cell.

Source = issue (one GitHub issue + its comments).
Chunk = issue body (position 0) OR individual comment (position 1..N).

Entry point:
    python -m flex.modules.github.compile.worker \
        --cell github \
        --repos "anthropics/claude-code,punkpeye/awesome-mcp-servers" \
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
    repo TEXT,
    issue_number INTEGER,
    state TEXT,
    labels TEXT,
    embedding BLOB
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'github',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

-- TYPES LAYER (github-specific metadata per chunk)
CREATE TABLE IF NOT EXISTS _types_github (
    chunk_id TEXT PRIMARY KEY,
    item_type TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    url TEXT,
    repo TEXT,
    issue_number INTEGER,
    state TEXT,
    labels TEXT
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

def group_into_threads(issues):
    """Group issues into threads (source + children).

    Each issue already has _comments pre-loaded by the API client.
    Returns list of (issue_dict, [comment_dicts]) sorted by created_utc.
    """
    threads = []
    for issue in issues:
        comments = issue.pop("_comments", [])
        # Sort comments by created_utc
        comments.sort(key=lambda c: c.get("created_utc", 0))
        threads.append((issue, comments))

    threads.sort(key=lambda t: t[0].get("created_utc", 0))
    return threads


# =====================================================
# Ingest
# =====================================================

def ingest(threads, db):
    """INSERT threads into chunk-atom tables.

    Each issue -> 1 source + 1 chunk (the issue body).
    Each comment -> 1 chunk linked to the issue's source.
    """
    total_sources = 0
    total_chunks = 0

    for issue, comments in threads:
        source_id = issue.get("source_id", "")
        title = issue.get("title", "")
        author = issue.get("author", "")
        score = issue.get("score", 0)
        num_comments = issue.get("num_comments", 0)
        url = issue.get("url", "")
        created_utc = issue.get("created_utc", 0)
        repo = issue.get("repo", "")
        issue_number = issue.get("issue_number", 0)
        state = issue.get("state", "open")
        labels = issue.get("labels", "[]")

        file_date = ""
        if created_utc:
            dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
            file_date = dt.strftime("%y%m%d")

        # INSERT source
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, author,
             score, num_comments, url, repo, issue_number,
             state, labels, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (source_id, title, url, file_date, author,
              score, num_comments, url, repo, issue_number,
              state, labels))

        # INSERT issue body as chunk 0
        content = issue.get("content", "") or issue.get("body", "") or title
        chunk_id = f"{source_id}:0"

        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, content, created_utc))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'github', 0)
        """, (chunk_id, source_id))

        db.execute("""
            INSERT OR IGNORE INTO _types_github
            (chunk_id, item_type, author, score, url, repo, issue_number, state, labels)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chunk_id, "issue", author, score, url, repo, issue_number, state, labels))

        total_chunks += 1

        # INSERT comments as chunks 1..N
        for i, comment in enumerate(comments, 1):
            c_chunk_id = f"{source_id}:{i}"
            c_content = comment.get("content", "") or comment.get("body", "") or ""
            c_author = comment.get("author", "")
            c_score = comment.get("score", 0)
            c_created = comment.get("created_utc", 0)
            c_url = comment.get("url", "")

            if not c_content or c_content in ("[deleted]", "[removed]"):
                continue

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
                VALUES (?, ?, NULL, ?)
            """, (c_chunk_id, c_content, c_created))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'github', ?)
            """, (c_chunk_id, source_id, i))

            db.execute("""
                INSERT OR IGNORE INTO _types_github
                (chunk_id, item_type, author, score, url, repo, issue_number, state, labels)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (c_chunk_id, "comment", c_author, c_score, c_url,
                  repo, issue_number, state, labels))

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
        description='Index GitHub Issues data into a Flex cell')
    parser.add_argument('--cell', default='github',
                        help='Cell name or path (default: github)')
    parser.add_argument('--repos', default=None,
                        help='Comma-separated repos (owner/name)')
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
    parser.add_argument('--max-issues', type=int, default=None,
                        help='Max issues to pull per run (default: 50)')
    parser.add_argument('--max-comments', type=int, default=None,
                        help='Max comments per issue (default: 25)')
    parser.add_argument('--refresh-interval', type=int, default=86400,
                        help='Registry refresh interval in seconds (default: 86400)')
    parser.add_argument('--no-schedule', action='store_true',
                        help='Register as static instead of scheduled refresh')
    args = parser.parse_args()

    from flex.modules.github.compile.github_api import (
        pull_issues, DEFAULT_REPOS, DEFAULT_QUERIES,
        DEFAULT_MAX_ISSUES, DEFAULT_MAX_COMMENTS_PER_ISSUE,
    )

    max_issues = DEFAULT_MAX_ISSUES if args.max_issues is None else args.max_issues
    max_comments = (
        DEFAULT_MAX_COMMENTS_PER_ISSUE
        if args.max_comments is None
        else args.max_comments
    )

    # Parse --since
    since_days = int(args.since.strip().lower().rstrip('d'))
    after_ts = int(time.time()) - (since_days * 86400)

    repos = [v.strip() for v in args.repos.split(',') if v.strip()] if args.repos is not None else DEFAULT_REPOS
    queries = [v.strip() for v in args.queries.split(',') if v.strip()] if args.queries is not None else DEFAULT_QUERIES

    # Pull data
    print(f"Pulling GitHub issues (since {since_days}d ago)...")
    print(f"  Repos: {repos}")
    print(f"  Queries: {queries}")
    print(f"  Limits: max_issues={max_issues}, max_comments={max_comments}")
    all_issues = pull_issues(
        queries=queries,
        repos=repos,
        after_ts=after_ts,
        max_issues=max_issues,
        max_comments_per_issue=max_comments,
    )

    # Group into threads
    threads = group_into_threads(all_issues)
    print(f"  {len(threads)} issues with comments")

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
    log_op(db, 'github_ingest', '_raw_chunks',
           params={'sources': sources, 'chunks': chunks, 'embedded': embedded},
           rows_affected=chunks,
           source='github/compile/worker.py')
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
    regenerate_views(db, {'chunks': 'chunk', 'sources': 'source'})

    # Presets
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / 'retrieve' / 'presets' / 'general'
    if preset_dir.exists():
        install_presets(db, preset_dir)
    platform_preset_dir = Path(__file__).parent.parent / 'stock' / 'presets'
    if platform_preset_dir.exists():
        install_presets(db, platform_preset_dir)

    # Metadata
    set_meta(db, 'cell_type', 'github')
    set_meta(db, 'description', args.description or 'GitHub Issues content')
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'repos', json.dumps(repos))
    set_meta(db, 'queries', json.dumps(queries))
    set_meta(db, 'max_issues', str(max_issues))
    set_meta(db, 'max_comments_per_issue', str(max_comments))

    # Register
    from flex.registry import register_cell
    cell_name = args.cell if not args.cell.endswith('.db') else Path(args.cell).stem
    lifecycle = 'static' if args.no_schedule else 'refresh'
    register_cell(
        name=cell_name, path=cell_path, cell_type='github',
        description=args.description or 'GitHub Issues content',
        lifecycle=lifecycle,
        refresh_interval=None if args.no_schedule else args.refresh_interval,
        refresh_module=None if args.no_schedule else 'flex.modules.github.compile.refresh',
    )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s -- {cell_path}")
    db.close()


if __name__ == '__main__':
    main()
