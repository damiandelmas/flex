"""
Reddit cell compiler — ingests Reddit JSONL corpus into a Flex cell.

Reads posts.jsonl + comments.jsonl from corpus directory, creates
chunk-atom tables, embeds, builds similarity graph.

Source = thread (one post).
Chunk = post body OR individual comment.

Entry point:
    python -m flex.modules.reddit.compile.worker \
        --corpus /path/to/corpus/subreddit \
        --cell reddit \
        --graph
"""

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from flex.core import open_cell, get_meta, set_meta, validate_cell, log_op
from flex.modules.reddit.compile.scope import should_skip_post, should_skip_comment


# ═════════════════════════════════════════════════════
# Scope defaults — the LEVER
# ═════════════════════════════════════════════════════
#
# These are written to _meta at ingest time. Views and the similarity graph
# read them at query / enrichment time. To tune the cell, update _meta —
# no re-ingest required.
#
#   UPDATE _meta SET value='25' WHERE key='scope.posts.min_score';
#
# Or via the flex CLI once it ships:
#   flex scope reddit set posts.min_score 25
#
# Defaults are deliberately permissive — `all_threads` / `all_chunks` views
# bypass the filter entirely so the agent can always reach the raw data.
SCOPE_DEFAULTS = {
    'scope.posts.min_score':    '10',   # filter for `threads` / `chunks` views
    'scope.comments.min_score':  '2',
    'scope.comments.min_chars': '20',
    'scope.graph.min_score':    '50',   # stricter threshold for similarity graph
    'scope.graph.min_comments':  '5',
}


def ensure_scope_defaults(db):
    """Write scope defaults to _meta, without overwriting user tuning."""
    for key, default in SCOPE_DEFAULTS.items():
        existing = get_meta(db, key)
        if existing is None:
            set_meta(db, key, default)


def build_graph_where(db) -> str:
    """Build the WHERE fragment passed to flex.manage.meditate --where.

    Reads scope.graph.* from _meta so the lever stays tunable. Returns an
    empty string if no thresholds are set (meditate treats that as no filter).
    """
    min_score = get_meta(db, 'scope.graph.min_score')
    min_comments = get_meta(db, 'scope.graph.min_comments')
    parts = []
    if min_score is not None:
        parts.append(f"score >= {int(min_score)}")
    if min_comments is not None:
        parts.append(f"num_comments >= {int(min_comments)}")
    return " AND ".join(parts)


# ═════════════════════════════════════════════════════
# SCHEMA DDL
# ═════════════════════════════════════════════════════

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
    subreddit TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    num_comments INTEGER DEFAULT 0,
    url TEXT,
    embedding BLOB
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'reddit',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

-- TYPES LAYER (reddit-specific metadata per chunk)
CREATE TABLE IF NOT EXISTS _types_reddit (
    chunk_id TEXT PRIMARY KEY,
    post_type TEXT,
    author TEXT,
    subreddit TEXT,
    score INTEGER DEFAULT 0,
    parent_id TEXT,
    depth INTEGER DEFAULT 0,
    permalink TEXT
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


# ═════════════════════════════════════════════════════
# Parsing
# ═════════════════════════════════════════════════════


def load_jsonl(path):
    """Load a JSONL file, return list of dicts."""
    items = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def group_into_threads(posts, comments):
    """Group posts and comments into threads.

    Each thread = one post (source) + its comments (chunks).
    Returns list of (post_dict, [comment_dicts]) sorted by created_utc.
    """
    # Index comments by link_id (post they belong to)
    # link_id format: "t3_POSTID" — strip the "t3_" prefix
    comment_map = {}
    for c in comments:
        link_id = c.get('link_id', '')
        if link_id.startswith('t3_'):
            post_id = link_id[3:]
        else:
            post_id = link_id
        comment_map.setdefault(post_id, []).append(c)

    threads = []
    for post in posts:
        post_id = post.get('id', '')
        post_comments = comment_map.get(post_id, [])
        # Sort comments by created_utc
        post_comments.sort(key=lambda c: c.get('created_utc', 0))
        threads.append((post, post_comments))

    # Sort threads by created_utc
    threads.sort(key=lambda t: t[0].get('created_utc', 0))
    return threads


# ═════════════════════════════════════════════════════
# Ingest
# ═════════════════════════════════════════════════════


def ingest(threads, db, subreddit):
    """INSERT threads into chunk-atom tables.

    Each post → 1 source + 1 chunk (the post body).
    Each comment → 1 chunk linked to the post's source.

    Scope gate here is tombstone-only: drops posts with no recoverable content
    and comments with empty/deleted bodies. Quality thresholds (score, etc.)
    are NOT applied here — they live in _meta and are enforced at the view
    and graph layers so they can be tuned without re-ingest.
    """
    total_sources = 0
    total_chunks = 0
    skipped_posts = 0
    skipped_comments = 0

    for post, comments in threads:
        if should_skip_post(post):
            skipped_posts += 1
            continue

        post_id = post.get('id', '')
        source_id = f"{subreddit}_{post_id}"
        title = post.get('title', '')
        author = post.get('author', '[deleted]')
        score = post.get('score', 0)
        num_comments = post.get('num_comments', 0)
        url = post.get('url', '')
        created_utc = post.get('created_utc', 0)
        file_date = ''
        if created_utc:
            dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
            file_date = dt.strftime('%y%m%d')

        # INSERT source (thread)
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, subreddit, author,
             score, num_comments, url, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (source_id, title, url, file_date, subreddit, author,
              score, num_comments, url))

        # INSERT post body as chunk 0
        post_body = post.get('content', '') or post.get('body', '') or ''
        if not post_body:
            post_body = title  # self-posts with no body, use title
        chunk_id = f"{source_id}:0"

        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, post_body, created_utc))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'reddit', 0)
        """, (chunk_id, source_id))

        db.execute("""
            INSERT OR IGNORE INTO _types_reddit
            (chunk_id, post_type, author, subreddit, score, parent_id, depth, permalink)
            VALUES (?, 'post', ?, ?, ?, NULL, 0, ?)
        """, (chunk_id, author, subreddit, score, post.get('permalink', '')))

        total_chunks += 1

        # INSERT comments as chunks 1..N
        for i, comment in enumerate(comments, 1):
            # Scope gate: drop low-signal comments before any INSERT.
            if should_skip_comment(comment):
                skipped_comments += 1
                continue

            c_id = comment.get('id', f'c{i}')
            c_chunk_id = f"{source_id}:{i}"
            c_body = comment.get('content', '') or comment.get('body', '') or ''
            c_author = comment.get('author', '[deleted]')
            c_score = comment.get('score', 0)
            c_created = comment.get('created_utc', 0)
            c_parent = comment.get('parent_id', '')
            c_depth = comment.get('depth', 0)
            c_permalink = comment.get('permalink', '')

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
                VALUES (?, ?, NULL, ?)
            """, (c_chunk_id, c_body, c_created))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'reddit', ?)
            """, (c_chunk_id, source_id, i))

            db.execute("""
                INSERT OR IGNORE INTO _types_reddit
                (chunk_id, post_type, author, subreddit, score, parent_id, depth, permalink)
                VALUES (?, 'comment', ?, ?, ?, ?, ?, ?)
            """, (c_chunk_id, c_author, subreddit, c_score,
                  c_parent, c_depth, c_permalink))

            total_chunks += 1

        db.commit()
        total_sources += 1

    if skipped_posts or skipped_comments:
        print(f"  Scope (tombstones): skipped {skipped_posts} posts, {skipped_comments} comments")

    return total_sources, total_chunks


from flex.compile.embed import embed_new  # noqa: F401 — shared pipeline


# ═════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description='Index Reddit JSONL corpus into a Flex cell')
    parser.add_argument('--corpus', required=True,
                        help='Corpus directory containing posts.jsonl + comments.jsonl')
    parser.add_argument('--cell', default='reddit',
                        help='Cell name or path (default: reddit)')
    parser.add_argument('--subreddit', default=None,
                        help='Override subreddit name (auto-detected from corpus dir name)')
    parser.add_argument('--graph', action='store_true',
                        help='Build similarity graph after ingest')
    parser.add_argument('--append', action='store_true',
                        help='Append to existing cell (skip schema creation)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show corpus stats without indexing')
    parser.add_argument('--description', default=None,
                        help='Cell description')
    args = parser.parse_args()

    corpus_dir = Path(args.corpus)
    subreddit = args.subreddit or corpus_dir.name

    posts_file = corpus_dir / 'posts.jsonl'
    comments_file = corpus_dir / 'comments.jsonl'

    if not posts_file.exists():
        print(f"No posts.jsonl in {corpus_dir}")
        sys.exit(1)

    # Load corpus
    print(f"Loading r/{subreddit} corpus from {corpus_dir}...")
    posts = load_jsonl(str(posts_file))
    comments = load_jsonl(str(comments_file)) if comments_file.exists() else []
    print(f"  {len(posts)} posts, {len(comments)} comments")

    # Group into threads
    threads = group_into_threads(posts, comments)
    print(f"  {len(threads)} threads")

    if args.dry_run:
        # Show stats
        total_chunks = sum(1 + len(cs) for _, cs in threads)
        print(f"  Would create: {len(threads)} sources, ~{total_chunks} chunks")
        return

    # Resolve / create cell
    cell_path = args.cell
    if not cell_path.endswith('.db'):
        # Treat as cell name — put in ~/.flex/cells/
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

    # Write scope defaults to _meta (the lever). Non-destructive — keeps
    # existing values if a user has already tuned them.
    ensure_scope_defaults(db)
    db.commit()

    t0 = time.time()

    # Ingest
    sources, chunks = ingest(threads, db, subreddit)
    print(f"  Ingested: {sources} sources, {chunks} chunks")

    # Validate
    validate_cell(db)
    print("  Validation passed.")

    # Embed
    print("  Embedding...")
    embedded = embed_new(db)
    print(f"  Embedded: {embedded} chunks")

    # Log op
    log_op(db, 'reddit_ingest', '_raw_chunks',
           params={'subreddit': subreddit, 'sources': sources,
                   'chunks': chunks, 'embedded': embedded},
           rows_affected=chunks,
           source='reddit/compile/worker.py')
    db.commit()

    # Graph (optional — runs as subprocess to avoid engine import coupling).
    # Reads scope.graph.min_score and scope.graph.min_comments from _meta
    # so graph enrichment ignores low-signal sources without dropping them
    # from _raw_sources. Lever is tunable without re-ingest.
    if args.graph:
        import subprocess
        graph_where = build_graph_where(db)
        print(f"  Building similarity graph (where: {graph_where or 'none'})...")
        cmd = [sys.executable, '-m', 'flex.manage.meditate', '--cell', cell_path]
        if graph_where:
            cmd += ['--where', graph_where]
        subprocess.run(cmd, check=True)

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

    # Install presets (general + reddit-specific)
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / 'retrieve' / 'presets' / 'general'
    if preset_dir.exists():
        install_presets(db, preset_dir)
    reddit_preset_dir = Path(__file__).parent.parent / 'stock' / 'presets'
    if reddit_preset_dir.exists():
        install_presets(db, reddit_preset_dir)
    print("  Presets installed.")

    # Set metadata
    set_meta(db, 'cell_type', 'reddit')
    set_meta(db, 'description', args.description or f'r/{subreddit} posts and comments')
    set_meta(db, 'corpus_path', str(corpus_dir))
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())

    # Cursor tracking for incremental refresh
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())

    # Track all subreddits in cell
    existing = db.execute(
        "SELECT DISTINCT subreddit FROM _raw_sources"
    ).fetchall()
    all_subs = sorted({r[0] for r in existing if r[0]})
    set_meta(db, 'subreddits', json.dumps(all_subs))

    # Register
    from flex.registry import register_cell
    cell_name = args.cell if not args.cell.endswith('.db') else Path(args.cell).stem
    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type='reddit',
        description=args.description or f'r/{subreddit} posts and comments',
        corpus_path=str(corpus_dir),
    )
    print(f"  Registered as '{cell_name}'")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — {cell_path}")
    db.close()


if __name__ == '__main__':
    main()
