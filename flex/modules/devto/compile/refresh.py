"""
Incremental Dev.to cell refresh.

Reads last_pull_ts from cell _meta, pulls new articles since then,
ingests, embeds, and optionally rebuilds the graph.

Idempotent: INSERT OR IGNORE means re-running is safe.

Usage:
    python -m flex.modules.devto.compile.refresh --cell devto
    python -m flex.modules.devto.compile.refresh --cell devto --dry-run
    python -m flex.modules.devto.compile.refresh --cell devto --tags claude,ai,mcp
    python -m flex.modules.devto.compile.refresh --cell devto --since 30d --graph
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op
from flex.modules.devto.compile.forem_api import (
    pull_articles, pull_articles_by_author, pull_comments, DEFAULT_TAGS,
)
from flex.modules.devto.compile.worker import (
    SCHEMA_DDL, build_scope_meta, group_into_threads, ingest, embed_new,
    parse_csv, parse_days,
)


GRAPH_REFRESH_THRESHOLD = 20


def refresh(cell_path: str, tags: list[str] | None = None,
            graph: bool = False, dry_run: bool = False,
            since_days: int | None = None,
            limit: int | None = None,
            comment_limit: int | None = None,
            include_comments: bool | None = None,
            authors: list[str] | None = None) -> dict:
    """Pull new articles and ingest into existing devto cell.

    Args:
        since_days: Override cursor -- pull this many days back instead of
                    using last_pull_ts.

    Returns stats dict with counts.
    """
    db = open_cell(cell_path)

    # Ensure schema exists (idempotent)
    db.executescript(SCHEMA_DDL)

    # Read cursor (--since overrides stored cursor)
    if since_days is not None:
        last_pull_ts = int(time.time()) - (since_days * 86400)
    else:
        last_pull_ts = int(get_meta(db, 'last_pull_ts') or '0')

    # Resolve tags
    if not tags:
        stored = get_meta(db, 'tags')
        tags = json.loads(stored) if stored else DEFAULT_TAGS
    if authors is None:
        authors = json.loads(get_meta(db, 'authors') or '[]')

    if limit is None:
        stored_limit = get_meta(db, 'tag_limit')
        limit = int(stored_limit) if stored_limit else None
    if comment_limit is None:
        stored_comment_limit = get_meta(db, 'comment_limit')
        comment_limit = int(stored_comment_limit) if stored_comment_limit else None
    if include_comments is None:
        include_comments = get_meta(db, 'include_comments') != '0'

    after_dt = datetime.fromtimestamp(last_pull_ts, tz=timezone.utc) if last_pull_ts else None
    print(f"Cell: {cell_path}")
    print(f"Tags: {', '.join(tags)}")
    if authors:
        print(f"Authors: {', '.join(authors)}")
    if limit:
        print(f"Limit: {limit} articles per tag/author")
    if not include_comments:
        print("Comments: disabled")
    elif comment_limit is not None:
        print(f"Comments: max {comment_limit} per article")
    print(f"Last pull: {after_dt.isoformat() if after_dt else 'never'}")
    print()

    if dry_run:
        print("Dry run -- checking for new data per tag...")
        for tag in tags:
            articles = pull_articles(
                tag, after_ts=last_pull_ts, quiet=True, limit=limit)
            print(f"  tag={tag}: {len(articles)} articles available")
        for author in authors:
            articles = pull_articles_by_author(
                author, after_ts=last_pull_ts, quiet=True, limit=limit)
            print(f"  author={author}: {len(articles)} articles available")
        db.close()
        return {'dry_run': True}

    # Pull articles per tag (deduplicate by article ID)
    seen_ids = set()
    all_articles = []
    comments_map = {}

    for tag in tags:
        print(f"{'=' * 50}")
        print(f"Tag: {tag}")
        print(f"{'=' * 50}")

        articles = pull_articles(tag, after_ts=last_pull_ts, limit=limit)

        for article in articles:
            aid = article['id']
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            all_articles.append(article)

            # Pull comments for articles with comments
            if include_comments and article.get('num_comments', 0) > 0:
                article_comments = pull_comments(aid, limit=comment_limit)
                if article_comments:
                    comments_map[aid] = article_comments

    # ═════════════════════════════════════════════════════
    # Author self-pull — our articles
    # ═════════════════════════════════════════════════════
    author_cursors = json.loads(get_meta(db, 'author_cursors') or '{}')
    total_author_articles = 0

    for author in authors:
        print(f"{'=' * 50}")
        print(f"Author: {author}")
        print(f"{'=' * 50}")

        if since_days is not None:
            actor_after = last_pull_ts
        else:
            actor_after = author_cursors.get(author, last_pull_ts)

        a_articles = pull_articles_by_author(
            author, after_ts=actor_after, limit=limit)
        total_author_articles += len(a_articles)

        for article in a_articles:
            aid = article['id']
            if aid in seen_ids:
                continue
            seen_ids.add(aid)
            all_articles.append(article)
            if include_comments and article.get('num_comments', 0) > 0:
                article_comments = pull_comments(aid, limit=comment_limit)
                if article_comments:
                    comments_map[aid] = article_comments

        if a_articles:
            max_ts = max(a.get('created_utc', 0) for a in a_articles)
            author_cursors[author] = max(
                author_cursors.get(author, 0), max_ts)
            set_meta(db, 'author_cursors', json.dumps(author_cursors))
            db.commit()

    if not all_articles:
        print("\nNo new articles found.")
        db.close()
        return {'sources': 0, 'chunks': 0,
                'author_articles': total_author_articles}

    threads = group_into_threads(all_articles, comments_map)
    sources, chunks = ingest(threads, db)
    print(f"\n  Ingested: {sources} sources, {chunks} chunks "
          f"(incl. {total_author_articles} from authors)")

    if chunks == 0:
        print("\nNo new data to embed.")
        db.close()
        return {'sources': 0, 'chunks': 0}

    # Embed new chunks
    print(f"\nEmbedding {chunks} new chunks...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded}")

    # Graph refresh (subprocess to avoid engine import coupling)
    if graph or sources >= GRAPH_REFRESH_THRESHOLD:
        import subprocess
        print("Rebuilding similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Update cursor
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())

    # Update tags list
    set_meta(db, 'tags', json.dumps(tags))
    set_meta(db, 'authors', json.dumps(authors))
    set_meta(db, 'tag_limit', str(limit or ''))
    set_meta(db, 'comment_limit', str(comment_limit if include_comments else 0))
    set_meta(db, 'include_comments', '1' if include_comments else '0')
    stored_since_days = get_meta(db, 'since_days')
    scope_since_days = (
        since_days if since_days is not None
        else int(stored_since_days) if stored_since_days else 0
    )
    if since_days is not None:
        set_meta(db, 'since_days', str(since_days))
    set_meta(db, 'scope', json.dumps(build_scope_meta(
        tags, authors, scope_since_days, limit, comment_limit,
        include_comments,
    )))

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Log
    log_op(db, 'devto_refresh', '_raw_chunks',
           params={'tags': tags, 'authors': authors,
                   'sources': sources,
                   'chunks': chunks, 'embedded': embedded,
                   'author_articles': total_author_articles,
                   'after_ts': last_pull_ts},
           rows_affected=chunks,
           source='devto/compile/refresh.py')
    db.commit()

    stats = {
        'sources': sources,
        'chunks': chunks,
        'embedded': embedded,
    }

    print(f"\nRefresh complete: {sources} sources, {chunks} chunks, "
          f"{embedded} embedded")
    db.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Incremental refresh for Dev.to Flex cells')
    parser.add_argument('--cell', default='devto',
                        help='Cell name (default: devto)')
    parser.add_argument('--tags', default=None,
                        help='Comma-separated tags (auto-detected from cell)')
    parser.add_argument('--authors', default=None,
                        help='Comma-separated Dev.to usernames')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 30d, 7d). '
                             'Overrides stored cursor.')
    parser.add_argument('--limit', type=int, default=None,
                        help='Max articles per tag or author')
    parser.add_argument('--comment-limit', type=int, default=None,
                        help='Max comments per article; use 0 to skip comments')
    parser.add_argument('--no-comments', action='store_true',
                        help='Do not fetch article comments')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new data without ingesting')
    args = parser.parse_args()

    # Resolve cell path
    from flex.registry import resolve_cell
    cell_path = resolve_cell(args.cell)
    if not cell_path:
        print(f"Cell '{args.cell}' not found in registry.")
        sys.exit(1)

    tag_list = parse_csv(args.tags) if args.tags else None
    author_list = parse_csv(args.authors) if args.authors else None

    # Parse --since (e.g. "30d" -> 30)
    since_days = None
    if args.since:
        since_days = parse_days(args.since)

    include_comments = None
    if args.no_comments or args.comment_limit == 0:
        include_comments = False

    refresh(str(cell_path), tags=tag_list, graph=args.graph,
            dry_run=args.dry_run, since_days=since_days,
            limit=args.limit, comment_limit=args.comment_limit,
            include_comments=include_comments, authors=author_list)


if __name__ == '__main__':
    main()
