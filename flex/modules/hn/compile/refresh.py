"""
Incremental Hacker News cell refresh.

Reads last_pull_ts from cell _meta, pulls new stories/comments since then,
ingests, embeds, and optionally rebuilds the graph.

Idempotent: INSERT OR IGNORE means re-running is safe.

Usage:
    python -m flex.modules.hn.compile.refresh --cell hn
    python -m flex.modules.hn.compile.refresh --cell hn --dry-run
    python -m flex.modules.hn.compile.refresh --cell hn --since 30d
    python -m flex.modules.hn.compile.refresh --cell hn --queries "claude code,MCP server"
    python -m flex.modules.hn.compile.refresh --cell hn --graph
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op
from flex.modules.hn.compile.algolia import (
    pull_stories, pull_comments_for_story,
    pull_stories_by_author, pull_comments_by_author,
)
from flex.modules.hn.compile.worker import (
    SCHEMA_DDL, DEFAULT_QUERIES, group_into_threads, ingest, embed_new,
)


GRAPH_REFRESH_THRESHOLD = 20  # rebuild graph if >= N new sources


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False,
            since_days: int | None = None,
            queries: list[str] | None = None,
            max_pages: int | None = None,
            max_stories: int | None = None,
            max_comments_per_story: int | None = None,
            hits_per_page: int | None = None) -> dict:
    """Pull new data and ingest into existing HN cell.

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

    if not queries:
        stored = get_meta(db, 'queries')
        queries = json.loads(stored) if stored else DEFAULT_QUERIES

    after_dt = datetime.fromtimestamp(last_pull_ts, tz=timezone.utc) if last_pull_ts else None
    print(f"Cell: {cell_path}")
    print(f"Queries: {queries}")
    print(f"Last pull: {after_dt.isoformat() if after_dt else 'never'}")
    print()

    if dry_run:
        print("Dry run -- checking for new data...")
        for q in queries:
            from flex.modules.hn.compile.algolia import api_fetch
            params = {
                "query": q,
                "tags": "story",
                "hitsPerPage": 1,
            }
            if last_pull_ts:
                params["numericFilters"] = f"created_at_i>{last_pull_ts}"
            data = api_fetch("search", params)
            nb_hits = data.get("nbHits", 0)
            print(f"  '{q}': ~{nb_hits} new stories")
        db.close()
        return {'dry_run': True}

    # Pull all stories and comments
    all_stories = []
    all_comments = []

    for query in queries:
        print(f"{'=' * 50}")
        print(f"Query: {query}")
        print(f"{'=' * 50}")

        stories = pull_stories(
            query, after_ts=last_pull_ts, max_pages=max_pages,
            hits_per_page=hits_per_page)
        if max_stories is not None:
            remaining = max(0, max_stories - len(all_stories))
            stories = stories[:remaining]

        if not stories:
            print("  No new stories.")
            continue

        all_stories.extend(stories)

        # Pull comments for each new story
        for story in stories:
            sid = story.get('id', '')
            if sid:
                comments = pull_comments_for_story(
                    sid, after_ts=last_pull_ts, quiet=True,
                    max_pages=max_pages, hits_per_page=hits_per_page)
                if max_comments_per_story is not None:
                    comments = comments[:max(0, max_comments_per_story)]
                all_comments.extend(comments)
        if max_stories is not None and len(all_stories) >= max_stories:
            break

    # Deduplicate
    seen_stories = {}
    for s in all_stories:
        seen_stories.setdefault(s['id'], s)
    all_stories = list(seen_stories.values())

    seen_comments = {}
    for c in all_comments:
        seen_comments.setdefault(c['id'], c)
    all_comments = list(seen_comments.values())

    # ═════════════════════════════════════════════════════
    # Author self-pull — our stories + comments
    # ═════════════════════════════════════════════════════
    authors = json.loads(get_meta(db, 'authors') or '[]')
    author_cursors = json.loads(get_meta(db, 'author_cursors') or '{}')
    author_stories: list[dict] = []
    author_comments: list[dict] = []

    for author in authors:
        print(f"{'=' * 50}")
        print(f"Author: {author}")
        print(f"{'=' * 50}")

        if since_days is not None:
            actor_after = last_pull_ts
        else:
            actor_after = author_cursors.get(author, last_pull_ts)

        a_stories = pull_stories_by_author(
            author, after_ts=actor_after, max_pages=max_pages,
            hits_per_page=hits_per_page)
        a_comments = pull_comments_by_author(
            author, after_ts=actor_after, max_pages=max_pages,
            hits_per_page=hits_per_page)

        author_stories.extend(a_stories)
        author_comments.extend(a_comments)

        # Advance cursor per author
        all_items = a_stories + a_comments
        if all_items:
            max_item_ts = max(i.get('created_utc', 0) for i in all_items)
            author_cursors[author] = max(
                author_cursors.get(author, 0), max_item_ts)
            set_meta(db, 'author_cursors', json.dumps(author_cursors))
            db.commit()

    # Merge + dedupe (stories + comments both dedupe by id)
    for s in author_stories:
        seen_stories.setdefault(s['id'], s)
    all_stories = list(seen_stories.values())

    for c in author_comments:
        seen_comments.setdefault(c['id'], c)
    all_comments = list(seen_comments.values())

    total_author_items = len(author_stories) + len(author_comments)

    if not all_stories and not all_comments:
        print("\nNo new data.")
        db.close()
        return {'stories': 0, 'comments': 0, 'sources': 0, 'chunks': 0}

    print(f"\nTotal: {len(all_stories)} unique stories, "
          f"{len(all_comments)} unique comments "
          f"(incl. {total_author_items} from authors)")

    threads = group_into_threads(all_stories, all_comments)
    total_sources, total_chunks = ingest(threads, db)
    print(f"  Ingested: {total_sources} sources, {total_chunks} chunks")

    if total_chunks == 0:
        print("\nNo new data to embed.")
        db.close()
        return {'stories': len(all_stories), 'comments': len(all_comments),
                'sources': 0, 'chunks': 0}

    # Embed new chunks
    print(f"\nEmbedding {total_chunks} new chunks...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded}")

    # Graph refresh (subprocess to avoid engine import coupling)
    if graph or total_sources >= GRAPH_REFRESH_THRESHOLD:
        import subprocess
        print("Rebuilding similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Update cursor
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'queries', json.dumps(queries))

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Log
    log_op(db, 'hn_refresh', '_raw_chunks',
           params={'queries': queries, 'authors': authors,
                   'sources': total_sources,
                   'chunks': total_chunks, 'embedded': embedded,
                   'author_items': total_author_items,
                   'after_ts': last_pull_ts},
           rows_affected=total_chunks,
           source='hn/compile/refresh.py')
    db.commit()

    stats = {
        'stories': len(all_stories),
        'comments': len(all_comments),
        'sources': total_sources,
        'chunks': total_chunks,
        'embedded': embedded,
    }

    print(f"\nRefresh complete: {total_sources} sources, {total_chunks} chunks, "
          f"{embedded} embedded")
    db.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Incremental refresh for Hacker News Flex cells')
    parser.add_argument('--cell', default='hn',
                        help='Cell name (default: hn)')
    parser.add_argument('--queries', default=None,
                        help='Comma-separated search queries')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 30d, 7d). '
                             'Overrides stored cursor.')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new data without ingesting')
    parser.add_argument('--max-stories', type=int, default=None,
                        help='Maximum stories to ingest across all queries')
    parser.add_argument('--max-comments-per-story', type=int, default=None,
                        help='Maximum comments to ingest per story')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Maximum Algolia pages per API call')
    parser.add_argument('--hits-per-page', type=int, default=None,
                        help='Algolia hitsPerPage for each API call')
    args = parser.parse_args()

    # Resolve cell path
    from flex.registry import resolve_cell
    cell_path = resolve_cell(args.cell)
    if not cell_path:
        print(f"Cell '{args.cell}' not found in registry.")
        sys.exit(1)

    queries = args.queries.split(',') if args.queries else None

    # Parse --since (e.g. "30d" -> 30)
    since_days = None
    if args.since:
        since_days = int(args.since.strip().lower().rstrip('d'))

    refresh(str(cell_path), graph=args.graph, dry_run=args.dry_run,
            since_days=since_days, queries=queries,
            max_pages=args.max_pages, max_stories=args.max_stories,
            max_comments_per_story=args.max_comments_per_story,
            hits_per_page=args.hits_per_page)


if __name__ == '__main__':
    main()
