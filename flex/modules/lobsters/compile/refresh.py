"""
Incremental Lobsters cell refresh.

Reads last_pull_ts from cell _meta, pulls new data since then,
ingests, embeds, and optionally rebuilds the graph.

Idempotent: INSERT OR IGNORE means re-running is safe.

Usage:
    python -m flex.modules.lobsters.compile.refresh --cell lobsters
    python -m flex.modules.lobsters.compile.refresh --cell lobsters --dry-run
    python -m flex.modules.lobsters.compile.refresh --cell lobsters --since 30d
    python -m flex.modules.lobsters.compile.refresh --cell lobsters --tags ai,ml,python
    python -m flex.modules.lobsters.compile.refresh --cell lobsters --graph
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op
from flex.modules.lobsters.compile.lobsters_api import (
    pull_stories_with_comments, DEFAULT_TAGS,
)
from flex.modules.lobsters.compile.worker import (
    SCHEMA_DDL, group_into_threads, ingest, embed_new,
)


GRAPH_REFRESH_THRESHOLD = 20  # rebuild graph if >= N new sources


def refresh(cell_path: str, tags: list[str] | None = None,
            graph: bool = False, dry_run: bool = False,
            since_days: int | None = None,
            limit: int | None = None) -> dict:
    """Pull new data and ingest into existing lobsters cell.

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

    after_dt = datetime.fromtimestamp(last_pull_ts, tz=timezone.utc) if last_pull_ts else None
    print(f"Cell: {cell_path}")
    print(f"Tags: {', '.join(tags)}")
    print(f"Last pull: {after_dt.isoformat() if after_dt else 'never'}")
    print()

    if dry_run:
        print("Dry run — checking for new data...")
        # Quick check: pull tag feeds without fetching details
        from flex.modules.lobsters.compile.lobsters_api import pull_tag_feed
        total_new = 0
        for tag in tags[:3]:  # sample first 3 tags
            stories = pull_tag_feed(tag, quiet=True)
            new = [s for s in stories if s["created_utc"] >= last_pull_ts]
            total_new += len(new)
            print(f"  Tag '{tag}': {len(new)} new stories")
            time.sleep(2.0)
        if total_new > 0:
            print(f"\n  Estimated new stories across all tags: ~{total_new}")
        else:
            print("\n  No new stories found.")
        db.close()
        return {'dry_run': True}

    # Pull data
    print("Pulling stories with comments...")
    stories = pull_stories_with_comments(
        tags=tags, pages=3, after=last_pull_ts, limit=limit)

    if not stories:
        print("\nNo new data.")
        db.close()
        return {'sources': 0, 'chunks': 0}

    threads = group_into_threads(stories)
    sources, chunks = ingest(threads, db)
    print(f"  Ingested: {sources} sources, {chunks} chunks")

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
    set_meta(db, 'tags', json.dumps(tags))

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Log
    log_op(db, 'lobsters_refresh', '_raw_chunks',
           params={'tags': tags, 'sources': sources,
                   'chunks': chunks, 'embedded': embedded,
                   'after_ts': last_pull_ts},
           rows_affected=chunks,
           source='lobsters/compile/refresh.py')
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
        description='Incremental refresh for Lobsters Flex cells')
    parser.add_argument('--cell', default='lobsters',
                        help='Cell name (default: lobsters)')
    parser.add_argument('--tags', default=None,
                        help='Comma-separated tags (auto-detected from cell)')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 30d, 7d). '
                             'Overrides stored cursor.')
    parser.add_argument('--limit', default=None, type=int,
                        help='Maximum number of unique stories to ingest')
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

    tag_list = args.tags.split(',') if args.tags else None

    # Parse --since (e.g. "30d" -> 30)
    since_days = None
    if args.since:
        s = args.since.strip().lower().rstrip('d')
        since_days = int(s)

    refresh(str(cell_path), tags=tag_list, graph=args.graph,
            dry_run=args.dry_run, since_days=since_days, limit=args.limit)


if __name__ == '__main__':
    main()
