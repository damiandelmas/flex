"""
Incremental Reddit cell refresh.

Reads last_pull_ts from cell _meta, pulls new posts/comments since then
via Arctic Shift, ingests, embeds, and optionally rebuilds the graph.

Idempotent: INSERT OR IGNORE means re-running is safe.

Usage:
    python -m flex.modules.reddit.compile.refresh --cell reddit
    python -m flex.modules.reddit.compile.refresh --cell reddit --dry-run
    python -m flex.modules.reddit.compile.refresh --cell reddit --subreddits ClaudeCode,ClaudeAI
    python -m flex.modules.reddit.compile.refresh --cell reddit --graph
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op
from collections import defaultdict

from flex.modules.reddit.compile.arctic_shift import (
    pull_posts, pull_comments,
    pull_posts_by_author, pull_comments_by_author,
    pull_posts_by_ids,
)
from flex.modules.reddit.compile.worker import (
    SCHEMA_DDL, group_into_threads, ingest, embed_new,
    ensure_scope_defaults, build_graph_where,
)


GRAPH_REFRESH_THRESHOLD = 20  # rebuild graph if >= N new sources


def refresh(cell_path: str, subreddits: list[str] | None = None,
            graph: bool = False, dry_run: bool = False,
            since_days: int | None = None) -> dict:
    """Pull new data and ingest into existing reddit cell.

    Args:
        since_days: Override cursor — pull this many days back instead of
                    using last_pull_ts. Useful for first-time subreddit adds.

    Returns stats dict with counts.
    """
    db = open_cell(cell_path)

    # Ensure schema exists (idempotent)
    db.executescript(SCHEMA_DDL)

    # Make sure scope defaults are set — non-destructive, only fills gaps
    ensure_scope_defaults(db)
    db.commit()

    # Per-sub cursors: each subreddit tracks its own high-water mark
    # so a failure on one doesn't re-pull or block others.
    sub_cursors = json.loads(get_meta(db, 'sub_cursors') or '{}')

    # Read cursor (--since overrides stored cursor)
    if since_days is not None:
        last_pull_ts = int(time.time()) - (since_days * 86400)
    else:
        last_pull_ts = int(get_meta(db, 'last_pull_ts') or '0')
    stored_subs = json.loads(get_meta(db, 'subreddits') or '[]')

    if not subreddits:
        subreddits = stored_subs

    if not subreddits:
        print("No subreddits configured. Pass --subreddits or ingest first.")
        db.close()
        return {'error': 'no_subreddits'}

    after_dt = datetime.fromtimestamp(last_pull_ts, tz=timezone.utc) if last_pull_ts else None
    print(f"Cell: {cell_path}")
    print(f"Subreddits: {', '.join(f'r/{s}' for s in subreddits)}")
    print(f"Last pull: {after_dt.isoformat() if after_dt else 'never'}")
    print(f"Pulling posts/comments after timestamp {last_pull_ts}")
    print()

    if dry_run:
        # Quick count check per subreddit
        from flex.modules.reddit.compile.arctic_shift import api_fetch
        for sub in subreddits:
            params = {"subreddit": sub, "limit": 1, "sort": "desc"}
            if last_pull_ts:
                params["after"] = last_pull_ts
            data = api_fetch("posts/search", params)
            results = data.get("data", [])
            latest = results[0].get("created_utc", 0) if results else 0
            if latest:
                latest_dt = datetime.fromtimestamp(latest, tz=timezone.utc)
                print(f"  r/{sub}: has new data up to {latest_dt.date()}")
            else:
                print(f"  r/{sub}: no new data")
        db.close()
        return {'dry_run': True}

    total_posts = 0
    total_comments = 0
    total_sources = 0
    total_chunks = 0

    for sub in subreddits:
        print(f"{'=' * 50}")
        print(f"r/{sub}")
        print(f"{'=' * 50}")

        # Per-sub cursor: use sub-specific cursor if available,
        # fall back to global last_pull_ts
        if since_days is not None:
            sub_after = last_pull_ts  # explicit --since always wins
        else:
            sub_after = sub_cursors.get(sub, last_pull_ts)

        # Pull new posts
        print(f"Pulling posts (after={sub_after})...")
        posts = pull_posts(sub, after=sub_after)
        total_posts += len(posts)

        # Pull new comments
        print(f"Pulling comments (after={sub_after})...")
        comments = pull_comments(sub, after=sub_after)
        total_comments += len(comments)

        if not posts and not comments:
            print("  No new data.")
            continue

        # Group and ingest
        threads = group_into_threads(posts, comments)
        sources, chunks = ingest(threads, db, sub)
        total_sources += sources
        total_chunks += chunks
        print(f"  Ingested: {sources} sources, {chunks} chunks")

        # Update per-sub cursor to max timestamp seen
        all_items = posts + comments
        if all_items:
            max_item_ts = max(
                item.get('created_utc', 0) for item in all_items)
            sub_cursors[sub] = max(
                sub_cursors.get(sub, 0), max_item_ts)
            set_meta(db, 'sub_cursors', json.dumps(sub_cursors))
            db.commit()

    # ═════════════════════════════════════════════════════
    # Author self-pull — our own authored output across subs
    # ═════════════════════════════════════════════════════
    authors = json.loads(get_meta(db, 'authors') or '[]')
    author_cursors = json.loads(get_meta(db, 'author_cursors') or '{}')
    total_author_posts = 0
    total_author_comments = 0
    total_author_sources = 0
    total_author_chunks = 0

    for author in authors:
        print(f"{'=' * 50}")
        print(f"author: u/{author}")
        print(f"{'=' * 50}")

        if since_days is not None:
            author_after = last_pull_ts
        else:
            author_after = author_cursors.get(author, last_pull_ts)

        print(f"Pulling posts by u/{author} (after={author_after})...")
        a_posts = pull_posts_by_author(author, after=author_after)
        total_author_posts += len(a_posts)

        print(f"Pulling comments by u/{author} (after={author_after})...")
        a_comments = pull_comments_by_author(author, after=author_after)
        total_author_comments += len(a_comments)

        if not a_posts and not a_comments:
            print("  No new author data.")
            continue

        # Backfill parent posts for orphan comments (comments whose link_id
        # isn't in the author's own posts). Without this, group_into_threads
        # drops orphan comments on the floor.
        have_post_ids = {p.get('id') for p in a_posts}
        orphan_parent_ids = set()
        for c in a_comments:
            link_id = c.get('link_id', '')
            pid = link_id[3:] if link_id.startswith('t3_') else link_id
            if pid and pid not in have_post_ids:
                orphan_parent_ids.add(pid)

        if orphan_parent_ids:
            print(f"  Backfilling {len(orphan_parent_ids)} parent posts...")
            parent_posts = pull_posts_by_ids(list(orphan_parent_ids))
            a_posts = a_posts + parent_posts

        # Group author content by subreddit — ingest() is per-sub
        by_sub: dict[str, tuple[list, list]] = defaultdict(lambda: ([], []))
        for p in a_posts:
            by_sub[p.get('subreddit', '')][0].append(p)
        for c in a_comments:
            by_sub[c.get('subreddit', '')][1].append(c)

        for sub, (ps, cs) in by_sub.items():
            if not sub:
                continue
            threads = group_into_threads(ps, cs)
            sources, chunks = ingest(threads, db, sub)
            total_author_sources += sources
            total_author_chunks += chunks
            print(f"  r/{sub}: {sources} sources, {chunks} chunks")

        # Advance author cursor to max ts seen
        all_items = a_posts + a_comments
        if all_items:
            max_item_ts = max(i.get('created_utc', 0) for i in all_items)
            author_cursors[author] = max(
                author_cursors.get(author, 0), max_item_ts)
            set_meta(db, 'author_cursors', json.dumps(author_cursors))
            db.commit()

    total_sources += total_author_sources
    total_chunks += total_author_chunks

    if total_chunks == 0:
        print("\nNo new data to embed.")
        db.close()
        return {'posts': total_posts, 'comments': total_comments,
                'author_posts': total_author_posts,
                'author_comments': total_author_comments,
                'sources': 0, 'chunks': 0}

    # Embed new chunks
    print(f"\nEmbedding {total_chunks} new chunks...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded}")

    # Graph refresh (subprocess to avoid engine import coupling).
    # Honors scope.graph.* from _meta so the graph ignores low-signal sources
    # without dropping them from _raw_sources. Lever stays tunable.
    if graph or total_sources >= GRAPH_REFRESH_THRESHOLD:
        import subprocess
        graph_where = build_graph_where(db)
        print(f"Rebuilding similarity graph (where: {graph_where or 'none'})...")
        cmd = [sys.executable, '-m', 'flex.manage.meditate', '--cell', cell_path]
        if graph_where:
            cmd += ['--where', graph_where]
        subprocess.run(cmd, check=True)

    # Update cursor
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())

    # Update subreddits list
    existing = db.execute(
        "SELECT DISTINCT subreddit FROM _raw_sources"
    ).fetchall()
    all_subs = sorted({r[0] for r in existing if r[0]})
    set_meta(db, 'subreddits', json.dumps(all_subs))

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Log
    log_op(db, 'reddit_refresh', '_raw_chunks',
           params={'subreddits': subreddits, 'authors': authors,
                   'sources': total_sources,
                   'chunks': total_chunks, 'embedded': embedded,
                   'author_posts': total_author_posts,
                   'author_comments': total_author_comments,
                   'after_ts': last_pull_ts},
           rows_affected=total_chunks,
           source='reddit/compile/refresh.py')
    db.commit()

    stats = {
        'posts': total_posts,
        'comments': total_comments,
        'author_posts': total_author_posts,
        'author_comments': total_author_comments,
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
        description='Incremental refresh for Reddit Flex cells')
    parser.add_argument('--cell', default='reddit',
                        help='Cell name (default: reddit)')
    parser.add_argument('--subreddits', default=None,
                        help='Comma-separated subreddit names (auto-detected from cell)')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 30d, 7d). '
                             'Overrides stored cursor. Use for first-time subreddit adds.')
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

    subs = args.subreddits.split(',') if args.subreddits else None

    # Parse --since (e.g. "30d" → 30)
    since_days = None
    if args.since:
        s = args.since.strip().lower().rstrip('d')
        since_days = int(s)

    refresh(str(cell_path), subreddits=subs, graph=args.graph,
            dry_run=args.dry_run, since_days=since_days)


if __name__ == '__main__':
    main()
