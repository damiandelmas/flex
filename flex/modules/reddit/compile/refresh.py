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


class RedditRefreshError(RuntimeError):
    """A refresh unit was incomplete and must be replayed on its next turn."""


def _json_object(value: str | None) -> dict:
    """Read legacy metadata defensively (old cells may contain NULL/invalid JSON)."""
    try:
        parsed = json.loads(value or '{}')
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or '[]')
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [str(item) for item in parsed if item]



def _as_timestamp(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _cursor_for(current: dict, legacy: dict, unit: str, fallback: int) -> int:
    """Prefer the split feed cursor; seed old cells without a global leap."""
    if unit in current:
        return _as_timestamp(current[unit])
    if unit in legacy:
        return _as_timestamp(legacy[unit])
    return fallback


def _configured_units(subreddits: list[str], authors: list[str]) -> list[tuple[str, str]]:
    """Return stable, de-duplicated configured work units in round-robin order."""
    units: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for kind, names in (('subreddit', subreddits), ('author', authors)):
        for name in names:
            unit = (kind, name)
            if name and unit not in seen:
                seen.add(unit)
                units.append(unit)
    return units


def _unit_token(unit: tuple[str, str]) -> str:
    return f'{unit[0]}:{unit[1]}'


def _select_scheduled_unit(db, units: list[tuple[str, str]]) -> tuple[tuple[str, str], int]:
    """Select exactly one durable round-robin unit, recovering NULL/stale state."""
    wanted = get_meta(db, 'reddit_refresh_next_unit')
    tokens = [_unit_token(unit) for unit in units]
    index = tokens.index(wanted) if wanted in tokens else 0
    return units[index], index


def _parent_ids(comments: list[dict], posts: list[dict]) -> set[str]:
    post_ids = {str(post.get('id') or '') for post in posts}
    needed = set()
    for comment in comments:
        link_id = str(comment.get('link_id') or '')
        parent_id = link_id[3:] if link_id.startswith('t3_') else link_id
        if not parent_id:
            raise RedditRefreshError(
                f"comment {comment.get('id', '<unknown>')} has no link_id"
            )
        if parent_id not in post_ids:
            needed.add(parent_id)
    return needed


def _backfill_parent_posts(posts: list[dict], comments: list[dict]) -> list[dict]:
    """Fetch every missing comment parent before a comment feed can advance."""
    needed = _parent_ids(comments, posts)
    if not needed:
        return posts
    parent_posts = pull_posts_by_ids(sorted(needed))
    found = {str(post.get('id') or '') for post in parent_posts}
    missing = needed - found
    if missing:
        raise RedditRefreshError(
            "comment parent backfill incomplete: " + ', '.join(sorted(missing))
        )
    return posts + parent_posts


def _ingest(db, posts: list[dict], comments: list[dict], subreddit: str) -> tuple[int, int]:
    """Ingest a complete thread delta; comments are never grouped without parents."""
    if not posts and not comments:
        return 0, 0
    complete_posts = _backfill_parent_posts(posts, comments) if comments else posts
    return ingest(group_into_threads(complete_posts, comments), db, subreddit)


def _set_meta_many(db, values: dict[str, str]) -> None:
    """Persist all cursor/round-robin metadata in one final transaction."""
    for key, value in values.items():
        db.execute(
            "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
            (key, value),
        )


def refresh(cell_path: str, subreddits: list[str] | None = None,
            graph: bool = False, dry_run: bool = False,
            since_days: int | None = None, scheduled: bool = False) -> dict:
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

    # `sub_cursors` / `author_cursors` are the safe legacy seed only.  The
    # split maps below become authoritative per posts/comments feed and never
    # borrow a MAX timestamp from another configured unit.
    legacy_sub_cursors = _json_object(get_meta(db, 'sub_cursors'))
    legacy_author_cursors = _json_object(get_meta(db, 'author_cursors'))
    sub_post_cursors = _json_object(get_meta(db, 'sub_post_cursors'))
    sub_comment_cursors = _json_object(get_meta(db, 'sub_comment_cursors'))
    author_post_cursors = _json_object(get_meta(db, 'author_post_cursors'))
    author_comment_cursors = _json_object(get_meta(db, 'author_comment_cursors'))

    fallback_cursor = (
        int(time.time()) - (since_days * 86400)
        if since_days is not None else _as_timestamp(get_meta(db, 'last_pull_ts'))
    )
    stored_subs = _json_list(get_meta(db, 'subreddits'))
    authors = _json_list(get_meta(db, 'authors'))
    requested_subs = [str(sub) for sub in (subreddits or stored_subs) if sub]
    # Scheduled refreshes are deliberately constrained to configured scope,
    # rather than an ad-hoc call argument or previously ingested rows.
    configured_subs = stored_subs if scheduled else requested_subs
    units = _configured_units(configured_subs, authors)

    if not units:
        print("No Reddit work units configured. Pass --subreddits or configure the cell.")
        db.close()
        return {'error': 'no_work_units', 'refresh_pending': 0}

    selected_index = None
    if scheduled:
        selected_unit, selected_index = _select_scheduled_unit(db, units)
        units_to_run = [selected_unit]
    else:
        units_to_run = units

    print(f"Cell: {cell_path}")
    print(f"Work units: {', '.join(_unit_token(unit) for unit in units_to_run)}")
    print(f"Legacy cursor seed: {fallback_cursor or 'never'}")
    print()

    if dry_run:
        from flex.modules.reddit.compile.arctic_shift import api_fetch
        for kind, name in units_to_run:
            params = {kind: name, "limit": 1, "sort": "desc"}
            if kind == 'subreddit':
                after = _cursor_for(sub_post_cursors, legacy_sub_cursors, name, fallback_cursor)
            else:
                after = _cursor_for(author_post_cursors, legacy_author_cursors, name, fallback_cursor)
            if after:
                params["after"] = after
            data = api_fetch("posts/search", params)
            results = data.get("data", [])
            latest = results[0].get("created_utc", 0) if results else 0
            if latest:
                latest_dt = datetime.fromtimestamp(latest, tz=timezone.utc)
                print(f"  {kind}:{name}: has new data up to {latest_dt.date()}")
            else:
                print(f"  {kind}:{name}: no new data")
        db.close()
        return {'dry_run': True, 'refresh_pending': max(0, len(units) - 1) if scheduled else 0}

    total_posts = 0
    total_comments = 0
    total_sources = 0
    total_chunks = 0

    total_author_posts = 0
    total_author_comments = 0
    for kind, name in units_to_run:
        print(f"{'=' * 50}\n{kind}: {name}\n{'=' * 50}")
        if kind == 'subreddit':
            post_after = fallback_cursor if since_days is not None else _cursor_for(
                sub_post_cursors, legacy_sub_cursors, name, fallback_cursor)
            comment_after = fallback_cursor if since_days is not None else _cursor_for(
                sub_comment_cursors, legacy_sub_cursors, name, fallback_cursor)
            posts = pull_posts(name, after=post_after)
            comments = pull_comments(name, after=comment_after)
            total_posts += len(posts)
            total_comments += len(comments)
            sources, chunks = _ingest(db, posts, comments, name)
            total_sources += sources
            total_chunks += chunks
            if posts:
                sub_post_cursors[name] = max(post_after, max(_as_timestamp(p.get('created_utc')) for p in posts))
            if comments:
                sub_comment_cursors[name] = max(comment_after, max(_as_timestamp(c.get('created_utc')) for c in comments))
        else:
            post_after = fallback_cursor if since_days is not None else _cursor_for(
                author_post_cursors, legacy_author_cursors, name, fallback_cursor)
            comment_after = fallback_cursor if since_days is not None else _cursor_for(
                author_comment_cursors, legacy_author_cursors, name, fallback_cursor)
            posts = pull_posts_by_author(name, after=post_after)
            comments = pull_comments_by_author(name, after=comment_after)
            total_author_posts += len(posts)
            total_author_comments += len(comments)
            complete_posts = _backfill_parent_posts(posts, comments) if comments else posts
            by_sub: dict[str, tuple[list, list]] = defaultdict(lambda: ([], []))
            for post in complete_posts:
                by_sub[str(post.get('subreddit') or '')][0].append(post)
            for comment in comments:
                by_sub[str(comment.get('subreddit') or '')][1].append(comment)
            for sub, (sub_posts, sub_comments) in by_sub.items():
                if not sub:
                    raise RedditRefreshError(f"author unit {name} returned content without subreddit")
                sources, chunks = ingest(group_into_threads(sub_posts, sub_comments), db, sub)
                total_sources += sources
                total_chunks += chunks
            if posts:
                author_post_cursors[name] = max(post_after, max(_as_timestamp(p.get('created_utc')) for p in posts))
            if comments:
                author_comment_cursors[name] = max(comment_after, max(_as_timestamp(c.get('created_utc')) for c in comments))

    # Embed new chunks
    embedded = 0
    if total_chunks:
        print(f"\nEmbedding {total_chunks} new chunks...")
        embedded = embed_new(db)
        print(f"Embedded: {embedded}")

    # Graph refresh (subprocess to avoid engine import coupling).
    # Honors scope.graph.* from _meta so the graph ignores low-signal sources
    # without dropping them from _raw_sources. Lever stays tunable.
    if graph or (not scheduled and total_sources >= GRAPH_REFRESH_THRESHOLD):
        import subprocess
        graph_where = build_graph_where(db)
        print(f"Rebuilding similarity graph (where: {graph_where or 'none'})...")
        cmd = [sys.executable, '-m', 'flex.manage.meditate', '--cell', cell_path]
        if graph_where:
            cmd += ['--where', graph_where]
        subprocess.run(cmd, check=True)

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Log
    log_op(db, 'reddit_refresh', '_raw_chunks',
           params={'subreddits': requested_subs, 'authors': authors,
                   'sources': total_sources,
                   'chunks': total_chunks, 'embedded': embedded,
                   'author_posts': total_author_posts,
                   'author_comments': total_author_comments,
                   'scheduled': scheduled},
           rows_affected=total_chunks,
           source='reddit/compile/refresh.py')
    pending = max(0, len(units) - 1) if scheduled else 0
    metadata = {
        'sub_post_cursors': json.dumps(sub_post_cursors, sort_keys=True),
        'sub_comment_cursors': json.dumps(sub_comment_cursors, sort_keys=True),
        'author_post_cursors': json.dumps(author_post_cursors, sort_keys=True),
        'author_comment_cursors': json.dumps(author_comment_cursors, sort_keys=True),
        'last_pull_at': datetime.now(timezone.utc).isoformat(),
    }
    if scheduled and selected_index is not None:
        metadata['reddit_refresh_next_unit'] = _unit_token(
            units[(selected_index + 1) % len(units)]
        )
    _set_meta_many(db, metadata)
    db.commit()

    stats = {
        'posts': total_posts,
        'comments': total_comments,
        'author_posts': total_author_posts,
        'author_comments': total_author_comments,
        'sources': total_sources,
        'chunks': total_chunks,
        'embedded': embedded,
        'refresh_pending': pending,
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
