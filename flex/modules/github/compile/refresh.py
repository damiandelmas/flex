"""
Incremental GitHub Issues cell refresh.

Reads last_pull_ts from cell _meta, pulls new issues since then,
ingests, embeds, and optionally rebuilds the graph.

Idempotent: INSERT OR IGNORE means re-running is safe.

Usage:
    python -m flex.modules.github.compile.refresh --cell github
    python -m flex.modules.github.compile.refresh --cell github --dry-run
    python -m flex.modules.github.compile.refresh --cell github --since 30d
    python -m flex.modules.github.compile.refresh --cell github --repos anthropics/claude-code
    python -m flex.modules.github.compile.refresh --cell github --queries "session history,memory"
    python -m flex.modules.github.compile.refresh --cell github --graph
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op
from flex.modules.github.compile.github_api import (
    pull_issues, DEFAULT_REPOS, DEFAULT_QUERIES,
    DEFAULT_MAX_ISSUES, DEFAULT_MAX_COMMENTS_PER_ISSUE,
)
from flex.modules.github.compile.worker import (
    SCHEMA_DDL, group_into_threads, ingest, embed_new,
)


GRAPH_REFRESH_THRESHOLD = 20  # rebuild graph if >= N new sources


def refresh(cell_path: str, repos: list[str] | None = None,
            queries: list[str] | None = None,
            graph: bool = False, dry_run: bool = False,
            since_days: int | None = None,
            max_issues: int | None = None,
            max_comments_per_issue: int | None = None) -> dict:
    """Pull new data and ingest into existing github cell."""
    db = open_cell(cell_path)
    db.executescript(SCHEMA_DDL)

    # Cursor
    if since_days is not None:
        last_pull_ts = int(time.time()) - (since_days * 86400)
    else:
        last_pull_ts = int(get_meta(db, 'last_pull_ts') or '0')

    # Repos
    if not repos:
        stored = get_meta(db, 'repos')
        repos = json.loads(stored) if stored else DEFAULT_REPOS

    # Queries
    if not queries:
        stored = get_meta(db, 'queries')
        queries = json.loads(stored) if stored else DEFAULT_QUERIES

    if max_issues is None:
        stored = get_meta(db, 'max_issues')
        max_issues = int(stored) if stored else DEFAULT_MAX_ISSUES
    if max_comments_per_issue is None:
        stored = get_meta(db, 'max_comments_per_issue')
        max_comments_per_issue = (
            int(stored) if stored else DEFAULT_MAX_COMMENTS_PER_ISSUE
        )

    after_dt = datetime.fromtimestamp(last_pull_ts, tz=timezone.utc) if last_pull_ts else None
    print(f"Cell: {cell_path}")
    print(f"Repos: {repos}")
    print(f"Queries: {queries}")
    print(f"Limits: max_issues={max_issues}, max_comments={max_comments_per_issue}")
    print(f"Last pull: {after_dt.isoformat() if after_dt else 'never'}")
    print()

    if dry_run:
        print("Dry run -- checking for new data...")
        from flex.modules.github.compile.github_api import api_fetch
        import urllib.parse
        for repo in repos:
            owner, name = repo.split("/", 1)
            params = {"state": "all", "per_page": 1, "sort": "created", "direction": "desc"}
            if last_pull_ts:
                dt = datetime.fromtimestamp(last_pull_ts, tz=timezone.utc)
                params["since"] = f"{dt.strftime('%Y-%m-%d')}T00:00:00Z"
            qs = urllib.parse.urlencode(params)
            url = f"https://api.github.com/repos/{owner}/{name}/issues?{qs}"
            data, _ = api_fetch(url)
            if isinstance(data, list) and data:
                created = data[0].get("created_at", "unknown")
                print(f"  {repo}: has new data (latest: {created})")
            else:
                print(f"  {repo}: no new data")
        db.close()
        return {'dry_run': True}

    # Pull all issues
    print("Pulling issues...")
    all_issues = pull_issues(
        queries=queries,
        repos=repos,
        after_ts=last_pull_ts,
        max_issues=max_issues,
        max_comments_per_issue=max_comments_per_issue,
    )

    if not all_issues:
        print("\nNo new data.")
        db.close()
        return {'sources': 0, 'chunks': 0}

    threads = group_into_threads(all_issues)
    sources, chunks = ingest(threads, db)
    print(f"  Ingested: {sources} sources, {chunks} chunks")

    if chunks == 0:
        print("\nNo new data to embed.")
        db.close()
        return {'sources': 0, 'chunks': 0}

    # Embed
    print(f"\nEmbedding {chunks} new chunks...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded}")

    # Graph (subprocess to avoid engine import coupling)
    if graph or sources >= GRAPH_REFRESH_THRESHOLD:
        import subprocess
        print("Rebuilding similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Update cursor
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'repos', json.dumps(repos))
    set_meta(db, 'queries', json.dumps(queries))
    set_meta(db, 'max_issues', str(max_issues))
    set_meta(db, 'max_comments_per_issue', str(max_comments_per_issue))

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db, {'chunks': 'chunk', 'sources': 'source'})

    # Log
    log_op(db, 'github_refresh', '_raw_chunks',
           params={'repos': repos, 'queries': queries,
                   'sources': sources, 'chunks': chunks,
                   'embedded': embedded, 'after_ts': last_pull_ts,
                   'max_issues': max_issues,
                   'max_comments_per_issue': max_comments_per_issue},
           rows_affected=chunks,
           source='github/compile/refresh.py')
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
        description='Incremental refresh for GitHub Issues Flex cells')
    parser.add_argument('--cell', default='github',
                        help='Cell name (default: github)')
    parser.add_argument('--repos', default=None,
                        help='Comma-separated repos (owner/name)')
    parser.add_argument('--queries', default=None,
                        help='Comma-separated search queries')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 30d, 7d)')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new data without ingesting')
    parser.add_argument('--max-issues', type=int, default=None,
                        help='Max issues to pull for this refresh')
    parser.add_argument('--max-comments', type=int, default=None,
                        help='Max comments per issue for this refresh')
    args = parser.parse_args()

    from flex.registry import resolve_cell
    cell_path = resolve_cell(args.cell)
    if not cell_path:
        print(f"Cell '{args.cell}' not found in registry.")
        sys.exit(1)

    repos = args.repos.split(',') if args.repos else None
    queries = args.queries.split(',') if args.queries else None

    since_days = None
    if args.since:
        since_days = int(args.since.strip().lower().rstrip('d'))

    refresh(str(cell_path), repos=repos, queries=queries,
            graph=args.graph, dry_run=args.dry_run, since_days=since_days,
            max_issues=args.max_issues,
            max_comments_per_issue=args.max_comments)


if __name__ == '__main__':
    main()
