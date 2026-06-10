"""
Incremental skills cell refresh.

Runs the full discovery funnel: GitHub search → awesome list re-crawl →
enrichment → README fetch → skill artifact discovery → embed → graph.

Each phase is idempotent — dedup by source_id (owner/repo), NULL checks
for unenriched rows, NOT IN subqueries for missing README/skills.

Usage:
    python -m flex.modules.skills.compile.refresh --cell tools
    python -m flex.modules.skills.compile.refresh --cell tools --dry-run
    python -m flex.modules.skills.compile.refresh --cell tools --since 7d
    python -m flex.modules.skills.compile.refresh --cell tools --mode catalog,enrich
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op


GRAPH_REFRESH_THRESHOLD = 50  # rebuild graph if >= N new sources


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False,
            since_days: int | None = None, modes: list[str] | None = None) -> dict:
    """Run the skills discovery funnel on an existing cell.

    The full funnel (default):
        1. search   — GitHub API topic search for new repos
        2. catalog  — awesome list re-crawl for seed coverage
        3. enrich   — GitHub metadata backfill (stars, language, etc.)
        4. readme   — fetch + split READMEs into span chunks
        5. skills   — discover Claude Code artifacts (.claude/skills/, hooks)

    Args:
        cell_path: Path to the skills cell .db file.
        graph: Force graph rebuild after ingest.
        dry_run: Show stats without making changes.
        since_days: Override search recency — push:>DATE filter for GitHub.
        modes: Subset of phases to run (default: all 5).

    Returns stats dict with counts.
    """
    from flex.modules.skills.compile.expand import (
        expand_search, expand_catalog, expand_enrich,
        expand_readme, expand_skills,
    )
    from flex.modules.skills.compile.worker import (
        embed_new, DEFAULT_AWESOME_REPOS,
    )

    db = open_cell(cell_path)

    # Stored awesome repos (fall back to defaults)
    awesome_repos = json.loads(get_meta(db, 'awesome_repos') or 'null')
    if not awesome_repos:
        awesome_repos = DEFAULT_AWESOME_REPOS

    all_modes = ['search', 'catalog', 'enrich', 'readme', 'skills']
    run_modes = modes if modes else all_modes

    last_refresh = get_meta(db, 'last_refresh_at')
    print(f"Cell: {cell_path}")
    print(f"Last refresh: {last_refresh or 'never'}")
    print(f"Modes: {', '.join(run_modes)}")
    print(f"Awesome repos: {len(awesome_repos)}")
    print()

    t0 = time.time()
    stats = {}

    # Build search queries with pushed: date filter for recency
    search_queries = None
    if since_days is not None and 'search' in run_modes:
        # GitHub search supports pushed:>YYYY-MM-DD
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).strftime('%Y-%m-%d')
        # Append pushed: filter to default queries
        from flex.modules.skills.compile.github_api import DEFAULT_SEARCH_QUERIES
        search_queries = [f"{q} pushed:>{cutoff}" for q in DEFAULT_SEARCH_QUERIES]

    for mode in run_modes:
        print(f"\n{'=' * 50}")
        print(f"Phase: {mode}")
        print(f"{'=' * 50}")

        if mode == 'search':
            expand_search(db, queries=search_queries, dry_run=dry_run)
        elif mode == 'catalog':
            expand_catalog(db, awesome_repos, dry_run=dry_run)
        elif mode == 'enrich':
            expand_enrich(db, dry_run=dry_run)
        elif mode == 'readme':
            expand_readme(db, dry_run=dry_run)
        elif mode == 'skills':
            expand_skills(db, dry_run=dry_run)

    if dry_run:
        db.close()
        return {'dry_run': True}

    # Final embed sweep (catches anything missed by individual phases)
    print(f"\n{'=' * 50}")
    print("Final embed sweep...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded} chunks")
    stats['embedded'] = embedded

    # Count new sources since start
    total_sources = db.execute(
        "SELECT COUNT(*) FROM _raw_sources"
    ).fetchone()[0]
    total_chunks = db.execute(
        "SELECT COUNT(*) FROM _raw_chunks"
    ).fetchone()[0]
    stats['total_sources'] = total_sources
    stats['total_chunks'] = total_chunks

    # Graph
    if graph or embedded >= GRAPH_REFRESH_THRESHOLD:
        import subprocess
        print("Rebuilding similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Update metadata
    set_meta(db, 'last_refresh_at', datetime.now(timezone.utc).isoformat())
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())

    # Regenerate views
    from flex.views import regenerate_views, install_views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Log
    elapsed = time.time() - t0
    log_op(db, 'skills_refresh', '_raw_chunks',
           params={'modes': run_modes, 'embedded': embedded,
                   'total_sources': total_sources, 'total_chunks': total_chunks},
           rows_affected=embedded,
           source='skills/compile/refresh.py')
    db.commit()

    print(f"\nRefresh complete in {elapsed:.1f}s: {total_sources} sources, "
          f"{total_chunks} chunks, {embedded} newly embedded")
    db.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Incremental refresh for Skills Flex cells')
    parser.add_argument('--cell', default='tools',
                        help='Cell name (default: tools)')
    parser.add_argument('--since', default=None, type=str,
                        help='GitHub search recency filter (e.g. 7d → pushed:>7 days ago)')
    parser.add_argument('--mode', default=None,
                        help='Comma-separated phases (default: all). '
                             'Options: search,catalog,enrich,readme,skills')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show stats without making changes')
    args = parser.parse_args()

    from flex.registry import resolve_cell
    cell_path = resolve_cell(args.cell)
    if not cell_path:
        print(f"Cell '{args.cell}' not found in registry.")
        sys.exit(1)

    since_days = None
    if args.since:
        since_days = int(args.since.strip().lower().rstrip('d'))

    modes = args.mode.split(',') if args.mode else None

    refresh(str(cell_path), graph=args.graph, dry_run=args.dry_run,
            since_days=since_days, modes=modes)


if __name__ == '__main__':
    main()
