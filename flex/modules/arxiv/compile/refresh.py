"""
Incremental arXiv cell refresh.

Reads stored queries and last_pull_ts from cell _meta, pulls new papers
since then, ingests, embeds, and optionally rebuilds the graph.

Idempotent: dedup by arxiv_id_base, INSERT OR IGNORE on chunks.

Usage:
    python -m flex.modules.arxiv.compile.refresh --cell arxiv
    python -m flex.modules.arxiv.compile.refresh --cell arxiv --dry-run
    python -m flex.modules.arxiv.compile.refresh --cell arxiv --with-source
    python -m flex.modules.arxiv.compile.refresh --cell arxiv --since 30d
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.core import open_cell, get_meta, set_meta, log_op


GRAPH_REFRESH_THRESHOLD = 20  # rebuild graph if >= N new sources
DEFAULT_REFRESH_MAX_PAPERS = 25


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False,
            since_days: int | None = None, with_source: bool = False,
            max_papers_per_query: int = DEFAULT_REFRESH_MAX_PAPERS) -> dict:
    """Pull new papers and ingest into existing arxiv cell.

    Args:
        cell_path: Path to the arxiv cell .db file.
        graph: Force graph rebuild after ingest.
        dry_run: Show stats without making changes.
        since_days: Override cursor — pull this many days back.
        with_source: Download LaTeX source for full-section parsing.
        max_papers_per_query: per-query pull cap. Public default is small so
            scheduler refreshes stay polite on the no-auth arXiv API.

    Returns stats dict with counts.
    """
    from flex.modules.arxiv.compile.arxiv_api import pull_papers, download_source, DELAY
    from flex.modules.arxiv.compile.latex_parser import split_sections, build_tree_edges
    from flex.modules.arxiv.compile.worker import (
        ingest_paper, ingest_paper_abstract_only, embed_new,
    )

    db = open_cell(cell_path)

    # Read stored queries
    queries = json.loads(get_meta(db, 'queries') or '[]')
    if not queries:
        print("No queries stored in cell _meta. Run worker.py first to seed queries.")
        db.close()
        return {'error': 'no_queries'}

    # Read cursor
    if since_days is not None:
        after_ts = int(time.time()) - (since_days * 86400)
    else:
        after_ts = int(get_meta(db, 'last_pull_ts') or '0')

    after_dt = datetime.fromtimestamp(after_ts, tz=timezone.utc) if after_ts else None
    print(f"Cell: {cell_path}")
    print(f"Queries: {len(queries)}")
    print(f"Last pull: {after_dt.isoformat() if after_dt else 'never'}")
    print(f"Pulling papers after timestamp {after_ts}")
    if with_source:
        print("LaTeX source: enabled")
    print()

    # Pull papers across all stored queries, dedup by arxiv_id_base
    all_papers = {}
    for q in queries:
        q = q.strip()
        papers = pull_papers(q, after_ts=after_ts,
                             max_total=max_papers_per_query)
        before = len(all_papers)
        for p in papers:
            all_papers[p["arxiv_id_base"]] = p
        new = len(all_papers) - before
        print(f"  {q[:60]:60s} → {len(papers)} fetched, {new} new")

    print(f"\nTotal new unique papers: {len(all_papers)}")

    if dry_run:
        for pid, p in list(all_papers.items())[:10]:
            print(f"  {pid}: {p['title'][:80]}")
        if len(all_papers) > 10:
            print(f"  ... ({len(all_papers)} total)")
        db.close()
        return {'dry_run': True, 'papers': len(all_papers)}

    if not all_papers:
        print("No new papers.")
        db.close()
        return {'sources': 0, 'chunks': 0}

    # Ingest
    t0 = time.time()
    total_sources = 0
    total_chunks = 0

    for i, (pid, paper) in enumerate(all_papers.items()):
        print(f"  [{i+1}/{len(all_papers)}] {pid}: {paper['title'][:60]}...", end="")

        if with_source:
            from flex.modules.arxiv.compile.arxiv_api import download_source as dl_src
            latex = dl_src(pid, quiet=True)
            time.sleep(DELAY)
        else:
            latex = None

        if latex:
            sections = split_sections(latex)
            tree_edges = build_tree_edges(sections, pid)
            s, c = ingest_paper(paper, sections, tree_edges, latex, db)
            src_label = f"({len(sections)} sections)"
        else:
            s, c = ingest_paper_abstract_only(paper, db)
            src_label = "(abstract only)"

        total_sources += s
        total_chunks += c
        print(f" {src_label}")

    if total_chunks == 0:
        print("\nAll papers already ingested (dedup).")
        db.close()
        return {'sources': 0, 'chunks': 0}

    # Embed
    print(f"\nEmbedding {total_chunks} new chunks...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded}")

    # Graph
    if graph or total_sources >= GRAPH_REFRESH_THRESHOLD:
        import subprocess
        print("Rebuilding similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Update cursor
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
    log_op(db, 'arxiv_refresh', '_raw_chunks',
           params={'queries': len(queries), 'sources': total_sources,
                   'chunks': total_chunks, 'embedded': embedded,
                   'after_ts': after_ts, 'with_source': with_source},
           rows_affected=total_chunks,
           source='arxiv/compile/refresh.py')
    db.commit()

    stats = {
        'sources': total_sources,
        'chunks': total_chunks,
        'embedded': embedded,
    }

    print(f"\nRefresh complete in {elapsed:.1f}s: {total_sources} papers, "
          f"{total_chunks} chunks, {embedded} embedded")
    db.close()
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Incremental refresh for arXiv Flex cells')
    parser.add_argument('--cell', default='arxiv',
                        help='Cell name (default: arxiv)')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 30d). Overrides cursor.')
    parser.add_argument('--with-source', action='store_true',
                        help='Download LaTeX source for full-section parsing')
    parser.add_argument('--max-papers', type=int,
                        default=DEFAULT_REFRESH_MAX_PAPERS,
                        help=f'Max papers per query (default: {DEFAULT_REFRESH_MAX_PAPERS})')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new papers without ingesting')
    args = parser.parse_args()

    from flex.registry import resolve_cell
    cell_path = resolve_cell(args.cell)
    if not cell_path:
        print(f"Cell '{args.cell}' not found in registry.")
        sys.exit(1)

    since_days = None
    if args.since:
        since_days = int(args.since.strip().lower().rstrip('d'))

    refresh(str(cell_path), graph=args.graph, dry_run=args.dry_run,
            since_days=since_days, with_source=args.with_source,
            max_papers_per_query=args.max_papers)


if __name__ == '__main__':
    main()
