"""
Expand a skills cell via awesome list re-crawl, GitHub enrichment,
README fetch, skill artifact discovery, and registry pulls.

Five expansion modes:
1. catalog: Re-parse awesome lists, find new entries
2. enrich: GitHub API metadata backfill (stars, language, etc.)
3. readme: Fetch + split READMEs into span chunks
4. skills: Discover Claude Code skill artifacts (.claude/skills/, agents, hooks)
5. registry: Pull from Smithery/Glama/skillsindex (stub)

Usage:
    python -m flex.modules.skills.compile.expand --cell tools --mode catalog
    python -m flex.modules.skills.compile.expand --cell tools --mode enrich
    python -m flex.modules.skills.compile.expand --cell tools --mode readme
    python -m flex.modules.skills.compile.expand --cell tools --mode skills
    python -m flex.modules.skills.compile.expand --cell tools --mode all
"""

import argparse
import os
import sys
import time
from pathlib import Path

from flex.core import open_cell, log_op
from flex.modules.skills.compile.worker import (
    embed_new, ingest_catalog, ingest_readme, ingest_skill_artifacts,
    _insert_identity_edges, DEFAULT_AWESOME_REPOS,
)


def _budget(name: str, default: int) -> int:
    """Return per-refresh phase budget from env, with a conservative default."""
    raw = os.environ.get(name, str(default))
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _ensure_probe_status(db) -> None:
    """Create durable negative-probe markers for expensive GitHub checks."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS _skills_probe_status (
            source_id TEXT NOT NULL,
            probe_type TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT,
            PRIMARY KEY (source_id, probe_type)
        )
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_skills_probe_type_status
        ON _skills_probe_status(probe_type, status)
    """)


def _mark_probe(db, source_id: str, probe_type: str,
                status: str, detail: str = '') -> None:
    """Record the latest result for an expensive repo-level probe."""
    from datetime import datetime, timezone

    db.execute("""
        INSERT INTO _skills_probe_status
            (source_id, probe_type, checked_at, status, detail)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id, probe_type) DO UPDATE SET
            checked_at = excluded.checked_at,
            status = excluded.status,
            detail = excluded.detail
    """, (
        source_id,
        probe_type,
        datetime.now(timezone.utc).isoformat(),
        status,
        detail,
    ))


# ═════════════════════════════════════════════════════
# Mode 1: catalog
# ═════════════════════════════════════════════════════

def expand_catalog(db, awesome_repos: list[str], dry_run: bool = False):
    """Re-parse awesome lists, find new entries not already in cell."""
    from flex.modules.skills.compile.awesome_parser import parse_awesome_list

    existing = {r[0] for r in db.execute(
        "SELECT source_id FROM _raw_sources"
    ).fetchall()}

    total_new = 0
    for repo in awesome_repos:
        repo = repo.strip()
        entries = parse_awesome_list(repo)
        registry_name = repo.split('/')[-1] if '/' in repo else repo

        new_entries = []
        for entry in entries:
            if entry.github_owner and entry.github_repo:
                source_id = f"{entry.github_owner}/{entry.github_repo}"
            else:
                import hashlib
                source_id = hashlib.sha256(entry.url.encode()).hexdigest()[:16]

            if source_id not in existing:
                new_entries.append(entry)
                existing.add(source_id)

        if dry_run:
            print(f"  {repo}: {len(new_entries)} new entries")
            total_new += len(new_entries)
            continue

        if new_entries:
            s, c = ingest_catalog(new_entries, db, registry_name=registry_name)
            print(f"  {repo}: +{s} sources, +{c} chunks")
            total_new += s

    if dry_run:
        print(f"  Would add {total_new} new entries total")
        return

    if total_new > 0:
        embedded = embed_new(db)
        print(f"  Embedded: {embedded} new chunks")

    log_op(db, 'skills_expand_catalog', '_raw_chunks',
           params={'repos': awesome_repos, 'new': total_new},
           rows_affected=total_new,
           source='skills/compile/expand.py')
    db.commit()


# ═════════════════════════════════════════════════════
# Mode 2: enrich
# ═════════════════════════════════════════════════════

def expand_enrich(db, dry_run: bool = False):
    """Enrich sources missing GitHub metadata."""
    from flex.modules.skills.compile.github_api import get_repo_metadata

    _ensure_probe_status(db)
    limit = _budget('FLEX_SKILLS_ENRICH_LIMIT', 50)
    unenriched = db.execute("""
        SELECT DISTINCT t.github_owner, t.github_repo, es.source_id
        FROM _types_skills t
        JOIN _edges_source es ON t.chunk_id = es.chunk_id
        LEFT JOIN _skills_probe_status ps
          ON ps.source_id = es.source_id
         AND ps.probe_type = 'metadata'
         AND ps.status = 'not_found'
        WHERE t.chunk_type = 'catalog'
        AND t.stars IS NULL
        AND t.github_owner IS NOT NULL
        AND ps.source_id IS NULL
        LIMIT ?
    """, (limit,)).fetchall()

    if dry_run:
        print(f"  Would enrich up to {len(unenriched)} repos (budget {limit})")
        return

    token = os.environ.get('GITHUB_TOKEN')
    enriched = 0
    for i, (owner, repo, source_id) in enumerate(unenriched):
        meta = get_repo_metadata(owner, repo, token)
        if meta:
            db.execute("""
                UPDATE _types_skills SET
                    stars = ?, language = ?, license = ?, topics = ?,
                    last_commit = ?, open_issues = ?, github_id = ?
                WHERE chunk_id = ?
            """, (meta['stars'], meta['language'], meta['license'],
                  meta['topics'], meta['last_commit'], meta['open_issues'],
                  meta.get('github_id'),
                  f"{source_id}:0"))
            db.execute("UPDATE _raw_sources SET score = ? WHERE source_id = ?",
                       (meta['stars'], source_id))
            if meta['description']:
                db.execute("UPDATE _raw_chunks SET content = ? WHERE id = ?",
                           (meta['description'], f"{source_id}:0"))
            enriched += 1
            _mark_probe(db, source_id, 'metadata', 'found')
        else:
            _mark_probe(db, source_id, 'metadata', 'not_found')

        if (i + 1) % 100 == 0:
            print(f"  Enriched {i+1}/{len(unenriched)}")
            db.commit()

        time.sleep(0.1)

    # Backfill repo identity for ALL chunks sharing the same source_id
    try:
        db.execute("""
            INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked)
            SELECT es2.chunk_id, 'github:' || t.github_id, 1
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            JOIN _edges_source es2 ON es.source_id = es2.source_id
            WHERE t.github_id IS NOT NULL
            AND t.chunk_type = 'catalog'
            AND es2.chunk_id NOT IN (SELECT chunk_id FROM _edges_repo_identity)
        """)
    except Exception:
        pass  # Table may not exist in older cells

    db.commit()
    print(f"  Enriched: {enriched}/{len(unenriched)} repos")

    log_op(db, 'skills_expand_enrich', '_types_skills',
           params={'enriched': enriched, 'total': len(unenriched)},
           rows_affected=enriched,
           source='skills/compile/expand.py')
    db.commit()


# ═════════════════════════════════════════════════════
# Mode 3: readme
# ═════════════════════════════════════════════════════

def expand_readme(db, dry_run: bool = False):
    """Fetch + split READMEs for sources without readme chunks."""
    from flex.modules.skills.compile.github_api import get_readme

    _ensure_probe_status(db)
    limit = _budget('FLEX_SKILLS_README_LIMIT', 25)
    sources_without_readme = db.execute("""
        SELECT DISTINCT t.github_owner, t.github_repo, es.source_id
        FROM _types_skills t
        JOIN _edges_source es ON t.chunk_id = es.chunk_id
        LEFT JOIN _skills_probe_status ps
          ON ps.source_id = es.source_id
         AND ps.probe_type = 'readme'
         AND ps.status = 'not_found'
        WHERE t.chunk_type = 'catalog'
        AND t.github_owner IS NOT NULL
        AND ps.source_id IS NULL
        AND es.source_id NOT IN (
            SELECT es2.source_id FROM _types_skills t2
            JOIN _edges_source es2 ON t2.chunk_id = es2.chunk_id
            WHERE t2.chunk_type = 'readme'
        )
        LIMIT ?
    """, (limit,)).fetchall()

    if dry_run:
        print(f"  Would fetch up to {len(sources_without_readme)} READMEs (budget {limit})")
        return

    token = os.environ.get('GITHUB_TOKEN')
    readme_total = 0
    span_total = 0
    for i, (owner, repo, source_id) in enumerate(sources_without_readme):
        result = get_readme(owner, repo, token)
        if result:
            readme_content, readme_blob_hash = result
            r, s = ingest_readme(source_id, readme_content, db,
                                 blob_hash=readme_blob_hash)
            readme_total += r
            span_total += s
            _mark_probe(db, source_id, 'readme', 'found')
        else:
            _mark_probe(db, source_id, 'readme', 'not_found')

        if (i + 1) % 100 == 0:
            print(f"  Fetched {i+1}/{len(sources_without_readme)}")

        time.sleep(0.1)

    if readme_total > 0:
        embedded = embed_new(db)
        print(f"  Embedded: {embedded} new chunks")

    print(f"  READMEs: {readme_total}, Spans: {span_total}")

    log_op(db, 'skills_expand_readme', '_raw_chunks',
           params={'readmes': readme_total, 'spans': span_total},
           rows_affected=readme_total + span_total,
           source='skills/compile/expand.py')
    db.commit()


# ═════════════════════════════════════════════════════
# Mode 4: skills
# ═════════════════════════════════════════════════════

def expand_skills(db, dry_run: bool = False):
    """Discover Claude Code skill artifacts in indexed repos."""
    from flex.modules.skills.compile.github_api import discover_skill_artifacts

    _ensure_probe_status(db)
    limit = _budget('FLEX_SKILLS_ARTIFACT_LIMIT', 25)
    sources_without_skills = db.execute("""
        SELECT DISTINCT t.github_owner, t.github_repo, es.source_id
        FROM _types_skills t
        JOIN _edges_source es ON t.chunk_id = es.chunk_id
        LEFT JOIN _skills_probe_status ps
          ON ps.source_id = es.source_id
         AND ps.probe_type = 'artifacts'
         AND ps.status = 'not_found'
        WHERE t.chunk_type = 'catalog'
        AND t.github_owner IS NOT NULL
        AND ps.source_id IS NULL
        AND es.source_id NOT IN (
            SELECT es2.source_id FROM _types_skills t2
            JOIN _edges_source es2 ON t2.chunk_id = es2.chunk_id
            WHERE t2.chunk_type IN ('skill', 'agent', 'hook', 'command', 'manifest')
        )
        LIMIT ?
    """, (limit,)).fetchall()

    if dry_run:
        print(f"  Would check up to {len(sources_without_skills)} repos for skill artifacts (budget {limit})")
        return

    token = os.environ.get('GITHUB_TOKEN')
    found = 0
    for i, (owner, repo, source_id) in enumerate(sources_without_skills):
        artifacts = discover_skill_artifacts(owner, repo, token)
        if artifacts:
            n = ingest_skill_artifacts(source_id, artifacts, db)
            found += n
            _mark_probe(db, source_id, 'artifacts', 'found', str(n))
        else:
            _mark_probe(db, source_id, 'artifacts', 'not_found')

        if (i + 1) % 100 == 0:
            print(f"  Checked {i+1}/{len(sources_without_skills)}")
            db.commit()

    if found > 0:
        embedded = embed_new(db)
        print(f"  Embedded: {embedded} new chunks")

    print(f"  Found {found} skill artifacts")

    log_op(db, 'skills_expand_skills', '_raw_chunks',
           params={'found': found, 'checked': len(sources_without_skills)},
           rows_affected=found,
           source='skills/compile/expand.py')
    db.commit()


# ═════════════════════════════════════════════════════
# Mode 5: registry (stub)
# ═════════════════════════════════════════════════════

def expand_registry(db, dry_run: bool = False):
    """Pull from MCP registries (Smithery, Glama, skillsindex). Stub."""
    print("  Registry expansion not yet implemented")
    print("  Pending: Smithery REST API, Glama RSS, skillsindex SSR")


# ═════════════════════════════════════════════════════
# Mode 6: search (GitHub Search API — primary funnel)
# ═════════════════════════════════════════════════════

def expand_search(db, queries: list[str] | None = None,
                  min_stars: int = 100, dry_run: bool = False):
    """Discover repos via GitHub Search API and ingest as catalog entries.

    This is the primary growth funnel. Awesome lists seed the initial corpus;
    search expands it to the full ecosystem. Dedup by source_id (owner/repo).

    Args:
        db: cell database connection
        queries: GitHub search queries (default: DEFAULT_SEARCH_QUERIES)
        min_stars: minimum star count (default: 100)
        dry_run: show stats without making changes
    """
    from flex.modules.skills.compile.github_api import (
        search_repos, DEFAULT_SEARCH_QUERIES,
    )
    from flex.modules.skills.compile.awesome_parser import AwesomeEntry
    import hashlib
    import time as _time

    if queries is None:
        queries = DEFAULT_SEARCH_QUERIES

    # Existing sources for dedup
    existing = {r[0] for r in db.execute(
        "SELECT source_id FROM _raw_sources"
    ).fetchall()}

    token = os.environ.get('GITHUB_TOKEN')
    all_new = []
    seen = set()

    for query in queries:
        print(f"  Searching: {query} (stars>={min_stars})")
        repos = search_repos(query, min_stars=min_stars, token=token)
        new_count = 0

        for meta in repos:
            full_name = meta['full_name']
            if full_name in existing or full_name in seen:
                continue
            seen.add(full_name)

            parts = full_name.split('/')
            if len(parts) != 2:
                continue
            owner, repo = parts

            # Build an AwesomeEntry from search metadata
            entry = AwesomeEntry(
                name=meta['name'],
                url=f"https://github.com/{full_name}",
                description=meta['description'] or meta['name'],
                category='search',
                subcategory=query,
                github_owner=owner,
                github_repo=repo,
                author=owner,
                emoji_badges=None,
                position=0,
                heading_depth=2,
            )
            all_new.append((entry, meta))
            new_count += 1

        print(f"    {len(repos)} results, {new_count} new")

    if dry_run:
        print(f"\n  Would add {len(all_new)} new repos")
        # Show top by stars
        all_new.sort(key=lambda x: x[1].get('stars', 0), reverse=True)
        for entry, meta in all_new[:20]:
            print(f"    {meta['full_name']:40s} {meta['stars']:>6}★  {meta['description'][:60]}")
        if len(all_new) > 20:
            print(f"    ... and {len(all_new) - 20} more")
        return

    if not all_new:
        print("  No new repos found")
        return

    # Ingest catalog entries
    entries = [e for e, _ in all_new]
    s, c = ingest_catalog(entries, db, registry_name='github-search')
    print(f"  Ingested: {s} sources, {c} chunks")

    # Pre-populate metadata from search results (saves enrich API calls)
    for entry, meta in all_new:
        source_id = f"{entry.github_owner}/{entry.github_repo}"
        chunk_id = f"{source_id}:0"
        db.execute("""
            UPDATE _types_skills SET
                stars = ?, language = ?, license = ?, topics = ?,
                last_commit = ?, open_issues = ?, github_id = ?
            WHERE chunk_id = ? AND stars IS NULL
        """, (meta['stars'], meta['language'], meta['license'],
              meta['topics'], meta['last_commit'], meta['open_issues'],
              meta.get('github_id'), chunk_id))

        if meta.get('description'):
            db.execute("UPDATE _raw_chunks SET content = ? WHERE id = ?",
                       (meta['description'], chunk_id))
        db.execute("UPDATE _raw_sources SET score = ? WHERE source_id = ?",
                   (meta['stars'], source_id))

    # Backfill repo identity from newly set github_ids
    try:
        db.execute("""
            INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked)
            SELECT es2.chunk_id, 'github:' || t.github_id, 1
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            JOIN _edges_source es2 ON es.source_id = es2.source_id
            WHERE t.github_id IS NOT NULL
            AND t.chunk_type = 'catalog'
            AND es2.chunk_id NOT IN (SELECT chunk_id FROM _edges_repo_identity)
        """)
    except Exception:
        pass

    db.commit()
    print(f"  Metadata pre-populated from search results (skips enrich for these)")

    log_op(db, 'skills_expand_search', '_raw_chunks',
           params={'queries': queries, 'min_stars': min_stars,
                   'new': len(all_new)},
           rows_affected=len(all_new),
           source='skills/compile/expand.py')
    db.commit()


# ═════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Expand a skills cell with new data')
    parser.add_argument('--cell', default='tools',
                        help='Cell name or path (default: tools)')
    parser.add_argument('--mode', required=True,
                        choices=['catalog', 'enrich', 'readme', 'skills',
                                 'registry', 'search', 'all'],
                        help='Expansion mode')
    parser.add_argument('--awesome', default=None,
                        help='Comma-separated awesome-list repos (for catalog mode)')
    parser.add_argument('--queries', default=None,
                        help='Comma-separated GitHub search queries (for search mode)')
    parser.add_argument('--min-stars', type=int, default=100,
                        help='Minimum stars for search mode (default: 100)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show stats without making changes')
    args = parser.parse_args()

    # Resolve cell
    cell_path = args.cell
    if not cell_path.endswith('.db'):
        from flex.registry import CELLS_DIR
        cell_path = str(CELLS_DIR / f"{args.cell}.db")

    if not os.path.exists(cell_path):
        print(f"Cell not found: {cell_path}", file=sys.stderr)
        print("Run worker.py first to create the cell.", file=sys.stderr)
        sys.exit(1)

    db = open_cell(cell_path)

    awesome_repos = (args.awesome.split(',') if args.awesome
                     else DEFAULT_AWESOME_REPOS)

    modes = (['search', 'catalog', 'enrich', 'readme', 'skills']
             if args.mode == 'all' else [args.mode])

    search_queries = (args.queries.split(',') if args.queries else None)

    for mode in modes:
        print(f"\n{'=' * 50}")
        print(f"Expand: {mode}")
        print(f"{'=' * 50}")

        if mode == 'catalog':
            expand_catalog(db, awesome_repos, args.dry_run)
        elif mode == 'enrich':
            expand_enrich(db, args.dry_run)
        elif mode == 'readme':
            expand_readme(db, args.dry_run)
        elif mode == 'skills':
            expand_skills(db, args.dry_run)
        elif mode == 'registry':
            expand_registry(db, args.dry_run)
        elif mode == 'search':
            expand_search(db, queries=search_queries,
                          min_stars=args.min_stars, dry_run=args.dry_run)

    # Regenerate views after expansion
    if not args.dry_run:
        from flex.views import regenerate_views, install_views
        views_dir = Path(__file__).parent.parent / 'stock' / 'views'
        if views_dir.exists():
            install_views(db, views_dir)
        regenerate_views(db)
        print("\nViews regenerated.")

    db.close()
    print("Done.")


if __name__ == '__main__':
    main()
