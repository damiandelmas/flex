"""
Expand an arXiv cell via snowball citation graph + category sweep.

Three expansion modes:
1. snowball: Take top-N most relevant papers, get their references/citations via S2
2. category: Pull recent papers from specific arXiv categories
3. source: Download LaTeX source for top-N most relevant papers (adds sections + tree)

Usage:
    python -m flex.modules.arxiv.compile.expand --cell arxiv --mode snowball --top 20
    python -m flex.modules.arxiv.compile.expand --cell arxiv --mode category --categories cs.IR,cs.DB
    python -m flex.modules.arxiv.compile.expand --cell arxiv --mode source --top 50
    python -m flex.modules.arxiv.compile.expand --cell arxiv --mode all --top 20
"""

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np

from flex.core import open_cell, set_meta, log_op
from flex.modules.arxiv.compile.worker import (
    SCHEMA_DDL, embed_new, ingest_paper, ingest_paper_abstract_only,
)


def _get_top_papers(db, query_text: str, top_n: int = 20) -> list[str]:
    """Get top-N most relevant paper IDs by embedding similarity to query."""
    from flex.onnx.embed import ONNXEmbedder

    rows = db.execute(
        "SELECT id, embedding FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchall()
    if not rows:
        return []

    ids = [r[0] for r in rows]
    matrix = np.array([np.frombuffer(r[1], dtype=np.float32) for r in rows])
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1
    matrix = matrix / norms

    embedder = ONNXEmbedder()
    q = embedder.encode([query_text])[0]
    q = q / np.linalg.norm(q)

    sims = matrix @ q
    top_k = np.argsort(sims)[::-1][:top_n]

    # Extract unique source_ids
    seen = set()
    result = []
    for idx in top_k:
        source_id = ids[idx].split(":")[0]
        if source_id not in seen:
            seen.add(source_id)
            result.append(source_id)

    return result


def expand_snowball(db, cell_path: str, top_n: int = 20,
                    max_per_seed: int = 50, dry_run: bool = False):
    """Snowball expansion: top papers → S2 references/citations → ingest."""
    from flex.modules.arxiv.compile.semantic_scholar import snowball

    query = (
        "composable operators for vector retrieval scoring modulation "
        "diversity MMR temporal decay contrastive suppression trajectory "
        "SQL pre-filter embedding search RAG pipeline"
    )

    print("Finding seed papers...")
    seeds = _get_top_papers(db, query, top_n)
    print(f"Seeds: {len(seeds)} papers")
    for s in seeds[:5]:
        title = db.execute(
            "SELECT title FROM _raw_sources WHERE source_id = ?", (s,)
        ).fetchone()
        print(f"  {s}: {title[0][:70] if title else '?'}")
    if len(seeds) > 5:
        print(f"  ... and {len(seeds) - 5} more")

    if dry_run:
        print("\nDry run — would snowball from these seeds.")
        return

    print(f"\nSnowballing (max {max_per_seed} refs+cites per seed)...")
    new_papers = snowball(seeds, max_per_seed=max_per_seed)
    print(f"\nDiscovered {len(new_papers)} new papers via citation graph")

    # Ingest
    total_s, total_c = 0, 0
    for i, paper in enumerate(new_papers):
        if i % 50 == 0 and i > 0:
            print(f"  Ingested {i}/{len(new_papers)}...")
        s, c = ingest_paper_abstract_only(paper, db)
        total_s += s
        total_c += c

    print(f"Ingested: {total_s} new papers, {total_c} chunks")
    return total_s, total_c


def expand_category(db, cell_path: str, categories: list[str],
                    max_per_cat: int = 200, dry_run: bool = False):
    """Category sweep: pull recent papers from specific arXiv categories."""
    from flex.modules.arxiv.compile.arxiv_api import pull_papers

    total_s, total_c = 0, 0

    for cat in categories:
        print(f"\n{'=' * 50}")
        print(f"Category: {cat}")
        print(f"{'=' * 50}")

        papers = pull_papers(
            f"cat:{cat}",
            max_total=max_per_cat,
            sort_by="submittedDate",
            sort_order="descending",
        )
        print(f"  Found {len(papers)} papers")

        if dry_run:
            continue

        for paper in papers:
            s, c = ingest_paper_abstract_only(paper, db)
            total_s += s
            total_c += c

        print(f"  Ingested: {total_s} new (cumulative)")

    print(f"\nCategory sweep: {total_s} new papers, {total_c} chunks")
    return total_s, total_c


def expand_source(db, cell_path: str, top_n: int = 50, dry_run: bool = False):
    """Download LaTeX source for top-N most relevant papers. Adds sections + tree."""
    from flex.modules.arxiv.compile.arxiv_api import download_source, DELAY
    from flex.modules.arxiv.compile.latex_parser import split_sections, build_tree_edges

    query = (
        "composable operators for vector retrieval scoring modulation "
        "diversity MMR contrastive suppression trajectory embedding "
        "SQL pre-filter sqlite numpy RAG pipeline community detection"
    )

    print("Finding papers to expand with LaTeX source...")
    seeds = _get_top_papers(db, query, top_n)

    # Filter to papers that only have abstract (1 chunk)
    abstract_only = []
    for s in seeds:
        count = db.execute(
            "SELECT COUNT(*) FROM _edges_source WHERE source_id = ?", (s,)
        ).fetchone()[0]
        has_latex = db.execute(
            "SELECT COUNT(*) FROM _edges_raw_content WHERE source_id = ?", (s,)
        ).fetchone()[0]
        if count <= 1 and not has_latex:
            abstract_only.append(s)

    print(f"Papers needing source: {len(abstract_only)} of {len(seeds)}")

    if dry_run:
        for s in abstract_only[:10]:
            title = db.execute(
                "SELECT title FROM _raw_sources WHERE source_id = ?", (s,)
            ).fetchone()
            print(f"  {s}: {title[0][:70] if title else '?'}")
        return

    expanded = 0
    for i, source_id in enumerate(abstract_only):
        title_row = db.execute(
            "SELECT title FROM _raw_sources WHERE source_id = ?", (source_id,)
        ).fetchone()
        title = title_row[0][:60] if title_row else "?"
        print(f"  [{i+1}/{len(abstract_only)}] {source_id}: {title}...", end="")

        latex = download_source(source_id, quiet=True)
        time.sleep(DELAY)

        if not latex:
            print(" (no source)")
            continue

        sections = split_sections(latex)
        if len(sections) <= 1:
            print(f" ({len(sections)} section, skip)")
            continue

        tree_edges = build_tree_edges(sections, source_id)

        # Delete existing single-chunk data for this paper
        old_chunks = db.execute(
            "SELECT chunk_id FROM _edges_source WHERE source_id = ?", (source_id,)
        ).fetchall()
        for (cid,) in old_chunks:
            db.execute("DELETE FROM _raw_chunks WHERE id = ?", (cid,))
            db.execute("DELETE FROM _types_arxiv WHERE chunk_id = ?", (cid,))
            db.execute("DELETE FROM _edges_tree WHERE id = ?", (cid,))
        db.execute("DELETE FROM _edges_source WHERE source_id = ?", (source_id,))
        db.execute("DELETE FROM _raw_sources WHERE source_id = ?", (source_id,))

        # Get original paper metadata from types
        paper_meta = {
            "arxiv_id": source_id, "arxiv_id_base": source_id,
            "title": title_row[0] if title_row else "",
            "abstract": "", "authors": [], "authors_str": "",
            "primary_category": "", "categories_str": "",
            "published": "", "created_utc": 0,
            "pdf_url": f"https://arxiv.org/pdf/{source_id}",
            "abs_url": f"https://arxiv.org/abs/{source_id}",
            "comment": "", "journal_ref": "", "doi": "",
        }
        # Try to recover metadata from types table backup
        meta_row = db.execute(
            "SELECT primary_category, categories, authors, published, doi "
            "FROM _types_arxiv WHERE arxiv_id = ? LIMIT 1", (source_id,)
        ).fetchone()
        if meta_row:
            paper_meta.update({
                "primary_category": meta_row[0] or "",
                "categories_str": meta_row[1] or "",
                "authors_str": meta_row[2] or "",
                "published": meta_row[3] or "",
                "doi": meta_row[4] or "",
            })

        s, c = ingest_paper(paper_meta, sections, tree_edges, latex, db)
        print(f" ({len(sections)} sections, {len(tree_edges)} tree edges)")
        expanded += 1

    print(f"\nExpanded {expanded} papers with full LaTeX sections")
    return expanded


def main():
    parser = argparse.ArgumentParser(
        description="Expand arXiv cell via snowball, category sweep, or source download")
    parser.add_argument("--cell", default="arxiv", help="Cell name")
    parser.add_argument("--mode", required=True,
                        choices=["snowball", "category", "source", "all"],
                        help="Expansion mode")
    parser.add_argument("--top", type=int, default=20,
                        help="Top-N papers for snowball/source (default: 20)")
    parser.add_argument("--categories", default="cs.IR,cs.DB",
                        help="arXiv categories for category mode (default: cs.IR,cs.DB)")
    parser.add_argument("--max-per-cat", type=int, default=200,
                        help="Max papers per category (default: 200)")
    parser.add_argument("--max-per-seed", type=int, default=50,
                        help="Max refs+cites per seed in snowball (default: 50)")
    parser.add_argument("--graph", action="store_true",
                        help="Rebuild graph after expansion")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from flex.registry import resolve_cell
    cell_path = str(resolve_cell(args.cell))

    db = open_cell(cell_path)
    db.executescript(SCHEMA_DDL)

    before = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]

    if args.mode in ("snowball", "all"):
        expand_snowball(db, cell_path, top_n=args.top,
                        max_per_seed=args.max_per_seed, dry_run=args.dry_run)

    if args.mode in ("category", "all"):
        cats = [c.strip() for c in args.categories.split(",")]
        expand_category(db, cell_path, cats,
                        max_per_cat=args.max_per_cat, dry_run=args.dry_run)

    if args.mode in ("source", "all"):
        expand_source(db, cell_path, top_n=args.top, dry_run=args.dry_run)

    if not args.dry_run:
        # Embed new chunks
        print("\nEmbedding new chunks...")
        embedded = embed_new(db)
        print(f"Embedded: {embedded}")

        after = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
        print(f"\nCell: {before} → {after} papers (+{after - before})")

        # Graph
        if args.graph:
            import subprocess
            print("Rebuilding similarity graph...")
            subprocess.run([sys.executable, "-m", "flex.manage.meditate",
                            "--cell", cell_path], check=True)

        # Update views
        from flex.views import regenerate_views, install_views
        views_dir = Path(__file__).parent.parent / "stock" / "views"
        if views_dir.exists():
            install_views(db, views_dir)
        regenerate_views(db)

        set_meta(db, "last_pull_at",
                 __import__("datetime").datetime.now(
                     __import__("datetime").timezone.utc).isoformat())

        log_op(db, "arxiv_expand", "_raw_chunks",
               params={"mode": args.mode, "before": before, "after": after},
               rows_affected=after - before,
               source="arxiv/compile/expand.py")
        db.commit()

    db.close()


if __name__ == "__main__":
    main()
