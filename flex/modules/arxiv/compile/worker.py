"""
arXiv cell compiler — ingests arXiv papers into a Flex cell.

Source = paper (one arXiv submission).
Chunk = section within the paper (abstract, introduction, methods, etc.).
Tree = heading hierarchy via _edges_tree (first cell to implement the substrate spec).

Non-destructive ingestion:
  - arxiv IDs preserved raw (e.g. "2401.12345")
  - Section headings stored raw in _types_arxiv.section_heading
  - Canonical section_type mapping happens at VIEW level only
  - Full LaTeX source stored in _raw_content when available
  - API metadata preserved: categories, DOI, journal_ref, comment

Two-tier structure:
  paper (source) > section (chunk) — same pattern as:
  session > message, post > comment, document > section

Entry point:
    python -m flex.modules.arxiv.compile.worker \
        --queries "all:composable vector retrieval" \
        --cell arxiv \
        --graph
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

from flex.core import open_cell, set_meta, validate_cell, log_op

DEFAULT_CELL_NAME = "arxiv"
DEFAULT_DESCRIPTION = "arXiv papers — public research literature cell"
DEFAULT_REFRESH_INTERVAL = 6 * 60 * 60
REFRESH_MODULE = "flex.modules.arxiv.compile.refresh"
DEFAULT_MAX_PAPERS = 25
PUBLIC_SEED_QUERIES = [
    "all:retrieval augmented generation",
]


# ═════════════════════════════════════════════════════
# SCHEMA DDL
# ═════════════════════════════════════════════════════

SCHEMA_DDL = """
-- RAW LAYER
CREATE TABLE IF NOT EXISTS _raw_chunks (
    id TEXT PRIMARY KEY,
    content TEXT,
    embedding BLOB,
    timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS _raw_sources (
    source_id TEXT PRIMARY KEY,
    title TEXT,
    source TEXT,
    file_date TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    num_comments INTEGER DEFAULT 0,
    url TEXT,
    embedding BLOB
);

-- RAW CONTENT (non-destructive: stores full LaTeX source)
CREATE TABLE IF NOT EXISTS _raw_content (
    content_hash TEXT PRIMARY KEY,
    content TEXT
);

CREATE TABLE IF NOT EXISTS _edges_raw_content (
    source_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_type TEXT DEFAULT 'latex'
);
CREATE INDEX IF NOT EXISTS idx_erc_source ON _edges_raw_content(source_id);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'arxiv',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

-- TREE LAYER (substrate spec: heading hierarchy as DAG)
-- First cell to implement _edges_tree per the universal cell substrate design.
-- Schema: /context/intended/design/260306-2145_universal-cell-substrate.md
CREATE TABLE IF NOT EXISTS _edges_tree (
    id TEXT NOT NULL,
    parent_id TEXT,
    branch_at TEXT,
    relation TEXT DEFAULT 'child',
    depth INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_et_id ON _edges_tree(id);
CREATE INDEX IF NOT EXISTS idx_et_parent ON _edges_tree(parent_id);

-- TYPES LAYER (raw metadata per chunk — no editorializing)
CREATE TABLE IF NOT EXISTS _types_arxiv (
    chunk_id TEXT PRIMARY KEY,
    arxiv_id TEXT,
    section_heading TEXT,
    heading_command TEXT,
    heading_depth INTEGER DEFAULT 0,
    primary_category TEXT,
    categories TEXT,
    authors TEXT,
    published TEXT,
    doi TEXT,
    journal_ref TEXT,
    comment TEXT,
    source_type TEXT DEFAULT 'api'
);

-- ENRICHMENT LAYER
CREATE TABLE IF NOT EXISTS _enrich_source_graph (
    source_id TEXT PRIMARY KEY,
    centrality REAL,
    is_hub INTEGER DEFAULT 0,
    is_bridge INTEGER DEFAULT 0,
    community_id INTEGER
);

-- PRESETS
CREATE TABLE IF NOT EXISTS _presets (
    name TEXT PRIMARY KEY,
    description TEXT,
    params TEXT DEFAULT '',
    sql TEXT
);

-- METADATA + FTS
CREATE TABLE IF NOT EXISTS _meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_au AFTER UPDATE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
    INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
END;
"""


# ═════════════════════════════════════════════════════
# Ingest
# ═════════════════════════════════════════════════════

def ingest_paper(paper: dict, sections, tree_edges, latex_source: str | None,
                 db) -> tuple[int, int]:
    """Ingest a single paper with its sections and tree edges.

    Args:
        paper: normalized paper dict from arxiv_api
        sections: list of LatexSection from latex_parser
        tree_edges: list of (id, parent_id, branch_at, relation, depth) from build_tree_edges
        latex_source: raw LaTeX string (stored non-destructively), or None
        db: SQLite connection

    Returns:
        (sources_added, chunks_added)
    """
    source_id = paper["arxiv_id_base"]
    title = paper["title"]
    authors_str = paper["authors_str"]
    pub_ts = paper["created_utc"]
    file_date = ""
    if pub_ts:
        dt = datetime.fromtimestamp(pub_ts, tz=timezone.utc)
        file_date = dt.strftime("%y%m%d")

    # Check if already ingested
    existing = db.execute(
        "SELECT source_id FROM _raw_sources WHERE source_id = ?", (source_id,)
    ).fetchone()
    if existing:
        return 0, 0

    # INSERT source
    db.execute("""
        INSERT OR IGNORE INTO _raw_sources
        (source_id, title, source, file_date, author,
         score, num_comments, url, embedding)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
    """, (
        source_id, title, "arxiv", file_date, authors_str,
        0,  # score (citation count could go here via enrichment)
        len(sections),  # num_comments = num_sections
        paper["abs_url"],
    ))

    # Store raw LaTeX source non-destructively
    if latex_source:
        content_hash = hashlib.sha256(latex_source.encode()).hexdigest()[:16]
        db.execute(
            "INSERT OR IGNORE INTO _raw_content (content_hash, content) VALUES (?, ?)",
            (content_hash, latex_source))
        db.execute(
            "INSERT OR IGNORE INTO _edges_raw_content (source_id, content_hash, content_type) VALUES (?, ?, 'latex')",
            (source_id, content_hash))

    # INSERT sections as chunks
    chunks_added = 0
    for section in sections:
        chunk_id = f"{source_id}:{section.position}"
        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, section.content, pub_ts))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'arxiv', ?)
        """, (chunk_id, source_id, section.position))

        # Types: raw heading preserved, no canonical mapping here
        db.execute("""
            INSERT OR IGNORE INTO _types_arxiv
            (chunk_id, arxiv_id, section_heading, heading_command,
             heading_depth, primary_category, categories, authors,
             published, doi, journal_ref, comment, source_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chunk_id,
            paper["arxiv_id"],
            section.heading,
            section.heading_command,
            section.depth,
            paper["primary_category"],
            paper["categories_str"],
            authors_str,
            paper["published"],
            paper["doi"],
            paper["journal_ref"],
            paper["comment"],
            "latex" if latex_source else "api",
        ))

        chunks_added += 1

    # INSERT tree edges (heading hierarchy)
    for edge in tree_edges:
        db.execute("""
            INSERT OR IGNORE INTO _edges_tree
            (id, parent_id, branch_at, relation, depth)
            VALUES (?, ?, ?, ?, ?)
        """, edge)

    db.commit()
    return 1, chunks_added


def ingest_paper_abstract_only(paper: dict, db) -> tuple[int, int]:
    """Fallback: ingest a paper using only its API abstract (no LaTeX source).

    Still creates a proper source + chunk structure.
    The abstract becomes chunk :0. Title metadata preserved in _types_arxiv.
    """
    from flex.modules.arxiv.compile.latex_parser import LatexSection, build_tree_edges

    sections = [LatexSection(
        heading="Abstract",
        heading_command="abstract",
        depth=0,
        content=paper["abstract"],
        position=0,
        line_start=0,
    )]

    # No tree edges for a single section
    tree_edges = [(f"{paper['arxiv_id_base']}:0", None, None, "root", 0)]

    return ingest_paper(paper, sections, tree_edges, None, db)


from flex.compile.embed import embed_new  # noqa: F401 — shared pipeline


# ═════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════

DEFAULT_QUERIES = [
    "all:vector retrieval composable",
    "all:embedding modulation semantic search",
    "all:sqlite vector database",
    "all:maximal marginal relevance diversity",
    "all:contrastive retrieval query",
    "all:trajectory embedding space directional",
    "all:community detection retrieval results",
    "all:RAG retrieval augmented generation pipeline",
]


def build_cell(
    cell: str = DEFAULT_CELL_NAME,
    queries: list[str] | None = None,
    ids: list[str] | None = None,
    max_papers: int = DEFAULT_MAX_PAPERS,
    with_source: bool = False,
    graph: bool = False,
    append: bool = False,
    dry_run: bool = False,
    description: str | None = None,
) -> str | None:
    """Build or update an arXiv cell.

    Public defaults intentionally keep API usage bounded. Callers can pass
    larger query lists or max_papers explicitly for operator-managed expansions.
    """
    from flex.modules.arxiv.compile.arxiv_api import pull_papers, pull_by_ids, download_source, DELAY
    from flex.modules.arxiv.compile.latex_parser import split_sections, build_tree_edges

    all_papers = {}

    # ID-based pull (exact papers)
    if ids:
        print(f"\nFetching {len(ids)} papers by ID...")
        papers = pull_by_ids(ids)
        for p in papers:
            all_papers[p["arxiv_id_base"]] = p

    # Query-based pull
    query_list = queries or PUBLIC_SEED_QUERIES
    if not ids:
        for q in query_list:
            q = q.strip()
            print(f"\n{'=' * 50}")
            print(f"Query: {q}")
            print(f"{'=' * 50}")
            papers = pull_papers(q, max_total=max_papers)
            for p in papers:
                all_papers[p["arxiv_id_base"]] = p

    print(f"\nTotal unique papers: {len(all_papers)}")

    if dry_run:
        for pid, p in list(all_papers.items())[:10]:
            print(f"  {pid}: {p['title'][:80]}")
        print(f"  ... ({len(all_papers)} total)")
        return None

    # Resolve / create cell
    cell_path = cell
    if not cell_path.endswith(".db"):
        from flex.registry import CELLS_DIR
        CELLS_DIR.mkdir(parents=True, exist_ok=True)
        cell_path = str(CELLS_DIR / f"{cell}.db")

    if not append and os.path.exists(cell_path):
        os.remove(cell_path)

    db = open_cell(cell_path)
    if not append:
        db.executescript(SCHEMA_DDL)

    t0 = time.time()
    total_sources = 0
    total_chunks = 0

    for i, (pid, paper) in enumerate(all_papers.items()):
        print(f"  [{i+1}/{len(all_papers)}] {pid}: {paper['title'][:60]}...", end="")

        if with_source:
            latex = download_source(pid, quiet=True)
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

    print(f"\nIngested: {total_sources} papers, {total_chunks} chunks")

    validate_cell(db)

    # Embed
    print("Embedding...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded} chunks")

    # Log
    log_op(db, "arxiv_ingest", "_raw_chunks",
           params={"sources": total_sources, "chunks": total_chunks,
                   "embedded": embedded,
                   "queries": query_list if not ids else None,
                   "ids": ",".join(ids) if ids else None,
                   "max_papers": max_papers},
           rows_affected=total_chunks,
           source="arxiv/compile/worker.py")
    db.commit()

    # Graph
    if graph:
        import subprocess
        print("Building similarity graph...")
        subprocess.run([sys.executable, "-m", "flex.manage.meditate",
                        "--cell", cell_path], check=True)

    # Views
    views_dir = Path(__file__).parent.parent / "stock" / "views"
    if views_dir.exists():
        from flex.views import install_views
        install_views(db, views_dir)
    from flex.views import regenerate_views
    regenerate_views(db)

    # Presets
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / "retrieve" / "presets" / "general"
    if preset_dir.exists():
        install_presets(db, preset_dir)
    platform_preset_dir = Path(__file__).parent.parent / "stock" / "presets"
    if platform_preset_dir.exists():
        install_presets(db, platform_preset_dir)

    # Metadata
    cell_desc = description or DEFAULT_DESCRIPTION
    set_meta(db, "cell_type", "arxiv")
    set_meta(db, "description", cell_desc)
    set_meta(db, "created_at", datetime.now(timezone.utc).isoformat())
    set_meta(db, "retrieval:primary_view", "papers")
    set_meta(db, "retrieval:source_view", "sources")
    set_meta(db, "rate_limit:arxiv", "3 seconds between API requests")
    set_meta(db, "rate_limit:semantic_scholar", "optional SEMANTIC_SCHOLAR_API_KEY; module waits between requests")
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, "last_pull_ts", str(max_ts))
    set_meta(db, "last_pull_at", datetime.now(timezone.utc).isoformat())
    if not ids:
        # Merge into any stored query list so --append runs widen the scope
        # that scheduled refresh reads, instead of silently ignoring it.
        from flex.core import get_meta
        stored = get_meta(db, "queries")
        merged = list(dict.fromkeys(
            (json.loads(stored) if stored else []) + list(query_list)
        ))
        set_meta(db, "queries", json.dumps(merged))

    # Register
    from flex.registry import register_cell
    cell_name = cell if not cell.endswith(".db") else Path(cell).stem
    register_cell(
        name=cell_name, path=cell_path, cell_type="arxiv",
        description=cell_desc,
        lifecycle="refresh",
        refresh_interval=DEFAULT_REFRESH_INTERVAL,
        refresh_module=REFRESH_MODULE,
        active=True,
        unlisted=False,
    )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — {cell_path}")
    db.close()
    return cell_path


def main():
    parser = argparse.ArgumentParser(
        description="Index arXiv papers into a Flex cell")
    parser.add_argument("--cell", default="arxiv",
                        help="Cell name or path (default: arxiv)")
    parser.add_argument("--queries", default=None,
                        help="Comma-separated arXiv search queries")
    parser.add_argument("--ids", default=None,
                        help="Comma-separated arXiv paper IDs (e.g. 2007.04612,1703.05175)")
    parser.add_argument("--max-papers", type=int, default=DEFAULT_MAX_PAPERS,
                        help=f"Max papers per query (default: {DEFAULT_MAX_PAPERS})")
    parser.add_argument("--with-source", action="store_true",
                        help="Download LaTeX source for full-section parsing")
    parser.add_argument("--graph", action="store_true",
                        help="Build similarity graph after ingest")
    parser.add_argument("--append", action="store_true",
                        help="Append to existing cell")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show stats without indexing")
    parser.add_argument("--description", default=None,
                        help="Cell description")
    args = parser.parse_args()

    ids = [i.strip() for i in args.ids.split(",") if i.strip()] if args.ids else None
    queries = [q.strip() for q in args.queries.split(",") if q.strip()] if args.queries else DEFAULT_QUERIES
    build_cell(
        cell=args.cell,
        queries=queries,
        ids=ids,
        max_papers=args.max_papers,
        with_source=args.with_source,
        graph=args.graph,
        append=args.append,
        dry_run=args.dry_run,
        description=args.description,
    )


if __name__ == "__main__":
    main()
