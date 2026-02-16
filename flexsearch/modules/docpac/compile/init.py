#!/usr/bin/env python3
"""
Reusable doc-pac cell initializer — chunk-atom fresh ingest.

Composes flexsearch COMPILE primitives into a pipeline:
  parse_docpac → extract_frontmatter → normalize_headers → split_sections
  → embed → mean-pool → validate → meditate → enrich_types → regenerate_views

Usage:
  python flexsearch/modules/docpac/compile/init.py \
    --corpus /path/to/docpac/root \
    --cell ~/.qmem/cells/projects/qmem \
    --threshold 0.55 \
    --description "QMEM documentation..."

Proven on qmem-test (102 sources, 718 chunks, 17.4s).
"""
import argparse
import hashlib
import os
import shutil
import subprocess
import time
from datetime import datetime

import numpy as np
from pathlib import Path

# scripts/ -> docpac/ -> modules/ -> flexsearch/ -> main/
FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent

from flexsearch.modules.docpac.compile.docpac import parse_docpac
from flexsearch.compile.markdown import normalize_headers, extract_frontmatter, split_sections
from flexsearch.core import open_cell, set_meta, run_sql, validate_cell
from flexsearch.views import regenerate_views


# ═════════════════════════════════════════════════
# SCHEMA DDL
# ═════════════════════════════════════════════════

SCHEMA_DDL = """
-- RAW LAYER (immutable, COMPILE writes here)
CREATE TABLE IF NOT EXISTS _raw_chunks (
    id TEXT PRIMARY KEY,
    content TEXT,
    embedding BLOB,
    timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS _raw_sources (
    source_id TEXT PRIMARY KEY,
    file_date TEXT,
    temporal TEXT,
    doc_type TEXT,
    title TEXT,
    summary TEXT,
    source_path TEXT,
    type TEXT,
    status TEXT,
    keywords TEXT,
    embedding BLOB
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'markdown',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

CREATE TABLE IF NOT EXISTS _edges_url_identity (
    chunk_id TEXT NOT NULL,
    url_uuid TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eui_chunk ON _edges_url_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eui_uuid ON _edges_url_identity(url_uuid);

-- TYPES LAYER (immutable COMPILE classification — pipeline signature)
CREATE TABLE IF NOT EXISTS _types_docpac (
    chunk_id TEXT PRIMARY KEY,
    temporal TEXT,
    doc_type TEXT,
    facet TEXT,
    section_title TEXT,
    yaml_type TEXT,
    yaml_status TEXT
);

-- ENRICHMENT LAYER (mutable, meditate writes here)
CREATE TABLE IF NOT EXISTS _enrich_source_graph (
    source_id TEXT PRIMARY KEY,
    centrality REAL,
    is_hub INTEGER DEFAULT 0,
    is_bridge INTEGER DEFAULT 0,
    community_id INTEGER
);

CREATE TABLE IF NOT EXISTS _enrich_types (
    chunk_id TEXT PRIMARY KEY,
    semantic_role TEXT,
    confidence REAL DEFAULT 1.0
);

-- PRESETS (baked from .sql files at init time)
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

-- FTS auto-sync triggers
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


def _git_creation_date(filepath: str) -> str | None:
    """Get file creation date from git log. Returns YYMMDD-HHMM or None."""
    try:
        result = subprocess.run(
            ['git', 'log', '--follow', '--format=%at', '--diff-filter=A', '--', filepath],
            capture_output=True, text=True, timeout=5,
            cwd=os.path.dirname(filepath)
        )
        if result.returncode == 0 and result.stdout.strip():
            # Take the earliest (last line = first commit)
            timestamps = result.stdout.strip().split('\n')
            ts = int(timestamps[-1])
            dt = datetime.fromtimestamp(ts)
            return dt.strftime('%y%m%d-%H%M')
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass
    return None


def _mtime_date(filepath: str) -> str | None:
    """Get file modification date from filesystem. Returns YYMMDD-HHMM or None."""
    try:
        ts = os.path.getmtime(filepath)
        dt = datetime.fromtimestamp(ts)
        return dt.strftime('%y%m%d-%H%M')
    except OSError:
        return None


def backfill_file_dates(db, corpus_root: str):
    """Backfill NULL file_date values from git creation date, then filesystem mtime."""
    nulls = db.execute(
        "SELECT source_id, source_path FROM _raw_sources WHERE file_date IS NULL"
    ).fetchall()

    if not nulls:
        return 0

    # Check if corpus is in a git repo
    is_git = os.path.isdir(os.path.join(corpus_root, '.git'))

    filled = 0
    for row in nulls:
        path = row[1]  # source_path (absolute)
        date = None

        if is_git:
            date = _git_creation_date(path)

        if not date:
            date = _mtime_date(path)

        if date:
            db.execute(
                "UPDATE _raw_sources SET file_date = ? WHERE source_id = ?",
                (date, row[0])
            )
            filled += 1

    db.commit()
    return filled


def make_source_id(path: str) -> str:
    """Deterministic source ID from file path."""
    return hashlib.sha256(path.encode()).hexdigest()[:16]


def make_chunk_id(source_id: str, position: int) -> str:
    """Deterministic chunk ID: source:position."""
    return f"{source_id}:{position}"


from flexsearch.registry import CELLS_ROOT, register_cell as _registry_register


def derive_cell_name(corpus_path: str) -> str:
    """Derive cell name from corpus path: /home/axp/projects/foo/context → foo-context."""
    p = Path(corpus_path).resolve()
    # Use parent + name to distinguish context/ folders across projects
    # e.g. flexsearch/context → flexsearch-context
    if p.name in ('context', 'docs', 'documentation'):
        return f"{p.parent.name}-{p.name}"
    return p.name


def main():
    parser = argparse.ArgumentParser(description='Initialize a doc-pac chunk-atom cell')
    parser.add_argument('corpus', nargs='?', help='Root directory of the doc-pac corpus')
    parser.add_argument('--corpus', dest='corpus_flag', help=argparse.SUPPRESS)
    parser.add_argument('--cell', default=None,
                        help='Cell directory (auto-derived from corpus if omitted)')
    parser.add_argument('--threshold', type=float, default=0.55,
                        help='Similarity threshold for graph building (default: 0.55)')
    parser.add_argument('--description', default=None,
                        help='Cell description for _meta (auto-generated if omitted)')
    args = parser.parse_args()

    # Support both positional and --corpus flag
    corpus_input = args.corpus or args.corpus_flag
    if not corpus_input:
        parser.error('corpus path is required')

    corpus_root = os.path.abspath(corpus_input)
    if args.cell:
        cell_dir = os.path.expanduser(args.cell)
    else:
        cell_name = derive_cell_name(corpus_root)
        cell_dir = str(CELLS_ROOT / cell_name)
        print(f"Auto-derived cell name: {cell_name}")
    db_path = os.path.join(cell_dir, 'main.db')

    t0 = time.time()

    # ═════════════════════════════════════════════════
    # 1. CREATE CELL DIRECTORY
    # ═════════════════════════════════════════════════
    if os.path.exists(cell_dir):
        print(f"Removing existing cell at {cell_dir}...")
        shutil.rmtree(cell_dir)
    os.makedirs(cell_dir, exist_ok=True)
    print(f"Cell directory: {cell_dir}")

    # ═════════════════════════════════════════════════
    # 2. CREATE SCHEMA
    # ═════════════════════════════════════════════════
    db = open_cell(db_path)
    db.executescript(SCHEMA_DDL)
    db.commit()
    print("Schema created.")

    # ═════════════════════════════════════════════════
    # 3. PARSE CORPUS via docpac
    # ═════════════════════════════════════════════════
    entries = parse_docpac(corpus_root)
    indexable = [e for e in entries if not e.skip]
    print(f"Docpac: {len(entries)} total, {len(indexable)} indexable, "
          f"{len(entries) - len(indexable)} skipped")

    # ═════════════════════════════════════════════════
    # 4. INDEX PIPELINE
    # ═════════════════════════════════════════════════
    source_count = 0
    chunk_count = 0

    for entry in indexable:
        filepath = Path(entry.path)
        try:
            content = filepath.read_text(encoding='utf-8')
        except (UnicodeDecodeError, FileNotFoundError) as e:
            print(f"  SKIP {filepath.name}: {e}")
            continue

        source_id = make_source_id(entry.path)

        # Extract frontmatter
        frontmatter, body = extract_frontmatter(content)

        # Normalize headers (ephemeral, source file untouched)
        normalized = normalize_headers(body)

        # Split into sections
        sections = split_sections(normalized, level=2)

        if not sections:
            sections = [('', body.strip(), 0)]

        # INSERT _raw_sources
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, file_date, temporal, doc_type, title, source_path,
             type, status, keywords)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_id,
            entry.file_date,
            entry.temporal,
            entry.doc_type,
            entry.title,
            entry.path,
            frontmatter.get('type'),
            frontmatter.get('status'),
            ','.join(frontmatter.get('keywords', [])) if isinstance(frontmatter.get('keywords'), list)
                else frontmatter.get('keywords'),
        ))
        source_count += 1

        # INSERT _raw_chunks + _edges_source + _types_docpac
        for section_title, section_content, position in sections:
            chunk_id = make_chunk_id(source_id, position)

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, timestamp)
                VALUES (?, ?, ?)
            """, (chunk_id, section_content, None))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'markdown', ?)
            """, (chunk_id, source_id, position))

            db.execute("""
                INSERT OR IGNORE INTO _types_docpac
                (chunk_id, temporal, doc_type, facet, section_title,
                 yaml_type, yaml_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                chunk_id,
                entry.temporal,
                entry.doc_type,
                None,  # facet assigned by human, not docpac
                section_title or None,
                frontmatter.get('type'),
                frontmatter.get('status'),
            ))

            chunk_count += 1

    db.commit()
    print(f"Indexed: {source_count} sources, {chunk_count} chunks")

    # ═════════════════════════════════════════════════
    # 5. VALIDATE
    # ═════════════════════════════════════════════════
    validate_cell(db)
    print("Validation passed (no orphans, no duplicate source edges).")

    # ═════════════════════════════════════════════════
    # 5b. BACKFILL FILE DATES
    # ═════════════════════════════════════════════════
    null_dates = db.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE file_date IS NULL"
    ).fetchone()[0]
    if null_dates:
        print(f"Backfilling {null_dates} missing file_date values...")
        filled = backfill_file_dates(db, corpus_root)
        print(f"  Filled {filled}/{null_dates} from git/mtime.")

    # ═════════════════════════════════════════════════
    # 6. POPULATE _meta
    # ═════════════════════════════════════════════════
    desc = args.description or (
        f"Doc-pac cell from {os.path.basename(corpus_root)}. "
        f"~{source_count} docs, ~{chunk_count} chunks."
    )
    set_meta(db, 'description', desc)
    set_meta(db, 'version', '2.0.0')
    set_meta(db, 'schema', 'chunk-atom')

    # View config
    set_meta(db, 'view:sections:level', 'chunk')
    set_meta(db, 'view:documents:level', 'source')

    # Domain renames (graph terms stay exact)
    set_meta(db, 'view:sections:rename:source_id', 'doc_id')
    set_meta(db, 'view:sections:rename:title', 'doc_title')
    set_meta(db, 'view:sections:rename:semantic_role', 'kind')

    # Retrieval contract — the cell describes its own search model
    set_meta(db, 'retrieval:phase1',
             'PRE-SELECTION masks (numpy on full N): '
             'community:N→community_id, kind:TYPE→kind, limit:N')
    set_meta(db, 'retrieval:phase2',
             'LANDSCAPE scoring (numpy on full N): '
             'recent[:N]→timestamp, diverse, unlike:TEXT')
    set_meta(db, 'retrieval:phase3',
             'ENRICH (query-time topology on K candidates): '
             'detect_communities→_community column (per-query Louvain)')
    set_meta(db, 'retrieval:phase4',
             'SQL COMPOSITION (on K candidates): '
             'JOIN sections s ON v.id = s.id — kind, community_id, centrality, '
             'is_hub, temporal, doc_type, doc_title, section_title')

    print("Meta populated.")

    # ═════════════════════════════════════════════════
    # 7. EMBED CHUNKS
    # ═════════════════════════════════════════════════
    print("Embedding chunks...")
    from flexsearch.onnx.embed import ONNXEmbedder

    embedder = ONNXEmbedder()

    chunks = run_sql(db, "SELECT id, content FROM _raw_chunks")
    chunk_ids = [c['id'] for c in chunks]
    chunk_texts = [c['content'] for c in chunks]

    embeddings = embedder.encode(chunk_texts, batch_size=32)
    print(f"Embedded {len(embeddings)} chunks ({embeddings.shape[1]}d)")

    for i, chunk_id in enumerate(chunk_ids):
        blob = embeddings[i].astype(np.float32).tobytes()
        db.execute("UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
                   (blob, chunk_id))
    db.commit()
    print("Chunk embeddings stored.")

    # ═════════════════════════════════════════════════
    # 8. MEAN-POOL SOURCE EMBEDDINGS
    # ═════════════════════════════════════════════════
    print("Mean-pooling source embeddings...")
    sources = run_sql(db, "SELECT DISTINCT source_id FROM _edges_source")

    for src in sources:
        sid = src['source_id']
        chunk_rows = db.execute("""
            SELECT c.embedding FROM _raw_chunks c
            JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.source_id = ? AND c.embedding IS NOT NULL
        """, (sid,)).fetchall()

        if not chunk_rows:
            continue

        vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunk_rows]
        mean_vec = np.mean(vecs, axis=0).astype(np.float32)

        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm

        db.execute("UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                   (mean_vec.tobytes(), sid))

    db.commit()
    print(f"Source embeddings: {len(sources)} sources mean-pooled.")

    # ═════════════════════════════════════════════════
    # 9. GRAPH / MEDITATE
    # ═════════════════════════════════════════════════
    print("Building similarity graph...")
    from flexsearch.manage.meditate import build_similarity_graph, compute_scores, persist

    G, edge_count = build_similarity_graph(
        db, table='_raw_sources', id_col='source_id',
        threshold=args.threshold
    )

    if G is not None:
        scores = compute_scores(G)
        persist(db, scores)
        hubs = len(scores.get('hubs', []))
        comms = len(scores.get('communities', []))
        print(f"Meditate: {hubs} hubs, {comms} communities, {edge_count} edges")
    else:
        print("No graph built (no embeddings?)")
        regenerate_views(db)

    # ═════════════════════════════════════════════════
    # 10. ENRICH TYPES (heuristic pre-population)
    # ═════════════════════════════════════════════════
    print("Pre-populating _enrich_types...")
    db.execute("""
        INSERT OR IGNORE INTO _enrich_types (chunk_id, semantic_role, confidence)
        SELECT c.id,
            CASE WHEN td.doc_type = 'changelog' THEN 'changelog'
                 WHEN td.doc_type = 'architecture' THEN 'architecture'
                 WHEN td.doc_type = 'design' THEN 'design'
                 WHEN td.doc_type = 'plan' THEN 'plan'
                 WHEN td.doc_type = 'testing' THEN 'testing'
                 WHEN td.doc_type = 'ast' THEN 'code_artifact'
                 ELSE 'prose'
            END,
            0.5
        FROM _raw_chunks c
        LEFT JOIN _types_docpac td ON c.id = td.chunk_id
    """)
    db.commit()

    enrich_count = db.execute("SELECT COUNT(*) FROM _enrich_types").fetchone()[0]
    print(f"Enriched {enrich_count} chunks with doc_type-based heuristics.")

    # Regenerate views to pick up _enrich_types
    regenerate_views(db)

    # ═════════════════════════════════════════════════
    # 11. INSTALL PRESETS
    # ═════════════════════════════════════════════════
    print("Installing presets...")
    from flexsearch.retrieve.presets import install_presets

    general_presets = FLEX_ROOT / 'flexsearch' / 'retrieve' / 'presets' / 'general'
    install_presets(db, general_presets)
    preset_count = db.execute("SELECT COUNT(*) FROM _presets").fetchone()[0]
    print(f"Installed {preset_count} general presets.")

    # ═════════════════════════════════════════════════
    # 12. REGISTER IN CELL REGISTRY
    # ═════════════════════════════════════════════════
    cell_name_final = os.path.basename(cell_dir)
    _registry_register(cell_name_final, db_path, cell_type='docpac',
                       description=desc, corpus_path=corpus_root)
    print(f"Registered in cell registry: {cell_name_final} (corpus={corpus_root})")

    # ═════════════════════════════════════════════════
    # 13. DONE
    # ═════════════════════════════════════════════════
    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"Cell created in {elapsed:.1f}s")
    print(f"  Sources: {source_count}")
    print(f"  Chunks:  {chunk_count}")
    print(f"  Path:    {db_path}")
    print(f"{'='*50}")

    db.close()


if __name__ == '__main__':
    main()
