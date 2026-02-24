#!/usr/bin/env python3
"""
Reusable doc-pac cell initializer — chunk-atom fresh ingest.

Composes flex COMPILE primitives into a pipeline:
  parse_docpac → extract_frontmatter → normalize_headers → split_sections
  → embed → mean-pool → validate → meditate → enrich_types → regenerate_views

Usage:
  python flex/modules/docpac/compile/init.py \
    --corpus /path/to/docpac/root \
    --cell ~/.qmem/cells/projects/qmem \
    --threshold 0.55 \
    --description "QMEM documentation..."

Proven on qmem-test (102 sources, 718 chunks, 17.4s).
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
import uuid as _uuid
from datetime import datetime

import numpy as np
from pathlib import Path

# scripts/ -> docpac/ -> modules/ -> flex/ -> main/
FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent

from flex.modules.docpac.compile.docpac import parse_docpac
from flex.compile.markdown import normalize_headers, extract_frontmatter, split_sections
from flex.core import open_cell, set_meta, run_sql, validate_cell
from flex.views import regenerate_views


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
    file_uuid TEXT,
    type TEXT,
    status TEXT,
    keywords TEXT,
    confidence REAL DEFAULT 1.0,
    validity REAL DEFAULT 1.0,
    maturity REAL DEFAULT 1.0,
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


import uuid as _uuid
from flex.registry import CELLS_DIR, resolve_cell as _registry_resolve, register_cell as _registry_register


def derive_cell_name(corpus_path: str) -> str:
    """Derive cell name from corpus path: ~/projects/foo/context → foo-context."""
    p = Path(corpus_path).resolve()
    # Use parent + name to distinguish context/ folders across projects
    # e.g. flex/context → flex-context
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
    cell_name = derive_cell_name(corpus_root)
    print(f"Auto-derived cell name: {cell_name}")

    if args.cell:
        db_path = os.path.expanduser(args.cell)
    else:
        # Check if cell already exists in registry — reuse its path
        existing = _registry_resolve(cell_name)
        if existing:
            db_path = str(existing)
        else:
            # New cell: assign UUID, land in ~/.flex/cells/
            cell_uuid = str(_uuid.uuid4())
            CELLS_DIR.mkdir(parents=True, exist_ok=True)
            db_path = str(CELLS_DIR / f"{cell_uuid}.db")

    t0 = time.time()

    # ═════════════════════════════════════════════════
    # 1. REMOVE OLD CELL IF EXISTS
    # ═════════════════════════════════════════════════
    if os.path.exists(db_path):
        print(f"Removing existing cell at {db_path}...")
        os.remove(db_path)

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

    # Load external classifications from _flex_types.json if present.
    # Format: {"file-uuid": {"doc_type": "change.code", "validity": 1.0, ...}, ...}
    # Generated by the /flex-index-docpac skill — non-destructive sidecar, user files untouched.
    # temporal is derived deterministically from doc_type prefix.
    TEMPORAL_MAP = {
        'change':   'past',
        'current':  'present',
        'design':   'future',
        'external': 'exogenous',
    }

    def _derive_temporal(doc_type, fallback):
        """Derive temporal from compound doc_type prefix (change.code → past)."""
        if doc_type:
            prefix = doc_type.split('.')[0]
            return TEMPORAL_MAP.get(prefix, fallback)
        return fallback

    def _get_file_uuid(path):
        """Get stable file UUID via SOMA FileIdentity, falling back to deterministic uuid5."""
        try:
            from flex.modules.soma.lib.identity.file_identity import FileIdentity
            return FileIdentity().assign(str(path))
        except Exception:
            return str(_uuid.uuid5(_uuid.NAMESPACE_URL, str(path)))

    _types_file = Path(corpus_root) / '_flex_types.json'
    _ext = {}
    _ext_by_path = {}  # path-keyed fallback for legacy format
    if _types_file.exists():
        raw = json.loads(_types_file.read_text())
        for k, v in raw.items():
            # uuid-keyed (new) vs path-keyed (legacy)
            if len(k) == 36 and k.count('-') == 4:
                _ext[k] = v
            else:
                _ext_by_path[k] = v

    _overridden = 0
    for entry in entries:
        if entry.skip:
            continue
        # Try uuid-keyed first, fall back to relative path
        file_uuid = _get_file_uuid(entry.path)
        t = _ext.get(file_uuid)
        if t is None:
            try:
                rel = str(Path(entry.path).relative_to(corpus_root))
            except ValueError:
                rel = entry.path
            t = _ext_by_path.get(rel)
        if t:
            doc_type = t.get('doc_type')
            if doc_type:
                entry.doc_type = doc_type
                entry.temporal = _derive_temporal(doc_type, entry.temporal)
            entry._meta = {
                'file_uuid':  file_uuid,
                'confidence': t.get('confidence', 1.0),
                'validity':   t.get('validity',   1.0),
                'maturity':   t.get('maturity',   1.0),
                'summary':    t.get('summary'),
                'keywords':   t.get('keywords'),
                'type':       t.get('type'),
            }
            _overridden += 1
        else:
            entry._meta = {'file_uuid': file_uuid}

    if _types_file.exists():
        print(f"External types: {_overridden}/{len(entries)} files matched from _flex_types.json")

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

        # Frontmatter doc_type is highest priority — author-written wins over
        # _flex_types.json (Haiku) and FOLDER_MAP (path inference).
        fm_doc_type = frontmatter.get('doc_type')
        if fm_doc_type:
            entry.doc_type = fm_doc_type
            entry.temporal = _derive_temporal(fm_doc_type, entry.temporal)

        # Merge metadata: frontmatter > _flex_types.json > defaults
        _m = getattr(entry, '_meta', {})
        _file_uuid   = _m.get('file_uuid')
        _confidence  = frontmatter.get('confidence',  _m.get('confidence',  1.0))
        _validity    = frontmatter.get('validity',    _m.get('validity',    1.0))
        _maturity    = frontmatter.get('maturity',    _m.get('maturity',    1.0))
        _summary     = frontmatter.get('summary',     _m.get('summary'))
        _keywords    = frontmatter.get('keywords',    _m.get('keywords'))
        _type        = frontmatter.get('type',        _m.get('type'))

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
             file_uuid, type, status, keywords, summary,
             confidence, validity, maturity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_id,
            entry.file_date,
            entry.temporal,
            entry.doc_type,
            entry.title,
            entry.path,
            _file_uuid,
            _type,
            frontmatter.get('status'),
            ','.join(_keywords) if isinstance(_keywords, list) else _keywords,
            _summary,
            _confidence,
            _validity,
            _maturity,
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

    # Retrieval contract — the cell describes its own search model
    set_meta(db, 'retrieval:phase1',
             'SQL PRE-FILTER (4th arg to vec_ops): '
             'Any SQL returning chunk_ids. Restricts which chunks enter the landscape.')
    set_meta(db, 'retrieval:phase2',
             'LANDSCAPE (numpy on filtered N): '
             'diverse, recent[:N], unlike:TEXT, like:id1,id2, from:TEXT to:TEXT')
    set_meta(db, 'retrieval:phase3',
             'ENRICH (query-time topology on K candidates): '
             'local_communities→_community column (per-query Louvain)')
    set_meta(db, 'retrieval:phase4',
             'SQL COMPOSITION (on K candidates): '
             'JOIN sections s ON v.id = s.id — community_id, centrality, '
             'is_hub, temporal, doc_type, doc_title, section_title')

    print("Meta populated.")

    # ═════════════════════════════════════════════════
    # 7. EMBED CHUNKS
    # ═════════════════════════════════════════════════
    print("Embedding chunks...")
    from flex.onnx.embed import ONNXEmbedder

    embedder = ONNXEmbedder()

    chunks = run_sql(db, "SELECT id, content FROM _raw_chunks")
    chunk_ids = [c['id'] for c in chunks]
    chunk_texts = [c['content'] for c in chunks]

    embeddings = embedder.encode(chunk_texts)
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
    from flex.manage.meditate import build_similarity_graph, compute_scores, persist

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
        regenerate_views(db, views={'sections': 'chunk', 'documents': 'source'})

    # ═════════════════════════════════════════════════
    # 10. _enrich_types: stopped writing heuristic values (Plan 9).
    # AI queries doc_type + temporal directly via curated views.
    # Table kept as reserved slot for future semantic classification.
    # ═════════════════════════════════════════════════

    regenerate_views(db, views={'sections': 'chunk', 'documents': 'source'})

    # ═════════════════════════════════════════════════
    # 11. INSTALL PRESETS
    # ═════════════════════════════════════════════════
    print("Installing presets...")
    from flex.retrieve.presets import install_presets

    general_presets = FLEX_ROOT / 'flex' / 'retrieve' / 'presets' / 'general'
    install_presets(db, general_presets)
    preset_count = db.execute("SELECT COUNT(*) FROM _presets").fetchone()[0]
    print(f"Installed {preset_count} general presets.")

    # ═════════════════════════════════════════════════
    # 12. REGISTER IN CELL REGISTRY
    # ═════════════════════════════════════════════════
    _registry_register(cell_name, db_path, cell_type='docpac',
                       description=desc, corpus_path=corpus_root)
    print(f"Registered in cell registry: {cell_name} (corpus={corpus_root})")

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
