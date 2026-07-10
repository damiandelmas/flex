#!/usr/bin/env python3
"""
Reusable doc-pac cell initializer — chunk-atom fresh ingest.

Composes flex COMPILE primitives into a pipeline:
  parse_docpac → extract_frontmatter → normalize_headers → split_sections
  → embed → mean-pool → validate → meditate → enrich_types → regenerate_views

Usage:
  python flex/modules/docpac/compile/init.py \
    --corpus /path/to/docpac/root \
    --cell ~/.flex/cells/projects/notes \
    --threshold 0.55 \
    --description "Project documentation..."
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import re
import uuid as _uuid
from datetime import datetime

import numpy as np
from pathlib import Path

# scripts/ -> docpac/ -> modules/ -> flex/ -> main/
FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent

from flex.modules.docpac.compile.docpac import parse_docpac
from flex.modules.docpac.compile.classify import (
    derive_temporal, load_sidecar, apply_sidecar_overrides,
)
from flex.modules.markdown.compile.profile import docpac_profile
from flex.modules.docpac.compile.context_config import load_context_config
from flex.compile.markdown import normalize_headers, extract_frontmatter, split_sections
from flex.sdk import link
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
    embedding BLOB,
    content_hash TEXT
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

-- CONTAINMENT TREE (heading hierarchy: section ⊃ subsection)
CREATE TABLE IF NOT EXISTS _edges_tree (
    id TEXT NOT NULL,
    parent_id TEXT,
    branch_at TEXT,
    relation TEXT NOT NULL,
    depth INTEGER DEFAULT 0,
    PRIMARY KEY (id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_tree_parent ON _edges_tree(parent_id);

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


def backfill_file_dates(db, corpus_root: str = None):
    """Fill NULL/empty file_date from git creation date then mtime. Fill-only —
    never overwrites an existing date. `corpus_root` is vestigial (kept for
    call-compat): dates resolve per-row from source_path on disk via
    resolve_file_date, whose git step self-discovers an ancestor repo (e.g.
    ~/notes) — the old `corpus_root/.git` gate wrongly skipped git for corpora
    nested under an ancestor repo, which is why these cells stayed NULL."""
    from flex.modules.docpac.compile.docpac import resolve_file_date
    nulls = db.execute(
        "SELECT source_id, source_path FROM _raw_sources "
        "WHERE file_date IS NULL OR file_date = ''"
    ).fetchall()
    if not nulls:
        return 0
    filled = 0
    for source_id, path in nulls:
        date = resolve_file_date(None, path)
        if date:
            db.execute(
                "UPDATE _raw_sources SET file_date = ? WHERE source_id = ?",
                (date, source_id))
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


def _deterministic_file_uuid(path: str) -> str:
    """Stable, local file UUID that does not need the SOMA identity DB."""
    return str(_uuid.uuid5(_uuid.NAMESPACE_URL, str(Path(path).resolve())))


def _make_file_uuid_resolver(mode: str, needs_soma_identity: bool = False):
    """Return (resolver, active_mode) for docpac file UUID assignment.

    Plain docpac bootstraps should be fast and self-contained. SOMA file identity
    is only required when uuid-keyed _flex_types.json metadata must be joined.
    """
    requested = (mode or 'auto').strip().lower()
    aliases = {
        'path': 'deterministic',
        'uuid5': 'deterministic',
        'none': 'deterministic',
        'skip-soma': 'deterministic',
    }
    requested = aliases.get(requested, requested)
    if requested not in {'auto', 'deterministic', 'soma'}:
        raise ValueError(f"invalid file identity mode: {mode}")

    if requested == 'deterministic' or (requested == 'auto' and not needs_soma_identity):
        return _deterministic_file_uuid, 'deterministic'

    try:
        from flex.modules.soma.lib.identity.file_identity import FileIdentity
        identity = FileIdentity()
    except Exception:
        if requested == 'soma':
            raise
        return _deterministic_file_uuid, 'deterministic-fallback'

    def _resolve(path: str) -> str:
        try:
            return identity.assign(str(path))
        except Exception:
            if requested == 'soma':
                raise
            return _deterministic_file_uuid(path)

    return _resolve, 'soma'


# Canonical identity edge — IDENTICAL DDL to index_file/instant/soma-backfill.
_FS_IDENTITY_DDL = """
CREATE TABLE IF NOT EXISTS _edges_fs_identity (
    source_id TEXT PRIMARY KEY,
    file_uuid TEXT
);
"""
# SOMA identity-applicability exclusions (soma contract: reuse verbatim, don't invent
# a second list). Single-sourced from soma's IDENTITY_APPLICABILITY so BOTH mint paths
# (batch init here + index_file in worker.py) exclude ephemeral corpora symmetrically.
def _is_identity_excluded(path: str) -> bool:
    """True if `path` is under a SOMA exclude_paths prefix (ephemeral: /tmp, /var/tmp,
    /dev) — so fixture/test cells never pollute the shared ~/.soma authority."""
    try:
        from flex.modules.soma.compile import IDENTITY_APPLICABILITY
        pats = IDENTITY_APPLICABILITY['file_uuid']['exclude_paths']
    except Exception:
        pats = [r'^/tmp/', r'^/var/tmp/', r'^/dev/']  # fallback = the same list
    return any(re.match(p, path) for p in pats)


def _mint_batch_identity(db, entries) -> int:
    """SOMA-mint _edges_fs_identity for the corpus in one batch (G0-identity; batch
    parity with index_file's per-file _mint_file_identity, instant's assign_batch
    pattern). ADDITIVE — SOMA is the sole minting authority for the canonical edge;
    the _raw_sources.file_uuid hint (deterministic) is untouched, matching index_file.
    Never raises — identity must never fail a compile (instant's discipline). Skips
    ephemeral corpora (soma exclude_paths) so fixture/test cells don't pollute ~/.soma.
    Only mints for sources actually inserted this build (no dangling edges). Returns
    the count stamped."""
    try:
        inserted = {r[0] for r in db.execute("SELECT source_id FROM _raw_sources")}
        pairs = []  # (resolved_path, source_id) for real, inserted, non-ephemeral files
        for e in entries:
            sid = make_source_id(e.path)
            if sid not in inserted:
                continue
            rp = str(Path(e.path).resolve())
            if _is_identity_excluded(rp):
                continue
            pairs.append((rp, sid))
        if not pairs:
            return 0
        from flex.modules.soma.lib.identity.file_identity import get_instance
        uuids = get_instance().assign_batch([rp for rp, _ in pairs])
        db.execute(_FS_IDENTITY_DDL)
        rows = [(sid, uuids.get(rp)) for rp, sid in pairs if uuids.get(rp)]
        if rows:
            db.executemany(
                "INSERT OR REPLACE INTO _edges_fs_identity (source_id, file_uuid) VALUES (?, ?)",
                rows)
        return len(rows)
    except Exception:
        return 0  # best-effort; the compile must still succeed


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
    parser.add_argument('--file-identity',
                        default=os.environ.get('FLEX_DOCPAC_FILE_IDENTITY', 'auto'),
                        choices=('auto', 'soma', 'deterministic'),
                        help='File UUID strategy: auto uses SOMA only for uuid-keyed _flex_types.json')
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
    context_cfg = load_context_config(corpus_root)
    entries = parse_docpac(corpus_root, config=context_cfg)

    # Load external classifications from the _flex_types.json sidecar (uuid- or
    # path-keyed). Classify semantics live in classify.py so the worker and the
    # shared compile pipeline drive identical typing.
    uuid_keyed, path_keyed = load_sidecar(corpus_root)

    _get_file_uuid, _file_identity_mode = _make_file_uuid_resolver(
        args.file_identity,
        needs_soma_identity=bool(uuid_keyed),
    )
    print(f"File identity: {_file_identity_mode}")

    _overridden = apply_sidecar_overrides(
        entries, corpus_root, _get_file_uuid, uuid_keyed, path_keyed)

    if (Path(corpus_root) / '_flex_types.json').exists():
        print(f"External types: {_overridden}/{len(entries)} files matched from _flex_types.json")

    indexable = [e for e in entries if not e.skip]
    print(f"Docpac: {len(entries)} total, {len(indexable)} indexable, "
          f"{len(entries) - len(indexable)} skipped")

    # ═════════════════════════════════════════════════
    # 4. INDEX PIPELINE
    # ═════════════════════════════════════════════════
    source_count = 0
    chunk_count = 0
    _docpac_prof = docpac_profile()  # the IngestProfile seam: classify authority

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

        # Classification authority is the docpac profile (the IngestProfile
        # seam). classify_source applies the frontmatter > sidecar > folder merge
        # (mutating entry so classify_chunk sees the merged temporal/doc_type)
        # and returns the _raw_sources typed columns.
        src = _docpac_prof.classify_source(entry, frontmatter)

        # Declarative split strategy (profile-as-data): resolve_chunker picks the split
        # from context_cfg['chunking'] + the .flexchunk.json cascade; no chunking block
        # → heading@split_level = today (parity). Adapt dicts → the loop's tuple shape.
        from flex.compile.chunk_config import resolve_chunker
        from pathlib import Path as _P
        _coord = tuple((src['doc_type'] or '').split('.', 1))
        _nodes = resolve_chunker(_coord, str(_P(entry.path).parent), entry.path,
                                 context_cfg.get('chunking'),
                                 default_level=context_cfg['split_level'])(body)
        sections = [(n['title'], n['content'], n['position'], n['depth']) for n in _nodes]

        if not sections:
            sections = [('', body.strip(), 0, 0)]

        # INSERT _raw_sources
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, file_date, temporal, doc_type, title, source_path,
             file_uuid, type, status, keywords, summary,
             confidence, validity, maturity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_id,
            src['file_date'],
            src['temporal'],
            src['doc_type'],
            src['title'],
            src['source_path'],
            src['file_uuid'],
            src['type'],
            src['status'],
            src['keywords'],
            src['summary'],
            src['confidence'],
            src['validity'],
            src['maturity'],
        ))
        source_count += 1

        # INSERT _raw_chunks + _edges_source + _types_docpac
        chunk_depths = []  # (chunk_id, depth) in document order, for the tree
        for section in sections:
            section_content = section[1]
            position = section[2]
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

            row = _docpac_prof.classify_chunk(entry, frontmatter, section[:3])
            db.execute("""
                INSERT OR IGNORE INTO _types_docpac
                (chunk_id, temporal, doc_type, facet, section_title,
                 yaml_type, yaml_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                chunk_id,
                row['temporal'],
                row['doc_type'],
                row['facet'],
                row['section_title'],
                row['yaml_type'],
                row['yaml_status'],
            ))

            chunk_depths.append((chunk_id, section[3] if len(section) > 3 else 0))
            chunk_count += 1

        # Heading hierarchy → _edges_tree (parent_slots; matches compile_vault)
        if len(chunk_depths) > 1:
            # size to the deepest heading, not a fixed 6 (deep-heading content loss)
            n_slots = max(6, max((d for _, d in chunk_depths if d > 0), default=0))
            parent_slots = [None] * n_slots
            for cid, depth in chunk_depths:
                if depth > 0:
                    parent_slots[depth - 1] = cid
                    for d in range(depth, n_slots):
                        parent_slots[d] = None
                    if depth > 1 and parent_slots[depth - 2]:
                        link(db, child_id=cid, parent_id=parent_slots[depth - 2],
                             relation='subsection', depth=depth)

    db.commit()
    print(f"Indexed: {source_count} sources, {chunk_count} chunks")

    # SOMA file identity (canonical G0 edge; batch parity with index_file). Additive:
    # _raw_sources.file_uuid stays deterministic (the hint), _edges_fs_identity is the
    # SOMA-minted spine — so a batch-init cell carries the same identity edge an
    # index_file-built cell does. Best-effort; never fails a compile.
    _minted = _mint_batch_identity(db, indexable)
    if _minted:
        print(f"SOMA identity: {_minted} file UUIDs stamped")
    db.commit()

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
             'SQL PRE-FILTER (2nd arg to vec_ops): '
             'Any SQL returning chunk_ids. Restricts which chunks enter the landscape.')
    set_meta(db, 'retrieval:phase2',
             'LANDSCAPE (compiled scoring on filtered N): '
             'diverse, recent[:N], suppress:TEXT, centroid:id1,id2, from:TEXT to:TEXT')
    set_meta(db, 'retrieval:phase3',
             'ENRICH (query-time topology on K candidates): '
             'communities→_community column (per-query Louvain)')
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
    db.commit()  # flush before subprocess
    result = subprocess.run([
        sys.executable, '-m', 'flex.manage.meditate',
        '--cell', db_path,
        '--threshold', str(args.threshold),
    ], capture_output=True, text=True)
    from flex.views import install_views

    docpac_views = FLEX_ROOT / 'flex' / 'modules' / 'docpac' / 'stock' / 'views'

    if result.returncode == 0:
        print(result.stdout.strip() if result.stdout.strip() else "Graph built.")
    else:
        print("No graph built (no embeddings?)")

    # ═════════════════════════════════════════════════
    # 10. _enrich_types: stopped writing heuristic values (Plan 9).
    # AI queries doc_type + temporal directly via curated views.
    # Table kept as reserved slot for future semantic classification.
    # ═════════════════════════════════════════════════

    install_views(db, docpac_views)
    regenerate_views(db, views={'sections': 'chunk', 'documents': 'source'})

    # ═════════════════════════════════════════════════
    # 11. INSTALL PRESETS
    # ═════════════════════════════════════════════════
    print("Installing presets...")
    from flex.retrieve.presets import install_presets

    general_presets = FLEX_ROOT / 'flex' / 'retrieve' / 'presets' / 'general'
    install_presets(db, general_presets)
    docpac_presets = FLEX_ROOT / 'flex' / 'modules' / 'docpac' / 'stock' / 'presets'
    install_presets(db, docpac_presets)
    # Phase B: cell-shipped presets (.flexpresets.json) install AFTER stock so a
    # fresh-named cell preset survives a rebuild that wipes+reinstalls stock.
    from flex.compile.flexpresets import install_flexpresets
    _fp = install_flexpresets(db, corpus_root, warn=lambda m: print(m))
    if _fp['installed']:
        print(f"Installed {_fp['installed']} cell preset(s) from .flexpresets.json.")
    preset_count = db.execute("SELECT COUNT(*) FROM _presets").fetchone()[0]
    print(f"Installed {preset_count} presets.")

    # ═════════════════════════════════════════════════
    # 12. REGISTER IN CELL REGISTRY
    # ═════════════════════════════════════════════════
    _registry_register(
        cell_name,
        db_path,
        cell_type='docpac',
        description=desc,
        corpus_path=corpus_root,
        lifecycle='watch',
        watch_path=corpus_root,
        watch_pattern='**/*.md',
        unlisted=True,
        active=True,
    )
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
