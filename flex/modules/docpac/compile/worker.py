"""
Docpac incremental index worker — single-file upsert into chunk-atom cells.

Scans registered docpac corpus dirs by file size, indexes changed .md files.
Mirror of claude_code scan_sessions() — Filebeat pattern. No hooks, no queue.

Pipeline per file:
  resolve_cell_for_path → parse_docpac_file → frontmatter → normalize →
  split_sections → embed → upsert (delete old + re-insert) → mean-pool source

Auto graph refresh when staleness threshold (20 sources) exceeded.
"""

import hashlib
import os
import sqlite3
import sys
import time

import numpy as np
from pathlib import Path

from flex.registry import resolve_cell_for_path, FLEX_HOME
from flex.core import log_op
from flex.modules.docpac.compile.docpac import parse_docpac_file, resolve_file_date
from flex.modules.docpac.compile.classify import load_sidecar, apply_sidecar_overrides
from flex.modules.docpac.compile.context_config import load_context_config
from flex.modules.docpac.compile.init import _make_file_uuid_resolver, SCHEMA_DDL
from flex.modules.markdown.compile.profile import docpac_profile
from flex.compile.markdown import normalize_headers, extract_frontmatter, split_sections
from flex.sdk import link

GRAPH_REFRESH_THRESHOLD = 20

# The IngestProfile seam — same classification authority the batch pipeline uses,
# so incremental and batch typing agree (frontmatter override + sidecar + grammar).
_PROFILE = docpac_profile()
SCAN_SKIP_FOLDERS = {'buffer', '_raw', '_stale', 'cache', '__pycache__', '.git'}

# Directories the reconciliation walk never descends into. The walk cost — not
# the corpus — dominates the scan (a .git tree is 90%+ of the traversal of a
# large-corpus pass: 682ms→43ms once pruned). Pruning at TRAVERSAL (os.walk +
# dirnames filter) instead of post-filtering rglob results is the CPU win, and
# for the SCAN_SKIP_FOLDERS members it is behaviour-identical (a file under one
# was already dropped by _in_skip_folder — now it is simply never walked). The
# added dep/build dirs are new prunes (their .md are dependency noise, not
# corpus); parity-verified on the golden corpora.
WALK_PRUNE_DIRS = SCAN_SKIP_FOLDERS | {
    'node_modules', 'venv', '.venv', '.mypy_cache', '.pytest_cache',
    'dist', 'build', '.next',
}


def _walk_md(root: Path, exclude_dirs=None):
    """Yield ``*.md`` paths under ``root``, pruning WALK_PRUNE_DIRS at traversal
    (never descends into .git/venv/node_modules/…). Replaces
    ``rglob('*.md')`` + ``_in_skip_folder``: a file previously dropped by
    _in_skip_folder is now never walked, so results are identical for those
    dirs while the traversal cost collapses.

    ``exclude_dirs`` (per-cell, from _meta) prunes those subtrees IN ADDITION to
    WALK_PRUNE_DIRS — a general scoping knob so a cell can carve a sub-region out
    of its corpus (e.g. a monorepo cell excludes a nested subtree so it never
    double-indexes cells built from it). Empty/None → identical to before."""
    prune = WALK_PRUNE_DIRS | set(exclude_dirs or ())
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in prune]
        for fn in filenames:
            if fn.endswith('.md'):
                yield Path(dirpath) / fn


def _cell_exclude_dirs(conn) -> set:
    """Per-cell walk exclusions declared in the cell's own _meta (key
    ``exclude_dirs`` = a JSON list of directory names). Per-cell by construction
    — one cell's exclusions live in that cell's db and never affect another.
    Absent/malformed → empty set (no exclusion, zero behaviour change)."""
    try:
        row = conn.execute("SELECT value FROM _meta WHERE key='exclude_dirs'").fetchone()
        if row and row[0]:
            import json
            v = json.loads(row[0])
            if isinstance(v, list):
                return {str(x) for x in v}
    except (sqlite3.OperationalError, ValueError, TypeError):
        pass
    return set()


def _delete_source_rows(conn, source_id: str, drop_source: bool = False) -> bool:
    """Delete rows for one source_id. With drop_source=False (index_file's
    re-insert path) it clears chunks/types/tree/edges_source but LEAVES the
    `_raw_sources` row, which index_file re-upserts preservingly (keeping
    worker-external columns like file_uuid). With drop_source=True (the
    reconcile-delete of a vanished file) it ALSO removes the `_raw_sources`
    row, because nothing re-inserts it. Returns True if it removed anything."""
    old_chunk_ids = [r[0] for r in conn.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id = ?", (source_id,)
    ).fetchall()]
    if old_chunk_ids:
        ph = ','.join('?' * len(old_chunk_ids))
        conn.execute(f"DELETE FROM _raw_chunks WHERE id IN ({ph})", old_chunk_ids)
        conn.execute(f"DELETE FROM _types_docpac WHERE chunk_id IN ({ph})", old_chunk_ids)
        try:  # _enrich_types may not exist in all cells
            conn.execute(f"DELETE FROM _enrich_types WHERE chunk_id IN ({ph})", old_chunk_ids)
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(f"DELETE FROM _edges_tree WHERE id IN ({ph})", old_chunk_ids)
        except sqlite3.OperationalError:
            pass
        conn.execute("DELETE FROM _edges_source WHERE source_id = ?", (source_id,))
    if drop_source:
        conn.execute("DELETE FROM _raw_sources WHERE source_id = ?", (source_id,))
        # Identity edge is source_id-keyed; prune it with the source or a vanished
        # file leaks an orphaned _edges_fs_identity row (docpac now mints identity
        # via _mint_file_identity — parallel to the code-path leak the live delete
        # co-verify caught). Only on drop_source: the re-insert path (drop_source=
        # False) re-mints idempotently, so we leave identity there — and never risk
        # a transient-mint-failure window deleting it without a re-stamp.
        try:
            conn.execute("DELETE FROM _edges_fs_identity WHERE source_id = ?", (source_id,))
        except sqlite3.OperationalError:
            pass  # cell predates the identity table (never minted) — nothing to prune
    return bool(old_chunk_ids) or drop_source


def _reconcile_deleted_sources(conn, on_disk_source_ids: set) -> int:
    """Drop rows for sources whose file no longer exists on disk.

    The stat-poll walk only ever sees files that EXIST, so a deleted .md would
    otherwise leave its chunks in the cell forever (the latent stale-row bug).
    This is the delete AUTHORITY — deletes are never keyed on inotify (WSL and
    bind-mounts drop delete events).

    Over-prune (dropping a LIVE source's rows) is the worse failure, so this is
    belt-and-suspenders: `on_disk_source_ids` (the walk set) is only a fast
    'definitely present' filter; the actual prune decision is a **stat of the
    stored source_path**. A path-normalization mismatch (resolved vs raw,
    symlink, trailing slash) that makes a live file absent from the walk set can
    therefore never false-prune it — the file has to be genuinely gone on disk."""
    # Empty walk set ⇒ almost always a transient walk/mount failure, not a real
    # "every file deleted". Refuse to reconcile — the one catastrophic case.
    if not on_disk_source_ids:
        return 0
    try:
        rows = conn.execute(
            "SELECT source_id, source_path FROM _raw_sources").fetchall()
    except sqlite3.OperationalError:
        return 0
    deleted = 0
    for source_id, source_path in rows:
        if source_id in on_disk_source_ids:
            continue                                    # walk saw it → present
        # Absent from the walk set — CONFIRM genuinely gone before pruning.
        # normpath() first: os.path.exists('/x/f.md/') is False on Linux (a
        # trailing slash means "directory"), which would false-prune a live file
        # whose stored path carries one. normpath collapses trailing '/', '/./',
        # '//' and '..' — hardening the whole normalization class, not just one.
        if source_path and os.path.exists(os.path.normpath(source_path)):
            continue                                    # live file, sid/path mismatch → keep
        if _delete_source_rows(conn, source_id, drop_source=True):
            deleted += 1
    return deleted


def make_source_id(path: str) -> str:
    return hashlib.sha256(path.encode()).hexdigest()[:16]


def make_chunk_id(source_id: str, position: int) -> str:
    return f"{source_id}:{position}"


def _corpus_root_for(file_path: str) -> Path | None:
    """Resolve the owning docpac corpus root from the cell registry.

    Bindings live in the registry, not in path conventions — a corpus dir
    may be named context/, .context/, or anything else. Longest
    corpus_path match wins, mirroring resolve_cell_for_path().
    """
    from flex.registry import list_cells
    file_str = str(Path(file_path).resolve())
    best = None
    for cell in list_cells():
        cp = cell.get('corpus_path')
        if (cell.get('cell_type') == 'docpac' and cp
                and file_str.startswith(cp.rstrip('/') + '/')):
            if best is None or len(cp) > len(best):
                best = cp
    return Path(best) if best else None


def _in_skip_folder(path: Path) -> bool:
    """True if any path component is in the skip set."""
    return bool(set(path.parts) & SCAN_SKIP_FOLDERS)


def _resolve_cell_embed_fn(conn: sqlite3.Connection, fallback=None):
    """Resolve the doc-embedder for THIS cell from its `vec:model` _meta tag.

    The watch-scan drain into index_file
    used to be handed ONE shared MiniLM `encode_fn` for every watched cell,
    regardless of that cell's own tag — so a nomic-v1.5-tagged cell's live
    incremental chunks would get embedded with MiniLM-128 into the Nomic
    column (silent cross-space corruption).

    minilm/untagged cells are left COMPLETELY untouched — `fallback` (the
    caller's shared encode_fn) is returned as-is, byte-identical to the
    pre-existing behavior (this also keeps every call site free to pass a
    test double for the untagged path, exactly as before). Only a
    non-default tag triggers resolution,
    mirroring flex.compile.embed._resolve_ingest_target (the batch embed_new
    path, which is also tag-aware) so the two ingest paths never
    diverge: nomic-v1.5 -> the dedicated Nomic embedder at its native 768d
    (matches embed_new; the Matryoshka slice to serve_dim happens at
    query/serve time, not here). Resolution of an explicit tag fails closed:
    falling back would write another model's vectors into this cell's space.
    """
    try:
        from flex.compile.embed import _cell_tag, _resolve_ingest_target
        tag = _cell_tag(conn)
    except Exception:
        return fallback
    if tag is None or tag == 'minilm':
        return fallback
    embed_doc, _dim, _tag = _resolve_ingest_target(conn)
    if embed_doc is None:
        raise RuntimeError(f"no document embedder available for vec:model={tag!r}")
    return embed_doc


def _embed_texts(texts: list[str], embed_fn) -> list[bytes | None]:
    """Embed texts using the shared ONNX embedder. Returns list of blobs."""
    if not texts:
        return []
    try:
        vecs = embed_fn(texts)
        if hasattr(vecs, 'shape') and len(vecs.shape) == 2:
            return [v.astype(np.float32).tobytes() for v in vecs]
        return [vecs.astype(np.float32).tobytes()]
    except Exception as e:
        print(f"[docpac-worker] embed error: {e}", file=sys.stderr)
        return [None] * len(texts)


# Canonical identity edge table — IDENTICAL DDL to instant/install.py + soma's
# backfill (identity-seam G0: _edges_fs_identity is the schema every engine
# converges on, the view-join target, NOT docpac's _raw_sources.file_uuid).
_FS_IDENTITY_DDL = """
CREATE TABLE IF NOT EXISTS _edges_fs_identity (
    source_id TEXT PRIMARY KEY,
    file_uuid TEXT
);
"""


def _mint_file_identity(conn: sqlite3.Connection, source_id: str, file_path: str) -> bool:
    """Mint (or resolve) this file's SOMA uuid and stamp _edges_fs_identity — the
    canonical identity spine (identity-seam G0, contract handed by flex:engine:soma
    2026-07-07). SOMA is the SOLE minting authority: no deterministic/frontmatter
    id ever populates this edge (those may still ride _raw_sources as a hint).

    Never raises — identity must never fail a compile (instant's discipline,
    install.py:714-724). Idempotent: assign_batch resolves an existing path to its
    existing uuid, so a re-index leaves file_uuid byte-identical (the directional-
    parity contract). Returns True iff a uuid was stamped.
    """
    try:
        from flex.modules.soma.lib.identity.file_identity import get_instance
        from flex.modules.docpac.compile.init import _is_identity_excluded
        rp = str(Path(file_path).resolve())
        # Ephemeral corpora (/tmp,/var/tmp,/dev — soma exclude_paths) never mint, so
        # fixture/test cells don't pollute the shared ~/.soma authority. Symmetric with
        # batch init's _mint_batch_identity (single-sourced exclusion).
        if _is_identity_excluded(rp):
            return False
        uuid = get_instance().assign_batch([rp]).get(rp)
        if uuid:
            conn.execute(_FS_IDENTITY_DDL)  # idempotent, transactional (no premature commit)
            conn.execute(
                "INSERT OR REPLACE INTO _edges_fs_identity (source_id, file_uuid) VALUES (?, ?)",
                (source_id, uuid),
            )
            return True
    except Exception:
        pass  # best-effort; the chunk/source write must still land
    return False


def cell_is_no_embed(conn: sqlite3.Connection) -> bool:
    """PUBLIC: True if this cell self-declares embed-off (_meta.embed='false').

    The single-source parser for the canonical `_meta.embed` signal — the ONE flag
    every embed-off consumer keys on (index_file skip, runtime's VectorCache
    warm-skip, interface's @orient re-stamp). Embed-off = the row is present AND its
    value is falsey ({false,0,off,no}); absent/true = embed-on. Lives in _meta, so it
    survives regen. Import this rather than re-parsing so the semantics stay single-
    sourced. A no-embed cell = instant's structural surface (FTS + node tree +
    declared edges) with docpac's incremental refresh; no vectors, no similarity."""
    try:
        row = conn.execute("SELECT value FROM _meta WHERE key='embed'").fetchone()
        return bool(row) and str(row[0]).lower() in ('false', '0', 'off', 'no')
    except sqlite3.OperationalError:
        return False


# internal alias — existing call sites (index_file, _refresh_graph) keep the private name
_cell_is_no_embed = cell_is_no_embed


def set_cell_embed(conn: sqlite3.Connection, embed: bool) -> None:
    """Stamp the cell's embed mode into _meta (called once at cell creation)."""
    conn.execute("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT OR REPLACE INTO _meta (key, value) VALUES ('embed', ?)",
                 ('true' if embed else 'false',))


def index_file(conn: sqlite3.Connection, file_path: str, embed_fn,
               corpus_root: str | Path | None = None,
               place: str | None = None, no_embed: bool = False) -> bool:
    """Index a single markdown file into its docpac (or product) cell.

    Upsert semantics: delete old chunks for this source, re-insert.
    corpus_root comes from the cell's registry row; falls back to a
    registry lookup — never inferred from directory names.

    `embed_fn` is the CALLER's shared encode_fn (historically MiniLM) — it is
    now only a fallback. The actual embedder is resolved per-cell from THIS
    cell's `vec:model` _meta tag via `_resolve_cell_embed_fn` (same 2-way
    resolver embed_new's batch path uses), so a nomic-v1.5-tagged cell's
    incremental chunks are never silently embedded with MiniLM.

    `place` (place-scoped cells only): the scope path under a corpus's place
    root. When set, the source_id is keyed on (place, path) and the `place`
    column is stamped, matching the place-scoped builder's own id scheme so
    incremental updates agree with a full rebuild. place=None is the plain
    docpac path (unchanged).
    """
    p = Path(file_path)
    if not p.exists():
        return False

    context_root = Path(corpus_root) if corpus_root else _corpus_root_for(file_path)
    if not context_root:
        return False

    cfg = load_context_config(str(context_root))
    entry = parse_docpac_file(file_path, str(context_root), config=cfg)
    if entry.skip:
        return False

    try:
        content = p.read_text(encoding='utf-8')
    except (UnicodeDecodeError, FileNotFoundError):
        return False

    source_id = (
        hashlib.sha256((place + '|' + file_path).encode()).hexdigest()[:16]
        if place is not None else make_source_id(file_path)
    )

    # Content-hash skip — size_cache is in-memory, so every daemon restart
    # rescans all files; without this, each restart re-embeds the full corpus
    # and the chunk delete/re-insert churns cell mtimes, forcing flex.serve
    # vec_cache rebuilds.
    content_hash = hashlib.sha256(content.encode()).hexdigest()
    row = None
    try:
        row = conn.execute(
            "SELECT content_hash, file_date FROM _raw_sources WHERE source_id = ?",
            (source_id,)).fetchone()
        if row and row[0] == content_hash:
            return False
    except sqlite3.OperationalError:
        # Older cells predate the column; add it (commits with the upsert)
        conn.execute("ALTER TABLE _raw_sources ADD COLUMN content_hash TEXT")
        row = None
    existing_file_date = row[1] if row else None

    frontmatter, body = extract_frontmatter(content)

    # Unify typing with the batch pipeline: sidecar (_flex_types.json) +
    # frontmatter doc_type override + grammar, all via the docpac profile. This
    # is the S5 fix — the incremental path previously skipped both the sidecar
    # and the frontmatter override, diverging from batch.
    uuid_keyed, path_keyed = load_sidecar(str(context_root))
    get_uuid, _ = _make_file_uuid_resolver(
        'deterministic', needs_soma_identity=bool(uuid_keyed))
    apply_sidecar_overrides([entry], str(context_root), get_uuid, uuid_keyed, path_keyed)
    src = _PROFILE.classify_source(entry, frontmatter)  # mutates entry; typed cols

    # Declarative split strategy (profile-as-data): resolve_chunker picks the split
    # from cfg['chunking'] + the .flexchunk.json cascade; no chunking block →
    # heading@split_level = today (parity). Adapt dicts → the loop's tuple shape.
    from flex.compile.chunk_config import resolve_chunker
    _coord = tuple((src['doc_type'] or '').split('.', 1))
    _nodes = resolve_chunker(_coord, str(Path(file_path).parent), file_path,
                             cfg.get('chunking'), default_level=cfg['split_level'])(body)
    sections = [(n['title'], n['content'], n['position'], n['depth']) for n in _nodes]
    if not sections:
        sections = [('', body.strip(), 0, 0)]

    # Embed all section texts — unless this is an embed-off cell. The cell's
    # own _meta.embed declaration wins over the caller, so a no-embed cell stays
    # no-embed. All-None embeddings → _raw_chunks.embedding NULL + the mean-pool
    # below is skipped by its `if valid:` guard → byte-identical to embed-on except
    # the two embedding columns.
    section_texts = [s[1] for s in sections]
    if no_embed or _cell_is_no_embed(conn):
        embeddings = [None] * len(section_texts)
    else:
        # Per-cell resolution: never trust the caller's shared
        # encode_fn blindly — resolve from THIS cell's vec:model tag first.
        cell_embed_fn = _resolve_cell_embed_fn(conn, fallback=embed_fn)
        embeddings = _embed_texts(section_texts, cell_embed_fn)

    # --- Upsert: delete old data for this source (shared with reconcile-delete) ---
    _delete_source_rows(conn, source_id)

    # --- Insert source ---
    # Upsert preserves columns this worker doesn't own (file_uuid,
    # confidence, validity, maturity, summary) — INSERT OR REPLACE
    # would null them on every incremental touch.
    # fill-only date: filename > existing stored > git/mtime (no drift on edit).
    raw_fdate = resolve_file_date(src['file_date'], file_path, existing=existing_file_date)
    from flex.modules.docpac.compile.classify import record_file_date_health
    fdate = record_file_date_health(conn, source_id, raw_fdate)
    if place is not None:
        conn.execute("""
            INSERT INTO _raw_sources
            (source_id, place, file_date, temporal, doc_type, title, source_path,
             type, status, keywords, content_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
              place=excluded.place, file_date=excluded.file_date,
              temporal=excluded.temporal, doc_type=excluded.doc_type,
              title=excluded.title, source_path=excluded.source_path,
              type=excluded.type, status=excluded.status,
              keywords=excluded.keywords, content_hash=excluded.content_hash
        """, (source_id, place, fdate, src['temporal'], src['doc_type'], src['title'],
              file_path, src['type'], src['status'], src['keywords'], content_hash))
    else:
        conn.execute("""
            INSERT INTO _raw_sources
            (source_id, file_date, temporal, doc_type, title, source_path,
             type, status, keywords, content_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
              file_date=excluded.file_date, temporal=excluded.temporal,
              doc_type=excluded.doc_type, title=excluded.title,
              source_path=excluded.source_path, type=excluded.type,
              status=excluded.status, keywords=excluded.keywords,
              content_hash=excluded.content_hash
        """, (source_id, fdate, src['temporal'], src['doc_type'], src['title'],
              file_path, src['type'], src['status'], src['keywords'], content_hash))

    # --- SOMA file identity (canonical spine; G0-identity, same txn as the source
    # upsert so a mid-write crash can't leave a source without an identity attempt) ---
    _mint_file_identity(conn, source_id, file_path)

    # --- Insert chunks + edges + types ---
    chunk_depths = []  # (chunk_id, depth) in document order, for the containment tree
    for section in sections:
        section_content = section[1]
        position = section[2]
        chunk_id = make_chunk_id(source_id, position)
        emb = embeddings[position] if position < len(embeddings) else None

        conn.execute("""
            INSERT OR REPLACE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, ?, ?)
        """, (chunk_id, section_content, emb, None))

        conn.execute("""
            INSERT OR REPLACE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'markdown', ?)
        """, (chunk_id, source_id, position))

        row = _PROFILE.classify_chunk(entry, frontmatter, section[:3])
        conn.execute("""
            INSERT OR REPLACE INTO _types_docpac
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

    # --- Heading hierarchy → _edges_tree (parent_slots; matches compile_vault) ---
    # Migrate pre-0.50 cells forward: _edges_tree was only ever created at build
    # time, so a cell built before it existed has no such table and link()'s
    # unguarded INSERT would raise, half-indexing this file. CREATE IF NOT EXISTS
    # is transactional (no premature commit) and idempotent.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _edges_tree (
            id TEXT NOT NULL, parent_id TEXT, branch_at TEXT,
            relation TEXT NOT NULL, depth INTEGER DEFAULT 0,
            PRIMARY KEY (id, parent_id)
        )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tree_parent ON _edges_tree(parent_id)")
    if len(chunk_depths) > 1:
        # parent_slots must span the DEEPEST heading, not a fixed 6. Verbatim
        # content (e.g. recall session transcripts) carries headings past H6 —
        # seen to depth 15 (`###############`) — and a fixed [None]*6 IndexErrors
        # on `parent_slots[depth-1]`, dropping the WHOLE file (content loss).
        n_slots = max(6, max((d for _, d in chunk_depths if d > 0), default=0))
        parent_slots = [None] * n_slots
        for cid, depth in chunk_depths:
            if depth > 0:
                parent_slots[depth - 1] = cid
                for d in range(depth, n_slots):
                    parent_slots[d] = None
                if depth > 1 and parent_slots[depth - 2]:
                    link(conn, child_id=cid, parent_id=parent_slots[depth - 2],
                         relation='subsection', depth=depth)

    # --- Mean-pool source embedding ---
    valid = [e for e in embeddings if e is not None]
    if valid:
        vecs = [np.frombuffer(e, dtype=np.float32) for e in valid]
        mean_vec = np.mean(vecs, axis=0).astype(np.float32)
        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm
        conn.execute(
            "UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
            (mean_vec.tobytes(), source_id))

    # --- Log to _ops ---
    log_op(conn, 'docpac_incremental_index', '_raw_chunks',
           params={'file': str(p.name), 'sections': len(sections)},
           rows_affected=len(sections),
           source='docpac/compile/worker.py')

    return True


def _derive_context_cell_name(context_dir: Path) -> str:
    """Derive a cell name from a .context dir: parent for `.context`/`context`,
    else the dir name, suffixed `-context`."""
    cdir = Path(context_dir)
    base = cdir.parent.name if cdir.name in ('.context', 'context') else cdir.name
    return f"{base}-context" if base else cdir.name


def register_context_dir(context_dir, name: str = None) -> str | None:
    """Self-register a `.context` dir carrying a `context.json` as a docpac cell.

    Membership-only on the registry side: if a cell already binds this corpus
    path, no-op. Otherwise bootstrap an EMPTY docpac cell (schema + curated views
    + presets) and register it `watch`/`unlisted`; the existing watch loop fills
    content on its next tick. Never ingests or embeds here. Returns the new cell
    name, or None if already registered / no context.json.
    """
    from flex.registry import register_cell, list_cells, FLEX_HOME as _FH
    from flex.core import open_cell
    from flex.views import install_views, regenerate_views
    from flex.retrieve.presets import install_presets
    from flex.modules.docpac.compile.init import SCHEMA_DDL, FLEX_ROOT
    import uuid as _uuid

    cdir = Path(context_dir).resolve()
    if not (cdir / 'context.json').exists():
        return None
    for cell in list_cells():
        cp = cell.get('corpus_path')
        if cp and Path(cp).resolve() == cdir:
            return None  # already bound — membership-only

    cell_name = name or _derive_context_cell_name(cdir)
    cells_dir = Path(_FH) / 'cells'
    cells_dir.mkdir(parents=True, exist_ok=True)
    db_path = str(cells_dir / f"{_uuid.uuid4()}.db")

    db = open_cell(db_path)
    db.executescript(SCHEMA_DDL)
    prof = _PROFILE
    if prof.views_dir:
        install_views(db, prof.views_dir)
    regenerate_views(db, views={'sections': 'chunk', 'documents': 'source'})
    install_presets(db, FLEX_ROOT / 'flex' / 'retrieve' / 'presets' / 'general')
    for pd in prof.presets_dirs:
        install_presets(db, pd)
    # Phase B: cell-shipped presets survive regen — install AFTER stock (see init.py).
    from flex.compile.flexpresets import install_flexpresets
    install_flexpresets(db, str(cdir), warn=lambda m: print(m, file=sys.stderr))
    db.commit()
    db.close()

    register_cell(cell_name, db_path, cell_type='docpac',
                  description=f"Docpac context cell ({cell_name})",
                  corpus_path=str(cdir), lifecycle='watch',
                  watch_path=str(cdir), watch_pattern='**/*.md',
                  unlisted=True, active=True)
    return cell_name


def discover_self_declared_context(search_roots) -> list[str]:
    """Register `.context` dirs (carrying `context.json`) under `search_roots`
    not yet in the registry. Membership-only — bounded by the explicit roots,
    no whole-FS scan; the watch loop does the actual ingest. Returns new cells.
    """
    registered = []
    for root in (search_roots or []):
        root = Path(root)
        if not root.is_dir():
            continue
        for cj in root.rglob('context.json'):
            new = register_context_dir(cj.parent)
            if new:
                registered.append(new)
    return registered


def _graph_stale(conn) -> bool:
    """True if enough new sources indexed since last graph build."""
    try:
        last_graph = conn.execute("""
            SELECT MAX(timestamp) FROM _ops
            WHERE operation = 'build_similarity_graph'
        """).fetchone()[0]
    except sqlite3.OperationalError:
        return False  # no _ops table yet

    if last_graph is None:
        return True  # never built

    try:
        new_sources = conn.execute("""
            SELECT COUNT(*) FROM _ops
            WHERE operation = 'docpac_incremental_index'
              AND timestamp > ?
        """, (last_graph,)).fetchone()[0]
    except sqlite3.OperationalError:
        return False

    return new_sources >= GRAPH_REFRESH_THRESHOLD


def _refresh_graph(cell_path: str, cell_name: str):
    """Rebuild similarity graph on a docpac cell (subprocess to avoid engine import coupling)."""
    import subprocess
    # G4: an embed-off cell has no vectors, so it has no similarity edges to build.
    # Skip the meditate pass entirely — its structural surface (FTS + node tree +
    # declared edges) is complete without a similarity graph. Defensive: even if a
    # scheduler calls this on a no-embed cell, it must never spawn the vector pass.
    try:
        _c = sqlite3.connect(cell_path, timeout=5)
        try:
            if _cell_is_no_embed(_c):
                print(f"[docpac] graph refresh skipped on {cell_name}: embed-off cell", file=sys.stderr)
                return
        finally:
            _c.close()
    except sqlite3.OperationalError:
        pass
    print(f"[docpac] graph refresh on {cell_name}...", file=sys.stderr)
    t0 = time.time()
    subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                    '--cell', cell_path], check=True)
    elapsed = time.time() - t0
    print(f"[docpac] graph refresh done in {elapsed:.1f}s", file=sys.stderr)


def _ensure_docpac_views(conn) -> bool:
    """Ensure the presentation views (sections/documents) exist on a docpac cell.
    Idempotent + guarded: no-op when they're already present (the common case), so
    the drain never takes the write lock for this after the first heal. The view
    DEFINITIONS are identical to the full-init/bootstrap path (same
    prof.views_dir + regenerate_views mapping) — this only changes WHERE the logic
    runs, never the resulting views. Returns True iff it healed."""
    views = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'")}
    # Full-init produces FOUR views: {chunks, sources} (the base/default set, made by
    # meditate's regenerate_views(db) — views=None default) + {sections, documents}
    # (the curated docpac set, init.py:609). The drain skips meditate, so a healed
    # cell needs BOTH. Guard on all four — a cell with only sections/documents (the
    # earlier incomplete heal / runtime's interim step) is NOT complete.
    if {'chunks', 'sources', 'sections', 'documents'} <= views:
        return False
    from flex.views import install_views, regenerate_views
    prof = _PROFILE
    # base default set — byte-identical to meditate's regenerate_views(db) default.
    regenerate_views(conn, views={'chunks': 'chunk', 'sources': 'source'})
    # curated docpac set (sections/documents .sql).
    if prof.views_dir:
        install_views(conn, prof.views_dir)
    regenerate_views(conn, views={'sections': 'chunk', 'documents': 'source'})
    conn.commit()
    return True


def scan_docpac_cells(embed_fn, size_cache: dict) -> dict:
    """Scan registered docpac corpora by stat signature, fairly bounded per tick.

    Mirror of claude_code scan_sessions() — Filebeat pattern.

    Args:
        embed_fn: ONNX encode callable (shared embedder).
        size_cache: Mutable dict {cache_key: ``size:mtime_ns``}. Persisted in memory
                    across ticks. Empty dict triggers full initial scan.

    Returns:
        dict with 'indexed' and 'skipped' counts.
    """
    from flex.registry import list_cells, update_refresh_status

    stats = {'indexed': 0, 'skipped': 0}

    from flex.modules.markdown.compile.profile import profile_name_for_cell
    cells = [
        c for c in list_cells()
        if c.get('cell_type') == 'docpac'
        and profile_name_for_cell(c) == 'docpac'
        and c.get('corpus_path')
        and c.get('lifecycle') == 'watch'
        and c.get('active', 1)
    ]

    for cell in cells:
        cell_name = cell['name']
        corpus = Path(cell['corpus_path'])
        if not corpus.is_dir():
            continue

        conn = sqlite3.connect(cell['path'], timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")

        # Skip cells with broken schema — prevents spin-loop on every tick
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if '_raw_chunks' not in tables or '_edges_source' not in tables:
            print(f"[docpac] skipping {cell_name}: missing core tables", file=sys.stderr)
            conn.close()
            continue

        # Migrate pre-0.50 cells forward: SCHEMA_DDL is fully idempotent
        # (every statement is IF NOT EXISTS), so this back-fills new tables like
        # _edges_tree that were only ever created at build time. Without it,
        # index_file()'s unguarded INSERT INTO _edges_tree raises "no such table"
        # on any cell built before that table existed, half-indexing the file.
        # Gate on the actual missing table so we don't take SQLite's write lock on
        # every ~2s tick (a no-op executescript still blocks for busy_timeout and
        # can raise "database is locked" against a concurrent write txn).
        if '_edges_tree' not in tables:
            conn.executescript(SCHEMA_DDL)

        # Self-heal presentation views. regenerate_views is only called by the FULL
        # docpac init + register_context_dir bootstrap — NEVER by index_file — so a
        # cell built purely incrementally (a drain/migration build) ships WITHOUT
        # sections/documents views, and @orient then advertises view-backed queries
        # that error. Ensure them here, guarded on absence so the drain doesn't take
        # the write lock every ~2s tick (same discipline as the _edges_tree guard).
        _ensure_docpac_views(conn)

        cell_indexed = 0
        on_disk_sids = set()
        candidates = []
        _exclude = _cell_exclude_dirs(conn)   # per-cell subtree exclusions from _meta
        for md in _walk_md(corpus, exclude_dirs=_exclude):
            path_key = str(md)
            # Record the source_id BEFORE stat() so a transient stat failure on a
            # live file can never make the reconcile-delete drop it.
            on_disk_sids.add(make_source_id(path_key))
            try:
                stat = md.stat()
            except (FileNotFoundError, OSError):
                continue
            signature = f"{stat.st_size}:{stat.st_mtime_ns}"
            cache_key = f"docpac:{cell_name}:{path_key}"
            cached = size_cache.get(cache_key, size_cache.get(path_key))
            # Accept the legacy integer cache for one release so a rolling daemon
            # upgrade does not needlessly re-index every document.
            if cached in (signature, stat.st_size):
                continue
            candidates.append((path_key, (md, signature, cache_key)))

        from flex.watch import fair_batch
        limit = max(1, int(os.environ.get('FLEX_DRAIN_FILES_PER_CELL', '200')))
        batch = fair_batch(conn, 'docpac', candidates, limit)
        if batch:
            conn.commit()  # cursor is a durable fairness receipt, even on failure
        for path_key, (md, signature, cache_key) in batch:
            try:
                if index_file(conn, path_key, embed_fn, corpus_root=corpus):
                    stats['indexed'] += 1
                    cell_indexed += 1
                else:
                    stats['skipped'] += 1
            except Exception as e:
                print(f"[docpac] error on {md.name}: {e}", file=sys.stderr)
                stats['skipped'] += 1
            else:
                # A failed write remains a candidate on the next tick.
                size_cache[cache_key] = signature

        # Reconciliation-delete: drop rows for files that vanished since a prior
        # pass (the walk only sees existing files, so deletes need this diff).
        deleted = _reconcile_deleted_sources(conn, on_disk_sids)
        stats['deleted'] = stats.get('deleted', 0) + deleted

        if cell_indexed > 0 or deleted:
            conn.commit()
            log_op(conn, 'docpac_scan_index', '_raw_chunks',
                   params={'cell': cell_name, 'files': cell_indexed},
                   rows_affected=cell_indexed,
                   source='docpac/compile/worker.py')
            # G3 observability: the incremental index_file drain path never
            # stamped registry.last_refresh_at (only the refresh scheduler did),
            # so continuously-fresh watch cells read `last_refresh=never`. Stamp
            # it here so freshness is visible. Telemetry only — no cell content
            # change. Best-effort: update_refresh_status swallows its own errors.
            update_refresh_status(cell_name, 'ok')
            conn.commit()

        conn.close()

    return stats
