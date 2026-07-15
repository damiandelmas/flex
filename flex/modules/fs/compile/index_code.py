"""Seam-0b — `index_file_code`: per-file incremental code-graph extraction.

The code analogue of docpac's `index_file`. Compiles ONE code file's node tree
into a code cell (FTS5 + structural SQL + call/import graph), scoped entirely to
that file. No embeddings — code never embeds.

Design: Option A of `specs/seam0b-incremental-codegraph.md` (owner-ratified
2026-07-08). The cross-file call FK (`_edges_call.callee_id`) is **eliminated**,
not reconciled. Instead:

  - `_edges_call` stores only within-file data: `(caller_id, callee_name)`.
  - a durable `_symbols` table persists the corpus def surface (the persisted
    form of instant's in-memory `name_to_ids`).
  - `@callers/@callees/@impact` resolve name→def by a `_symbols` JOIN at READ
    time (see `install_code_presets`), so every write is purely per-file and a
    dangling edge is impossible (the FK is never stored).

Every table row this primitive writes is keyed to this one file:
  - `_raw_chunks / _edges_source / _types_instant / _edges_tree / _edges_import
     / _edges_fs_identity` by the file's resolved path (`source_id`, matching
     instant's full-regen exactly → byte-identical chunk_ids).
  - `_symbols` by the passed `file_id` (the SOMA file_uuid / reconcile delete key).

File DELETE is NOT handled here — it is reconciliation (runtime doctrine); the
drain's `_delete_source_rows(..., drop_source=True)` prunes vanished files.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
from pathlib import Path


# ── Schema ────────────────────────────────────────────────────────────────────
# Code-cell tables. Mirrors instant/install.py's _TYPES_DDL / _edges_import /
# _edges_fs_identity, but _edges_call DROPS callee_id (Option A: the cross-file
# FK is eliminated, resolution is late-bound via _symbols) and adds _symbols.

_TYPES_DDL = """
CREATE TABLE IF NOT EXISTS _types_instant (
    chunk_id      TEXT PRIMARY KEY,
    section_title TEXT,
    section_type  TEXT,
    position      INTEGER,
    depth         INTEGER,
    container_id  TEXT,
    content_hash  TEXT
);
"""

# _edges_call — WITHIN-FILE ONLY. No callee_id (Option A). The call site's target
# is resolved by name against _symbols at query time, never materialized here.
_CALL_DDL = """
CREATE TABLE IF NOT EXISTS _edges_call (
    caller_id   TEXT NOT NULL,
    callee_name TEXT NOT NULL,
    PRIMARY KEY (caller_id, callee_name)
);
CREATE INDEX IF NOT EXISTS _edges_call_name ON _edges_call(callee_name);
"""

_IMPORT_DDL = """
CREATE TABLE IF NOT EXISTS _edges_import (
    source_id TEXT NOT NULL,   -- the importing file
    module    TEXT NOT NULL,   -- the imported module ('os', 'flex.core', '.state')
    name      TEXT,            -- the imported symbol (NULL for plain `import module`)
    PRIMARY KEY (source_id, module, name)
);
CREATE INDEX IF NOT EXISTS _edges_import_module ON _edges_import(module);
"""

# _symbols — the corpus def surface, one row per definition. The persisted form
# of instant's in-memory name_to_ids. The resolution key for late-bind reads.
_SYMBOLS_DDL = """
CREATE TABLE IF NOT EXISTS _symbols (
    name    TEXT NOT NULL,     -- def/class/method name (the resolution key)
    def_id  TEXT NOT NULL,     -- that def's chunk_id  (→ _types_instant.chunk_id)
    file_id TEXT NOT NULL,     -- SOMA file_uuid / source_id  (the per-file delete key)
    kind    TEXT,              -- func|class|method  (future disambiguation)
    PRIMARY KEY (name, def_id)
);
CREATE INDEX IF NOT EXISTS _symbols_name ON _symbols(name);
CREATE INDEX IF NOT EXISTS _symbols_file ON _symbols(file_id);
"""

_FS_IDENTITY_DDL = """
CREATE TABLE IF NOT EXISTS _edges_fs_identity (
    source_id TEXT PRIMARY KEY,
    file_uuid TEXT
);
"""

_SOURCE_STATE_DDL = """
CREATE TABLE IF NOT EXISTS _code_source_state (
    source_id    TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    mtime_ns     INTEGER NOT NULL
);
"""

CODE_SCHEMA_DDL = (
    _TYPES_DDL + _CALL_DDL + _IMPORT_DDL + _SYMBOLS_DDL + _FS_IDENTITY_DDL
    + _SOURCE_STATE_DDL
)


# ── Late-bind presets (resolution moves to READ time via _symbols) ────────────

# @callers symbol=X: who calls X. Already name-keyed in instant — no change:
# _edges_call.callee_name = X → caller_id → its section_title.
_CALLERS_SQL = (
    "WITH resolution AS ("
    " SELECT name, COUNT(*) AS candidate_count FROM _symbols GROUP BY name"
    ") "
    "SELECT DISTINCT t.section_title AS caller, e.caller_id, "
    "s.def_id AS candidate_def_id, ds.source_id AS candidate_file, "
    "CASE WHEN COALESCE(r.candidate_count, 0)=0 THEN 'unresolved' "
    "     WHEN r.candidate_count=1 THEN 'unique' ELSE 'ambiguous' END "
    "AS resolution_state, COALESCE(r.candidate_count, 0) AS candidate_count "
    "FROM _edges_call e JOIN _types_instant t ON e.caller_id = t.chunk_id "
    "LEFT JOIN resolution r ON r.name=e.callee_name "
    "LEFT JOIN _symbols s ON s.name=e.callee_name "
    "LEFT JOIN _edges_source ds ON ds.chunk_id=s.def_id "
    "WHERE e.callee_name = :symbol"
)

# @callees symbol=X: what X calls, resolved to a def via _symbols. LEFT JOIN so an
# external/unresolved call still returns (callee_id NULL). Set-valued under
# ambiguity (two files defining `run` → both def_ids) — owner-ratified.
_CALLEES_SQL = (
    "WITH resolution AS ("
    " SELECT name, COUNT(*) AS candidate_count FROM _symbols GROUP BY name"
    ") "
    "SELECT DISTINCT e.callee_name AS callee, s.def_id AS callee_id, "
    "e.caller_id, cs.source_id AS caller_file, ds.source_id AS callee_file, "
    "CASE WHEN COALESCE(r.candidate_count, 0)=0 THEN 'unresolved' "
    "     WHEN r.candidate_count=1 THEN 'unique' ELSE 'ambiguous' END "
    "AS resolution_state, COALESCE(r.candidate_count, 0) AS candidate_count "
    "FROM _edges_call e JOIN _types_instant t ON e.caller_id = t.chunk_id "
    "JOIN _edges_source cs ON cs.chunk_id=e.caller_id "
    "LEFT JOIN resolution r ON r.name=e.callee_name "
    "LEFT JOIN _symbols s ON s.name = e.callee_name "
    "LEFT JOIN _edges_source ds ON ds.chunk_id=s.def_id "
    "WHERE t.section_title = :symbol "
    "AND (:def_id='*' OR e.caller_id=:def_id) "
    "AND (:file='*' OR cs.source_id=:file)"
)

# @impact symbol=X: transitive callers. Recursive CTE walks callee_name →
# _symbols.def_id each hop (NOT a stored callee_id): from a symbol name, find its
# def_ids, then any caller whose callee_name resolves to one of those defs, repeat.
_IMPACT_SQL = (
    "WITH RECURSIVE resolution(name, candidate_count) AS ("
    "  SELECT name, COUNT(*) FROM _symbols GROUP BY name"
    "), up(id) AS ("
    "  SELECT e.caller_id FROM _edges_call e WHERE e.callee_name = :symbol "
    "  UNION "
    "  SELECT e.caller_id FROM _edges_call e "
    "  JOIN _symbols s ON s.name = e.callee_name "
    "  JOIN _types_instant t ON t.chunk_id = s.def_id "
    "  JOIN up ON up.id = s.def_id"
    ") "
    "SELECT DISTINCT t.section_title AS affected, up.id AS affected_id, "
    "es.source_id AS affected_file, "
    "CASE WHEN COALESCE(ar.candidate_count,0)=0 THEN 'unresolved' "
    "     WHEN ar.candidate_count=1 THEN 'unique' ELSE 'ambiguous' END "
    "AS resolution_state, COALESCE(ar.candidate_count,0) AS candidate_count, "
    "CASE WHEN COALESCE(rr.candidate_count,0)=0 THEN 'unresolved' "
    "     WHEN rr.candidate_count=1 THEN 'unique' ELSE 'ambiguous' END "
    "AS root_resolution_state, COALESCE(rr.candidate_count,0) AS root_candidate_count "
    "FROM up JOIN _types_instant t ON up.id = t.chunk_id "
    "JOIN _edges_source es ON es.chunk_id=up.id "
    "LEFT JOIN resolution ar ON ar.name=t.section_title "
    "LEFT JOIN resolution rr ON rr.name=:symbol"
)

_SUBTREE_PRESET_SQL = (
    "WITH RECURSIVE sub(id, depth) AS ("
    "SELECT id, depth FROM _edges_tree WHERE parent_id = :root "
    "UNION ALL "
    "SELECT e.id, e.depth FROM _edges_tree e JOIN sub ON e.parent_id = sub.id) "
    "SELECT c.id, es.source_id, t.section_title, t.section_type, t.position, "
    "t.depth, t.container_id, c.content "
    "FROM sub JOIN _raw_chunks c ON c.id=sub.id "
    "JOIN _types_instant t ON t.chunk_id=sub.id "
    "JOIN _edges_source es ON es.chunk_id=sub.id "
    "ORDER BY sub.depth, t.position"
)

_CODE_PRESETS = (
    ("subtree", "Recursive descendants of a container node (over _edges_tree).",
     "root", _SUBTREE_PRESET_SQL),
    ("callers", "Who calls a symbol (call graph). @callers symbol=NAME",
     "symbol", _CALLERS_SQL),
    ("callees", "What a symbol definition calls; ambiguity is explicit. "
     "Qualify with def_id or file. @callees symbol=NAME",
     "symbol, def_id (default: *), file (default: *)", _CALLEES_SQL),
    ("impact", "Multi-hop callers — blast radius of a symbol. @impact symbol=NAME",
     "symbol", _IMPACT_SQL),
)


# DEDICATED code-cell stock presets (interface-authored @orient) — a code cell is a
# distinct use-case from a plain fs cell: its @orient describes the graph tables
# (_symbols/_edges_call/import/tree), names the nav verbs, states vec_ops INERT as a
# FIXED fact (code is always no-embed), and is honest about call-graph coverage. NOT
# the fs orient (which only discovers columns). Colocated with this module's build.
_CODE_STOCK_PRESETS = Path(__file__).resolve().parent / "stock" / "presets"


def install_code_presets(conn: sqlite3.Connection) -> None:
    """Install a code cell's read surface: the dedicated code-cell @orient (so the
    shipped flex-code skill's 'always start with @orient' holds) THEN the late-bind
    call-graph nav (subtree/callers/callees/impact via _symbols). The nav goes LAST so
    its late-bind SQL INSERT-OR-REPLACE-wins over any same-named stock preset.
    Idempotent."""
    try:
        from flex.retrieve.presets import install_presets
        if _CODE_STOCK_PRESETS.is_dir():
            install_presets(conn, _CODE_STOCK_PRESETS)      # @orient (code-cell contract)
    except Exception:
        pass  # stock absent (interface .sql not yet landed) — nav presets below still install
    for name, desc, params, sql in _CODE_PRESETS:
        conn.execute(
            "INSERT OR REPLACE INTO _presets (name, description, params, sql) "
            "VALUES (?, ?, ?, ?)",
            (name, desc, params, sql),
        )


def _ensure_code_surface(conn: sqlite3.Connection) -> bool:
    """Self-heal a code cell's QUERY SURFACE — the `chunks` view + @orient + the 4 nav
    presets. index_file_code writes the DATA (_types_instant/_edges_*/_symbols/
    _raw_chunks) but never built the view or installed @orient, so migrated codegraph
    cells shipped degraded (no `chunks` view — even @subtree, which does `FROM chunks`,
    was broken — and no @orient the flex-code skill assumes). Mirrors docpac's
    _ensure_docpac_views: guarded on presence so the drain takes no write lock once
    complete. Returns True iff it healed."""
    views = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'")}
    presets = {r[0] for r in conn.execute("SELECT name FROM _presets")} \
        if any(r[0] == '_presets' for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")) else set()
    # Guard on the CODE orient specifically, not merely an 'orient' being present: a
    # cell_type=code cell that somehow carries the generic fs orient (which says
    # nothing about the call graph) must still be upgraded. Detect the code orient by
    # its graph markers. (scan_code_cells only calls this on cell_type=code cells,
    # whose _symbols/_edges_call shape the code orient describes — so upgrading is
    # always correct here.)
    orient_sql = next((r[0] for r in conn.execute(
        "SELECT sql FROM _presets WHERE name='orient'")), "") if 'orient' in presets else ""
    orient_is_code = any(m in orient_sql for m in ('graph_surface', '_symbols', '_edges_call'))
    if 'chunks' in views and orient_is_code and \
            {'subtree', 'callers', 'callees', 'impact'} <= presets:
        return False
    from flex.views import regenerate_views
    regenerate_views(conn)                       # views=None on a viewless cell → {chunks, sources}
    install_code_presets(conn)                   # @orient + late-bind nav
    conn.commit()
    return True


# ── The primitive ─────────────────────────────────────────────────────────────

_BASE_CELL_TABLES = ("_raw_sources", "_raw_chunks", "_edges_source")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    # The BASE cell tables (_raw_sources/_raw_chunks/_edges_source) are created by
    # the SDK create() at cell birth — runtime-owned, NOT index_file_code's to
    # invent. Fail fast + clear if they're absent (a bare conn), rather than
    # crashing deep in the content-hash ALTER with an opaque "no such table". This
    # only ADDS the code-specific tables onto an already-created cell.
    present = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    missing = [t for t in _BASE_CELL_TABLES if t not in present]
    if missing:
        raise RuntimeError(
            f"index_file_code requires an already-created cell — base table(s) "
            f"{missing} absent. Create the cell first (flex.sdk.create(..., "
            f"schema=CODE_SCHEMA_DDL)); index_file_code only adds the code tables.")
    conn.executescript(CODE_SCHEMA_DDL)
    conn.executescript(
        "CREATE TABLE IF NOT EXISTS _edges_tree ("
        " id TEXT NOT NULL, parent_id TEXT, branch_at TEXT,"
        " relation TEXT NOT NULL, depth INTEGER DEFAULT 0,"
        " PRIMARY KEY (id, parent_id));"
        "CREATE INDEX IF NOT EXISTS idx_tree_parent ON _edges_tree(parent_id);"
    )


def _stored_content_hash(conn: sqlite3.Connection, source_id: str) -> str | None:
    try:
        row = conn.execute(
            "SELECT content_hash FROM _raw_sources WHERE source_id = ?",
            (source_id,)).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE _raw_sources ADD COLUMN content_hash TEXT")
        return None


def _delete_file_rows(conn: sqlite3.Connection, source_id: str, file_id: str,
                      drop_identity: bool = False) -> None:
    """Delete this one file's rows across every code table (per-file upsert). No
    cross-file work: chunk-keyed tables prune the file's chunk_ids; import/identity
    prune by source_id; _symbols prunes by file_id."""
    old_chunk_ids = [r[0] for r in conn.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id = ?", (source_id,)
    ).fetchall()]
    if old_chunk_ids:
        ph = ",".join("?" * len(old_chunk_ids))
        conn.execute(f"DELETE FROM _raw_chunks WHERE id IN ({ph})", old_chunk_ids)
        conn.execute(f"DELETE FROM _types_instant WHERE chunk_id IN ({ph})", old_chunk_ids)
        conn.execute(f"DELETE FROM _edges_tree WHERE id IN ({ph})", old_chunk_ids)
        conn.execute(f"DELETE FROM _edges_call WHERE caller_id IN ({ph})", old_chunk_ids)
        conn.execute("DELETE FROM _edges_source WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM _edges_import WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM _symbols WHERE file_id = ?", (file_id,))
    # Identity edge only on a TRUE delete (drop_identity=True, the reconcile path).
    # NOT on the re-index path: _mint_file_identity re-stamps with INSERT OR REPLACE
    # (idempotent), so deleting-then-re-minting every re-index would open a transient
    # window where a best-effort mint failure (soma unreachable) leaves identity gone
    # until the next index. Reconcile-only avoids that (mirrors docpac's drop_source).
    if drop_identity:
        conn.execute("DELETE FROM _edges_fs_identity WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM _code_source_state WHERE source_id = ?", (source_id,))


def _mint_file_identity(conn: sqlite3.Connection, source_id: str, file_path: str) -> bool:
    """Mint (or resolve) this file's SOMA uuid → _edges_fs_identity. Never raises —
    identity must never fail a compile (instant/docpac discipline). Idempotent."""
    try:
        from flex.modules.soma.lib.identity.file_identity import get_instance
        rp = str(Path(file_path).resolve())
        uuid = get_instance().assign_batch([rp]).get(rp)
        if uuid:
            conn.execute(
                "INSERT OR REPLACE INTO _edges_fs_identity (source_id, file_uuid) "
                "VALUES (?, ?)", (source_id, uuid))
            return True
    except Exception:
        pass
    return False


def _extract_imports(abs_path: str, ext: str) -> list[tuple]:
    """(source_id, module, name) rows for this one file (py ast / ts tree-sitter).
    Mirrors instant/install.py's import extraction, scoped to one path."""
    rows: list[tuple] = []
    if ext == "py":
        import ast as _ast
        try:
            tree = _ast.parse(Path(abs_path).read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            return rows
        for nd in _ast.walk(tree):
            if isinstance(nd, _ast.Import):
                for a in nd.names:
                    rows.append((abs_path, a.name, None))
            elif isinstance(nd, _ast.ImportFrom):
                mod = ("." * (nd.level or 0)) + (nd.module or "")
                for a in nd.names:
                    rows.append((abs_path, mod, a.name))
        return rows

    from flex.compile.chunkers import _TS_EXTS, _ts_language
    if ext not in _TS_EXTS:
        return rows
    lang = _ts_language(ext)
    if lang is None:
        return rows
    try:
        from tree_sitter import Parser
        src = Path(abs_path).read_text(encoding="utf-8", errors="ignore").encode("utf-8")
        root = Parser(lang).parse(src).root_node
    except Exception:
        return rows

    def _imp(m):
        for c in m.children:
            if c.type == "import_statement":
                srcn = c.child_by_field_name("source")
                if srcn is not None:
                    mod = srcn.text.decode("utf-8", "ignore").strip("\"'`")
                    if mod:
                        rows.append((abs_path, mod, None))
            _imp(c)
    _imp(root)
    return rows


def index_file_code(conn: sqlite3.Connection, file_path: str, *,
                    file_id: str, corpus_root: str | Path | None = None) -> bool:
    """Index a single code file's node tree + call/import graph into a code cell.

    Per-file upsert, scoped entirely to this one file. No embeddings. Returns True
    iff the cell was written (False on a content-hash skip or a non-code/empty file).

    Args:
        conn: open code-cell connection (CODE_SCHEMA_DDL applied — ensured here too).
        file_path: the code file to index.
        file_id: SOMA file_uuid / stable per-file id — the `_symbols` key + the
                 reconcile delete key.
        corpus_root: accepted for drain-signature parity with docpac's index_file;
                     unused (code resolution is per-file, no corpus config).
    """
    from flex.compile.chunkers import _TS_EXTS, _build_code_tree, _build_code_tree_ts

    p = Path(file_path)
    if not p.exists():
        return False
    abs_path = str(p.resolve())
    ext = abs_path.rsplit(".", 1)[-1].lower() if "." in abs_path else ""
    if ext != "py" and ext not in _TS_EXTS:
        return False  # code cells only index py + JS/TS-family files

    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
        stat = p.stat()
    except Exception:
        return False
    if not text.strip():
        return False

    _ensure_schema(conn)

    # 1) content-hash skip (mirror docpac worker's per-file pattern).
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if _stored_content_hash(conn, abs_path) == content_hash:
        conn.execute(
            "INSERT OR REPLACE INTO _code_source_state "
            "(source_id,content_hash,size_bytes,mtime_ns) VALUES (?,?,?,?)",
            (abs_path, content_hash, stat.st_size, stat.st_mtime_ns),
        )
        conn.commit()
        return False

    # 2) node tree for this file.
    nodes = _build_code_tree(abs_path, text) if ext == "py" \
        else _build_code_tree_ts(abs_path, text, ext)
    if not nodes:
        return False

    # 3) delete this file's existing rows across every code table, then re-insert.
    _delete_file_rows(conn, abs_path, file_id)

    # _raw_sources upsert (carries the content-hash skip key; source_id = abs_path).
    conn.execute(
        "INSERT INTO _raw_sources (source_id, title, content_hash) VALUES (?, ?, ?) "
        "ON CONFLICT(source_id) DO UPDATE SET title=excluded.title, "
        "content_hash=excluded.content_hash",
        (abs_path, p.name, content_hash))

    # 8) SOMA identity mint (same txn; best-effort).
    _mint_file_identity(conn, abs_path, file_path)

    # 4) _raw_chunks + _edges_source + _types_instant + _edges_tree.
    for n in nodes:
        cid = n["id"]
        node_hash = hashlib.sha256((n.get("content") or "").encode("utf-8")).hexdigest()
        conn.execute(
            "INSERT OR IGNORE INTO _raw_chunks (id, content, timestamp) VALUES (?, ?, ?)",
            (cid, n.get("content", ""), None))
        conn.execute(
            "INSERT OR IGNORE INTO _edges_source (chunk_id, source_id) VALUES (?, ?)",
            (cid, abs_path))
        conn.execute(
            "INSERT OR IGNORE INTO _types_instant "
            "(chunk_id, section_title, position, depth, container_id, content_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cid, n.get("section_title"), n.get("position"), n.get("depth"),
             n.get("container_id"), node_hash))
        conn.execute(
            "INSERT OR IGNORE INTO _edges_tree (id, parent_id, branch_at, relation, depth) "
            "VALUES (?, ?, ?, ?, ?)",
            (cid, n.get("container_id"), None, "subsection", n.get("depth", 0)))

    # 5) _edges_call — LATE-BIND: store only (caller_id, callee_name), NO callee_id.
    call_rows = [(n["id"], nm) for n in nodes for nm in n.get("_calls", ())]
    if call_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO _edges_call (caller_id, callee_name) VALUES (?, ?)",
            call_rows)

    # 6) _symbols — one row per def node in this file (skip the "(module)" preamble).
    sym_rows = [
        (st, n["id"], file_id, None)
        for n in nodes
        if (st := n.get("section_title")) and not st.startswith("(")
    ]
    if sym_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO _symbols (name, def_id, file_id, kind) VALUES (?, ?, ?, ?)",
            sym_rows)

    # 7) _edges_import — file → imported module/symbol.
    import_rows = _extract_imports(abs_path, ext)
    if import_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO _edges_import (source_id, module, name) VALUES (?, ?, ?)",
            import_rows)

    conn.execute(
        "INSERT OR REPLACE INTO _code_source_state "
        "(source_id,content_hash,size_bytes,mtime_ns) VALUES (?,?,?,?)",
        (abs_path, content_hash, stat.st_size, stat.st_mtime_ns),
    )

    conn.commit()
    return True


# ── Code-cell drain (Seam-0c runtime wiring) ──────────────────────────────────
# The incremental counterpart to docpac's scan_docpac_cells, for cell_type='code'
# cells: walk code exts, per changed file → index_file_code, reconcile-delete via
# _symbols.file_id. Routing a code cell here (refresh_module=NULL so the
# coding-agent watch skips it) RETIRES instant/refresh.py's 15-min full-regen —
# the B-child burn dies. No embeddings (code).

_CODE_WALK_EXTS = {'.py', '.ts', '.tsx', '.js', '.jsx', '.go', '.rs', '.java',
                   '.rb', '.c', '.cc', '.cpp', '.h', '.hpp', '.cs', '.php',
                   '.swift', '.kt', '.scala'}
# Build output + deps, not source. `out`/`_next`/`.output`/`.turbo`/`.svelte-kit`
# are framework build dumps (Next.js/SvelteKit/etc.) that carry MINIFIED webpack
# chunks — indexing them enshrines thousands of minified-token pseudo-defs as the
# code graph (caught at scale: a Next.js `out/_next` dump was 82% of one cell's
# defs). `target` = Rust/Java build. Pruned at traversal like the rest.
_CODE_PRUNE = {'.git', 'node_modules', 'venv', '.venv', '__pycache__', 'dist',
               'build', '.next', '.mypy_cache', '.pytest_cache', 'out', '_next',
               '.output', '.turbo', '.svelte-kit', 'coverage', 'target', '.nuxt'}


def _walk_code(root: Path, exclude_dirs=None):
    """Yield code-ext files under root, pruning heavy dirs + per-cell excludes
    at traversal (the same discipline as docpac's _walk_md). Minified files
    (`*.min.js`/`*.min.css`) are vendored/built artifacts, never source — skipped."""
    prune = _CODE_PRUNE | set(exclude_dirs or ())
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in prune]
        for fn in filenames:
            if fn.endswith(('.min.js', '.min.css')):
                continue
            if os.path.splitext(fn)[1] in _CODE_WALK_EXTS:
                yield Path(dirpath) / fn


def _reconcile_code_deletes(conn, on_disk_source_ids: set) -> int:
    """Drop a code file's rows (all code tables) when it vanished. Stat-confirms
    the stored source_id (an abs path) before pruning — over-prune guard, same as
    docpac's reconcile. Empty on-disk set ⇒ no-op (transient walk failure)."""
    if not on_disk_source_ids:
        return 0
    try:
        rows = conn.execute(
            "SELECT s.source_id, i.file_uuid FROM _raw_sources s "
            "LEFT JOIN _edges_fs_identity i ON i.source_id = s.source_id").fetchall()
    except sqlite3.OperationalError:
        return 0
    deleted = 0
    for source_id, file_uuid in rows:
        if source_id in on_disk_source_ids:
            continue
        if os.path.exists(os.path.normpath(source_id)):
            continue                       # live file, keep
        _delete_file_rows(conn, source_id, file_uuid or source_id, drop_identity=True)
        conn.execute("DELETE FROM _raw_sources WHERE source_id = ?", (source_id,))
        deleted += 1
    return deleted


def _cell_selections(conn) -> list:
    """The code cell's indexed source dirs from _meta.selections (instant carries
    this — a cell can index multiple repos/dirs). Returns [] if absent/malformed so
    the caller falls back to [corpus_path]. Never raises."""
    try:
        row = conn.execute("SELECT value FROM _meta WHERE key='selections'").fetchone()
        if row and row[0]:
            import json
            v = json.loads(row[0])
            if isinstance(v, list):
                return [str(x) for x in v if x]
    except (sqlite3.OperationalError, ValueError, TypeError):
        pass
    return []


def scan_code_cells(size_cache: dict) -> dict:
    """Incremental drain for cell_type='code' cells. Mirrors scan_docpac_cells:
    stat-poll walk (pruned + exclude_dirs), content-hash skip inside
    index_file_code, per-file index_file_code, reconcile-delete, last_refresh
    stamp. No embeddings. This is what replaces the full-regen for code cells."""
    from flex.registry import list_cells, update_refresh_status
    from flex.modules.docpac.compile.init import _is_identity_excluded
    try:
        # get_instance() (the singleton) — same mint entry point as docpac's
        # _mint_file_identity, so a test can airtight-redirect the SOMA DB once.
        from flex.modules.soma.lib.identity.file_identity.identity import get_instance
    except Exception:
        get_instance = None

    stats = {'indexed': 0, 'skipped': 0, 'deleted': 0}
    cells = [c for c in list_cells()
             if c.get('cell_type') == 'code' and c.get('corpus_path')
             and c.get('lifecycle') == 'watch'
             and c.get('active', 1)]
    if not cells:
        return stats
    fid = get_instance() if get_instance else None

    for cell in cells:
        cell_name = cell['name']
        conn = sqlite3.connect(cell['path'], timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not _BASE_CELL_TABLES[0] in tables:   # not a created cell yet — skip
            conn.close()
            continue
        _ensure_schema(conn)  # includes durable per-source state on upgraded cells
        # Self-heal the query surface (chunks view + @orient + nav presets) so
        # already-migrated code cells that shipped without it get fixed LIVE on the
        # next drain, not just fresh builds. Guarded → no-op once complete.
        _ensure_code_surface(conn)
        # per-cell exclusions (reuse docpac's _meta.exclude_dirs reader)
        try:
            from flex.modules.docpac.compile.worker import _cell_exclude_dirs
            exclude = _cell_exclude_dirs(conn)
        except Exception:
            exclude = set()

        # A code cell may index MULTIPLE source dirs (instant's _meta.selections),
        # not just one corpus_path — walk ALL of them, fallback to [corpus_path].
        # The reconcile-delete spans the UNION of every selection's on-disk set, so a
        # file deleted from any selection is pruned. Same shape as the docpac
        # multi-source drain (union over all indexed roots). corpus_root is
        # per-selection (unused by index_file_code, but kept honest).
        selections = _cell_selections(conn) or [cell.get('corpus_path')]
        sel_dirs = [Path(s) for s in selections if s and Path(s).is_dir()]
        if not sel_dirs:
            conn.close()
            continue

        cell_indexed = 0
        on_disk_sids = set()
        candidates = []
        for corpus in sel_dirs:
            for f in _walk_code(corpus, exclude):
                abs_path = str(f.resolve())
                on_disk_sids.add(abs_path)          # source_id = abs path (index_file_code's key)
                key = str(f)
                try:
                    stat = f.stat()
                    signature = f"{stat.st_size}:{stat.st_mtime_ns}"
                except OSError:
                    continue
                cache_key = f"code:{cell_name}:{key}"
                if signature == size_cache.get(cache_key):
                    continue
                if cache_key not in size_cache:
                    stored = conn.execute(
                        "SELECT size_bytes,mtime_ns FROM _code_source_state "
                        "WHERE source_id=?", (abs_path,),
                    ).fetchone()
                    if stored and stored == (stat.st_size, stat.st_mtime_ns):
                        size_cache[cache_key] = signature
                        continue
                candidates.append((abs_path, (f, corpus, signature, cache_key)))

        from flex.watch import fair_batch
        batch_limit = max(1, int(os.environ.get("FLEX_DRAIN_FILES_PER_CELL", "200")))
        batch = fair_batch(conn, "code", candidates, batch_limit)
        if batch:
            conn.commit()  # cursor is a crash-safe fairness fact, not process memory
        for _, (f, corpus, signature, cache_key) in batch:
                abs_path = str(f.resolve())
                try:
                    # Ephemeral corpora (/tmp,/var/tmp,/dev — soma exclude_paths) never mint,
                    # so fixture/test code cells don't pollute the shared ~/.soma authority.
                    # Symmetric with index_file's _mint_file_identity + batch init's
                    # _mint_batch_identity (single-sourced exclusion).
                    if fid and not _is_identity_excluded(abs_path):
                        file_id = fid.assign(abs_path)
                    else:
                        file_id = abs_path
                    if index_file_code(conn, str(f), file_id=file_id,
                                       corpus_root=str(corpus)):
                        stats['indexed'] += 1
                        cell_indexed += 1
                    else:
                        stats['skipped'] += 1
                except Exception as e:
                    print(f"[code] error on {f.name}: {e}", file=sys.stderr)
                    stats['skipped'] += 1
                size_cache[cache_key] = signature

        deleted = _reconcile_code_deletes(conn, on_disk_sids)
        stats['deleted'] += deleted
        if cell_indexed or deleted:
            conn.commit()
            update_refresh_status(cell_name, 'ok')
        conn.close()
    return stats
