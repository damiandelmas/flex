"""Instant install hook — v5: recursive-node projection (tree metadata + content-hash).

Compiles a UNION of folder selections into a single no-embed projection cell
(default name: fs). Each --path adds a selection to the cell's recipe; every run
wipes and recompiles the union. FTS5 + structural SQL, unlisted, regenerable from
its own stored recipe. SOMA file UUIDs stamped per source.

No embeddings, no graph, ever: run = compile stays free because nothing durable is
paid into the cell. Moves and renames are handled by recompiling. Regenerate, never
heal.

v5 adds, purely additively (parity with v4 holds — leaves are byte-identical):
  - `_types_instant` (chunk_id PK → auto-joins into the `chunks` view): persists the
    section_title / position / depth / container_id that `chunk_file_body` already
    computes and the SDK ingest otherwise drops, plus a per-leaf content_hash (the
    future embed key; mirrors claude_code's _edges_content_identity).
  - a stock `subtree(:root)` recursive-CTE preset over `_edges_tree` — the query
    artifact for container subtrees (dormant at depth-1; the seat for multi-level).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

# Instant's own stock presets (its @orient — the lean instant/fs contract, not the
# docpac/semantic-framed general orient whose vec_ops('…') doc-literal trips the
# instant materializer). Installed AFTER general so it INSERT-OR-REPLACE-wins.
_INSTANT_STOCK_PRESETS = Path(__file__).resolve().parent / "stock" / "presets"

# General presets that can't run on a no-embed / no-graph instant cell — dropped
# after create() installs the general set (interface-confirmed hygiene list).
_INSTANT_DROP_PRESETS = ("bridges", "genealogy")


CLI_NAME = "instant"
MODULE_SUMMARY = "compile folder selections into one no-embed projection cell — FTS5 + SQL, tree metadata, unlisted, regenerable"

MODULE = {
    "cell_type": "instant",
    "maturity": "stable",
    "auth": "none",
    "selection": "set of folder paths (--path / --remove)",
    "resolver": "node_tree@v1",
    "description": "Instant no-embed projection of selected folders. keyword()/SQL only, with section_title/position/depth/container_id/content_hash. Unlisted. Regenerate, never heal.",
    "query_examples": ("@orient", "SELECT section_title, position FROM chunks LIMIT 5"),
}

_FS_IDENTITY_DDL = """
CREATE TABLE IF NOT EXISTS _edges_fs_identity (
    source_id TEXT PRIMARY KEY,
    file_uuid TEXT
);
"""

# v5: leaf metadata table — the dominant flex convention (_types_*). chunk_id PK
# means the view generator auto-joins these columns into the `chunks` view.
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

# Call graph (Python, nest/config): caller def -> callee def (callee_id NULL = external/unresolved).
_CALL_DDL = """
CREATE TABLE IF NOT EXISTS _edges_call (
    caller_id   TEXT NOT NULL,
    callee_id   TEXT,
    callee_name TEXT NOT NULL,
    PRIMARY KEY (caller_id, callee_name)
);
CREATE INDEX IF NOT EXISTS _edges_call_callee ON _edges_call(callee_id);
CREATE INDEX IF NOT EXISTS _edges_call_name   ON _edges_call(callee_name);

CREATE TABLE IF NOT EXISTS _edges_import (
    source_id TEXT NOT NULL,   -- the importing file
    module    TEXT NOT NULL,   -- the imported module ('os', 'flex.core', '.state')
    name      TEXT,            -- the imported symbol (NULL for plain `import module`)
    PRIMARY KEY (source_id, module, name)
);
CREATE INDEX IF NOT EXISTS _edges_import_module ON _edges_import(module);
"""

_SOURCE_STATE_DDL = """
CREATE TABLE IF NOT EXISTS _instant_source_state (
    source_id    TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    mtime_ns     INTEGER NOT NULL
);
"""

_INSTANT_DDL = _FS_IDENTITY_DDL + _TYPES_DDL + _CALL_DDL + _SOURCE_STATE_DDL

# Recursive query artifact (mirrors claude_code/stock/presets/delegation-tree.sql).
# Dormant at depth-1 (no _edges_tree rows); the seat for multi-level containers.
_SUBTREE_PRESET_SQL = (
    "WITH RECURSIVE sub(id, depth) AS ("
    "SELECT id, depth FROM _edges_tree WHERE parent_id = :root "
    "UNION ALL "
    "SELECT e.id, e.depth FROM _edges_tree e JOIN sub ON e.parent_id = sub.id) "
    "SELECT c.* FROM chunks c JOIN sub ON c.id = sub.id ORDER BY sub.depth, c.position"
)

# Call-graph presets — CodeGraph's callers/callees/impact as SQL (daemon-free).
_CALLERS_SQL = (
    "SELECT DISTINCT t.section_title AS caller, e.caller_id "
    "FROM _edges_call e JOIN _types_instant t ON e.caller_id = t.chunk_id "
    "WHERE e.callee_name = :symbol"
)
_CALLEES_SQL = (
    "SELECT DISTINCT e.callee_name AS callee, e.callee_id "
    "FROM _edges_call e JOIN _types_instant t ON e.caller_id = t.chunk_id "
    "WHERE t.section_title = :symbol"
)
_IMPACT_SQL = (
    "WITH RECURSIVE up(id) AS ("
    "SELECT caller_id FROM _edges_call WHERE callee_name = :symbol "
    "UNION "
    "SELECT e.caller_id FROM _edges_call e JOIN up ON e.callee_id = up.id) "
    "SELECT DISTINCT t.section_title AS affected FROM up JOIN _types_instant t ON up.id = t.chunk_id"
)


def _add_arg(parser, *flags, **kwargs) -> None:
    existing = {opt for action in parser._actions for opt in action.option_strings}
    if not any(flag in existing for flag in flags):
        parser.add_argument(*flags, **kwargs)


def register_args(parser) -> None:
    _add_arg(parser, "--path", default=None,
             help="Add a folder to the cell's selections and recompile")
    _add_arg(parser, "--regen", nargs="?", const="fs", default=None, metavar="NAME",
             help="Recompile a cell from its stored selections (default cell: fs)")
    _add_arg(parser, "--remove", default=None, metavar="PATH",
             help="Remove a selection and recompile")
    _add_arg(parser, "--name", default=None,
             help="Target cell (default: fs)")
    _add_arg(parser, "--description", default=None,
             help="Cell description")
    _add_arg(parser, "--no-soma", action="store_true",
             help="Skip SOMA file UUID stamping")
    _add_arg(parser, "--nest", action="store_true",
             help="Multi-level: build the heading tree (container > container > leaf) + _edges_tree")
    _add_arg(parser, "--config", action="store_true",
             help="Cascade: each file split by the nearest .flexchunk.json 'split' rule (per-folder rules)")
    _add_arg(parser, "--code", action="store_true",
             help="Code cell: index only code extensions (.py/.ts/.js/.go/.rs/...), skip docs/data/config")
    _add_arg(parser, "--no-watch", action="store_true",
             help="Register the cell static (disposable): skip the worker's size-signature watch + regen-on-change")


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", text).strip("-").lower()
    return slug or "fs"


def _error(console, message: str, details: list[str] | None = None) -> None:
    """Print the failure and EXIT NON-ZERO.

    Exiting is the contract, not a nicety: the worker's watch runs regen as a
    subprocess (`refresh.py`) and keys off its returncode. When this printed and
    returned 0, a failed regen was indistinguishable from a successful one — the
    watch then stamped the signature it had computed over an empty recipe, and the
    cell was permanently marked fresh. Fail closed here so that can't recur.
    """
    console.print(f"[red]instant:[/red] {message}")
    for d in details or []:
        console.print(f"    {d}")
    raise SystemExit(1)


def _load_selections(name: str) -> list[str]:
    """Read the stored recipe. Legacy v3 cells migrate via their source_path key."""
    from flex.core import get_meta, open_cell_readonly
    from flex.registry import resolve_cell

    cell_path = resolve_cell(name)
    if cell_path is None:
        return []
    ro = open_cell_readonly(str(cell_path))
    try:
        raw = get_meta(ro, "selections")
        if raw:
            return json.loads(raw)
        legacy = get_meta(ro, "source_path")
        return [legacy] if legacy else []
    finally:
        ro.close()


def _load_chunking_mode(name: str) -> str:
    """Read the stored split mode from the recipe ('nest' | 'config' | 'flat').
    Lets a bare --regen preserve the mode — the cell carries its own derivation."""
    from flex.core import get_meta, open_cell_readonly
    from flex.registry import resolve_cell

    cell_path = resolve_cell(name)
    if cell_path is None:
        return "flat"
    ro = open_cell_readonly(str(cell_path))
    try:
        raw = get_meta(ro, "chunking")
        if raw:
            try:
                return json.loads(raw).get("split_mode", "flat")
            except (ValueError, AttributeError):
                return "flat"
        return "flat"
    finally:
        ro.close()


def _load_code_flag(name: str) -> bool:
    """Read the stored code-extension flag from the recipe — so a bare --regen of a
    code cell stays code-only (the cell carries its own derivation)."""
    from flex.core import get_meta, open_cell_readonly
    from flex.registry import resolve_cell

    cell_path = resolve_cell(name)
    if cell_path is None:
        return False
    ro = open_cell_readonly(str(cell_path))
    try:
        raw = get_meta(ro, "chunking")
        if raw:
            try:
                return bool(json.loads(raw).get("code", False))
            except (ValueError, AttributeError):
                return False
        return False
    finally:
        ro.close()


# Noise dirs pruned during the walk — the .gitignore'd bulk (venv/build/caches/deps).
_WALK_EXCLUDE = {
    "__pycache__", "venv", ".venv", "env", "build", "dist", "node_modules",
    "site-packages", ".git", ".mypy_cache", ".pytest_cache", ".tox", ".ruff_cache",
    ".idea", ".vscode", "target", ".next", "out", ".wrangler", ".turbo", ".cache",
    "coverage", "htmlcov", "_corpus",
}
# Dot-dirs that are known CONTENT, not junk — kept past the dot-prune.
_WALK_KEEP = {".context", ".work", ".teams"}


def _git_listed(folder):
    """Files git would NOT ignore under `folder` — tracked + untracked-but-not-ignored
    (`git ls-files --cached --others --exclude-standard`). Returns absolute Paths, or
    None if `folder` is not in a git repo. This is the right filter for a code cell:
    it honors .gitignore, so build artifacts, data dumps, vendored deps, and logs are
    excluded by the repo's own declaration — no hand-rolled denylist needed."""
    import subprocess
    from pathlib import Path  # noqa: F811
    folder = Path(folder).resolve()
    try:
        top = subprocess.run(
            ["git", "-C", str(folder), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=15)
        if top.returncode != 0:
            return None
        toplevel = Path(top.stdout.strip())
        r = subprocess.run(
            ["git", "-C", str(folder), "ls-files", "--cached", "--others",
             "--exclude-standard", "--full-name", "-z"],
            capture_output=True, timeout=120)
        if r.returncode != 0:
            return None
        return [toplevel / x for x in r.stdout.decode("utf-8", "ignore").split("\0") if x]
    except Exception:
        return None


_CODE_EXTS = (".py", ".ts", ".tsx", ".js", ".jsx", ".go", ".rs", ".java", ".rb",
              ".c", ".cc", ".cpp", ".h", ".hpp", ".cs", ".php", ".swift", ".kt", ".scala")
_DOC_EXTS = (".md", ".txt", ".py", ".js", ".ts", ".jsx", ".tsx",
             ".rs", ".go", ".yaml", ".yml", ".json", ".csv")


def _iter_indexed_paths(folder, code=False, include_config=False):
    """Yield the Paths instant would index under `folder` — the SINGLE SOURCE OF TRUTH
    for the indexed set, shared by the compiler (`_walk_files`) and the watch signature
    (`refresh._signature`) so the two can never drift. PRIMARY: git — only non-ignored
    files (respects .gitignore). FALLBACK (non-git dirs): os.walk pruning the noise-dir
    denylist + the known content dot-dir whitelist (.context/.work/.teams). No file
    reads, so it is cheap enough for the watch to poll. code=True → only code extensions.
    include_config=True also yields .flexchunk.json files (NOT indexed as content — this
    is so the watch signature notices config edits and re-compiles)."""
    import os
    from pathlib import Path  # noqa: F811
    exts = _CODE_EXTS if code else _DOC_EXTS
    listed = _git_listed(folder)
    if listed is not None:
        candidates = listed
    else:
        candidates = []
        for dirpath, dirnames, filenames in os.walk(folder):
            dirnames[:] = sorted(
                d for d in dirnames
                if d not in _WALK_EXCLUDE and not d.endswith(".egg-info")
                and (not d.startswith(".") or d in _WALK_KEEP)
            )
            for fn in sorted(filenames):
                candidates.append(Path(dirpath) / fn)
    for p in sorted(set(candidates)):
        if p.name.startswith("."):
            if include_config and p.name == _FLEXCHUNK:
                yield p  # config, watched-for-edits only — never a content chunk
            continue
        if p.suffix not in exts or not p.is_file():
            continue
        yield p


def _walk_files(folder, code=False):
    """Yield (Path, text) for indexable files — `_iter_indexed_paths` + read, skipping
    unreadable/empty files. The compiler's input."""
    for p in _iter_indexed_paths(folder, code=code):
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if not text.strip():
            continue
        yield p, text


# _build_code_tree, _build_code_tree_ts, _TS_EXTS, _ts_language now live in
# flex.compile.chunkers (the shared home also used by the markdown chunk_resolver
# kernel for `nest`/`heading` on code) — imported where needed below.


def _build_tree(abs_path: str, text: str, level: int = 1) -> list[dict]:
    """Build the node tree for one file.

    Markdown → full-depth heading tree (container > container > leaf) via a depth
    stack over split_sections(return_depth=True). Every heading-section is a content
    node (its body) AND a container (parent of deeper sections) — "container and
    chunk are the same node at different levels." container_id = the parent node's id
    (or the file path for top-level). Python → ast tree; JS/TS → tree-sitter tree;
    other non-markdown → flat chunk_file_body under root.
    """
    from flex.compile.chunkers import (
        _TS_EXTS, _build_code_tree, _build_code_tree_ts, chunk_file_body,
    )
    from flex.compile.markdown import normalize_headers, split_sections
    from flex.sdk import _make_chunk_id

    ext = abs_path.rsplit(".", 1)[-1].lower() if "." in abs_path else ""
    if ext == "py":
        return _build_code_tree(abs_path, text)
    if ext in _TS_EXTS:
        return _build_code_tree_ts(abs_path, text, ext)
    nodes: list[dict] = []

    if ext == "md":
        sections = split_sections(normalize_headers(text), level=level, return_depth=True)
        stack: list[tuple[int, str]] = []  # (depth, node_id) for parent resolution
        for title, body, pos, depth in sections:
            cid = _make_chunk_id(abs_path, pos, body)
            while stack and stack[-1][0] >= depth:
                stack.pop()
            parent = stack[-1][1] if stack else abs_path
            nodes.append({"id": cid, "content": body, "section_title": title,
                          "position": pos, "depth": depth, "container_id": parent})
            stack.append((depth, cid))
        if not sections:  # no headings → whole file, one leaf under root
            nodes.append({"id": _make_chunk_id(abs_path, 0, text), "content": text,
                          "section_title": "", "position": 0, "depth": 1,
                          "container_id": abs_path})
    else:
        for i, part in enumerate(chunk_file_body(text, abs_path)):
            body = part.get("content") or part.get("body") or text
            nodes.append({"id": _make_chunk_id(abs_path, i, body), "content": body,
                          "section_title": part.get("title", "") or "",
                          "position": part.get("position", i), "depth": 1,
                          "container_id": abs_path})
    return nodes


_FLEXCHUNK = ".flexchunk.json"


def _load_flexchunk(d, cache: dict):
    """Parsed .flexchunk.json in directory `d` (or None), memoized per-dir."""
    if d in cache:
        return cache[d]
    cfg = None
    cj = d / _FLEXCHUNK
    if cj.is_file():
        try:
            parsed = json.loads(cj.read_text())
            cfg = parsed if isinstance(parsed, dict) else None
        except Exception:
            cfg = None
    cache[d] = cfg
    return cfg


def _compile_types(types_map: dict) -> list:
    """Compile a {canonical: [alias | '/regex/flags']} map into an ordered matcher
    list [(canonical, [(kind, matcher)])]. '/pat/flags' → regex (i flag honored);
    any other alias → case-insensitive exact match. Declaration order = precedence."""
    compiled = []
    for canon, aliases in (types_map or {}).items():
        matchers = []
        for a in (aliases or []):
            a = str(a)
            if len(a) >= 2 and a.startswith("/") and a.rfind("/") > 0:
                body, flags_s = a[1:a.rfind("/")], a[a.rfind("/") + 1:]
                try:
                    matchers.append(("re", re.compile(body, re.IGNORECASE if "i" in flags_s else 0)))
                    continue
                except re.error:
                    pass
            matchers.append(("ci", a.strip().lower()))
        compiled.append((canon, matchers))
    return compiled


def _classify(title: str, compiled: list):
    """First canonical type whose matcher hits `title` (None if no match). The
    retype step: a write-time heading → a canonical schema type, regardless of name."""
    if not title or not compiled:
        return None
    norm = title.strip().lower()
    for canon, matchers in compiled:
        for kind, m in matchers:
            if kind == "ci" and norm == m:
                return canon
            if kind == "re" and m.search(title):
                return canon
    return None


def _resolve_profile(file_path: str, cache: dict, default_split: str = "flat") -> dict:
    """The general ingestion config resolver. Cascade over every .flexchunk.json from
    the filesystem root down to the file's directory. Per-key semantics:
      split / level / rollup — nearest-ancestor (deepest) wins (replace)
      types                  — merge root→leaf (a child overrides a canonical key,
                               keeps the rest), so the vocabulary accumulates down.
    Returns {split, level, rollup:set, types:compiled}."""
    from pathlib import Path  # noqa: F811
    chain = []
    d = Path(file_path).parent
    while True:
        chain.append(d)
        if d.parent == d:
            break
        d = d.parent
    chain.reverse()  # root → leaf

    split, level, rollup = None, None, None
    types: dict = {}
    for dd in chain:
        cfg = _load_flexchunk(dd, cache)
        if not cfg:
            continue
        if "split" in cfg:
            split = cfg["split"]
        if "level" in cfg:
            level = cfg["level"]
        if "rollup" in cfg:
            rollup = cfg["rollup"]
        t = cfg.get("types")
        if isinstance(t, dict):
            types.update(t)  # child overrides this canonical key; rest accumulate
    return {
        "split": split or default_split,
        "level": int(level) if isinstance(level, int) else 1,
        "rollup": {str(x) for x in (rollup or [])},
        "types": _compile_types(types),
    }


def _apply_rollup(nodes: list, rollup: set) -> list:
    """Collapse every node whose section_title is in `rollup`: it absorbs its full
    subtree's content (ordered by position) and the descendants are dropped. This is
    'pull the children in' — a Failures section becomes one node carrying its body.
    Shallowest-first, so an outer rollup also absorbs inner rollups."""
    if not rollup or not nodes:
        return nodes
    children: dict = {}
    for n in nodes:
        children.setdefault(n.get("container_id"), []).append(n)
    drop = set()
    for n in sorted(nodes, key=lambda x: x.get("depth", 1)):
        if n["id"] in drop or (n.get("section_title") or "") not in rollup:
            continue
        stack = list(children.get(n["id"], []))
        sub = []
        while stack:
            c = stack.pop()
            if c["id"] in drop:
                continue
            sub.append(c)
            stack.extend(children.get(c["id"], []))
        if sub:
            sub.sort(key=lambda x: x.get("position", 0))
            n["content"] = "\n\n".join(
                [n.get("content") or ""] + [c.get("content") or "" for c in sub]
            ).strip()
            drop.update(c["id"] for c in sub)
    return [n for n in nodes if n["id"] not in drop]


def _build_profiled(abs_path: str, text: str, profile: dict) -> list[dict]:
    """Build nodes under a resolved profile, then retype them. split drives shape
    (nest|flat|whole), level/rollup refine the nest tree, types stamps section_type
    onto every node (the schema projection — query by type, not by write-time name)."""
    split = profile.get("split", "flat")
    if split == "whole":
        nodes = _build_nodes(abs_path, text, "whole")
    elif split == "nest":
        nodes = _build_tree(abs_path, text, level=profile.get("level", 1))
        nodes = _apply_rollup(nodes, profile.get("rollup") or set())
    else:
        nodes = _build_nodes(abs_path, text, "flat")
    compiled = profile.get("types") or []
    for n in nodes:
        n["section_type"] = _classify(n.get("section_title"), compiled)
    return nodes


def _build_nodes(abs_path: str, text: str, mode: str) -> list[dict]:
    """Build nodes under a declared split mode:
    'nest' (heading tree) | 'flat' (chunk_file_body, depth-1) | 'whole' (one leaf)."""
    from flex.sdk import _make_chunk_id
    if mode == "whole":
        return [{"id": _make_chunk_id(abs_path, 0, text), "content": text,
                 "section_title": "", "position": 0, "depth": 1, "container_id": abs_path}]
    if mode == "nest":
        return _build_tree(abs_path, text)
    from flex.compile.chunkers import _flat_nodes
    return _flat_nodes(abs_path, text)


def _run_code_cell(args, console, *, name: str, desc: str,
                   selections: list[str]) -> None:
    """Build the one codegraph tier through the incremental per-file writer.

    The CLI used to compile an eager ``instant --code`` schema while the daemon
    maintained a different ``cell_type=code`` schema.  A codegraph now starts in
    the same shape it will keep for its lifetime.
    """
    from datetime import datetime, timezone
    from rich.panel import Panel
    from rich.text import Text

    from flex.core import set_meta
    from flex.registry import FLEX_HOME, register_cell
    from flex.sdk import create, register
    from flex.modules.fs.compile.index_code import (
        CODE_SCHEMA_DDL, _walk_code, index_file_code, install_code_presets,
    )
    from flex.modules.docpac.compile.init import _is_identity_excluded

    files = sorted({str(p.resolve()) for root in selections
                    for p in _walk_code(Path(root))})
    if not files:
        _error(console, "no indexable code in any selection — cell untouched")
        return

    cells_dir = FLEX_HOME / "cells" / "labs"
    cells_dir.mkdir(parents=True, exist_ok=True)
    db_path = cells_dir / f"{name}.db"
    for stale in (db_path, db_path.with_suffix(".db-wal"),
                  db_path.with_suffix(".db-shm")):
        if stale.exists():
            stale.unlink()

    db = create(name, desc, cell_type="code", db_path=db_path,
                schema=CODE_SCHEMA_DDL)
    set_meta(db, "selections", json.dumps(selections))
    set_meta(db, "chunking", json.dumps({"split_mode": "nest", "code": True}))
    set_meta(db, "resolver", "code_graph@v1")
    set_meta(db, "profile", "code")
    db.commit()  # recipe survives an interrupted compile

    identity = None
    try:
        from flex.modules.soma.lib.identity.file_identity.identity import get_instance
        identity = get_instance()
    except Exception:
        pass

    indexed = 0
    for file_path in files:
        file_id = file_path
        if identity is not None and not _is_identity_excluded(file_path):
            try:
                file_id = identity.assign(file_path)
            except Exception:
                pass
        if index_file_code(db, file_path, file_id=file_id):
            indexed += 1

    set_meta(db, "compiled_at", datetime.now(timezone.utc).isoformat())
    register(db, name, desc, cell_type="code")
    install_code_presets(db)  # wins over the general orient installed above
    db.commit()

    lifecycle = "static" if getattr(args, "no_watch", False) else "watch"
    register_cell(
        name, str(db_path), cell_type="code", description=desc,
        corpus_path=selections[0], unlisted=True, lifecycle=lifecycle,
        refresh_module=None, watch_path=selections[0], watch_pattern="**/*",
    )
    db.close()

    panel = Text()
    panel.append("Codegraph cell compiled.\n\n", style="cyan")
    panel.append("Cell        ").append(f"{name}\n", style="green")
    panel.append("Selections  ").append(f"{len(selections)}\n", style="green")
    panel.append("Compiled    ").append(
        f"{indexed} files through the incremental code writer\n", style="green")
    panel.append("Mode        ").append(
        "no embeddings · per-file refresh · explicit graph ambiguity\n", style="yellow")
    console.print(Panel(panel, padding=(1, 2), highlight=False))


def run(args, console) -> None:
    """Compile the union of selections into one unlisted, no-embed cell (v5)."""
    from datetime import datetime, timezone
    from pathlib import Path

    from rich.panel import Panel
    from rich.text import Text

    from flex.core import set_meta
    from flex.registry import FLEX_HOME, register_cell
    from flex.sdk import _make_chunk_id, _walk_and_chunk, create, ingest, link, register, source

    nest = getattr(args, "nest", False)
    config_mode = getattr(args, "config", False)
    code = getattr(args, "code", False)

    regen = getattr(args, "regen", None)
    name = (getattr(args, "name", None)
            or (regen if isinstance(regen, str) else None)
            or "fs")

    # ── resolve selections + recipe (the cell carries its own derivation) ──
    selections = _load_selections(name)
    # bare --regen (no explicit mode) → restore the recipe's stored split mode
    if regen is not None and not nest and not config_mode:
        stored = _load_chunking_mode(name)
        nest = stored == "nest"
        config_mode = stored == "config"
    if regen is not None and not code:
        code = _load_code_flag(name)
    tree = nest or config_mode  # both modes write _types_instant + the _edges_tree tree
    if regen is not None and not selections and not getattr(args, "path", None):
        _error(console, f"cell '{name}' has no stored selections — add one with --path")
        return

    add_path = getattr(args, "path", None)
    if add_path:
        selections = sorted(set(selections) | {str(Path(add_path).expanduser().resolve())})
    remove_path = getattr(args, "remove", None)
    if remove_path:
        target = str(Path(remove_path).expanduser().resolve())
        if target not in selections:
            _error(console, f"'{target}' is not a selection of cell '{name}'")
            return
        selections = [s for s in selections if s != target]

    if not selections:
        _error(console, "no selections — use --path FOLDER (or --regen NAME)")
    missing = [s for s in selections if not Path(s).is_dir()]
    if missing:
        _error(console, "selection folder(s) missing — recipe NOT modified, cell untouched:",
               missing)

    desc = (getattr(args, "description", None)
            or f"{name} — instant projection of {len(selections)} selection(s)")

    if code:
        return _run_code_cell(
            args, console, name=name,
            desc=(getattr(args, "description", None)
                  or f"{name} — incremental codegraph of {len(selections)} selection(s)"),
            selections=selections,
        )

    # ── union walk, dedup by absolute path (first selection to claim a file wins) ──
    by_file: dict[str, list] = {}
    skipped_dupes = 0
    if tree:  # nest or config — both resolve the general .flexchunk profile per file.
        # nest → default split is 'nest' (a .flexchunk can still override per folder);
        # config → default 'flat'. Either way split/level/rollup/types all apply.
        cfg_cache: dict = {}
        default_split = "nest" if nest else "flat"
        for root_str in selections:
            root = Path(root_str)
            for f, text in _walk_files(root, code=code):
                abs_path = str(f.resolve())
                if abs_path in by_file:
                    skipped_dupes += 1
                    continue
                profile = _resolve_profile(abs_path, cfg_cache, default_split=default_split)
                by_file[abs_path] = _build_profiled(abs_path, text, profile)
    else:
        for root_str in selections:
            root = Path(root_str)
            claimed_before = set(by_file)
            for chunk in _walk_and_chunk(root):
                rel = chunk.pop("_source", "default")
                abs_path = str((root / rel).resolve()) if rel != "default" else root_str
                if abs_path in claimed_before:
                    skipped_dupes += 1
                    continue
                by_file.setdefault(abs_path, []).append(chunk)

    if not by_file:
        _error(console, "no indexable content in any selection — cell untouched")
        return
    total_chunks = sum(len(v) for v in by_file.values())

    # ── compile (cell is a cache; selections + folders are the truth) ──
    cells_dir = FLEX_HOME / "cells" / "labs"
    cells_dir.mkdir(parents=True, exist_ok=True)
    db_path = cells_dir / f"{name}.db"
    for stale in (db_path, db_path.with_suffix(".db-wal"), db_path.with_suffix(".db-shm")):
        if stale.exists():
            stale.unlink()

    db = create(name, desc, cell_type="instant", db_path=db_path, schema=_INSTANT_DDL)

    # ── recipe stamp: BEFORE the compile, not after ──
    # A regen wipes the old cell (`stale.unlink()`) and rebuilds. Stamping the recipe
    # afterwards meant any interruption mid-compile left a cell that no longer knew
    # what it was built FROM — an unrecoverable, unretryable state that then froze the
    # watch permanently. The recipe is fully resolved by here; write and COMMIT it first
    # so a killed build is merely incomplete, never amnesiac. Moving the writes without
    # this commit would not cross the crash boundary: SQLite would roll them back.
    # `compiled_at` stays at the end as the completion receipt (recipe without it = partial).
    set_meta(db, "selections", json.dumps(selections))
    _split_mode = "config" if config_mode else ("nest" if nest else "flat")
    set_meta(db, "chunking", json.dumps({"split_mode": _split_mode, "code": bool(code)}))
    set_meta(db, "resolver", "node_tree@v1")
    set_meta(db, "profile", "instant")
    db.commit()

    for abs_path, file_chunks in by_file.items():
        source(db, abs_path, Path(abs_path).name)
        if tree:
            # nest/config: nodes already carry id/section_title/position/depth/container_id
            for ch in file_chunks:
                ch["content_hash"] = hashlib.sha256((ch.get("content") or "").encode("utf-8")).hexdigest()
            ingest(db, abs_path, file_chunks, types="_types_instant")
            for ch in file_chunks:  # container chain → _edges_tree (subtree preset reads this)
                link(db, ch["id"], ch["container_id"], relation="subsection", depth=ch["depth"])
        else:
            # depth-1: enrich the flat leaves chunk_file_body already produced. Does NOT
            # set 'id' or touch 'content' → chunk_id + _raw_chunks byte-identical to v4.
            for pos, ch in enumerate(file_chunks):
                ch["section_title"] = ch.get("title", "") or ""
                ch.setdefault("position", pos)
                ch["depth"] = 1
                ch["container_id"] = abs_path
                ch["content_hash"] = hashlib.sha256((ch.get("content") or "").encode("utf-8")).hexdigest()
            ingest(db, abs_path, file_chunks, types="_types_instant")

        # Per-source refresh cursor. Unlike the aggregate watch signature, this
        # identifies exactly which file changed, enabling incremental refresh.
        try:
            _p = Path(abs_path)
            _st = _p.stat()
            _hash = hashlib.sha256(_p.read_bytes()).hexdigest()
            db.execute(
                "INSERT OR REPLACE INTO _instant_source_state "
                "(source_id,content_hash,size_bytes,mtime_ns) VALUES (?,?,?,?)",
                (abs_path, _hash, _st.st_size, _st.st_mtime_ns),
            )
        except OSError:
            pass

    # ── call graph (Python nest/config): resolve call sites vs the cell-wide symbol map ──
    if tree:  # only tree-mode nodes carry 'id'/'_calls'; flat chunks don't
        name_to_ids: dict[str, list] = {}
        for _nodes in by_file.values():
            for n in _nodes:
                st = n.get("section_title")
                if st and not st.startswith("("):  # skip the (module) preamble leaf
                    name_to_ids.setdefault(st, []).append(n["id"])
        call_rows = []
        for _nodes in by_file.values():
            for n in _nodes:
                for nm in n.get("_calls", ()):  # only Python def nodes carry _calls
                    ids = name_to_ids.get(nm)
                    callee = ids[0] if ids and len(ids) == 1 else None  # unique name → resolved
                    call_rows.append((n["id"], callee, nm))
        if call_rows:
            db.executemany(
                "INSERT OR IGNORE INTO _edges_call (caller_id, callee_id, callee_name) VALUES (?, ?, ?)",
                call_rows,
            )

    # ── import graph (Python): file → imported module/symbol (codegraph's import edges) ──
    if tree:
        import ast as _ast2

        from flex.compile.chunkers import _TS_EXTS, _ts_language
        import_rows = []
        for abs_path in by_file:
            if not abs_path.endswith(".py"):
                continue
            try:
                _t = _ast2.parse(Path(abs_path).read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                continue
            for nd in _ast2.walk(_t):
                if isinstance(nd, _ast2.Import):
                    for a in nd.names:
                        import_rows.append((abs_path, a.name, None))
                elif isinstance(nd, _ast2.ImportFrom):
                    mod = ("." * (nd.level or 0)) + (nd.module or "")
                    for a in nd.names:
                        import_rows.append((abs_path, mod, a.name))
        for abs_path in by_file:                       # JS/TS imports (tree-sitter)
            ext = abs_path.rsplit(".", 1)[-1].lower() if "." in abs_path else ""
            if ext not in _TS_EXTS:
                continue
            lang = _ts_language(ext)
            if lang is None:
                continue
            try:
                from tree_sitter import Parser
                _src = Path(abs_path).read_text(encoding="utf-8", errors="ignore").encode("utf-8")
                _root = Parser(lang).parse(_src).root_node
            except Exception:
                continue

            def _imp(m):
                for c in m.children:
                    if c.type == "import_statement":
                        srcn = c.child_by_field_name("source")
                        if srcn is not None:
                            mod = srcn.text.decode("utf-8", "ignore").strip("\"'`")
                            if mod:
                                import_rows.append((abs_path, mod, None))
                    _imp(c)
            _imp(_root)
        if import_rows:
            db.executemany(
                "INSERT OR IGNORE INTO _edges_import (source_id, module, name) VALUES (?, ?, ?)",
                import_rows,
            )

    # ── SOMA file identity (machine-global spine; must never fail a compile) ──
    soma_status = "skipped (--no-soma)"
    if not getattr(args, "no_soma", False):
        try:
            from flex.modules.soma.lib.identity.file_identity import get_instance
            total = len(by_file)
            uuids = get_instance().assign_batch(list(by_file))
            stamped = sum(1 for u in uuids.values() if u)
            db.executemany(
                "INSERT OR REPLACE INTO _edges_fs_identity (source_id, file_uuid) VALUES (?, ?)",
                [(p, u) for p, u in uuids.items() if u],
            )
            if stamped == total:
                soma_status = f"{stamped}/{total} file UUIDs stamped"
            else:
                # Surface partial/zero coverage loudly — a stamped << total build
                # means the shared identity DB was contended (the read/write-lock
                # bugs). Never report a 0/N build as quiet success.
                soma_status = (
                    f"WARNING: only {stamped}/{total} file UUIDs stamped "
                    f"(identity DB contended?)"
                )
        except ImportError:
            soma_status = "absent (no edges)"
        except Exception as e:  # noqa: BLE001 — identity must never fail a compile
            soma_status = f"failed, skipped ({type(e).__name__}: {e})"

    # ── v5: ship the recursive subtree preset (the query artifact; dormant at depth-1) ──
    for _name, _desc, _sql in (
        ("subtree", "Recursive descendants of a container node (over _edges_tree).", _SUBTREE_PRESET_SQL),
        ("callers", "Who calls a symbol (call graph). @callers symbol=NAME", _CALLERS_SQL),
        ("callees", "What a symbol calls (call graph). @callees symbol=NAME", _CALLEES_SQL),
        ("impact", "Multi-hop callers — what's affected if a symbol changes. @impact symbol=NAME", _IMPACT_SQL),
    ):
        _params = "root" if _name == "subtree" else "symbol"
        db.execute(
            "INSERT OR REPLACE INTO _presets (name, description, params, sql) VALUES (?, ?, ?, ?)",
            (_name, _desc, _params, _sql),
        )

    # ── completion receipt: the recipe was stamped pre-compile (see above); this says
    #    the compile actually FINISHED. recipe-without-compiled_at = a partial cell.
    set_meta(db, "compiled_at", datetime.now(timezone.utc).isoformat())

    # embed() and graph() are deliberately absent, permanently.
    register(db, name, desc, cell_type="instant")

    # instant stock presets — MUST run AFTER register(): register() reinstalls the
    # general preset set (sdk install_presets(_GENERAL_PRESETS)), so anything installed
    # before it is clobbered. Installing here makes instant's @orient INSERT-OR-REPLACE
    # genuinely last (wins the 'orient' name over the general orient whose vec_ops('…')
    # doc-literal trips the instant materializer), then drops the graph/embed presets
    # (bridges/genealogy) that can't run on a no-embed/no-graph cell.
    from flex.retrieve.presets import install_presets
    install_presets(db, _INSTANT_STOCK_PRESETS)
    db.executemany("DELETE FROM _presets WHERE name = ?",
                   [(n,) for n in _INSTANT_DROP_PRESETS])
    # A --code cell must self-describe as a CODE cell, not the fs surface. Install the
    # dedicated code @orient (graph tables, nav verbs, coverage) AFTER the fs stock so
    # it wins the 'orient' name. Its stats block does COUNT(*) FROM _symbols; this
    # (eager) instant --code tier doesn't populate _symbols (that's the deferred
    # incremental cell_type=code tier), so ensure an EMPTY _symbols exists first — the
    # orient then runs and honestly shows symbols:0. The eager _edges_call nav stays.
    if code:
        from flex.modules.fs.compile.index_code import _SYMBOLS_DDL, _CODE_STOCK_PRESETS
        db.executescript(_SYMBOLS_DDL)
        if _CODE_STOCK_PRESETS.is_dir():
            install_presets(db, _CODE_STOCK_PRESETS)   # code @orient wins over the fs orient
    # Watch by default so the worker keeps the cell fresh (size-signature regen).
    # --no-watch opts a disposable cell out; a bare --regen preserves the existing
    # lifecycle (pass None → register_cell COALESCEs to the stored value), so the
    # worker's own regen never flips a cell's watch state.
    if getattr(args, "no_watch", False):
        _lifecycle, _refresh_module = "static", None
    elif regen is not None:
        _lifecycle, _refresh_module = None, None
    else:
        _lifecycle, _refresh_module = "watch", "flex.modules.instant.refresh"
    register_cell(name, str(db_path), unlisted=True,
                  lifecycle=_lifecycle, refresh_module=_refresh_module)
    db.commit()
    db.close()

    panel = Text()
    panel.append("Instant cell compiled (v5 recursive-node).\n\n", style="cyan")
    panel.append("Cell        ", style="")
    panel.append(f"{name}\n", style="green")
    panel.append("Selections  ", style="")
    shown = selections[:5]
    panel.append(f"{shown[0]}\n", style="green")
    for s in shown[1:]:
        panel.append(f"            {s}\n", style="green")
    if len(selections) > 5:
        panel.append(f"            (+{len(selections) - 5} more)\n", style="green")
    panel.append("Compiled    ", style="")
    dupe_note = f" ({skipped_dupes} overlap dupes skipped)" if skipped_dupes else ""
    panel.append(f"{total_chunks} chunks from {len(by_file)} files{dupe_note}\n", style="green")
    panel.append("Nodes       ", style="")
    panel.append("section_title · position · depth · container_id · content_hash → _types_instant\n", style="green")
    panel.append("Identity    ", style="")
    panel.append(f"{soma_status}\n", style="green")
    panel.append("Mode        ", style="")
    panel.append("no embeddings · no graph · unlisted · subtree preset shipped\n\n", style="yellow")
    panel.append("  flex core search --cell ", style="bold")
    panel.append(f"{name} ", style="bold green")
    panel.append('"@orient"\n', style="bold")
    panel.append("  grow:    flex init --module instant --path FOLDER\n", style="dim")
    panel.append("  regen:   flex init --module instant --regen", style="dim")
    panel.append(f"{'' if name == 'fs' else ' ' + name}\n", style="dim green")
    console.print(Panel(panel, padding=(1, 2), highlight=False))
