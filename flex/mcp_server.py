#!/usr/bin/env python3
"""
Flex MCP Server — one tool, SQL endpoint.

The AI writes SQL. The server executes it read-only.
vec_ops registered as a function for semantic queries.

Usage:
    python -m flex.serve                               # stdio (Claude Code)
    python -m flex.serve --http --port 7134             # streamable HTTP
    python -m flex.serve --cell claude_code --cell my_data # multi-cell
"""

import asyncio
import json
import os
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from mcp.server.lowlevel import Server
import mcp.types as types

from flex.core import get_meta, open_cell_readonly
from flex.mcp_core import execute_query as _execute_mcp_query
from flex.registry import (
    resolve_cell as registry_resolve,
    discover_cells as registry_discover,
)

# Engine facade — degrades gracefully when .whl engine not installed
try:
    from flex.engine import (
        get_embedder as _engine_get_embedder,
        warm_embedder,
        build_vec_state as _engine_build_vec_state,
        register_vec_udf,
        execute_preset as _engine_execute_preset,
        materialize as _engine_materialize,
    )
    HAS_ENGINE = True
except ImportError:
    HAS_ENGINE = False

# ============================================================
# Configuration
# ============================================================

# ============================================================
# Cell Management
# ============================================================

# VectorCache state — long-lived embedding matrices, independent of connections.
# {cell_name: {'caches': {table: VectorCache}, 'config': dict, 'mtime': float}}
_vec_state: dict = {}
_vec_locks: dict[str, threading.Lock] = {}  # per-cell locks (avoids global serialization)
_vec_locks_guard = threading.Lock()  # protects _vec_locks dict creation only

# Debounce VectorCache rebuilds — daemon writes every 2s but embeddings change
# much less often. Only rebuild if mtime is stale by more than this threshold.
_VEC_REBUILD_DEBOUNCE_S = 60

# Known cells (just names, for instructions). Populated at startup + lazily.
_known_cells: set[str] = set()
_explicit_cells: set[str] = set()  # cells explicitly requested via --cell

_warmup_lock = threading.Lock()
_warmup_state = {
    'status': 'idle',
    'total': 0,
    'completed': 0,
    'current': None,
    'cells': [],
    'errors': [],
    'started_at': None,
    'completed_at': None,
}


def _set_warmup_state(**updates):
    with _warmup_lock:
        _warmup_state.update(updates)


def get_warmup_state() -> dict:
    """Return a snapshot of background VectorCache warmup progress."""
    with _warmup_lock:
        state = dict(_warmup_state)
        state['cells'] = list(state.get('cells') or [])
        state['errors'] = list(state.get('errors') or [])
    state['vec_cached_cells'] = sorted(_vec_state.keys())
    state['vec_cached_count'] = len(_vec_state)
    return state


def discover_cells() -> list[str]:
    """Discover cells from registry + filesystem fallback."""
    return registry_discover()


def _db_path(name: str) -> Path:
    p = registry_resolve(name)
    if p is None:
        raise FileNotFoundError(f"Cell '{name}' not found in registry")
    return p


def _db_mtime(name: str) -> float:
    """Get mtime of cell db file. Returns 0 if missing."""
    p = _db_path(name)
    return p.stat().st_mtime if p.exists() else 0


def _open_query_cell(path: Path) -> sqlite3.Connection:
    """Open a cell for MCP reads, tolerating read-only sandboxes."""
    return open_cell_readonly(path)


@contextmanager
def get_cell(name: str, warm_vec: bool = True):
    """Open a fresh connection to a cell. Registers vec_ops UDF if cached.

    Yields None if cell doesn't exist on disk.
    Fresh connection every call = always see latest data.
    warm_vec=False is used by MCP schema generation so startup/list_tools can
    inspect cell metadata without forcing VectorCache construction.
    Usage: with get_cell('claude_code') as db: ...
    """
    # If cells were explicitly selected, reject requests for other cells
    if _explicit_cells and name not in _explicit_cells:
        yield None
        return

    p = _db_path(name)
    if not p.exists():
        yield None
        return

    db = _open_query_cell(p)
    try:
        is_new = name not in _known_cells
        _known_cells.add(name)

        # Rebuild instructions when a new cell is discovered lazily
        if is_new:
            try:
                server.instructions = build_instructions()
            except Exception:
                pass  # server may not be initialized yet during startup

        # Check if VectorCache needs warming or refreshing.
        # The daemon writes to cell .db every ~2s (mtime changes), but embedding
        # rows change much less often. Debounce: only rebuild if mtime drifted
        # by more than _VEC_REBUILD_DEBOUNCE_S, or if no cache exists yet (warmup).
        current_mtime = p.stat().st_mtime
        state = _vec_state.get(name)

        if not warm_vec:
            pass
        elif HAS_ENGINE and state and state['mtime'] == current_mtime:
            # Cache is fresh — just register UDF on this connection
            register_vec_udf(db, state)
        elif HAS_ENGINE and not _no_embed:
            # Debounce: if cache exists and mtime drift is small, reuse stale cache.
            # Embeddings are append-only — a slightly stale cache misses new rows
            # but doesn't return wrong results. First warmup (state is None) always runs.
            if state and (current_mtime - state['mtime']) < _VEC_REBUILD_DEBOUNCE_S:
                register_vec_udf(db, state)
            else:
                # Per-cell lock — rebuilding one cell doesn't block queries on others
                with _vec_locks_guard:
                    if name not in _vec_locks:
                        _vec_locks[name] = threading.Lock()
                    cell_lock = _vec_locks[name]

                with cell_lock:
                    # Re-check after acquiring lock (another thread may have built it)
                    state = _vec_state.get(name)
                    if state and (current_mtime - state['mtime']) < _VEC_REBUILD_DEBOUNCE_S:
                        register_vec_udf(db, state)
                    else:
                        new_state = _engine_build_vec_state(name, db, current_mtime)
                        if new_state:
                            _vec_state[name] = new_state
                            register_vec_udf(db, new_state)
                            print(f"[flex-mcp]   {name}: vec_cache {'refreshed' if state else 'warmed'}"
                                  f" ({list(new_state['caches'].keys())})", file=sys.stderr)

        yield db
    finally:
        db.close()


_no_embed = False


def init(
    cell_names: list[str],
    active_names: list[str] | None = None,
    no_embed: bool = False,
    warm: bool = True,
):
    """Initialize the server: discover cells, optionally warm caches, set instructions.

    Called by serve.py (the entrypoint) before transport starts.
    cell_names: all discoverable cells (appear in tool enum, queryable).
    active_names: subset to pre-warm VectorCaches at startup. Inactive cells
                  (in cell_names but not active_names) are lazy-loaded on first query.
    warm: when False, defer VectorCache warmup until after the MCP transport is
          accepting initialization/list_tools requests.
    """
    global _no_embed
    _no_embed = no_embed

    if active_names is None:
        active_names = cell_names  # backward compat: activate everything

    _known_cells.update(cell_names)
    _explicit_cells.update(cell_names)

    if no_embed:
        _set_warmup_state(
            status='skipped',
            total=0,
            completed=0,
            current=None,
            cells=[],
            errors=[],
            started_at=None,
            completed_at=time.time(),
        )
        print("[flex-mcp] Skipping embeddings", file=sys.stderr)
    elif warm:
        print(f"[flex-mcp] Warming {len(active_names)} of {len(cell_names)} cells...", file=sys.stderr)
        _warm_all(active_names)
    else:
        _set_warmup_state(
            status='deferred',
            total=len(active_names),
            completed=0,
            current=None,
            cells=list(active_names),
            errors=[],
            started_at=None,
            completed_at=None,
        )
        print(f"[flex-mcp] Deferring warmup for {len(active_names)} of {len(cell_names)} cells", file=sys.stderr)

    server.instructions = build_instructions()
    print(f"[flex-mcp] {len(_known_cells)} cells, {len(_vec_state)} cached", file=sys.stderr)


def get_server() -> Server:
    """Return the MCP Server instance for transport wiring."""
    return server


def _warm_all(cell_names: list[str]):
    """Pre-warm VectorCaches and ONNX embedder at startup."""
    if not HAS_ENGINE:
        _set_warmup_state(
            status='skipped',
            total=0,
            completed=0,
            current=None,
            cells=[],
            errors=[],
            completed_at=time.time(),
        )
        print("[flex-mcp] Engine not installed — skipping warmup", file=sys.stderr)
        return
    _set_warmup_state(
        status='running',
        total=len(cell_names),
        completed=0,
        current=None,
        cells=list(cell_names),
        errors=[],
        started_at=time.time(),
        completed_at=None,
    )
    try:
        warm_embedder()
    except Exception as e:
        _set_warmup_state(
            status='error',
            current=None,
            errors=[f"embedder: {e}"],
            completed_at=time.time(),
        )
        raise
    completed = 0
    errors = []
    for name in cell_names:
        _set_warmup_state(current=name)
        try:
            with get_cell(name):
                pass  # just warming the cache, context manager closes connection
        except Exception as e:
            msg = f"{name}: {e}"
            errors.append(msg)
            print(f"[flex-mcp]   {msg}", file=sys.stderr)
        completed += 1
        _set_warmup_state(completed=completed, errors=list(errors))
    _set_warmup_state(
        status='error' if errors else 'complete',
        current=None,
        completed=completed,
        errors=list(errors),
        completed_at=time.time(),
    )


def warm_cells(cell_names: list[str]):
    """Public lifecycle hook for deferred VectorCache warmup."""
    if not cell_names:
        return
    print(f"[flex-mcp] Warming {len(cell_names)} cells in background...", file=sys.stderr)
    try:
        _warm_all(cell_names)
    except Exception as e:
        print(f"[flex-mcp] background warmup error: {e}", file=sys.stderr)
        return
    state = get_warmup_state()
    if state.get('status') == 'error':
        print(
            f"[flex-mcp] Background warmup finished with "
            f"{len(state.get('errors') or [])} error(s) ({len(_vec_state)} cached)",
            file=sys.stderr,
        )
    else:
        print(f"[flex-mcp] Background warmup complete ({len(_vec_state)} cached)", file=sys.stderr)


def execute_preset(db: sqlite3.Connection, query: str) -> str:
    """Execute a @preset query. Delegates to engine."""
    if not HAS_ENGINE:
        return json.dumps({"error": "Engine not installed. Presets unavailable.",
                           "hint": "Install flex via: curl -sSL https://getflex.dev/install.sh | bash"})
    return _engine_execute_preset(db, query)


def _execute_attaches(db: sqlite3.Connection, sql: str) -> tuple[str, str | None]:
    """Extract ATTACH statements, resolve cell names to registry paths, execute on db.

    Returns (sql_without_attach_stmts, error_or_None).
    Only registered cells (via resolve_cell) can be attached — no arbitrary paths.
    """
    import re
    from flex.registry import resolve_cell

    pattern = re.compile(
        r"ATTACH\s+['\"]([^'\"]+)['\"]\s+AS\s+(\w+)\s*;?",
        re.IGNORECASE
    )
    matches = pattern.findall(sql)
    if not matches:
        return sql, None

    for cell_name, alias in matches:
        if _explicit_cells and cell_name not in _explicit_cells:
            return sql, f"Cell not allowed by --cell: '{cell_name}'. Allowed: {sorted(_explicit_cells)}"
        path = resolve_cell(cell_name)
        if path is None:
            return sql, f"Unknown cell: '{cell_name}'. Available: {sorted(_known_cells)}"
        if not path.exists():
            return sql, f"Cell path not found on disk: {path}"
        try:
            # Parameterize path to prevent SQL injection. Alias is validated
            # as \w+ by the regex pattern (alphanumeric only).
            db.execute(f"ATTACH DATABASE ? AS \"{alias}\"", (str(path),))
        except sqlite3.OperationalError as e:
            return sql, f"ATTACH failed for '{cell_name}': {e}"

    # Strip ATTACH statements — leave only the SELECT
    remaining = pattern.sub("", sql).strip().lstrip(";").strip()
    return remaining, None


def execute_query(db: sqlite3.Connection, query: str) -> str:
    """Execute read-only SQL or @preset on a cell. Returns JSON string."""
    sql = query.strip()

    # Cross-cell ATTACH — resolve cell names to registry paths before blocklist
    upper = sql.upper()
    if 'ATTACH' in upper:
        sql, err = _execute_attaches(db, sql)
        if err:
            return json.dumps({"error": err})

    return _execute_mcp_query(
        db,
        sql,
        preset_executor=execute_preset,
        materializer=_engine_materialize if HAS_ENGINE else None,
    )


# ============================================================
# Build Instructions
# ============================================================

def build_instructions() -> str:
    """Build server instructions — identity only.

    Query docs moved to the query parameter description to avoid
    the 2KB server-instructions cap (v2.1.84).
    """
    return (
        "Flex indexes the user's conversations and knowledge bases. "
        "Each cell is a self-describing SQLite database with chunks, embeddings, "
        "and graph intelligence. Use when the user asks to 'flex' or search their "
        "conversations, memories, changes, documentation, or knowledge."
    )


def _build_query_description() -> str:
    """Build the query parameter description — carries ALL retrieval docs.

    Moved here from server instructions to avoid the 2KB server-instructions cap
    (v2.1.84). Parameter descriptions are uncapped.
    """
    parts = [
        "SQL query, @preset name, or vec_ops expression.",
        "",
        "CELLS:",
    ]

    # --- Generated: cells + descriptions ---
    cell_views = {}  # {name: set of view names}
    for name in sorted(_known_cells):
        with get_cell(name, warm_vec=False) as db:
            if db:
                desc = get_meta(db, 'description') or f"Cell: {name}"
                parts.append(f"  {name}: {desc}")
                try:
                    views = {r[0] for r in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='view'"
                    ).fetchall()}
                    cell_views[name] = views
                except Exception:
                    cell_views[name] = set()

    # --- Generated: cell types grouped by view signature ---
    from collections import defaultdict
    sig_to_cells = defaultdict(list)
    for name, views in cell_views.items():
        sig = tuple(sorted(views))
        sig_to_cells[sig].append(name)

    parts.extend(["", "CELL TYPES:"])
    for sig, cells in sorted(sig_to_cells.items(), key=lambda x: x[1][0]):
        view_list = ', '.join(sig) if sig else '(no views)'
        cell_list = ', '.join(cells)
        parts.append(f"  {view_list}:  {cell_list}")

    # --- Query-writer instructions ---
    parts.extend([
        "",
        "# RETRIEVAL",
        "",
        "Single endpoint: mcp__flex__flex_search. Two params: query (SQL or @preset), cell (cell name).",
        "Every query must be valid SQL or a @preset. Plain text is not accepted — wrap in keyword() or vec_ops().",
        "",
        "Pipeline: SQL → vec_ops → SQL. Phase 1 narrows with SQL. Phase 2 scores with embeddings. Phase 3 composes with SQL.",
        "",
        "Scores are ordinal within a query. Do not compare scores across queries with different tokens.",
        "",
        "## PHASE 1: SQL PRE-FILTER",
        "",
        "The second argument to vec_ops narrows candidates **before** scoring. Push every known constraint here.",
        "A WHERE clause after vec_ops filters **after** the pool is filled — sparse post-filters (hitting <5% of chunks) cause pool starvation",
        "(e.g. 500 candidates scored, then WHERE drops 498). Pre-filter prevents this.",
        "",
        "```sql",
        "SELECT id FROM chunks WHERE type = 'user_prompt'",
        "SELECT id FROM chunks WHERE session_id LIKE 'abc123%'",
        "SELECT id FROM chunks WHERE tool_name = 'Edit'",
        "SELECT id FROM chunks WHERE created_at >= date('now', '-7 days')",
        "SELECT id FROM chunks WHERE type = 'user_prompt'",
        "```",
        "",
        "## PHASE 2: VECTOR OPERATIONS (vec_ops)",
        "",
        "Scores candidates using embeddings. Tokens reshape scoring before selection — spread across subtopics,",
        "suppress a dominant theme, weight recency, search from examples, or trace a direction.",
        "Tokens compose freely in one pass. Returns (id, score) pairs for Phase 3.",
        "",
        "`vec_ops('similar:query_text tokens', 'pre_filter_sql')`",
        "",
        "**IMPORTANT:** vec_ops is a table source — always after FROM or JOIN:",
        "```sql",
        "FROM vec_ops('similar:how the authentication system evolved over time diverse decay:7') v",
        "```",
        "",
        "The similar: text is embedded. Bare keywords cast a wide net; natural language (5-15 words) narrows focus.",
        "",
        "### Modulation Tokens",
        "",
        "Tokens compose: `'similar:how we handle auth and token refresh diverse suppress:JWT rotation boilerplate decay:7'`",
        "",
        "#### diverse",
        "MMR — penalizes similarity to already-selected results. Use for breadth across subtopics.",
        "",
        "#### suppress:TEXT",
        "Embeds TEXT, demotes chunks similar to it. Suppresses the dominant signal so edges surface.",
        "Stack multiple: `suppress:deployment pipeline suppress:CI/CD configuration`.",
        "Aim at the dominant cluster theme, not the whole topic.",
        "",
        "#### decay:N",
        "Temporal decay. N-day half-life. `decay:7` = weekly. `decay:1` = aggressive. `decay:0` = disabled.",
        "",
        "#### centroid:id1,id2,...",
        "Mean embedding of given chunk IDs as query. Use when examples define a concept better than words.",
        "",
        "#### from:TEXT to:TEXT",
        "Direction vector through embedding space. Finds content along the conceptual arc between two ideas.",
        "Anchors should be contrasting concepts: `from:quick hacky prototype to:principled production system`.",
        "",
        "#### pool:N",
        "Candidate pool size (default 500). Increase if post-filter WHERE is sparse: `pool:2000`.",
        "",
        "### Examples",
        "",
        "Broad survey with recency:",
        "```sql",
        "SELECT v.id, v.score, c.content, c.session_id",
        "FROM vec_ops('similar:key decisions tradeoffs and design choices we made this week diverse decay:7',",
        "  'SELECT id FROM chunks WHERE type = ''user_prompt''') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 15",
        "```",
        "",
        "Suppress dominant theme to find edges:",
        "```sql",
        "SELECT v.id, v.score, c.content",
        "FROM vec_ops('similar:what else happened beyond the main refactor this week diverse suppress:deployment pipeline suppress:CI configuration',",
        "  'SELECT id FROM chunks WHERE created_at >= date(''now'', ''-7 days'')') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "Trajectory — conceptual evolution:",
        "```sql",
        "SELECT v.id, v.score, c.content",
        "FROM vec_ops('similar:how the system architecture evolved from:monolithic worker to:cell-based design') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "### Edge Cases",
        "",
        "- `diverse` is boolean — no parameter",
        "- `decay:0` disables decay, `pool:0` falls back to default (500)",
        "- One vec_ops per query — for multiple, use CTEs: `WITH a AS (SELECT * FROM vec_ops(...) v) SELECT * FROM a`",
        "- Some sessions have NULL graph columns (centrality, community) — enrichment runs every 30 min. Use COALESCE.",
        "",
        "## PHASE 3: SQL COMPOSITION",
        "",
        "Join scored results back to views, boost with graph metadata, group, filter, paginate.",
        "",
        "```sql",
        "SELECT v.id, v.score, c.content",
        "FROM vec_ops('similar:authentication patterns and middleware design') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "# RECIPES",
        "",
        "**Structural** (no embeddings — free):",
        "```sql",
        "SELECT project, COUNT(*) as sessions",
        "FROM sessions GROUP BY project ORDER BY sessions DESC",
        "```",
        "",
        "**Semantic search** (the skeleton):",
        "```sql",
        "SELECT v.id, v.score, c.content",
        "FROM vec_ops('similar:YOUR TOPIC IN NATURAL LANGUAGE', 'PRE_FILTER') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "Pre-filter examples:",
        "- Session scope: `'SELECT id FROM chunks WHERE session_id LIKE ''abc%'''`",
        "- User messages: `'SELECT id FROM chunks WHERE type = ''user_prompt'''`",
        "- Recent: `'SELECT id FROM chunks WHERE created_at >= date(''now'', ''-7 days'')'`",
        "- By project: `'SELECT id FROM chunks WHERE session_id IN (SELECT session_id FROM sessions WHERE project = ''myapp'')'`",
        "- Files only: `'SELECT id FROM chunks WHERE type = ''file'''`",
        "- No filter: omit or `''`",
        "",
        "**Exact term** (FTS5 — filename, error, function name, UUID):",
        "```sql",
        "SELECT k.id, k.rank, k.snippet, c.content",
        "FROM keyword('term') k",
        "JOIN chunks c ON k.id = c.id",
        "ORDER BY k.rank DESC LIMIT 10",
        "-- keyword() is a table source — always after FROM or JOIN.",
        "-- Returns (id, rank, snippet). rank is positive — higher = better.",
        "```",
        "",
        "**Scoped keyword** (pre-filter restricts which chunks BM25 ranks — prevents pool starvation):",
        "```sql",
        "SELECT k.id, k.rank, k.snippet, c.content",
        "FROM keyword('authentication', 'SELECT id FROM chunks WHERE type = ''user_prompt''') k",
        "JOIN chunks c ON k.id = c.id",
        "ORDER BY k.rank DESC LIMIT 10",
        "-- 2nd arg = pre-filter SQL (must start with SELECT). Same pattern as vec_ops.",
        "-- Without pre-filter, BM25 ranks globally — scoped post-filters starve the pool.",
        "```",
        "",
        "**Hybrid intersection** (BOTH keyword and semantic — empty results mean no overlap, not broken syntax):",
        "```sql",
        "SELECT k.id, k.rank, v.score, c.content",
        "FROM keyword('sdk') k",
        "JOIN vec_ops('similar:cell creation pipeline and programmatic ingest workflow') v ON k.id = v.id",
        "JOIN chunks c ON k.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**FTS as pre-filter** (semantic results scoped to keyword matches):",
        "```sql",
        "SELECT v.id, v.score, c.content",
        "FROM vec_ops('similar:error handling and retry patterns',",
        "  'SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid",
        "   WHERE chunks_fts MATCH ''timeout''') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**Hub navigation** (most connected sessions):",
        "```sql",
        "SELECT v.id, v.score, s.title, s.centrality",
        "FROM vec_ops('similar:the main architectural decisions and system design') v",
        "JOIN chunks c ON v.id = c.id",
        "JOIN sessions s ON c.session_id = s.session_id",
        "WHERE s.is_hub = 1",
        "ORDER BY s.centrality DESC LIMIT 5",
        "```",
        "",
        "**File search:**",
        "```sql",
        "-- By path (structural, no embeddings)",
        "SELECT file, section, ext, substr(content, 1, 200)",
        "FROM chunks WHERE type = 'file' AND file LIKE '%/changes/code/2603%'",
        "ORDER BY created_at DESC LIMIT 10",
        "```",
        "```sql",
        "-- Semantic within files",
        "SELECT v.id, v.score, c.file, c.section, substr(c.content, 1, 200)",
        "FROM vec_ops('similar:how authentication middleware validates tokens', 'SELECT id FROM chunks WHERE type = ''file''') v",
        "JOIN chunks c ON v.id = c.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**One-liners:**",
        "- Session: `WHERE session_id LIKE 'd332a1a0%'`",
        "- Type: `WHERE type = 'file'` / `'user_prompt'` / `'tool_call'`",
        "- Extension: `WHERE ext = 'py'`",
        "- Drill-down: `@story session=d332a1a0`",
        "",
        "# METHODOLOGY",
        "",
        "**Right mode for the task:**",
        "- Known path/name → `WHERE file LIKE '%pattern%'` (structural, free)",
        "- Known exact term → `keyword('term')` (FTS5)",
        "- Conceptual/fuzzy → `vec_ops('similar:...')` (semantic)",
        "",
        "Start with `@orient`. Then `PRAGMA table_info(chunks)` to discover columns — they differ per cell.",
        "",
        "**Structural first.** `GROUP BY` / `COUNT(*)` / `DISTINCT` cost nothing. Get the shape before going semantic.",
        "",
        "**Discover then narrow.** Broad vec_ops → find themes → pre-filter next query with findings.",
        "Push constraints into the pre-filter (2nd arg), not WHERE after vec_ops.",
        "",
        "**Pivot on mode shift.** Theme → quantify with GROUP BY. ID → exact retrieval with JOIN + ORDER BY position.",
        "",
        "**Cross-cell** when needed. Different cells have different columns and date ranges.",
        "Column names vary: `created_at` (claude_code), `timestamp`, `file_date` (other cells). Always PRAGMA first.",
        "",
        "# PRESETS",
        "",
        "Use presets when possible. `@name` as the query. `@orient` discovers all presets per cell.",
        "Positional args: `@story session=abc123`, `@digest days=14`.",
        "",
        "# EXTREMELY IMPORTANT: ALWAYS START WITH @orient FOR THE REQUESTED CELL",
        "",
        "**`query=\"@orient\"`, `cell=\"cell_name\"`**",
        "",
        "Returns cell schema, views, communities, hubs, presets. Do this BEFORE any other queries.",
        "",
    ])
    return "\n".join(parts)


# ============================================================
# Tool Description & Schema
# ============================================================

def _build_tool_description() -> str:
    """Build tool description — one-liner for the 2KB tool description cap."""
    return (
        "Read-only SQL on knowledge cells. Single endpoint for semantic search, "
        "keyword search, and structural queries across all cells."
    )


def _build_tool_schema() -> dict:
    """Build JSON Schema. The query parameter description carries ALL retrieval docs."""
    cell_list = sorted(_known_cells)
    return {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": _build_query_description(),
            },
            "cell": {
                "type": "string",
                "description": "Knowledge cell to query",
                "default": "claude_code",
                "enum": cell_list if cell_list else ["claude_code"],
            },
            "params": {
                "type": "object",
                "description": "Named parameters for preset queries (e.g. {\"days\": 7} for @digest)",
                "additionalProperties": True,
            },
        },
        "required": ["query"],
    }


# ============================================================
# MCP Server
# ============================================================

server = Server("flex")


_LOG_MAX_BYTES = 50 * 1024 * 1024  # 50MB per history file
_LOG_MAX_AGE_DAYS = 7


def _log_query(cell: str, query: str, result_json: str, elapsed_ms: float):
    """Append query to cell's history JSONL. Fire-and-forget.

    Rotation: if the file exceeds 50MB or the oldest entry is >7 days old,
    rotate to {stem}-history.{date}.jsonl and start fresh.
    """
    try:
        cell_path = _db_path(cell)
        history_path = cell_path.parent / f"{cell_path.stem}-history.jsonl"
        parsed = json.loads(result_json)
        if isinstance(parsed, list):
            result_count = len(parsed)
        elif isinstance(parsed, dict) and 'error' in parsed:
            result_count = -1
        else:
            result_count = 0
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cell": cell,
            "query": query,
            "result_count": result_count,
            "elapsed_ms": round(elapsed_ms, 1),
            "result": parsed,
        }

        # Rotate if file is too large or too old
        if history_path.exists():
            rotate = False
            if history_path.stat().st_size > _LOG_MAX_BYTES:
                rotate = True
            else:
                # Check age of first line
                try:
                    with open(history_path, 'r') as f:
                        first_line = f.readline()
                    if first_line:
                        first_ts = json.loads(first_line).get("timestamp", "")
                        if first_ts:
                            from datetime import timedelta
                            first_dt = datetime.fromisoformat(first_ts)
                            if datetime.now(timezone.utc) - first_dt > timedelta(days=_LOG_MAX_AGE_DAYS):
                                rotate = True
                except Exception:
                    pass
            if rotate:
                rotated = cell_path.parent / f"{cell_path.stem}-history.{datetime.now().strftime('%Y%m%d')}.jsonl"
                history_path.rename(rotated)

        # Write with restrictive permissions
        fd = os.open(str(history_path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, 'a') as f:
            f.write(json.dumps(entry, default=str) + '\n')
    except Exception:
        pass  # never break queries for logging


def _relay_tool_available() -> bool:
    """Only expose flex_relay when the websockets extra is installed."""
    try:
        import websockets  # noqa: F401
        return True
    except ImportError:
        return False


_RELAY_TOOL = types.Tool(
    name="flex_relay",
    description=(
        "Enable or disable cloud access via the getflex.dev tunnel. "
        "When enabled, this flex server is reachable via a stable "
        "https://{id}.getflex.dev/sse endpoint usable from claude.ai "
        "and any MCP client. Per-machine, opt-in, reversible. "
        "The MCP service must be restarted (systemctl --user restart "
        "flex-mcp) for state changes to take effect — the tool returns "
        "restart_required=true on start/stop."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["start", "stop", "status"],
                "description": "start = enable relay, stop = disable, status = check current state",
            },
        },
        "required": ["action"],
    },
)


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """Return the flex_search tool with dynamic description and schema."""
    tools = [
        types.Tool(
            name="flex_search",
            description=_build_tool_description(),
            inputSchema=_build_tool_schema(),
        )
    ]
    if _relay_tool_available():
        tools.append(_RELAY_TOOL)
    return tools


async def _handle_flex_relay(arguments: dict | None) -> list[types.TextContent]:
    """Toggle the relay state. Mirrors `flex relay` CLI behavior.

    The MCP server cannot cleanly restart itself from inside a tool handler,
    so start/stop actions return ``restart_required: true`` and expect the
    caller to restart flex-mcp out-of-band.
    """
    arguments = arguments or {}
    action = arguments.get("action", "status")

    if not _relay_tool_available():
        return [types.TextContent(type="text", text=json.dumps({
            "error": "relay_unavailable",
            "message": "relay requires the websockets package",
            "install": "pip install 'getflex[relay]'",
        }))]

    from flex.registry import FLEX_HOME
    machine_id_path = FLEX_HOME / "machine_id"
    flag_path = FLEX_HOME / "relay_enabled"

    # Inline relay-state check keeps the public MCP tool self-contained.
    def _enabled() -> bool:
        if flag_path.exists():
            return True
        if machine_id_path.exists():
            try:
                flag_path.touch()
            except OSError:
                return False
            return True
        return False

    if action == "status":
        if _enabled() and machine_id_path.exists():
            mid = machine_id_path.read_text().strip()
            return [types.TextContent(type="text", text=json.dumps({
                "enabled": True,
                "machine_id": mid,
                "endpoint": f"https://{mid}.getflex.dev/sse",
            }))]
        return [types.TextContent(type="text", text=json.dumps({"enabled": False}))]

    if action == "stop":
        # Preserve machine_id as persistent identity — just drop the flag.
        # Keeps the claude.ai Connector URL stable across toggle cycles.
        if flag_path.exists():
            flag_path.unlink()
        return [types.TextContent(type="text", text=json.dumps({
            "enabled": False,
            "restart_required": True,
            "message": "Relay disabled. Restart flex-mcp service for changes to take effect.",
        }))]

    if action == "start":
        FLEX_HOME.mkdir(parents=True, exist_ok=True)
        if not machine_id_path.exists():
            import uuid
            machine_id_path.write_text(uuid.uuid4().hex[:8])
        flag_path.touch()
        mid = machine_id_path.read_text().strip()
        return [types.TextContent(type="text", text=json.dumps({
            "enabled": True,
            "machine_id": mid,
            "endpoint": f"https://{mid}.getflex.dev/sse",
            "restart_required": True,
            "message": "Relay enabled. Restart flex-mcp service to connect.",
        }))]

    return [types.TextContent(type="text", text=json.dumps({
        "error": "unknown_action", "action": action,
    }))]


_QUERY_TIMEOUT_S = 30  # max seconds per query before cancellation

# Response gate — prevent large results from eating context window
_CHARS_PER_TOKEN = 3.5
_GATE_TOKEN_LIMIT = 10_000  # gate results over ~10K tokens
_GATE_CHAR_LIMIT = int(_GATE_TOKEN_LIMIT * _CHARS_PER_TOKEN)
_GATE_FORCE_LIMIT = 100_000  # hard cap even with ! prefix (~350KB)
_GATE_FORCE_CHAR_LIMIT = int(_GATE_FORCE_LIMIT * _CHARS_PER_TOKEN)
_PREVIEW_ROWS = 10  # max rows in preview (actual count limited by char budget)
_PREVIEW_FIELD_LIMIT = 200  # max chars per string field in preview
_PREVIEW_CHAR_BUDGET = 2000  # max total chars for preview (~500 tokens)


def _execute_cell_query(cell: str, query: str) -> str:
    """Synchronous cell query — runs in executor to avoid blocking event loop."""
    with get_cell(cell) as db:
        if db is None:
            available = sorted(_known_cells)
            on_disk = set(discover_cells()) - set(available)
            msg = {"error": f"Unknown cell: {cell}", "available": available}
            if on_disk:
                msg["also_on_disk"] = sorted(on_disk)
            return json.dumps(msg)

        # SQLite progress handler — abort after timeout
        deadline = time.monotonic() + _QUERY_TIMEOUT_S
        def _check_timeout():
            if time.monotonic() > deadline:
                return 1  # non-zero = abort
            return 0
        db.set_progress_handler(_check_timeout, 1000)  # check every 1K opcodes

        try:
            start = time.monotonic()
            result = execute_query(db, query)
            elapsed_ms = (time.monotonic() - start) * 1000
            _log_query(cell, query, result, elapsed_ms)
            return result
        except sqlite3.OperationalError as e:
            if 'interrupt' in str(e).lower():
                error_msg = json.dumps({"error": f"Query timed out after {_QUERY_TIMEOUT_S}s"})
            else:
                msg = str(e)
                hint = None
                if "no such column" in msg or "no such table" in msg:
                    hint = 'Use @orient to see available views, columns, and tables.'
                elif "not valid SQL" in msg.lower() or "near " in msg:
                    hint = 'Use @orient to see query examples and syntax.'
                error_msg = json.dumps({"error": msg, **({"hint": hint} if hint else {})})
            _log_query(cell, query, error_msg, (time.monotonic() - start) * 1000)
            return error_msg
        except Exception as e:
            error_msg = json.dumps({"error": f"{type(e).__name__}: {e}"})
            _log_query(cell, query, error_msg, (time.monotonic() - start) * 1000)
            return error_msg


def _token_header(result_json: str) -> tuple[int, int, str]:
    """Parse result and build token estimate header.

    Returns (row_count, est_tokens, header_line).
    """
    n_chars = len(result_json)
    est_tokens = int(n_chars / _CHARS_PER_TOKEN)

    try:
        parsed = json.loads(result_json)
    except (json.JSONDecodeError, ValueError):
        parsed = None

    if isinstance(parsed, list):
        row_count = len(parsed)
    else:
        row_count = 0

    if est_tokens >= 1000:
        tok_str = f"~{est_tokens / 1000:.1f}K tok"
    else:
        tok_str = f"~{est_tokens} tok"

    header = f"[{row_count} rows, {tok_str}]"
    return row_count, est_tokens, header


def _truncate_row(row: dict) -> dict:
    """Truncate string fields in a row for preview."""
    out = {}
    for k, v in row.items():
        if isinstance(v, str) and len(v) > _PREVIEW_FIELD_LIMIT:
            out[k] = v[:_PREVIEW_FIELD_LIMIT] + '...'
        else:
            out[k] = v
    return out


def _gate_response(result_json: str, header: str, row_count: int, est_tokens: int) -> str:
    """Build gated preview response for large results.

    Truncates string fields per row and caps total preview size.
    Shows as many rows as fit within the char budget.
    """
    try:
        parsed = json.loads(result_json)
    except (json.JSONDecodeError, ValueError):
        return result_json

    if not isinstance(parsed, list) or row_count == 0:
        return result_json

    # Build preview: truncate fields, accumulate rows up to char budget
    preview_rows = []
    total_chars = 0
    for row in parsed[:_PREVIEW_ROWS]:
        truncated = _truncate_row(row)
        row_json = json.dumps(truncated, indent=2, default=str)
        if total_chars + len(row_json) > _PREVIEW_CHAR_BUDGET and preview_rows:
            break  # budget exceeded, but always show at least 1 row
        preview_rows.append(truncated)
        total_chars += len(row_json)

    preview = json.dumps(preview_rows, indent=2, default=str)
    shown = len(preview_rows)
    return (
        f"[{row_count} rows, ~{est_tokens / 1000:.1f}K tok — gated]\n"
        f"Preview ({shown} of {row_count} rows, fields truncated to {_PREVIEW_FIELD_LIMIT} chars):\n"
        f"{preview}\n\n"
        f"Add LIMIT to your query, or prefix with ! to bypass the gate."
    )


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    """Handle flex tool calls. Runs DB work in executor to avoid blocking."""
    if name == "flex_relay":
        return await _handle_flex_relay(arguments)

    if name != "flex_search":
        return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    if not arguments or "query" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: query"}))]

    query = arguments["query"]
    if len(query) > 1_000_000:
        return [types.TextContent(type="text", text=json.dumps({"error": "Query too large (max 1MB)"}))]
    cell = arguments.get("cell", "claude_code")

    # Merge external params dict into preset query string.
    # execute_preset parses "key=value" tokens from the query string — serializing
    # params here avoids threading a dict through the entire call stack.
    params_arg = arguments.get("params") or {}
    if params_arg and query.lstrip('!').startswith('@'):
        param_str = ' '.join(f'{k}={v}' for k, v in params_arg.items())
        query = f'{query} {param_str}'

    # ! prefix = force bypass gate
    force = False
    if query.startswith('!'):
        force = True
        query = query[1:].lstrip()

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _execute_cell_query, cell, query)

    # Token estimate header + gate
    row_count, est_tokens, header = _token_header(result)

    if not force and est_tokens > _GATE_TOKEN_LIMIT:
        gated = _gate_response(result, header, row_count, est_tokens)
        return [types.TextContent(type="text", text=gated)]

    # ! has a hard ceiling — never unbounded
    if force and est_tokens > _GATE_FORCE_LIMIT:
        truncated = result[:_GATE_FORCE_CHAR_LIMIT]
        warning = (
            f"\n\n[truncated at ~{_GATE_FORCE_LIMIT // 1000}K tokens — "
            f"add LIMIT to query]"
        )
        return [types.TextContent(type="text", text=f"{header}\n{truncated}{warning}")]

    return [types.TextContent(type="text", text=f"{header}\n{result}")]


# ============================================================
# Backward compat: python -m flex.mcp_server still works
# ============================================================

if __name__ == "__main__":
    from flex.serve import main
    main()


# ============================================================
# Transport and lifecycle moved to serve.py
# ============================================================
