#!/usr/bin/env python3
"""
Flex MCP Server — one tool, SQL endpoint.

The AI writes SQL. The server executes it read-only.
vec_ops registered as a function for semantic queries.

Usage:
    python -m flex.mcp_server                          # stdio (Claude Code)
    python -m flex.mcp_server --http --port 7134       # streamable HTTP
    python -m flex.mcp_server --cell claude_code --cell qmem # multi-cell
"""

import asyncio
import json
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
import mcp.types as types

from flex.core import open_cell, get_meta
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
        drain_queue,
        run_enrichment,
    )
    HAS_ENGINE = True
except ImportError:
    HAS_ENGINE = False

# ============================================================
# Configuration
# ============================================================

# SQLite authorizer for search pipe — whitelist read-only operations.
# Action codes: https://www.sqlite.org/c3ref/c_alter_table.html
_SQLITE_OK, _SQLITE_DENY = 0, 1
_SEARCH_ALLOW = {
    20,  # SQLITE_READ        — read column value
    21,  # SQLITE_SELECT      — SELECT statement
    29,  # SQLITE_CREATE_VTABLE — FTS5 vtable access (read, not create)
    31,  # SQLITE_FUNCTION    — use SQL function (incl. built-ins)
    33,  # SQLITE_RECURSIVE   — recursive CTE
}
_BLOCKED_PRAGMAS = frozenset({
    'database_list',      # leaks absolute file paths
    'wal_checkpoint',     # triggers WAL operations
    'integrity_check',    # full-DB scan, DoS vector
    'quick_check',        # same
    'writable_schema',    # dangerous config toggle
    'query_only',         # config toggle
})


def _search_authorizer(action, arg1, arg2, db_name, trigger_name):
    """Authorizer callback for search pipe. Allows reads + safe PRAGMAs only."""
    if action == 19:  # SQLITE_PRAGMA
        return _SQLITE_DENY if (arg1 or '').lower() in _BLOCKED_PRAGMAS else _SQLITE_OK
    return _SQLITE_OK if action in _SEARCH_ALLOW else _SQLITE_DENY

# ============================================================
# Cell Management
# ============================================================

# VectorCache state — long-lived embedding matrices, independent of connections.
# {cell_name: {'caches': {table: VectorCache}, 'config': dict, 'mtime': float}}
_vec_state: dict = {}
_vec_lock = threading.Lock()  # protects _vec_state writes in HTTP mode

# Known cells (just names, for instructions). Populated at startup + lazily.
_known_cells: set[str] = set()


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


@contextmanager
def get_cell(name: str):
    """Open a fresh connection to a cell. Registers vec_ops UDF if cached.

    Yields None if cell doesn't exist on disk.
    Fresh connection every call = always see latest data.
    Usage: with get_cell('claude_code') as db: ...
    """
    p = _db_path(name)
    if not p.exists():
        yield None
        return

    db = open_cell(str(p))
    try:
        is_new = name not in _known_cells
        _known_cells.add(name)

        # Rebuild instructions when a new cell is discovered lazily
        if is_new:
            try:
                server.instructions = build_instructions()
            except Exception:
                pass  # server may not be initialized yet during startup

        # Check if VectorCache needs warming or refreshing
        current_mtime = p.stat().st_mtime
        state = _vec_state.get(name)

        if HAS_ENGINE and state and state['mtime'] == current_mtime:
            # Cache is fresh — just register UDF on this connection
            register_vec_udf(db, state)
        elif HAS_ENGINE and not _no_embed:
            # Cache missing or stale — rebuild (locked for HTTP concurrency)
            with _vec_lock:
                # Re-check after acquiring lock (another thread may have built it)
                state = _vec_state.get(name)
                if state and state['mtime'] == current_mtime:
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


def warm_all(cell_names: list[str]):
    """Pre-warm VectorCaches and ONNX embedder at startup."""
    if not HAS_ENGINE:
        print("[flex-mcp] Engine not installed — skipping warmup", file=sys.stderr)
        return
    warm_embedder()
    for name in cell_names:
        with get_cell(name):
            pass  # just warming the cache, context manager closes connection


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
        path = resolve_cell(cell_name)
        if path is None:
            return sql, f"Unknown cell: '{cell_name}'. Available: {sorted(_known_cells)}"
        if not path.exists():
            return sql, f"Cell path not found on disk: {path}"
        try:
            db.execute(f"ATTACH DATABASE '{path}' AS \"{alias}\"")
        except sqlite3.OperationalError as e:
            return sql, f"ATTACH failed for '{cell_name}': {e}"

    # Strip ATTACH statements — leave only the SELECT
    remaining = pattern.sub("", sql).strip().lstrip(";").strip()
    return remaining, None


def _is_bare_text(query: str) -> bool:
    """Detect bare text queries that aren't SQL or presets."""
    q = query.strip()
    if q.startswith('@'):
        return False
    upper = q.upper().lstrip()
    sql_starts = ('SELECT', 'WITH', 'PRAGMA', 'EXPLAIN', 'INSERT', 'DELETE',
                  'UPDATE', 'DROP', 'CREATE', 'ALTER', 'ATTACH')
    return not any(upper.startswith(kw) for kw in sql_starts)


def execute_query(db: sqlite3.Connection, query: str) -> str:
    """Execute read-only SQL or @preset on a cell. Returns JSON string."""
    sql = query.strip()

    # Preset dispatch — @name [params]
    if sql.startswith('@'):
        return execute_preset(db, sql)

    # Detect bare text before executing — surface a helpful error
    if _is_bare_text(sql):
        escaped = sql.replace("'", "''")
        return json.dumps({
            "error": f"Not valid SQL: \"{sql}\"",
            "hint": "Use vec_ops() for semantic search, FTS for keyword match, or @preset syntax.",
            "semantic": (
                f"SELECT v.id, v.score, m.content "
                f"FROM vec_ops('_raw_chunks', '{escaped}') v "
                f"JOIN messages m ON v.id = m.id "
                f"ORDER BY v.score DESC LIMIT 10"
            ),
            "keyword": (
                f"SELECT k.id, k.rank, k.snippet, m.content "
                f"FROM keyword('{escaped}') k "
                f"JOIN messages m ON k.id = m.id "
                f"ORDER BY k.rank DESC LIMIT 10"
            ),
        })

    upper = sql.upper()

    # Cross-cell ATTACH — resolve cell names to registry paths before blocklist
    if 'ATTACH' in upper:
        sql, err = _execute_attaches(db, sql)
        if err:
            return json.dumps({"error": err})
        upper = sql.upper()

    # Materialize table sources into temp tables
    # (runs BEFORE authorizer — trusted code that needs temp table writes)
    if HAS_ENGINE:
        sql = _engine_materialize(db, sql)
        if sql.startswith('{"error"'):
            return sql

    # Read-only enforcement via SQLite authorizer
    # Replaces the old startswith keyword check which was bypassable via
    # CTE prefix (WITH x AS (DELETE ...) SELECT ...) or SQL comments.
    try:
        db.set_authorizer(_search_authorizer)
        rows = db.execute(sql).fetchall()
        results = [dict(r) for r in rows]
        return json.dumps(results, indent=2, default=str)
    except sqlite3.DatabaseError as e:
        err_str = str(e)
        if 'not authorized' in err_str.lower():
            return json.dumps({"error": "Write operations not allowed"})
        return json.dumps({"error": err_str})
    finally:
        db.set_authorizer(None)


# ============================================================
# Build Instructions
# ============================================================

def build_instructions() -> str:
    """Build server instructions. The cell describes itself via @orient."""
    parts = [
        "Flex indexes the USERS conversations and knowledge bases. "
        "Each cell is a self-describing SQLite database with chunks, embeddings, "
        "and graph intelligence. When the USER asks to 'flex' or flex search their "
        "conversations, memories, changes, documentation or knowledge they are "
        "referring to this tool.",
        "",
        "Read-only SQL on knowledge cells.",
        "",
        "CELLS:",
    ]

    # --- Generated: cells + descriptions ---
    cell_views = {}  # {name: set of view names}
    for name in sorted(_known_cells):
        with get_cell(name) as db:
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

    # --- Query-writer instructions (synced with context/assets/mcp-instructions-i1.md) ---
    parts.extend([
        "",
        "# RETRIEVAL",
        "",
        "Flex offers a single endpoint for all operations: mcp__flex__flex_search. Two parameters:",
        "query (SQL or @preset) and cell (cell name). You compose queries using the views, columns,",
        "and graph columns available for that cell.",
        "",
        "The retrieval pipeline for each query is SQL \u2192 vec_ops \u2192 SQL. Phase one narrows the corpus with SQL. Phase two scores it with embeddings. Phase three composes the final result with SQL.",
        "",
        "## PHASE 1: SQL PRE-FILTER",
        "",
        "**Phase 1 narrows the candidate set before any vector operations occur.** If you know you are looking for user messages or a date, for instance, you can filter for that. Push known constraints here, not into a WHERE clause after vector operations.",
        "",
        "```sql",
        "SELECT id FROM messages WHERE type = 'user_prompt'",
        "SELECT id FROM messages WHERE session_id LIKE 'abc123%'",
        "SELECT id FROM messages WHERE tool_name = 'Edit'",
        "SELECT id FROM messages WHERE type = 'user_prompt'",
        "  AND session_id IN (SELECT session_id FROM sessions WHERE community_id = 29)",
        "```",
        "",
        "## PHASE 2: VECTOR OPERATIONS (vec_ops)",
        "",
        "Phase 2 scores the candidate set from Phase 1 using embeddings. By default this is vanilla cosine similarity \u2014 however **tokens let you reshape the scoring before selection**. You can spread results across subtopics, suppress a dominant theme, weight toward recency, search from example chunks, or trace a direction through embedding space. Tokens are independent operations that compose freely and stack into one pass.",
        "",
        "This phase returns (id, score) pairs that Phase 3 joins, boosts, filters, and paginates.",
        "",
        "`vec_ops('_raw_chunks', 'query_text', 'tokens', 'pre_filter_sql')`",
        "",
        "**IMPORTANT:** vec_ops is a table source \u2014 always use after FROM or JOIN:",
        "```sql",
        "FROM vec_ops('_raw_chunks', 'query') v",
        "```",
        "",
        "### Modulation Tokens",
        "",
        "Tokens compose freely: `'diverse unlike:jwt recent:7'`",
        "",
        "#### diverse",
        "",
        "MMR \u2014 each successive result is penalized for similarity to already-selected results. Use when you want breadth across subtopics rather than 10 variations of the same answer.",
        "",
        "#### unlike:TEXT",
        "",
        "Contrastive search. Embeds TEXT separately, demotes chunks similar to it. The dominant signal gets suppressed \u2014 what surfaces are the edges that a normal query drowns out.",
        "",
        "#### recent:N",
        "",
        "Temporal decay with N-day half-life. A chunk from N days ago scores 50% of an identical chunk from today. Omit N for gentle decay. `recent:1` is aggressive \u2014 yesterday is half-weighted.",
        "",
        "#### like:id1,id2,...",
        "",
        "Centroid search. Computes the mean embedding of the given chunk IDs, then searches from that synthetic vantage point. Use when you found a good result and want more like it, or when 2-3 examples define a concept better than words can.",
        "",
        "#### from:TEXT to:TEXT",
        "",
        "Trajectory \u2014 a direction vector through embedding space. Finds content along the conceptual arc from one idea to another. Not \"search for X then Y\" \u2014 it's the delta between two concepts applied as a lens.",
        "",
        "#### limit:N",
        "",
        "Candidate pool size (default 500).",
        "",
        "### Token Examples",
        "",
        "Broad survey with recency bias:",
        "```sql",
        "SELECT v.id, v.score, m.content, m.project",
        "FROM vec_ops('_raw_chunks', 'decisions and tradeoffs', 'diverse recent:7',",
        "  'SELECT id FROM messages WHERE type = ''user_prompt''') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 15",
        "```",
        "",
        "Contrastive \u2014 suppress a dominant theme:",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'architecture design',",
        "  'diverse unlike:deployment pipeline shipping') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "Trajectory \u2014 evolution from one concept to another:",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'architecture design',",
        "  'from:single file monolith to:modular cell-based system') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "### Edge Cases",
        "",
        "- `diverse` is boolean \u2014 `diverse:0.5` has no effect, just use `diverse`",
        "- `recent:0` disables temporal decay (not 'zero days')",
        "- `limit:0` falls back to default (500), not zero results",
        "- Only ONE vec_ops per query \u2014 for multiple, use CTEs:",
        "  `WITH a AS (SELECT * FROM vec_ops(...) v) SELECT * FROM a`",
        "- Some sessions have NULL centrality/community \u2014 use COALESCE or LEFT JOIN.",
        "",
        "## PHASE 3: SQL COMPOSITION",
        "",
        "Phase 3 takes the scored results from vec_ops and composes the final output with full SQL. This is where you join back to views, boost scores with graph metadata, group by community or project, filter on columns that only exist in the views, and paginate. Hub/bridge reranking and graph arithmetic live here.",
        "",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'authentication') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC",
        "LIMIT 10",
        "```",
        "",
        "### Structure Tokens",
        "",
        "`local_communities` \u2014 per-query Louvain, adds `_community` column to candidates:",
        "",
        "```sql",
        "SELECT _community, COUNT(*) as n, MIN(m.content) as sample",
        "FROM vec_ops('_raw_chunks', 'authentication', 'local_communities') v",
        "JOIN messages m ON v.id = m.id",
        "GROUP BY _community",
        "```",
        "",
        "# RECIPES",
        "",
        "**Structural** (no embeddings):",
        "```sql",
        "SELECT project, COUNT(*) as sessions",
        "FROM sessions GROUP BY project ORDER BY sessions DESC",
        "```",
        "",
        "**Semantic search** (the skeleton):",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC', 'TOKENS', 'PRE_FILTER') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "Narrow the search with a pre-filter:",
        "- Session scope: `'SELECT id FROM messages WHERE session_id LIKE ''abc%'''`",
        "- Human voice only: `'SELECT id FROM messages WHERE type = ''user_prompt'''`",
        "- Date range: `'SELECT id FROM messages WHERE created_at >= ''2026-02-22'''`",
        "- By project: `'SELECT id FROM messages WHERE project = ''flexsearch'''`",
        "- No filter: omit or `''`",
        "",
        "**Exact term** (FTS5 \u2014 domain name, filename, error, UUID):",
        "```sql",
        "SELECT k.id, k.rank, k.snippet, m.content",
        "FROM keyword('term') k",
        "JOIN messages m ON k.id = m.id",
        "ORDER BY k.rank DESC LIMIT 10",
        "-- keyword() is a table source \u2014 always use after FROM or JOIN.",
        "-- Returns (id, rank, snippet). rank is positive \u2014 higher = better.",
        "-- Special chars (dots, operators) handled automatically via fallback quoting.",
        "```",
        "",
        "**Hybrid intersection** (only chunks matching BOTH keyword and semantic):",
        "```sql",
        "SELECT k.id, k.rank, v.score, m.content",
        "FROM keyword('auth') k",
        "JOIN vec_ops('_raw_chunks', 'authentication patterns') v ON k.id = v.id",
        "JOIN messages m ON k.id = m.id",
        "ORDER BY k.rank + v.score DESC",
        "LIMIT 10",
        "```",
        "",
        "**FTS as pre-filter** (all semantic results, scoped to keyword matches):",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC', '',",
        "  'SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid",
        "   WHERE chunks_fts MATCH ''term''') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**Hub navigation** (most connected sessions):",
        "```sql",
        "SELECT v.id, v.score, s.title, s.centrality",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC') v",
        "JOIN messages m ON v.id = m.id",
        "JOIN sessions s ON m.session_id = s.session_id",
        "WHERE s.is_hub = 1",
        "ORDER BY s.centrality DESC LIMIT 5",
        "```",
        "",
        "**One-liners:**",
        "- Filter by session: `WHERE m.session_id LIKE 'd332a1a0%'`",
        "- File dedup (SOMA): `GROUP BY COALESCE(json_extract(m.file_uuids, '$[0]'), m.target_file)`",
        "- Session drill-down: `@story session=d332a1a0`",
        "",
        "# METHODOLOGY",
        "",
        "Start with `@orient`. Every cell describes itself \u2014 shape, schema, views, communities, hubs, presets.",
        "",
        "**Feel the data** before writing complex queries.",
        "What projects exist? What date range? How many sessions?",
        "`GROUP BY` / `COUNT(*)` / `SELECT DISTINCT` is free \u2014 no embeddings.",
        "",
        "**Discover then narrow.**",
        "Broad vec_ops \u2192 discover themes \u2192 pre-filter the next query with what you found.",
        "Push known constraints (date, session, type, community) into the pre-filter, not WHERE.",
        "",
        "**Pivot when the mode shifts.**",
        "Found a theme? Count it. Group it. Quantify.",
        "Found an ID? Switch to exact retrieval \u2014 JOIN and ORDER BY position.",
        "",
        "**Cross-cell** when one cell isn't enough.",
        "Cells have different temporal coverage. Check date ranges before assuming a cell has recent data.",
        "",
        "# PRESETS",
        "",
        "Use presets when possible. Compose SQL for everything else. You may need both.",
        "",
        "Pass `@name` as the query parameter. Run `@orient` to discover all presets per cell.",
        "Presets accept positional args: `@story session=abc123`, `@digest days=14`.",
        "",
        "# EXTREMELY IMPORTANT",
        "",
        "**ALWAYS START WITH: `query=\"@orient\"` BEFORE RUNNING ANY QUERIES. THIS GIVES YOU UP TO DATE INFORMATION ON: cell schema, views, communities, hubs, and presets.**",
        "",
    ])
    return "\n".join(parts)


# ============================================================
# Tool Description & Schema
# ============================================================

def _build_tool_description() -> str:
    """Build tool description. Instructions carry the real context."""
    return (
        "Flex indexes the user's conversations and knowledge bases. "
        "Each cell is a self-describing SQLite database "
        "with chunks, embeddings, and graph intelligence."
    )


def _build_tool_schema() -> dict:
    """Build JSON Schema — lean interface contract. Instructions do the teaching."""
    cell_list = sorted(_known_cells)
    return {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query, @preset name, or vec_ops expression",
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


def _log_query(cell: str, query: str, result_json: str, elapsed_ms: float):
    """Append query to cell's history JSONL. Fire-and-forget."""
    try:
        cell_path = _db_path(cell)
        # History file sits alongside the cell .db: {uuid}-history.jsonl
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
        with open(history_path, 'a') as f:
            f.write(json.dumps(entry, default=str) + '\n')
    except Exception:
        pass  # never break queries for logging


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """Return the flex_search tool with dynamic description and schema."""
    return [
        types.Tool(
            name="flex_search",
            description=_build_tool_description(),
            inputSchema=_build_tool_schema(),
        )
    ]


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
        db.set_progress_handler(_check_timeout, 10000)  # check every 10K opcodes

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
    if name != "flex_search":
        return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    if not arguments or "query" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: query"}))]

    query = arguments["query"]
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
# HTTP Mode (streamable HTTP for Claude Code, claude.ai, Cursor)
# ============================================================

def run_http_server(port: int = 7134):
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    import uvicorn

    session_manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
        stateless=True,
    )

    async def handle_mcp(scope, receive, send):
        await session_manager.handle_request(scope, receive, send)

    async def health(request: Request) -> JSONResponse:
        return JSONResponse({
            "status": "ok",
            "cells": sorted(_known_cells),
            "on_disk": discover_cells(),
            "vec_cached": {k: list(v['caches'].keys()) for k, v in _vec_state.items()},
        })

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        # Start relay client in background if machine_id exists
        task = None
        if _machine_id():
            task = asyncio.create_task(_run_relay_client())
        async with session_manager.run():
            yield
        if task:
            task.cancel()

    app = Starlette(
        debug=False,
        lifespan=lifespan,
        routes=[
            Route("/health", health),
            Mount("/mcp", app=handle_mcp),
        ],
    )

    print(f"[flex-mcp] streamable-http on port {port}", file=sys.stderr)
    uvicorn.run(app, host="127.0.0.1", port=port)


# ============================================================
# Relay Client (outbound WS to getflex.dev)
# ============================================================

def _machine_id() -> str | None:
    """Read $FLEX_HOME/machine_id — generated by flex init. 8-char hex."""
    from flex.registry import FLEX_HOME
    p = FLEX_HOME / "machine_id"
    return p.read_text().strip() if p.exists() else None


async def _run_relay_client():
    """
    Maintain outbound WebSocket to the relay DO.

    Bridges MCP protocol over the WS:
      relay → ws.recv() → anyio read_stream → server.run()
      server.run() → anyio write_stream → ws.send() → relay → claude.ai SSE

    Reconnects automatically on disconnect.
    """
    import anyio
    import websockets
    from mcp.types import JSONRPCMessage
    from mcp.shared.session import SessionMessage

    machine_id = _machine_id()
    if not machine_id:
        print("[flex-mcp] No machine_id — relay disabled", file=sys.stderr)
        return

    url = f"wss://{machine_id}.getflex.dev/connect"
    print(f"[flex-mcp] Relay: {machine_id[:8]}...getflex.dev", file=sys.stderr)

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=30,
                open_timeout=10,
            ) as ws:
                print("[flex-mcp] Relay connected", file=sys.stderr)

                # anyio memory streams — MCP server reads/writes these
                read_send,  read_recv  = anyio.create_memory_object_stream(max_buffer_size=64)
                write_send, write_recv = anyio.create_memory_object_stream(max_buffer_size=64)

                async def ws_to_server():
                    """Relay → server: parse JSON-RPC, put in read stream."""
                    try:
                        async for raw in ws:
                            try:
                                msg = JSONRPCMessage.model_validate_json(raw)
                                await read_send.send(SessionMessage(message=msg))
                            except Exception as e:
                                print(f"[flex-mcp] Relay parse error: {e}", file=sys.stderr)
                    finally:
                        await read_send.aclose()

                async def server_to_ws():
                    """Server → relay: serialize JSON-RPC, send over WS."""
                    async with write_recv:
                        async for session_msg in write_recv:
                            try:
                                msg = session_msg.message if isinstance(session_msg, SessionMessage) else session_msg
                                await ws.send(
                                    msg.model_dump_json(by_alias=True, exclude_none=True)
                                )
                            except Exception:
                                break

                async with anyio.create_task_group() as tg:
                    tg.start_soon(ws_to_server)
                    tg.start_soon(server_to_ws)
                    tg.start_soon(
                        server.run,
                        read_recv,
                        write_send,
                        server.create_initialization_options(),
                    )

        except Exception as e:
            print(f"[flex-mcp] Relay disconnected ({e}), retrying in 5s", file=sys.stderr)
            await asyncio.sleep(5)


# ============================================================
# Main
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Flex MCP server")
    parser.add_argument("--cell", action="append", default=[],
                        help="Cell names to load (repeatable)")
    parser.add_argument("--no-embed", action="store_true",
                        help="Skip loading embeddings/VectorCache")
    parser.add_argument("--http", action="store_true",
                        help="Run as streamable HTTP server")
    parser.add_argument("--port", type=int, default=7134,
                        help="HTTP port (default: 7134)")
    args = parser.parse_args()

    global _no_embed
    _no_embed = args.no_embed

    # Discover cells: --cell flags override, otherwise scan filesystem
    if args.cell:
        cell_names = args.cell
    else:
        cell_names = discover_cells()
        print(f"[flex-mcp] Discovered {len(cell_names)} cells: {cell_names}", file=sys.stderr)

    _known_cells.update(cell_names)

    # Pre-warm VectorCaches (connections are ephemeral, caches persist)
    if not _no_embed:
        print(f"[flex-mcp] Warming VectorCaches...", file=sys.stderr)
        warm_all(cell_names)
    else:
        print("[flex-mcp] Skipping embeddings", file=sys.stderr)

    # Set instructions directly on the server — no private API access
    server.instructions = build_instructions()

    print(f"[flex-mcp] Ready — {len(_known_cells)} cells, {len(_vec_state)} cached", file=sys.stderr)

    if args.http:
        run_http_server(args.port)
    else:
        asyncio.run(_run_stdio())


async def _background_indexer():
    """Background task: scans for changed sessions and runs enrichment in stdio mode.

    Defense-in-depth. On platforms with a daemon (systemd/launchd), this
    finds nothing new. On platforms without a daemon (Windows, broken
    installs), this is the only thing keeping the cell fresh.
    """
    if not HAS_ENGINE:
        return

    from flex.registry import resolve_cell

    ENRICH_INTERVAL = 30 * 60  # 30 minutes
    POLL_INTERVAL = 2

    # Wait for cell to exist
    cell_path = None
    while cell_path is None:
        try:
            cell_path = resolve_cell("claude_code")
        except Exception:
            pass
        if cell_path is None or not cell_path.exists():
            cell_path = None
            await asyncio.sleep(5)

    print("[flex-mcp] Background indexer started", file=sys.stderr)

    loop = asyncio.get_running_loop()
    last_enrich = 0

    while True:
        try:
            # Drain queue
            await loop.run_in_executor(None, drain_queue, cell_path)

            # Enrichment every 30 minutes
            now = time.monotonic()
            if now - last_enrich >= ENRICH_INTERVAL:
                await loop.run_in_executor(None, run_enrichment, cell_path)
                last_enrich = now

            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[flex-mcp] bg indexer error: {e}", file=sys.stderr)
            await asyncio.sleep(POLL_INTERVAL)


async def _run_stdio():
    """Run the server over stdio transport with background indexer."""
    bg_task = asyncio.create_task(_background_indexer())
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream, write_stream, server.create_initialization_options()
            )
    finally:
        bg_task.cancel()
        try:
            await bg_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    main()
