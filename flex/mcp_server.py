#!/usr/bin/env python3
"""
Flex MCP Server — one tool, SQL endpoint.

The AI writes SQL. The server executes it read-only.
vec_ops registered as a function for semantic queries.

Usage:
    python -m flex.mcp_server                          # stdio (Claude Code)
    python -m flex.mcp_server --http --port 8080       # SSE  (claude.ai)
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

from mcp.server.lowlevel import Server, NotificationOptions
from mcp.server.stdio import stdio_server
import mcp.types as types

from flex.core import open_cell, get_meta
from flex.registry import (
    resolve_cell as registry_resolve,
    discover_cells as registry_discover,
)

# ============================================================
# Configuration
# ============================================================

# SQLite authorizer for search pipe — whitelist read-only operations.
# Action codes: https://www.sqlite.org/c3ref/c_alter_table.html
_SQLITE_OK, _SQLITE_DENY = 0, 1
_SEARCH_ALLOW = {
    20,  # SQLITE_READ        — read column value
    21,  # SQLITE_SELECT      — SELECT statement
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

# VectorCache state — long-lived numpy matrices, independent of connections.
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

        if state and state['mtime'] == current_mtime:
            # Cache is fresh — just register UDF on this connection
            _register_udf(db, state)
        elif not _no_embed:
            # Cache missing or stale — rebuild (locked for HTTP concurrency)
            with _vec_lock:
                # Re-check after acquiring lock (another thread may have built it)
                state = _vec_state.get(name)
                if state and state['mtime'] == current_mtime:
                    _register_udf(db, state)
                else:
                    new_state = _build_vec_state(name, db)
                    if new_state:
                        _vec_state[name] = new_state
                        _register_udf(db, new_state)
                        print(f"[flex-mcp]   {name}: vec_cache {'refreshed' if state else 'warmed'}"
                              f" ({list(new_state['caches'].keys())})", file=sys.stderr)

        yield db
    finally:
        db.close()


def _register_udf(db: sqlite3.Connection, state: dict):
    """Register vec_ops UDF on a connection using cached VectorCache."""
    try:
        from flex.retrieve.vec_ops import register_vec_ops
        embedder = _get_embedder()
        if embedder:
            embed_query = lambda text: embedder.encode(text, prefix='search_query: ')
            embed_doc   = lambda text: embedder.encode(text, prefix='search_document: ')
            register_vec_ops(db, state['caches'], embed_query, state['config'],
                             embed_doc_fn=embed_doc)
    except ImportError:
        pass


def _read_vec_config(db) -> dict:
    """Read vec:* keys from _meta for modulation config.

    Returns dict of all _meta keys starting with 'vec:'.
    Example: {'vec:recent:half_life': '30'}
    """
    config = {}
    try:
        rows = db.execute(
            "SELECT key, value FROM _meta WHERE key LIKE 'vec:%'"
        ).fetchall()
        for row in rows:
            config[row[0]] = row[1]
    except Exception:
        pass
    return config


_embedder = None
_embedder_lock = threading.Lock()
_no_embed = False


def _get_embedder():
    """Lazy-load embedder singleton (double-checked locking for HTTP mode)."""
    global _embedder
    if _embedder is not None:
        return _embedder
    if _no_embed:
        return None
    with _embedder_lock:
        if _embedder is not None:
            return _embedder
        try:
            from flex.onnx import get_model
            _embedder = get_model()
            return _embedder
        except ImportError:
            print("[flex-mcp] Embedding not available (onnx/transformers missing)", file=sys.stderr)
            return None


def _build_vec_state(name: str, db: sqlite3.Connection) -> dict | None:
    """Build VectorCache state for a cell. Returns state dict or None."""
    try:
        from flex.retrieve.vec_ops import VectorCache
    except ImportError:
        return None

    caches = {}
    for table, id_col in [('_raw_chunks', 'id'), ('_raw_sources', 'source_id')]:
        try:
            cache = VectorCache()
            cache.load_from_db(db, table, 'embedding', id_col)
            if cache.size > 0:
                cache.load_columns(db, table, id_col)
                caches[table] = cache
        except Exception:
            pass

    if not caches:
        return None

    return {
        'caches': caches,
        'config': _read_vec_config(db),
        'mtime': _db_mtime(name),
    }


def warm_all(cell_names: list[str]):
    """Pre-warm VectorCaches and ONNX embedder at startup."""
    # Load ONNX model first — lazy singleton, ~2-3s cold
    embedder = _get_embedder()
    if embedder:
        # Force session init by encoding a dummy string
        embedder.encode("warmup")
        print("[flex-mcp] ONNX embedder warmed", file=sys.stderr)

    for name in cell_names:
        with get_cell(name):
            pass  # just warming the cache, context manager closes connection


def execute_preset(db: sqlite3.Connection, query: str) -> str:
    """Execute a @preset query from the cell's _presets table. Returns JSON string."""
    from flex.retrieve.presets import PresetLoader

    parts = query[1:].split()
    preset_name = parts[0]

    # Alias common guesses to orient
    if preset_name in ('help', 'info', 'about', 'introspect', 'orientation'):
        preset_name = 'orient'
    params = {}
    positional = []
    for p in parts[1:]:
        if '=' in p:
            k, v = p.split('=', 1)
            try:
                params[k] = int(v)
            except ValueError:
                params[k] = v
        else:
            positional.append(p)

    loader = PresetLoader(db)
    if preset_name not in loader.list_presets():
        available = loader.list_presets()
        return json.dumps({"error": f"Preset not found: {preset_name}",
                            "available": available})

    # Bind positional args to required params (in declaration order)
    if positional:
        preset = loader.load(preset_name)
        param_str = preset.get('params', '')
        if param_str:
            declared = [p.strip().split()[0] for p in param_str.split(',')]
            for name, value in zip(declared, positional):
                if name not in params:
                    try:
                        params[name] = int(value)
                    except ValueError:
                        params[name] = value

    results = loader.execute(db, preset_name, params)
    return json.dumps(results, indent=2, default=str)


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
                f"SELECT c.id, c.content "
                f"FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid "
                f"WHERE chunks_fts MATCH '{escaped}' "
                f"ORDER BY bm25(chunks_fts) LIMIT 10"
            ),
        })

    upper = sql.upper()

    # Cross-cell ATTACH — resolve cell names to registry paths before blocklist
    if 'ATTACH' in upper:
        sql, err = _execute_attaches(db, sql)
        if err:
            return json.dumps({"error": err})
        upper = sql.upper()

    # Materialize vec_ops() table sources into temp tables
    # (runs BEFORE authorizer — trusted code that needs temp table writes)
    from flex.retrieve.vec_ops import materialize_vec_ops
    sql = materialize_vec_ops(db, sql)

    # vec_ops returned an error (bad pre-filter, missing column, etc)
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

    # --- Hardcoded: query-writer instructions ---
    parts.extend([
        "",
        "# RETRIEVAL",
        "",
        "Flex offers a single endpoint for all operations: mcp__flex__flex_search. Two parameters:",
        "query (SQL or @preset) and cell (cell name). You compose queries using the views, columns,",
        "and graph columns available for that cell, written in standard SQL. The retrieval pipeline",
        "has three phases: SQL Pre-Filter → Vector Operations (vec_ops) → SQL Composition.",
        "The ergonomics are as expected for SQL. Use your native intuition to compose elegant queries.",
        "",
        "The three phases run in order. Phase one narrows the corpus to a candidate set before",
        "touching any embeddings. Phase two performs vector operations (semantic similarity) on",
        "those candidates to rank and reshape the set. Phase three composes the ranked candidates",
        "into the final result using full SQL.",
        "",
        "## PHASE 1: SQL PRE-FILTER",
        "",
        "Narrows the candidate set before any embedding work. Free — no vectors.",
        "You already know something: a session, a tool, a date, a message type. Cut with it.",
        "Pre-filters use view vocabulary — query messages and sessions, not raw _ tables.",
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
        "Without tokens, vec_ops is vanilla cosine similarity. Tokens modify the scoring before",
        "selection — each one is an independent operation, they compose freely. Returns (id, score)",
        "pairs that SQL can join, boost, filter, and paginate.",
        "",
        "`vec_ops('_raw_chunks', 'query_text', 'tokens', 'pre_filter_sql')`",
        "",
        "**IMPORTANT:** vec_ops is a table source — always use after FROM or JOIN:",
        "```sql",
        "FROM vec_ops('_raw_chunks', 'query') v",
        "```",
        "Never use as a scalar: `SELECT vec_ops(...)` will fail.",
        "",
        "### Modulation Tokens",
        "",
        "| Token | Effect |",
        "|-------|--------|",
        "| `diverse` | MMR — spread results across subtopics |",
        "| `recent[:N]` | temporal decay — recent chunks score higher (N = half-life days) |",
        "| `unlike:TEXT` | contrastive — demote chunks similar to TEXT |",
        "| `like:id1,id2,...` | centroid — find chunks similar to these examples |",
        "| `from:TEXT to:TEXT` | trajectory — direction through embedding space |",
        "| `limit:N` | candidate pool size (default 500) |",
        "",
        "Tokens compose freely: `'diverse unlike:jwt recent:7'`",
        "",
        "### Edge Cases",
        "",
        "- `diverse` is boolean — `diverse:0.5` has no effect, just use `diverse`",
        "- `recent:0` disables temporal decay (not 'zero days') — scores increase",
        "- `limit:0` falls back to default (500), not zero results",
        "- Only ONE vec_ops per query — for multiple, use CTEs:",
        "  `WITH a AS (SELECT * FROM vec_ops(...) v) SELECT * FROM a`",
        "- Some sessions have NULL centrality/community — not all enter the graph. Use COALESCE or LEFT JOIN.",
        "",
        "## PHASE 3: SQL COMPOSITION",
        "",
        "Full SQL on the ranked candidates. Join back to views. Boost, filter, group, paginate.",
        "Hub/bridge reranking lives here. Graph arithmetic lives here.",
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
        "`local_communities` — per-query Louvain, adds `_community` column to candidates:",
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
        "A starting set. Mix and match for the purpose at hand.",
        "",
        "**Structural** (when/how much — no embeddings needed):",
        "```sql",
        "SELECT project, COUNT(*) as sessions",
        "FROM sessions GROUP BY project ORDER BY sessions DESC",
        "```",
        "",
        "**Exact term** (FTS5 — domain name, filename, error, UUID):",
        "```sql",
        "SELECT c.id, c.content",
        "FROM chunks_fts JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid",
        "WHERE chunks_fts MATCH 'term'",
        "ORDER BY bm25(chunks_fts) LIMIT 10",
        "-- wrap terms with special chars in double-quotes: MATCH '\"axp.systems\"'",
        "```",
        "",
        "**Hybrid FTS + vec_ops** (combine keyword match with semantic ranking):",
        "",
        "IMPORTANT: FTS returns integer rowids, vec_ops returns string IDs.",
        "Bridge through `_raw_chunks` to convert between them:",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC', '',",
        "  'SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid",
        "   WHERE f MATCH ''term''') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**Nearest neighbors:**",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**Within a session:**",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC', '',",
        "  'SELECT id FROM messages WHERE session_id LIKE ''abc123%''') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**User intent only** (what the human asked/decided, not tool output):",
        "```sql",
        "SELECT v.id, v.score, m.content",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC', '',",
        "  'SELECT id FROM messages WHERE type = ''user_prompt''') v",
        "JOIN messages m ON v.id = m.id LIMIT 10",
        "```",
        "",
        "**Broad discovery** (survey a topic across subtopics):",
        "```sql",
        "SELECT v.id, v.score, m.content, m.project",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC', 'diverse') v",
        "JOIN messages m ON v.id = m.id",
        "ORDER BY v.score DESC LIMIT 10",
        "```",
        "",
        "**Hub navigation** (most connected sessions on a topic):",
        "```sql",
        "SELECT v.id, v.score, s.title, s.centrality",
        "FROM vec_ops('_raw_chunks', 'YOUR TOPIC') v",
        "JOIN messages m ON v.id = m.id",
        "JOIN sessions s ON m.session_id = s.session_id",
        "WHERE s.is_hub = 1",
        "ORDER BY s.centrality DESC LIMIT 5",
        "```",
        "",
        "**Filter by community** (scope to a project neighborhood):",
        "```sql",
        "SELECT session_id, title, fingerprint_index",
        "FROM sessions",
        "WHERE community_id = 29",
        "ORDER BY centrality DESC LIMIT 10",
        "```",
        "",
        "**Filter by session:**",
        "`WHERE m.session_id = 'd332a1a0-...'` or `WHERE m.session_id LIKE 'd332a1a0%'`",
        "",
        "**File dedup** (SOMA — collapse renames to one identity):",
        "`GROUP BY COALESCE(json_extract(m.file_uuids, '$[0]'), m.target_file)`",
        "",
        "**Session drill-down:**",
        "`@story session=d332a1a0`",
        "",
        "# METHODOLOGY",
        "",
        "Start with `@orient`. Every cell describes itself — shape, schema, views, communities, hubs, presets.",
        "",
        "**Feel the data** before writing complex queries.",
        "What projects exist? What date range? How many sessions?",
        "`GROUP BY` / `COUNT(*)` / `SELECT DISTINCT` is free — no embeddings.",
        "",
        "**Discover then narrow.**",
        "Broad vec_ops → discover themes → pre-filter the next query with what you found.",
        "Push known constraints (date, session, type, community) into the vec_ops 4th argument, not WHERE.",
        "",
        "**Pivot when the mode shifts.**",
        "Found a theme? Count it. Group it. Quantify.",
        "Found an ID? Switch to exact retrieval — JOIN and ORDER BY position.",
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
                error_msg = json.dumps({"error": f"OperationalError: {e}"})
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
# HTTP/SSE Mode (for claude.ai)
# ============================================================

def run_http_server(port: int = 8080):
    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    import uvicorn

    sse = SseServerTransport("/messages")

    async def handle_sse(request: Request) -> Response:
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await server.run(
                streams[0], streams[1], server.create_initialization_options()
            )
        return Response()

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
        yield
        if task:
            task.cancel()

    app = Starlette(
        debug=False,
        lifespan=lifespan,
        routes=[
            Route("/health", health),
            Route("/sse", handle_sse, methods=["GET"]),
            Mount("/messages", app=sse.handle_post_message),
        ],
    )

    print(f"[flex-mcp] HTTP/SSE on port {port}", file=sys.stderr)
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
                        help="Run as HTTP/SSE server (for claude.ai)")
    parser.add_argument("--port", type=int, default=8080,
                        help="HTTP port (default: 8080)")
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


async def _run_stdio():
    """Run the server over stdio transport."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, write_stream, server.create_initialization_options()
        )


if __name__ == "__main__":
    main()
