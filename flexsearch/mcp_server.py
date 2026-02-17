#!/usr/bin/env python3
"""
FlexSearch MCP Server — one tool, SQL endpoint.

The AI writes SQL. The server executes it read-only.
vec_ops registered as a function for semantic queries.

Usage:
    python -m flexsearch.mcp_server                          # stdio (Claude Code)
    python -m flexsearch.mcp_server --http --port 8080       # SSE  (claude.ai)
    python -m flexsearch.mcp_server --cell claude_code --cell qmem # multi-cell
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

from flexsearch.core import open_cell, get_meta
from flexsearch.registry import (
    resolve_cell as registry_resolve,
    discover_cells as registry_discover,
)

# ============================================================
# Configuration
# ============================================================

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
                        print(f"[flexsearch-mcp]   {name}: vec_cache {'refreshed' if state else 'warmed'}"
                              f" ({list(new_state['caches'].keys())})", file=sys.stderr)

        yield db
    finally:
        db.close()


def _register_udf(db: sqlite3.Connection, state: dict):
    """Register vec_ops UDF on a connection using cached VectorCache."""
    try:
        from flexsearch.retrieve.vec_ops import register_vec_ops
        embedder = _get_embedder()
        if embedder:
            register_vec_ops(db, state['caches'], embedder.encode, state['config'])
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
            from flexsearch.onnx import get_model
            _embedder = get_model()
            return _embedder
        except ImportError:
            print("[flexsearch-mcp] Embedding not available (onnx/transformers missing)", file=sys.stderr)
            return None


def _build_vec_state(name: str, db: sqlite3.Connection) -> dict | None:
    """Build VectorCache state for a cell. Returns state dict or None."""
    try:
        from flexsearch.retrieve.vec_ops import VectorCache
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
        print("[flexsearch-mcp] ONNX embedder warmed", file=sys.stderr)

    for name in cell_names:
        with get_cell(name):
            pass  # just warming the cache, context manager closes connection


def execute_preset(db: sqlite3.Connection, query: str) -> str:
    """Execute a @preset query from the cell's _presets table. Returns JSON string."""
    from flexsearch.retrieve.presets import PresetLoader

    parts = query[1:].split()
    preset_name = parts[0]

    # Alias common guesses to orient
    if preset_name in ('help', 'info', 'about', 'introspect', 'orientation'):
        preset_name = 'orient'
    params = {}
    for p in parts[1:]:
        if '=' in p:
            k, v = p.split('=', 1)
            try:
                params[k] = int(v)
            except ValueError:
                params[k] = v

    loader = PresetLoader(db)
    if preset_name not in loader.list_presets():
        available = loader.list_presets()
        return json.dumps({"error": f"Preset not found: {preset_name}",
                            "available": available})
    results = loader.execute(db, preset_name, params)
    return json.dumps(results, indent=2, default=str)


def execute_query(db: sqlite3.Connection, query: str) -> str:
    """Execute read-only SQL or @preset on a cell. Returns JSON string."""
    sql = query.strip()

    # Preset dispatch — @name [params]
    if sql.startswith('@'):
        return execute_preset(db, sql)

    upper = sql.upper()

    # Read-only enforcement
    write_keywords = ('INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
                      'ATTACH', 'DETACH', 'REINDEX', 'VACUUM')
    for kw in write_keywords:
        if upper.startswith(kw):
            return json.dumps({"error": f"Write operations not allowed: {kw}"})

    # Materialize vec_ops() table sources into temp tables
    from flexsearch.retrieve.vec_ops import materialize_vec_ops
    sql = materialize_vec_ops(db, sql)

    try:
        rows = db.execute(sql).fetchall()
        results = [dict(r) for r in rows]
        return json.dumps(results, indent=2, default=str)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        return json.dumps({"error": str(e)})


# ============================================================
# Build Instructions
# ============================================================

def build_instructions() -> str:
    """Build server instructions. The cell describes itself via @orient."""
    parts = [
        "FlexSearch indexes the USERS conversations and knowledge bases. "
        "Each cell is a self-describing SQLite database with chunks, embeddings, "
        "and graph intelligence. When the USER asks to 'flex' or 'flexsearch' their "
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
        "START: query=\"@orient\" for cell schema, columns, presets, and graph intelligence.",
        "",
        "QUERY PATTERN — three phases: SQL → vec_ops → SQL",
        "",
        "  Phase 1 — SQL pre-filter. Restricts which chunks enter the landscape.",
        "  Phase 2 — vec_ops. Numpy on the filtered landscape (similarity, diversity, decay).",
        "  Phase 3 — SQL composition. JOINs, graph boost, GROUP BY on candidates.",
        "",
        "  SELECT v.id, v.score, m.content",
        "  FROM vec_ops('_raw_chunks', 'authentication', 'diverse recent:7',",
        "    'SELECT chunk_id FROM _types_message WHERE role = ''user'''",
        "  ) v",
        "  JOIN messages m ON v.id = m.id",
        "  ORDER BY v.score * (1 + m.centrality) DESC",
        "  LIMIT 10",
        "",
        "vec_ops('table', 'query_text', 'tokens', 'pre_filter_sql')",
        "",
        "  PRE-FILTER SQL:",
        "    Any SQL returning chunk_ids. Runs before numpy touches anything.",
        "    Run @orient to discover filterable tables (_types_*, _edges_*, _enrich_*).",
        "",
        "    'SELECT chunk_id FROM _types_message WHERE role = ''user'''",
        "",
        "    Compound:",
        "    'SELECT t.chunk_id FROM _types_message t",
        "     JOIN _edges_source e ON t.chunk_id = e.chunk_id",
        "     JOIN _enrich_source_graph g ON e.source_id = g.source_id",
        "     WHERE t.role = ''user'' AND g.community_id = 3'",
        "",
        "  TOKENS (space-separated, all optional):",
        "    (no tokens)         raw cosine similarity — nearest neighbors, no reshaping",
        "    diverse             MMR diversity — spreads across subtopics, use for discovery",
        "    recent[:N]          temporal decay (optional N-day half-life)",
        "    unlike:TEXT         contrastive — demote similarity to TEXT",
        "    like:id1,id2,...    centroid of example chunks — \"more like these\"",
        "    from:TEXT to:TEXT   trajectory — direction through embedding space",
        "    local_communities   per-query Louvain, adds _community column",
        "    limit:N             candidate count (default 500)",
        "",
        "    Tokens compose freely:",
        "    vec_ops('_raw_chunks', 'auth', 'diverse unlike:jwt recent:7')",
        "",
        "RECIPES:",
        "",
        "  Nearest neighbors (raw cosine, no reshaping):",
        "    SELECT v.id, v.score, m.content",
        "    FROM vec_ops('_raw_chunks', 'YOUR TOPIC') v",
        "    JOIN messages m ON v.id = m.id",
        "    ORDER BY v.score DESC",
        "    LIMIT 10",
        "",
        "  Semantic discovery (diverse — spreads across subtopics):",
        "    SELECT v.id, v.score, m.content, m.project",
        "    FROM vec_ops('_raw_chunks', 'YOUR TOPIC', 'diverse recent:7') v",
        "    JOIN messages m ON v.id = m.id",
        "    ORDER BY v.score * (1 + m.centrality) DESC",
        "    LIMIT 10",
        "",
        "  Human voice only (filter to what the user actually said):",
        "    SELECT v.id, v.score, m.content",
        "    FROM vec_ops('_raw_chunks', 'YOUR TOPIC', 'diverse',",
        "      'SELECT chunk_id FROM _types_message WHERE role = ''user''') v",
        "    JOIN messages m ON v.id = m.id",
        "    LIMIT 10",
        "",
        "  Hub navigation (find the important sessions about X):",
        "    SELECT v.id, v.score, m.title, m.centrality",
        "    FROM vec_ops('_raw_chunks', 'YOUR TOPIC', 'diverse') v",
        "    JOIN messages m ON v.id = m.id",
        "    WHERE m.is_hub = 1",
        "    ORDER BY m.centrality DESC",
        "    LIMIT 5",
        "",
        "  Structural (when/how much — no embeddings needed):",
        "    SELECT project, COUNT(*) as sessions",
        "    FROM sessions GROUP BY project ORDER BY sessions DESC",
        "",
        "METHODOLOGY:",
        "  1. Schema first. Run @orient before writing any query.",
        "  2. SQL for 'when/how much', vec_ops for 'what about'. Don't embed-search what SQL can answer.",
        "  3. diverse for discovery, vanilla for precision. diverse spreads across subtopics. Omit it when you want actual nearest neighbors.",
        "  4. Pre-filter for human voice: SELECT chunk_id FROM _types_message WHERE role = 'user'",
        "  5. Pivot semantic → structural. vec_ops finds the neighborhood, graph columns navigate it.",
        "  6. Escalate specificity. COUNT(*) → GROUP BY → vec_ops on the interesting cluster.",
        "  7. Cross-cell triangulation. Design intent in context cells, implementation in claude_code.",
        "  8. ID prefix as date. WHERE v.id LIKE '260207%' — chunk IDs encode YYMMDD creation date.",
        "",
        "PRESETS: pass @name as query parameter. Run @orient to discover all presets per cell.",
        "",
    ])
    return "\n".join(parts)


# ============================================================
# Tool Description & Schema
# ============================================================

def _build_tool_description() -> str:
    """Build tool description. Instructions carry the real context."""
    return (
        "FlexSearch indexes the user's conversations and knowledge bases. "
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
        },
        "required": ["query"],
    }


# ============================================================
# MCP Server
# ============================================================

server = Server("flexsearch")


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
    """Return the flexsearch tool with dynamic description and schema."""
    return [
        types.Tool(
            name="flexsearch",
            description=_build_tool_description(),
            inputSchema=_build_tool_schema(),
        )
    ]


_QUERY_TIMEOUT_S = 30  # max seconds per query before cancellation


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


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    """Handle flexsearch tool calls. Runs DB work in executor to avoid blocking."""
    if name != "flexsearch":
        return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    if not arguments or "query" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: query"}))]

    query = arguments["query"]
    cell = arguments.get("cell", "claude_code")

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _execute_cell_query, cell, query)
    return [types.TextContent(type="text", text=result)]


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

    app = Starlette(
        debug=False,
        routes=[
            Route("/health", health),
            Route("/sse", handle_sse, methods=["GET"]),
            Mount("/messages", app=sse.handle_post_message),
        ],
    )

    print(f"[flexsearch-mcp] HTTP/SSE on port {port}", file=sys.stderr)
    uvicorn.run(app, host="127.0.0.1", port=port)


# ============================================================
# Main
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="FlexSearch MCP server")
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
        print(f"[flexsearch-mcp] Discovered {len(cell_names)} cells: {cell_names}", file=sys.stderr)

    _known_cells.update(cell_names)

    # Pre-warm VectorCaches (connections are ephemeral, caches persist)
    if not _no_embed:
        print(f"[flexsearch-mcp] Warming VectorCaches...", file=sys.stderr)
        warm_all(cell_names)
    else:
        print("[flexsearch-mcp] Skipping embeddings", file=sys.stderr)

    # Set instructions directly on the server — no private API access
    server.instructions = build_instructions()

    print(f"[flexsearch-mcp] Ready — {len(_known_cells)} cells, {len(_vec_state)} cached", file=sys.stderr)

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
