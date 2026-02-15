#!/usr/bin/env python3
"""
FlexSearch MCP Server — one tool, SQL endpoint.

The AI writes SQL. The server executes it read-only.
vec_search registered as a function for semantic queries.

Usage:
    python -m flexsearch.mcp_server                          # stdio (Claude Code)
    python -m flexsearch.mcp_server --http --port 8080       # SSE  (claude.ai)
    python -m flexsearch.mcp_server --cell thread --cell qmem # multi-cell
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

# ============================================================
# Configuration
# ============================================================

CELLS_ROOT = Path.home() / ".qmem/cells/projects"

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
    """Scan CELLS_ROOT for directories containing main.db."""
    if not CELLS_ROOT.exists():
        return []
    return sorted(
        d.name for d in CELLS_ROOT.iterdir()
        if d.is_dir() and (d / "main.db").exists()
    )


def _db_path(name: str) -> Path:
    return CELLS_ROOT / name / "main.db"


def _db_mtime(name: str) -> float:
    """Get mtime of cell db file. Returns 0 if missing."""
    p = _db_path(name)
    return p.stat().st_mtime if p.exists() else 0


@contextmanager
def get_cell(name: str):
    """Open a fresh connection to a cell. Registers vec_search UDF if cached.

    Yields None if cell doesn't exist on disk.
    Fresh connection every call = always see latest data.
    Usage: with get_cell('thread') as db: ...
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
    """Register vec_search UDF on a connection using cached VectorCache."""
    try:
        from flexsearch.retrieve.vec_search import register_vec_search
        embedder = _get_embedder()
        if embedder:
            register_vec_search(db, state['caches'], embedder.encode, state['config'])
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
        from flexsearch.retrieve.vec_search import VectorCache
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
    """Pre-warm VectorCaches for all cells at startup."""
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

    # Materialize vec_search() table sources into temp tables
    from flexsearch.retrieve.vec_search import materialize_vec_search
    sql = materialize_vec_search(db, sql)

    try:
        rows = db.execute(sql).fetchall()
        results = [dict(r) for r in rows]
        return json.dumps(results, indent=2, default=str)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        return json.dumps({"error": str(e)})


# ============================================================
# Build Instructions
# ============================================================

def _build_retrieval_instructions(db) -> list[str]:
    """Build retrieval section from _meta retrieval:* keys, with fallback."""
    parts = []

    # Read retrieval contract from cell
    phase1 = get_meta(db, 'retrieval:phase1')
    phase2 = get_meta(db, 'retrieval:phase2')
    phase3 = get_meta(db, 'retrieval:phase3')

    if phase1 and phase2 and phase3:
        # Strip phase label prefix from values (they're standalone in _meta
        # but the formatted rendering adds its own labels)
        def _strip_prefix(val):
            """'PRE-SELECTION masks (numpy on full N): foo' → 'foo'"""
            if ': ' in val:
                _, rest = val.split(': ', 1)
                return rest
            return val

        parts.extend([
            "RETRIEVAL (3 phases — vec_search is numpy on full N, SQL composes after):",
            "",
            "  Phase 1 — PRE-SELECTION (masks before scoring):",
            f"    {_strip_prefix(phase1)}",
            "",
            "  Phase 2 — LANDSCAPE (score modulation on full N):",
            f"    {_strip_prefix(phase2)}",
            "",
            "  Phase 3 — SQL (full AI control on K candidates):",
            f"    {_strip_prefix(phase3)}",
        ])
    else:
        # Fallback for cells without retrieval keys yet
        parts.extend([
            "SEMANTIC SEARCH:",
            "  vec_search('table', 'query', 'modifiers') → (id, score)",
            "  Modifiers (3rd arg, space-separated, composable):",
            "    community:N  kind:TYPE  limit:N     — pre-selection masks",
            "    recent[:N]  diverse  unlike:TEXT          — score modulation",
            "  JOIN messages m ON v.id = m.id         — full SQL after",
        ])

    return parts


def build_instructions() -> str:
    """Build server instructions from cell _meta. The cell describes itself."""
    parts = [
        "FlexSearch — SQL-first knowledge engine. Execute read-only SQL on knowledge cells.",
        "",
        "CELLS:",
    ]
    # Open fresh connections to read descriptions, then close
    retrieval_parts = None
    for name in sorted(_known_cells):
        with get_cell(name) as db:
            if db:
                desc = get_meta(db, 'description') or f"Cell: {name}"
                parts.append(f"  {name}: {desc}")
                if retrieval_parts is None:
                    retrieval_parts = _build_retrieval_instructions(db)

    parts.extend([
        "",
        "ORIENTATION:",
        "  SELECT value FROM _meta WHERE key='description'       # What is this cell?",
        "  SELECT name FROM sqlite_master WHERE type='view'       # What views exist?",
        "  PRAGMA table_info('messages')                          # View schema",
        "  SELECT name FROM sqlite_master WHERE name LIKE '_edges_%'  # Edge tables",
        "",
    ])

    # Build retrieval section from first cell that has keys (all cells share the model)
    if retrieval_parts:
        parts.extend(retrieval_parts)
    parts.append("")

    parts.extend([
        "SEMANTIC SEARCH:",
        "  SELECT v.id, v.score, m.content",
        "  FROM vec_search('_raw_chunks', 'your query') v",
        "  JOIN messages m ON v.id = m.id",
        "  ORDER BY v.score DESC LIMIT 10",
        "",
        "  No modifiers = raw cosine similarity.",
        "  vec_search('_raw_chunks', 'auth', 'recent:7 diverse')",
        "  vec_search('_raw_chunks', 'SOMA identity', 'kind:delegation community:17')",
        "",
        "HYBRID (FTS + semantic + graph):",
        "  vec_search('_raw_chunks', 'query')                    # Semantic candidates",
        "  chunks_fts MATCH 'keyword'                             # FTS keyword search",
        "  WHERE is_hub = 1 ORDER BY centrality DESC              # Graph intelligence",
        "",
        "PRESETS (pass @name instead of SQL — batched multi-query, saves round trips):",
        "  @orient                             # Full cell orientation (always available)",
        "  SELECT name, description FROM _presets  # Discover all presets",
        "",
        "IMPORTANT: Presets are invoked by passing @name as the query parameter.",
        "  Examples: query=\"@orient\", query=\"@sessions limit=5\", query=\"@genealogy concept=caching\"",
        "  Start with @orient to orient, then SELECT name, description FROM _presets to see all available.",
        "",
    ])
    return "\n".join(parts)


# ============================================================
# Tool Description & Schema
# ============================================================

def _build_tool_description() -> str:
    """Build tool description. Instructions carry the real context."""
    return (
        "SQL-first knowledge engine. Each cell is a self-describing SQLite database "
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
                "description": "SQL query, @preset name, or vec_search expression",
            },
            "cell": {
                "type": "string",
                "description": "Knowledge cell to query",
                "default": "thread",
                "enum": cell_list if cell_list else ["thread"],
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
        history_path = CELLS_ROOT / cell / "flexsearch-history.jsonl"
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


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    """Handle flexsearch tool calls."""
    if name != "flexsearch":
        return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    if not arguments or "query" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: query"}))]

    query = arguments["query"]
    cell = arguments.get("cell", "thread")

    with get_cell(cell) as db:
        if db is None:
            available = sorted(_known_cells)
            on_disk = set(discover_cells()) - set(available)
            msg = {"error": f"Unknown cell: {cell}", "available": available}
            if on_disk:
                msg["also_on_disk"] = sorted(on_disk)
            return [types.TextContent(type="text", text=json.dumps(msg))]

        try:
            start = time.monotonic()
            result = execute_query(db, query)
            elapsed_ms = (time.monotonic() - start) * 1000
            _log_query(cell, query, result, elapsed_ms)
            return [types.TextContent(type="text", text=result)]
        except Exception as e:
            error_msg = json.dumps({"error": f"{type(e).__name__}: {e}"})
            _log_query(cell, query, error_msg, (time.monotonic() - start) * 1000)
            return [types.TextContent(type="text", text=error_msg)]


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
    uvicorn.run(app, host="0.0.0.0", port=port)


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
