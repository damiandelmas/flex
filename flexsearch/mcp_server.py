#!/usr/bin/env python3
"""
Flexsearch MCP Server — one tool, SQL endpoint.

The AI writes SQL. The server executes it read-only.
vec_search registered as a function for semantic queries.

Usage:
    python -m flexsearch.mcp_server                          # stdio (Claude Code)
    python -m flexsearch.mcp_server --http --port 8080       # SSE  (claude.ai)
    python -m flexsearch.mcp_server --cell thread --cell qmem # multi-cell
"""

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from flexsearch.core import open_cell, get_meta

# ============================================================
# Configuration
# ============================================================

CELLS_ROOT = Path.home() / ".qmem/cells/projects"
DEFAULT_CELLS = ['thread', 'claude', 'qmem', 'inventory', 'thread-codebase', 'flexsearch-context', 'axpstack-context']

# ============================================================
# Cell Management
# ============================================================

_cells: dict[str, sqlite3.Connection] = {}
_caches: dict = {}


def resolve_cell(name: str) -> Path:
    """Resolve cell name to db path."""
    db_path = CELLS_ROOT / name / "main.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Cell not found: {db_path}")
    return db_path


def load_cells(cell_names: list[str]) -> dict:
    """Load cell connections. Returns {name: connection}."""
    cells = {}
    for name in cell_names:
        try:
            db_path = resolve_cell(name)
            cells[name] = open_cell(str(db_path))
            print(f"[flexsearch-mcp]   {name}: ok", file=sys.stderr)
        except Exception as e:
            print(f"[flexsearch-mcp]   {name}: {e}", file=sys.stderr)
    return cells


def _read_vec_config(db) -> dict:
    """Read vec:* keys from _meta for modulation config.

    Returns dict of all _meta keys starting with 'vec:'.
    Example: {'vec:hubs:weight': '1.3', 'vec:recent:half_life': '30'}
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


def warm_caches(cells: dict) -> dict:
    """Load VectorCaches for all cells. Returns {name: {table: cache}}."""
    try:
        from flexsearch.retrieve.vec_search import VectorCache, register_vec_search
        from flexsearch.onnx import get_model
        embedder = get_model()
    except ImportError:
        print("[flexsearch-mcp] Embedding not available (onnx/transformers missing)", file=sys.stderr)
        return {}

    all_caches = {}
    for name, db in cells.items():
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
        if caches:
            cell_config = _read_vec_config(db)
            register_vec_search(db, caches, embedder.encode, cell_config)
            all_caches[name] = caches
    return all_caches


def execute_preset(db: sqlite3.Connection, query: str) -> str:
    """Execute a @preset query from the cell's _presets table. Returns JSON string."""
    from flexsearch.retrieve.presets import PresetLoader

    parts = query[1:].split()
    preset_name = parts[0]
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
    except sqlite3.OperationalError as e:
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
            "    hubs  recent[:N]  diverse  unlike:TEXT  — score modulation",
            "  JOIN messages m ON v.id = m.id         — full SQL after",
        ])

    return parts


def build_instructions() -> str:
    """Build server instructions from cell _meta. The cell describes itself."""
    parts = [
        "Flexsearch — SQL-first knowledge engine. Execute read-only SQL on knowledge cells.",
        "",
        "CELLS:",
    ]
    for name, db in _cells.items():
        desc = get_meta(db, 'description') or f"Cell: {name}"
        parts.append(f"  {name}: {desc}")

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
    for db in _cells.values():
        parts.extend(_build_retrieval_instructions(db))
        break
    parts.append("")

    parts.extend([
        "SEMANTIC SEARCH:",
        "  SELECT v.id, v.score, m.content",
        "  FROM vec_search('_raw_chunks', 'your query') v",
        "  JOIN messages m ON v.id = m.id",
        "  ORDER BY v.score DESC LIMIT 10",
        "",
        "  No modifiers = raw cosine similarity.",
        "  vec_search('_raw_chunks', 'auth', 'hubs recent:7 diverse')",
        "  vec_search('_raw_chunks', 'SOMA identity', 'kind:delegation community:17 hubs')",
        "",
        "HYBRID (FTS + semantic + graph):",
        "  vec_search('_raw_chunks', 'query')                    # Semantic candidates",
        "  chunks_fts MATCH 'keyword'                             # FTS keyword search",
        "  WHERE is_hub = 1 ORDER BY centrality DESC              # Graph intelligence",
        "",
        "PRESETS (pass @name instead of SQL — batched multi-query, saves round trips):",
        "  @introspect                        # Full cell orientation (always available)",
        "  SELECT name, description FROM _presets  # Discover all presets",
        "",
        "IMPORTANT: Presets are invoked by passing @name as the query parameter.",
        "  Examples: query=\"@orient\", query=\"@sessions limit=5\", query=\"@genealogy concept=caching\"",
        "  Start with @introspect to orient, then SELECT name, description FROM _presets to see all available.",
        "",
    ])
    return "\n".join(parts)


# ============================================================
# MCP Server
# ============================================================

mcp = FastMCP(name="flexsearch")


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


@mcp.tool()
def flexsearch(query: str, cell: str = "thread") -> str:
    """
    Execute read-only SQL on a knowledge cell.

    Args:
        query: SQL query string
        cell: Cell name (thread, claude, qmem, inventory, thread-codebase)
    """
    if cell not in _cells:
        available = list(_cells.keys())
        return json.dumps({"error": f"Unknown cell: {cell}", "available": available})

    start = time.monotonic()
    result = execute_query(_cells[cell], query)
    elapsed_ms = (time.monotonic() - start) * 1000
    _log_query(cell, query, result, elapsed_ms)
    return result


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
            await mcp._mcp_server.run(
                streams[0], streams[1], mcp._mcp_server.create_initialization_options()
            )
        return Response()

    async def health(request: Request) -> JSONResponse:
        return JSONResponse({
            "status": "ok",
            "cells": list(_cells.keys()),
            "caches": {k: list(v.keys()) for k, v in _caches.items()} if _caches else {}
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
    global _cells, _caches

    parser = argparse.ArgumentParser(description="Flexsearch MCP server")
    parser.add_argument("--cell", action="append", default=[],
                        help="Cell names to load (repeatable)")
    parser.add_argument("--no-embed", action="store_true",
                        help="Skip loading embeddings/VectorCache")
    parser.add_argument("--http", action="store_true",
                        help="Run as HTTP/SSE server (for claude.ai)")
    parser.add_argument("--port", type=int, default=8080,
                        help="HTTP port (default: 8080)")
    args = parser.parse_args()

    cell_names = args.cell or DEFAULT_CELLS

    # Load cells
    print(f"[flexsearch-mcp] Loading cells...", file=sys.stderr)
    _cells = load_cells(cell_names)
    print(f"[flexsearch-mcp] {len(_cells)} cells loaded", file=sys.stderr)

    # Update instructions with loaded cells
    mcp._mcp_server.instructions = build_instructions()

    # Warm caches
    if not args.no_embed:
        print(f"[flexsearch-mcp] Warming VectorCaches...", file=sys.stderr)
        _caches = warm_caches(_cells)
        if _caches:
            for name, c in _caches.items():
                tables = list(c.keys())
                print(f"[flexsearch-mcp]   {name}: {tables}", file=sys.stderr)
    else:
        print("[flexsearch-mcp] Skipping embeddings", file=sys.stderr)

    print(f"[flexsearch-mcp] Ready", file=sys.stderr)

    if args.http:
        run_http_server(args.port)
    else:
        mcp.run()


if __name__ == "__main__":
    main()
