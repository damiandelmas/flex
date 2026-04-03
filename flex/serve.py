"""
Flex MCP server entrypoint — transport + lifecycle orchestration.

Composes the query interface (mcp_server.py) with transport (stdio/HTTP)
and optional background lifecycle (daemon drain).

The query interface lives in mcp_server.py — pure read-only SQL execution.
This file handles everything around it: startup, warmup, transports.

Usage:
    python -m flex.serve                          # stdio (Claude Code)
    python -m flex.serve --http --port 7134       # streamable HTTP
    python -m flex.serve --no-embed               # skip VectorCache warmup
    python -m flex.serve --cell claude_code       # specific cells only
"""

import asyncio
import signal
import sys
import time


# ============================================================
# Background Indexer (defense-in-depth for platforms without daemon)
# ============================================================

async def _background_indexer():
    """Background task: scans for changed sessions and runs enrichment.

    Defense-in-depth. On platforms with a daemon (systemd/launchd), this
    finds nothing new. On platforms without a daemon (Windows, broken
    installs), this is the only thing keeping the cell fresh.
    """
    try:
        from flex.engine import drain_queue, drain_local_cells, run_enrichment
    except ImportError:
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
    last_chat_scan = 0
    CHAT_SCAN_INTERVAL = 30

    while True:
        try:
            await loop.run_in_executor(None, drain_queue, cell_path)

            now = time.monotonic()

            if now - last_chat_scan >= CHAT_SCAN_INTERVAL:
                await loop.run_in_executor(None, drain_local_cells)
                last_chat_scan = now

            if now - last_enrich >= ENRICH_INTERVAL:
                await loop.run_in_executor(None, run_enrichment, cell_path)
                last_enrich = now

            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[flex-mcp] bg indexer error: {e}", file=sys.stderr)
            await asyncio.sleep(POLL_INTERVAL)


# ============================================================
# Stdio Transport
# ============================================================

async def _run_stdio():
    """Run the server over stdio transport with background indexer."""
    from mcp.server.stdio import stdio_server
    from flex.mcp_server import get_server

    server = get_server()
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


# ============================================================
# HTTP Transport (streamable HTTP for Claude Code, claude.ai, Cursor)
# ============================================================

def run_http_server(port: int = 7134):
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    import uvicorn

    from flex.mcp_server import get_server, discover_cells, _vec_state, _known_cells

    server = get_server()

    session_manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
        stateless=True,
    )

    async def handle_mcp(scope, receive, send):
        await session_manager.handle_request(scope, receive, send)

    async def health(request: Request) -> JSONResponse:
        all_on_disk = discover_cells()
        # If cells were explicitly selected, only report those
        on_disk = sorted(set(all_on_disk) & _known_cells) if _known_cells != set(all_on_disk) else all_on_disk
        return JSONResponse({
            "status": "ok",
            "cells": sorted(_known_cells),
            "on_disk": on_disk,
            "vec_cached": {k: list(v['caches'].keys()) for k, v in _vec_state.items()},
        })

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        task = None
        try:
            from flex.ext import start_background
            task = asyncio.create_task(start_background())
        except (ImportError, Exception):
            pass
        async with session_manager.run():
            yield
        if task:
            task.cancel()

    from starlette.middleware.cors import CORSMiddleware

    app = Starlette(
        debug=False,
        lifespan=lifespan,
        routes=[
            Route("/health", health),
            Mount("/mcp", app=handle_mcp),
        ],
    )
    # Block cross-origin requests (prevents browser-based localhost attacks)
    app.add_middleware(CORSMiddleware, allow_origins=[])

    print(f"[flex-mcp] streamable-http on port {port}", file=sys.stderr)

    # Graceful shutdown: catch SIGTERM and exit 0 instead of letting
    # uvicorn exit 255 when SSE connections don't close cleanly.
    # This prevents systemd from logging "Failed with result 'exit-code'"
    # on every restart and keeps the MCP transport stable.
    _shutdown_event = asyncio.Event()

    def _handle_sigterm(signum, frame):
        print("[flex-mcp] SIGTERM received, shutting down gracefully", file=sys.stderr)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=port,
            timeout_keep_alive=300,
            limit_concurrency=20,
            limit_max_requests=10000,
        )
    except SystemExit as e:
        code = e.code if e.code is not None else 0
        print(f"[flex-mcp] exit {code}", file=sys.stderr)
        sys.exit(0 if code == 255 else code)


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

    from flex.mcp_server import init

    # Discover cells: --cell flags override, otherwise scan filesystem
    if args.cell:
        cell_names = args.cell
        active_names = args.cell  # explicit --cell = activate everything requested
    else:
        from flex.mcp_server import discover_cells
        from flex.registry import discover_active_cells
        cell_names = discover_cells()
        active_names = discover_active_cells()
        inactive_names = sorted(set(cell_names) - set(active_names))
        print(f"[flex-mcp] Discovered {len(cell_names)} cells: {cell_names}", file=sys.stderr)
        if inactive_names:
            print(f"[flex-mcp] Inactive (lazy-load): {inactive_names}", file=sys.stderr)

    init(cell_names, active_names=active_names, no_embed=args.no_embed)

    print(f"[flex-mcp] Ready", file=sys.stderr)

    if args.http:
        run_http_server(args.port)
    else:
        asyncio.run(_run_stdio())


if __name__ == "__main__":
    main()
