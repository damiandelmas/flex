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
import os
import signal
import sys
import threading


def _start_daemon_thread(target, *args, name: str):
    """Run process-local background work without delaying process exit.

    ``asyncio.to_thread`` uses non-daemon executor threads. A long vector-cache
    warmup can therefore keep Python alive after Uvicorn has shut down, until
    systemd kills the otherwise-finished service. Warmup state is expendable
    process-local cache state, so a daemon thread is the correct lifecycle.
    """
    thread = threading.Thread(target=target, args=args, name=name, daemon=True)
    thread.start()
    return thread


# ============================================================
# Stdio Transport
# ============================================================

async def _run_stdio(active_names: list[str] | None = None, no_embed: bool = False):
    """Run the server over stdio transport."""
    from mcp.server.stdio import stdio_server
    from flex.mcp_server import get_server, warm_cells

    server = get_server()
    async with stdio_server() as (read_stream, write_stream):
        if active_names and not no_embed:
            _start_daemon_thread(warm_cells, active_names, name="flex-vector-warmup")
        await server.run(
            read_stream, write_stream, server.create_initialization_options()
        )


# ============================================================
# HTTP Transport (streamable HTTP for Claude Code, claude.ai, Cursor)
# ============================================================

def run_http_server(port: int = 7134, active_names: list[str] | None = None, no_embed: bool = False):
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Route, Mount
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.requests import ClientDisconnect
    import anyio
    import uvicorn

    from flex.mcp_server import (
        get_server, discover_cells, get_warmup_state, warm_cells,
        _vec_state, _known_cells,
    )

    server = get_server()

    session_manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
        stateless=True,
    )

    async def handle_mcp(scope, receive, send):
        # MCP SDK 1.26.0 raises ExceptionGroup[ClosedResourceError] when the
        # client drops mid-request (tool timeout, /exit, window switch). The
        # inner session TaskGroup crashes and tears down the SSE stream.
        # Swallow these so only the single request dies, not the transport.
        try:
            await session_manager.handle_request(scope, receive, send)
        except* (anyio.ClosedResourceError, ClientDisconnect) as eg:
            print(
                f"[flex-mcp] client gone mid-request "
                f"({len(eg.exceptions)} inner)",
                file=sys.stderr,
            )

    async def health(request: Request) -> JSONResponse:
        from flex.health import refresh_summary, watcher_summary

        all_on_disk = discover_cells()
        # If cells were explicitly selected, only report those
        on_disk = sorted(set(all_on_disk) & _known_cells) if _known_cells != set(all_on_disk) else all_on_disk
        refresh = refresh_summary()
        warmup = get_warmup_state()
        try:
            watcher = watcher_summary()
        except Exception:
            watcher = {"status": "polling", "reason": "watcher summary unavailable"}
        status = "degraded" if (
            refresh.get("status") == "degraded"
            or warmup.get("status") == "error"
            or watcher.get("status") == "degraded"
        ) else "ok"
        return JSONResponse({
            "status": status,
            "cells": sorted(_known_cells),
            "on_disk": on_disk,
            "vec_cached": {k: list(v['caches'].keys()) for k, v in _vec_state.items()},
            "warmup": warmup,
            "refresh": refresh,
            "watcher": watcher,
        })

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        task = None
        warm_thread = None
        if active_names and not no_embed:
            warm_thread = _start_daemon_thread(
                warm_cells, active_names, name="flex-vector-warmup"
            )
        try:
            from flex.ext import start_background
            task = asyncio.create_task(start_background())
        except (ImportError, Exception):
            pass
        async with session_manager.run():
            yield
        for t in (task,):
            if t:
                t.cancel()

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
    def _handle_sigterm(signum, frame):
        print("[flex-mcp] SIGTERM received, shutting down gracefully", file=sys.stderr)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    http_concurrency = int(os.environ.get("FLEX_HTTP_CONCURRENCY", "200"))
    uvicorn_kwargs = {
        "host": "127.0.0.1",
        "port": port,
        "timeout_keep_alive": 120,
        "timeout_graceful_shutdown": 1,
    }
    if http_concurrency > 0:
        uvicorn_kwargs["limit_concurrency"] = http_concurrency

    try:
        uvicorn.run(app, **uvicorn_kwargs)
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
    parser.add_argument("--prewarm", action="store_true",
                        help="Prewarm all active cell VectorCaches at startup")
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
        restrict_to_cells = True
    else:
        from flex.mcp_server import discover_cells
        cell_names = discover_cells()
        active_names = cell_names
        restrict_to_cells = False
        print(f"[flex-mcp] Discovered {len(cell_names)} cells: {cell_names}", file=sys.stderr)

    # An unrestricted server may expose dozens of cells. Warming every matrix
    # eagerly defeats the LRU budget because allocator/RSS pressure survives
    # individual evictions. Keep broad servers lazy; explicit --cell servers
    # remain warm by default, and --prewarm is the opt-in for bulk warming.
    warm_names = active_names if (args.cell or args.prewarm) else []

    init(
        cell_names,
        active_names=warm_names,
        no_embed=args.no_embed,
        warm=False,
        restrict_to_cells=restrict_to_cells,
    )

    print(f"[flex-mcp] Ready", file=sys.stderr)

    if args.http:
        run_http_server(args.port, active_names=warm_names, no_embed=args.no_embed)
    else:
        asyncio.run(_run_stdio(active_names=warm_names, no_embed=args.no_embed))


if __name__ == "__main__":
    main()
