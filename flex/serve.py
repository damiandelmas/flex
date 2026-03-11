"""
Flex MCP server entrypoint — transport + lifecycle orchestration.

Composes the query interface (mcp_server.py) with transport (stdio/HTTP)
and optional background lifecycle (daemon drain, relay).

The query interface lives in mcp_server.py — pure read-only SQL execution.
This file handles everything around it: startup, warmup, transports, relay.

Usage:
    python -m flex.serve                          # stdio (Claude Code)
    python -m flex.serve --http --port 7134       # streamable HTTP
    python -m flex.serve --no-embed               # skip VectorCache warmup
    python -m flex.serve --cell claude_code       # specific cells only
"""

import asyncio
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
        from flex.engine import drain_queue, drain_claude_chat, run_enrichment
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
                await loop.run_in_executor(None, drain_claude_chat)
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
        return JSONResponse({
            "status": "ok",
            "cells": sorted(_known_cells),
            "on_disk": discover_cells(),
            "vec_cached": {k: list(v['caches'].keys()) for k, v in _vec_state.items()},
        })

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
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
    """Maintain outbound WebSocket to the relay DO.

    Bridges MCP protocol over the WS:
      relay → ws.recv() → anyio read_stream → server.run()
      server.run() → anyio write_stream → ws.send() → relay → claude.ai SSE

    Reconnects automatically on disconnect.
    """
    import anyio
    import websockets
    from mcp.types import JSONRPCMessage
    from mcp.shared.session import SessionMessage
    from flex.mcp_server import get_server

    server = get_server()

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

                read_send,  read_recv  = anyio.create_memory_object_stream(max_buffer_size=64)
                write_send, write_recv = anyio.create_memory_object_stream(max_buffer_size=64)

                async def ws_to_server():
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

    from flex.mcp_server import init

    # Discover cells: --cell flags override, otherwise scan filesystem
    if args.cell:
        cell_names = args.cell
    else:
        from flex.mcp_server import discover_cells
        cell_names = discover_cells()
        print(f"[flex-mcp] Discovered {len(cell_names)} cells: {cell_names}", file=sys.stderr)

    init(cell_names, no_embed=args.no_embed)

    print(f"[flex-mcp] Ready", file=sys.stderr)

    if args.http:
        run_http_server(args.port)
    else:
        asyncio.run(_run_stdio())


if __name__ == "__main__":
    main()
