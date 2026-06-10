"""Hacker News install hook."""

from __future__ import annotations

import subprocess
import sys
import time


CLI_NAME = "hn"
MODULE_SUMMARY = "index public Hacker News stories and comments via Algolia, no API key"

MODULE = {
    "cell_type": "hn",
    "maturity": "public",
    "license_intent": "MIT-compatible public module",
    "release_posture": "public",
    "description": "Public Hacker News stories and comments. Each source is a story thread, and chunks are the story plus comments.",
    "default_cell_name": "hn",
    "refresh_module": "flex.modules.hn.compile.refresh",
    "views_from": ("hn",),
    "presets_from": ("hn",),
    "instructions_from": ("hn",),
    "query_examples": ("@orient", "@me days=30", "SELECT title, score FROM threads ORDER BY score DESC LIMIT 20"),
}


def register_args(parser) -> None:
    existing = {opt for action in parser._actions for opt in action.option_strings}
    if "--hn-cell" not in existing:
        parser.add_argument("--hn-cell", default="hn", help="HN cell name")
    if "--hn-queries" not in existing:
        parser.add_argument(
            "--hn-queries",
            default="claude code,semantic search sqlite,MCP server",
            help="Comma-separated HN search queries",
        )
    if "--hn-since" not in existing:
        parser.add_argument("--hn-since", default="7d", help="HN lookback window")
    if "--hn-authors" not in existing:
        parser.add_argument(
            "--hn-authors",
            default=None,
            help="Comma-separated HN usernames for the @me preset",
        )
    if "--hn-max-stories" not in existing:
        parser.add_argument(
            "--hn-max-stories",
            type=int,
            default=25,
            help="Maximum stories to ingest during init",
        )
    if "--hn-max-comments-per-story" not in existing:
        parser.add_argument(
            "--hn-max-comments-per-story",
            type=int,
            default=20,
            help="Maximum comments to ingest per story during init",
        )
    if "--hn-max-pages" not in existing:
        parser.add_argument(
            "--hn-max-pages",
            type=int,
            default=1,
            help="Maximum Algolia pages per API call during init",
        )
    if "--hn-hits-per-page" not in existing:
        parser.add_argument(
            "--hn-hits-per-page",
            type=int,
            default=25,
            help="Algolia hitsPerPage during init",
        )
    if "--hn-no-comments" not in existing:
        parser.add_argument(
            "--hn-no-comments",
            action="store_true",
            help="Only ingest HN story chunks during init",
        )
    if "--hn-graph" not in existing:
        parser.add_argument("--hn-graph", action="store_true", help="Build graph after HN init")


def run(args, console) -> None:
    from flex.cli import _install_claude_assets
    _install_claude_assets(("flex:hn",))
    from rich.panel import Panel
    from rich.text import Text

    from flex.cli import (
        _install_launchd,
        _install_systemd,
        _patch_claude_json,
        _start_services_direct,
        _verify_services,
    )

    cell = getattr(args, "hn_cell", None) or MODULE["default_cell_name"]
    cmd = [
        sys.executable,
        "-m",
        "flex.modules.hn.compile.worker",
        "--cell",
        cell,
        "--queries",
        getattr(args, "hn_queries", None) or "",
        "--since",
        getattr(args, "hn_since", None) or "7d",
        "--max-stories",
        str(getattr(args, "hn_max_stories", 25)),
        "--max-comments-per-story",
        str(getattr(args, "hn_max_comments_per_story", 20)),
        "--max-pages",
        str(getattr(args, "hn_max_pages", 1)),
        "--hits-per-page",
        str(getattr(args, "hn_hits_per_page", 25)),
        "--description",
        MODULE["description"],
    ]
    if getattr(args, "hn_authors", None):
        cmd.extend(["--authors", args.hn_authors])
    if getattr(args, "hn_no_comments", False):
        cmd.append("--no-comments")
    if getattr(args, "hn_graph", False):
        cmd.append("--graph")

    console.print("  HN source           [green]public Algolia API, no credentials[/green]")
    console.print(f"  HN cell             [bold]{cell}[/bold]")
    subprocess.run(cmd, check=True)

    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        status = lambda ok: "[green]running[/green]" if ok else "[red]failed[/red]"
        console.print(f"  worker             {status(worker_ok)}")
        console.print(f"  MCP                {status(mcp_ok)}")

    _patch_claude_json()

    panel_content = Text()
    panel_content.append("Flex is ready.\n\n", style="cyan")
    panel_content.append("Hacker News cell      ", style="")
    panel_content.append(f"{cell}\n", style="green")
    panel_content.append("MCP Server            ", style="")
    panel_content.append("http://localhost:7134/mcp\n\n", style="green")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell} ", style="bold green")
    panel_content.append('"@orient"\n', style="bold")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell} ", style="bold green")
    panel_content.append('"SELECT title, score FROM threads ORDER BY score DESC LIMIT 20"\n', style="bold")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
    console.print()
