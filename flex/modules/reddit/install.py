"""reddit install hook — subreddit-scoped public source cell."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


CLI_NAME = "reddit"
MODULE_SUMMARY = "index selected public subreddits into a refreshable cell"

MODULE = {
    "cell_type": "reddit",
    "maturity": "public-source-module",
    "release_posture": "public",
    "description": (
        "Public Reddit archive cell scoped to explicit subreddits. "
        "Each source is a thread; each chunk is a post body or comment."
    ),
    "default_cell_name": "reddit",
    "refresh_module": "flex.modules.reddit.compile.refresh",
    "views_from": ("reddit",),
    "presets_from": ("reddit",),
    "instructions_from": ("reddit",),
    "query_examples": (
        "@orient",
        "SELECT subreddit, COUNT(*) FROM threads GROUP BY subreddit",
        "SELECT * FROM chunks WHERE subreddit = 'ClaudeCode' LIMIT 20",
    ),
}


def register_args(parser) -> None:
    """Register reddit-specific flags on the shared init parser."""
    existing = {
        opt
        for action in parser._actions
        for opt in getattr(action, "option_strings", [])
    }
    if "--subreddits" not in existing:
        parser.add_argument(
            "--subreddits",
            default=None,
            help=(
                "Comma-separated subreddit names for --module reddit, "
                "for example ClaudeCode,LocalLLaMA."
            ),
        )
    if "--since" not in existing:
        parser.add_argument(
            "--since",
            default="30d",
            help="Initial reddit backfill window for --module reddit, such as 7d or 30d.",
        )
    if "--cell" not in existing:
        parser.add_argument(
            "--cell",
            default="reddit",
            help="Cell name for --module reddit (default: reddit).",
        )
    if "--graph" not in existing:
        parser.add_argument(
            "--graph",
            action="store_true",
            help="Force graph rebuild during module init when supported.",
        )
    if "--dry-run" not in existing:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview module init without ingesting data when supported.",
        )


def _parse_subreddits(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip().lstrip("r/") for part in raw.split(",") if part.strip()]


def _parse_since_days(raw: str | None) -> int:
    value = (raw or "30d").strip().lower()
    if value.endswith("d"):
        value = value[:-1]
    days = int(value)
    if days <= 0:
        raise ValueError("--since must be a positive day count, for example 30d")
    return days


def _install_query_surface(db) -> None:
    """Install reddit views, presets, and self-description metadata."""
    from flex.core import set_meta
    from flex.retrieve.presets import install_presets
    from flex.views import install_views, regenerate_views

    root = Path(__file__).resolve().parent
    views_dir = root / "stock" / "views"
    presets_dir = root / "stock" / "presets"
    general_presets = root.parents[1] / "retrieve" / "presets" / "general"

    if views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS _presets (
            name TEXT PRIMARY KEY,
            description TEXT,
            params TEXT DEFAULT '',
            sql TEXT
        )
        """
    )
    if general_presets.exists():
        install_presets(db, general_presets)
    if presets_dir.exists():
        install_presets(db, presets_dir)

    set_meta(db, "cell_type", "reddit")
    set_meta(db, "description", MODULE["description"])


def _bootstrap_cell(cell_name: str, subreddits: list[str]) -> Path:
    """Create an empty reddit cell with query surface and refresh lifecycle."""
    from flex.core import open_cell, set_meta
    from flex.modules.reddit.compile.worker import SCHEMA_DDL, ensure_scope_defaults
    from flex.registry import CELLS_DIR, register_cell

    CELLS_DIR.mkdir(parents=True, exist_ok=True)
    cell_path = CELLS_DIR / f"{cell_name}.db"
    db = open_cell(str(cell_path))
    try:
        db.executescript(SCHEMA_DDL)
        ensure_scope_defaults(db)
        set_meta(db, "cell_type", "reddit")
        set_meta(db, "description", MODULE["description"])
        set_meta(db, "created_at", datetime.now(timezone.utc).isoformat())
        set_meta(db, "subreddits", json.dumps(subreddits))
        set_meta(db, "authors", "[]")
        _install_query_surface(db)
        db.commit()
    finally:
        db.close()

    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type="reddit",
        description=MODULE["description"],
        lifecycle="refresh",
        refresh_interval=6 * 60 * 60,
        refresh_module=MODULE["refresh_module"],
        active=True,
        unlisted=False,
    )
    return cell_path


def run(args, console) -> None:
    """Install reddit: bootstrap cell, pull bounded subreddit data, wire MCP."""
    from rich.panel import Panel
    from rich.text import Text

    from flex.cli import (
        _install_launchd,
        _install_systemd,
        _patch_claude_json,
        _start_services_direct,
        _verify_services,
    )
    from flex.modules.reddit.compile.refresh import refresh

    subreddits = _parse_subreddits(getattr(args, "subreddits", None))
    if not subreddits:
        console.print("  [yellow]Reddit is subreddit-scoped.[/yellow]")
        console.print(
            "  Pass explicit subreddits, for example: "
            "[bold]flex init --module reddit --subreddits ClaudeCode,LocalLLaMA --since 30d[/bold]"
        )
        console.print()
        return

    since_days = _parse_since_days(getattr(args, "since", "30d"))
    cell_name = getattr(args, "cell", None) or MODULE["default_cell_name"]
    graph = bool(getattr(args, "graph", False))
    dry_run = bool(getattr(args, "dry_run", False))

    console.print("  reddit scope        [green]subreddit list[/green]")
    console.print(f"  subreddits          {', '.join('r/' + s for s in subreddits)}")
    console.print(f"  since               {since_days}d")
    console.print(f"  cell                {cell_name}")

    cell_path = _bootstrap_cell(cell_name, subreddits)

    stats = refresh(
        str(cell_path),
        subreddits=subreddits,
        graph=graph,
        dry_run=dry_run,
        since_days=since_days,
    )
    if dry_run:
        console.print("  refresh             [yellow]dry run[/yellow]")
    else:
        console.print(
            "  refresh             "
            f"[green]{stats.get('sources', 0)} threads, {stats.get('chunks', 0)} chunks[/green]"
        )

    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        status = lambda ok: "[green]running[/green]" if ok else "[red]failed[/red]"
        console.print(f"  worker              {status(worker_ok)}")
        console.print(f"  MCP                 {status(mcp_ok)}")

    _patch_claude_json()
    console.print()

    panel_content = Text()
    panel_content.append("Flex Reddit is ready.\n\n", style="cyan")
    panel_content.append("Scope                 ", style="")
    panel_content.append(", ".join("r/" + s for s in subreddits) + "\n", style="green")
    panel_content.append("Cell                  ", style="")
    panel_content.append(f"{cell_name}\n", style="green")
    panel_content.append("MCP Server            ", style="")
    panel_content.append("http://localhost:7134/mcp\n\n", style="green")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@orient"\n', style="bold")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"SELECT subreddit, COUNT(*) FROM threads GROUP BY subreddit"\n', style="bold")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
    console.print()
