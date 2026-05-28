"""Lobsters install hook.

Installs a no-auth public Lobsters source cell. The default path is bounded so
new users can prove the module without spending much time on a small site.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.modules.lobsters.compile.lobsters_api import DEFAULT_TAGS


MODULE_SUMMARY = "index public Lobsters stories and comments - no auth"
CLI_NAME = "lobsters"

MODULE = {
    "cell_type": "lobsters",
    "maturity": "public-source-cell",
    "release_posture": "public",
    "license_intent": "MIT-compatible external module",
    "description": "Lobsters public stories and comments. Each source is a story thread; each chunk is the story or one comment.",
    "default_cell_name": "lobsters",
    "refresh_module": "flex.modules.lobsters.compile.refresh",
    "views_from": ("lobsters",),
    "presets_from": ("lobsters",),
    "instructions_from": ("lobsters",),
    "query_examples": ("@orient", "@recent", "@top", "@tag tag=python"),
}


def register_args(parser) -> None:
    existing = {opt for action in parser._actions for opt in action.option_strings}
    if "--lobsters-cell" not in existing:
        parser.add_argument(
            "--lobsters-cell",
            default="lobsters",
            help="Cell name for the Lobsters module (default: lobsters).",
        )
    if "--lobsters-tags" not in existing:
        parser.add_argument(
            "--lobsters-tags",
            default="programming,python,ai",
            help="Comma-separated Lobsters tags to seed (default: programming,python,ai).",
        )
    if "--lobsters-since" not in existing:
        parser.add_argument(
            "--lobsters-since",
            default="14d",
            help="How far back to seed, such as 7d or 30d (default: 14d).",
        )
    if "--lobsters-pages" not in existing:
        parser.add_argument(
            "--lobsters-pages",
            default=0,
            type=int,
            help="Newest pages to include in addition to tag feeds (default: 0).",
        )
    if "--lobsters-limit" not in existing:
        parser.add_argument(
            "--lobsters-limit",
            default=5,
            type=int,
            help="Maximum stories to seed during init (default: 5).",
        )
    if "--lobsters-graph" not in existing:
        parser.add_argument(
            "--lobsters-graph",
            action="store_true",
            help="Build a similarity graph after seeding.",
        )
    if "--lobsters-dry-run" not in existing:
        parser.add_argument(
            "--lobsters-dry-run",
            action="store_true",
            help="Fetch and count Lobsters data without writing a cell.",
        )


def _parse_days(value: str) -> int:
    raw = value.strip().lower()
    if raw.endswith("d"):
        raw = raw[:-1]
    days = int(raw)
    if days < 0:
        raise ValueError("days must be non-negative")
    return days


def run(args, console) -> None:
    from rich.panel import Panel
    from rich.text import Text

    from flex.cli import (
        _install_launchd,
        _install_systemd,
        _patch_claude_json,
        _start_services_direct,
        _verify_services,
    )
    from flex.core import open_cell, set_meta, validate_cell, log_op
    from flex.registry import CELLS_DIR, register_cell
    from flex.retrieve.presets import install_presets
    from flex.views import install_views, regenerate_views
    from flex.modules.lobsters.compile.lobsters_api import pull_stories_with_comments
    from flex.modules.lobsters.compile.worker import (
        SCHEMA_DDL,
        embed_new,
        group_into_threads,
        ingest,
    )

    cell_name = getattr(args, "lobsters_cell", None) or MODULE["default_cell_name"]
    tags = [
        tag.strip()
        for tag in (getattr(args, "lobsters_tags", "") or "").split(",")
        if tag.strip()
    ] or DEFAULT_TAGS
    since_days = _parse_days(getattr(args, "lobsters_since", "14d"))
    pages = max(0, int(getattr(args, "lobsters_pages", 0)))
    limit = getattr(args, "lobsters_limit", 5)
    limit = max(1, int(limit)) if limit is not None else None
    graph = bool(getattr(args, "lobsters_graph", False))
    dry_run = bool(getattr(args, "lobsters_dry_run", False))

    after_ts = int(time.time()) - (since_days * 86400)
    console.print(f"  Lobsters tags       [bold]{', '.join(tags)}[/bold]")
    console.print(f"  Seed window         {since_days}d")
    console.print(f"  Story limit         {limit if limit is not None else 'none'}")

    stories = pull_stories_with_comments(
        tags=tags,
        pages=pages,
        after=after_ts,
        limit=limit,
    )
    threads = group_into_threads(stories)
    estimated_chunks = sum(1 + len(comments) for _, comments in threads)

    if dry_run:
        console.print(f"  dry run             [green]{len(threads)} sources, ~{estimated_chunks} chunks[/green]")
        return

    CELLS_DIR.mkdir(parents=True, exist_ok=True)
    cell_path = CELLS_DIR / f"{cell_name}.db"
    db = open_cell(cell_path)

    try:
        db.executescript(SCHEMA_DDL)
        sources, chunks = ingest(threads, db)
        validate_cell(db)
        console.print(f"  ingest              [green]{sources} sources, {chunks} chunks[/green]")

        embedded = 0
        if getattr(args, "_model_ok", True):
            try:
                embedded = embed_new(db)
                console.print(f"  embeddings          [green]{embedded} chunks[/green]")
            except Exception as exc:
                console.print(f"  embeddings          [yellow]skipped: {exc}[/yellow]")
        else:
            console.print("  embeddings          [yellow]skipped: model unavailable[/yellow]")

        if graph:
            import subprocess

            console.print("  graph               building")
            subprocess.run(
                [sys.executable, "-m", "flex.manage.meditate", "--cell", str(cell_path)],
                check=True,
            )

        root = Path(__file__).resolve().parent
        for view_dir in [root / "stock" / "views"]:
            if view_dir.exists():
                install_views(db, view_dir)
        regenerate_views(db)

        for preset_dir in [
            root.parent.parent / "retrieve" / "presets" / "general",
            root / "stock" / "presets",
        ]:
            if preset_dir.exists():
                install_presets(db, preset_dir)

        set_meta(db, "cell_type", "lobsters")
        set_meta(db, "description", MODULE["description"])
        set_meta(db, "created_at", datetime.now(timezone.utc).isoformat())
        set_meta(db, "last_pull_ts", str(db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0))
        set_meta(db, "last_pull_at", datetime.now(timezone.utc).isoformat())
        set_meta(db, "tags", json.dumps(tags))
        set_meta(db, "refresh_module", MODULE["refresh_module"])

        log_op(
            db,
            "lobsters_init",
            "_raw_chunks",
            params={"tags": tags, "sources": sources, "chunks": chunks, "embedded": embedded},
            rows_affected=chunks,
            source="lobsters/install.py",
        )
        db.commit()
    finally:
        db.close()

    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type="lobsters",
        description=MODULE["description"],
        lifecycle="refresh",
        refresh_interval=6 * 60 * 60,
        refresh_module=MODULE["refresh_module"],
        active=True,
        unlisted=False,
    )
    console.print(f"  registry            [green]{cell_name}[/green]")

    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        status = lambda ok: "[green]running[/green]" if ok else "[yellow]not verified[/yellow]"
        console.print(f"  worker              {status(worker_ok)}")
        console.print(f"  MCP                 {status(mcp_ok)}")

    _patch_claude_json()
    console.print()

    panel = Text()
    panel.append("Lobsters cell ready.\n\n", style="cyan")
    panel.append("  flex core search --cell ", style="bold")
    panel.append(f"{cell_name} ", style="bold green")
    panel.append('"@orient"\n', style="bold")
    panel.append("  flex core search --cell ", style="bold")
    panel.append(f"{cell_name} ", style="bold green")
    panel.append('"@recent"\n', style="bold")
    panel.append("  flex refresh --cells ", style="bold")
    panel.append(f"{cell_name}\n", style="bold green")
    console.print(Panel(panel, padding=(1, 2), highlight=False))
    console.print()
