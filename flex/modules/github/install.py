"""GitHub install hook -- public issues module."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path


CLI_NAME = "github"
MODULE_SUMMARY = "index public GitHub issues with optional GITHUB_TOKEN auth"

MODULE = {
    "cell_type": "github",
    "maturity": "public",
    "license_intent": "MIT-compatible source module",
    "release_posture": "public",
    "description": (
        "GitHub Issues and comments. Each source is an issue, and each chunk "
        "is the issue body or one comment."
    ),
    "default_cell_name": "github",
    "views_from": ("github",),
    "presets_from": ("github",),
    "instructions_from": ("github",),
    "refresh_module": "flex.modules.github.compile.refresh",
    "query_examples": ("@orient", "@open-issues", "@reply-targets"),
}


def _has_option(parser, flag: str) -> bool:
    return any(flag in action.option_strings for action in parser._actions)


def register_args(parser) -> None:
    """Register GitHub module options on flex init."""
    options = [
        ("--github-cell", {"default": None, "help": "GitHub cell name (default: github)."}),
        ("--github-repos", {"default": None, "help": "Comma-separated owner/repo list."}),
        ("--github-queries", {"default": "", "help": "Comma-separated issue search queries. Empty disables search."}),
        ("--github-since", {"default": "7d", "help": "How far back to pull (default: 7d)."}),
        ("--github-max-issues", {"type": int, "default": 10, "help": "Max issues on first install (default: 10)."}),
        ("--github-max-comments", {"type": int, "default": 10, "help": "Max comments per issue (default: 10)."}),
        ("--github-refresh-interval", {"type": int, "default": 86400, "help": "Scheduled refresh interval in seconds (default: 86400)."}),
        ("--github-graph", {"action": "store_true", "help": "Build graph after ingest."}),
        ("--github-static", {"action": "store_true", "help": "Register without scheduled refresh."}),
    ]
    for flag, kwargs in options:
        if not _has_option(parser, flag):
            parser.add_argument(flag, **kwargs)


def _csv(value: str | None) -> list[str]:
    if value is None:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _parse_since_days(value: str) -> int:
    text = (value or "7d").strip().lower()
    if text.endswith("d"):
        text = text[:-1]
    days = int(text)
    if days < 0:
        raise ValueError("--github-since must be non-negative")
    return days


def run(args, console) -> None:
    """Install a bounded GitHub Issues cell and register its query surface."""
    from rich.panel import Panel
    from rich.text import Text

    from flex.core import open_cell, set_meta, validate_cell, log_op
    from flex.registry import CELLS_DIR, register_cell
    from flex.retrieve.presets import install_presets
    from flex.views import install_views, regenerate_views

    from flex.modules.github.compile.github_api import (
        DEFAULT_REPOS,
        _get_token,
        pull_issues,
    )
    from flex.modules.github.compile.worker import (
        SCHEMA_DDL,
        embed_new,
        group_into_threads,
        ingest,
    )

    cell_name = getattr(args, "github_cell", None) or MODULE["default_cell_name"]
    repos = _csv(getattr(args, "github_repos", None)) or DEFAULT_REPOS[:1]
    queries = _csv(getattr(args, "github_queries", ""))
    since_days = _parse_since_days(getattr(args, "github_since", "7d"))
    max_issues = getattr(args, "github_max_issues", 10)
    max_comments = getattr(args, "github_max_comments", 10)
    refresh_interval = getattr(args, "github_refresh_interval", 86400)

    token_status = "GITHUB_TOKEN/gh auth" if _get_token() else "unauthenticated"
    console.print(f"  GitHub auth         [green]{token_status}[/green]")
    console.print(f"  GitHub repos        {', '.join(repos)}")
    if queries:
        console.print(f"  GitHub search       {', '.join(queries)}")
    else:
        console.print("  GitHub search       [dim]disabled[/dim]")
    console.print(f"  GitHub limits       {max_issues} issues, {max_comments} comments/issue")

    after_ts = int(time.time()) - (since_days * 86400)
    issues = pull_issues(
        queries=queries,
        repos=repos,
        after_ts=after_ts,
        max_issues=max_issues,
        max_comments_per_issue=max_comments,
        quiet=True,
    )
    threads = group_into_threads(issues)

    CELLS_DIR.mkdir(parents=True, exist_ok=True)
    cell_path = CELLS_DIR / f"{cell_name}.db"
    if cell_path.exists():
        cell_path.unlink()

    db = open_cell(str(cell_path))
    try:
        db.executescript(SCHEMA_DDL)
        sources, chunks = ingest(threads, db)
        validate_cell(db)

        embedded = 0
        if getattr(args, "_model_ok", True) and chunks:
            embedded = embed_new(db)

        views_dir = Path(__file__).parent / "stock" / "views"
        install_views(db, views_dir)
        regenerate_views(db, {"chunks": "chunk", "sources": "source"})

        general_presets = Path(__file__).resolve().parents[2] / "retrieve" / "presets" / "general"
        install_presets(db, general_presets)
        install_presets(db, Path(__file__).parent / "stock" / "presets")

        now = datetime.now(timezone.utc).isoformat()
        max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
        set_meta(db, "cell_type", "github")
        set_meta(db, "description", MODULE["description"])
        set_meta(db, "created_at", now)
        set_meta(db, "last_pull_ts", str(max_ts))
        set_meta(db, "last_pull_at", now)
        set_meta(db, "repos", json.dumps(repos))
        set_meta(db, "queries", json.dumps(queries))
        set_meta(db, "max_issues", str(max_issues))
        set_meta(db, "max_comments_per_issue", str(max_comments))
        log_op(
            db,
            "github_init",
            "_raw_chunks",
            params={"sources": sources, "chunks": chunks, "embedded": embedded},
            rows_affected=chunks,
            source="github/install.py",
        )
        db.commit()
    finally:
        db.close()

    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type="github",
        description=MODULE["description"],
        lifecycle="static" if getattr(args, "github_static", False) else "refresh",
        refresh_interval=None if getattr(args, "github_static", False) else refresh_interval,
        refresh_module=None if getattr(args, "github_static", False) else MODULE["refresh_module"],
    )

    if getattr(args, "github_graph", False):
        import subprocess
        import sys

        subprocess.run(
            [sys.executable, "-m", "flex.manage.meditate", "--cell", str(cell_path)],
            check=True,
        )

    panel_content = Text()
    panel_content.append("GitHub cell ready.\n\n", style="cyan")
    panel_content.append("Cell                 ", style="")
    panel_content.append(f"{cell_name}\n", style="green")
    panel_content.append("Sources              ", style="")
    panel_content.append(f"{len(threads)}\n", style="green")
    panel_content.append("Chunks               ", style="")
    panel_content.append(f"{sum(1 + len(cs) for _, cs in threads)}\n\n", style="green")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@orient"\n', style="bold")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@open-issues days=30"\n', style="bold")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
