"""Dev.to install hook.

Dev.to uses the public Forem API. No token or account is required for read
access. The install path creates a refreshable cell scoped by tags and optional
public author usernames.
"""

from __future__ import annotations

import sys


CLI_NAME = "devto"
MODULE_SUMMARY = "index public Dev.to articles and comments by tag or author"

MODULE = {
    "cell_type": "devto",
    "maturity": "public",
    "license_intent": "MIT-compatible source module",
    "release_posture": "public",
    "auth": "none",
    "description": "Public Dev.to articles and comments from configured tags and authors. No credentials required.",
    "default_cell_name": "devto",
    "refresh_module": "flex.modules.devto.compile.refresh",
    "refresh_interval": 6 * 60 * 60,
    "views_from": ("devto",),
    "presets_from": ("devto",),
    "instructions_from": ("devto",),
    "query_examples": ("@orient", "@scope", "@tags", "@by-tag tag='python'"),
}


def _add_arg(parser, *flags, **kwargs) -> None:
    existing = {opt for action in parser._actions for opt in action.option_strings}
    if not any(flag in existing for flag in flags):
        parser.add_argument(*flags, **kwargs)


def register_args(parser) -> None:
    _add_arg(parser, "--name", default=None,
             help="Dev.to cell name (default: devto)")
    _add_arg(parser, "--tags", default=None,
             help="Comma-separated Dev.to tags to index")
    _add_arg(parser, "--authors", default=None,
             help="Comma-separated public Dev.to usernames to index")
    _add_arg(parser, "--since", default="30d",
             help="How far back to pull, such as 7d or 30d (default: 30d)")
    _add_arg(parser, "--limit", type=int, default=10,
             help="Max articles per tag or author (default: 10)")
    _add_arg(parser, "--comment-limit", type=int, default=20,
             help="Max comments per article; use 0 to skip comments (default: 20)")
    _add_arg(parser, "--no-comments", action="store_true",
             help="Do not fetch article comments")
    _add_arg(parser, "--devto-graph", action="store_true",
             help="Build Dev.to graph after indexing")


def run(args, console) -> None:
    """Install and index a Dev.to cell through the module worker."""
    from rich.panel import Panel
    from rich.text import Text

    from flex.modules.devto.compile.forem_api import DEFAULT_TAGS
    from flex.modules.devto.compile.worker import parse_csv

    cell_name = getattr(args, "name", None) or MODULE["default_cell_name"]
    tags = parse_csv(getattr(args, "tags", None)) or DEFAULT_TAGS
    authors = parse_csv(getattr(args, "authors", None))
    since = getattr(args, "since", None) or "30d"
    limit = getattr(args, "limit", 10)
    comment_limit = getattr(args, "comment_limit", 20)
    no_comments = bool(getattr(args, "no_comments", False))

    console.print(f"  cell                [bold]{cell_name}[/bold]")
    console.print(f"  auth                [green]none[/green]")
    console.print(f"  tags                {', '.join(tags)}")
    if authors:
        console.print(f"  authors             {', '.join(authors)}")
    console.print(f"  since               {since}")
    console.print(f"  limit               {limit} articles per tag/author")
    console.print("  comments            " + (
        "disabled" if no_comments or comment_limit == 0
        else f"max {comment_limit} per article"
    ))
    console.print()

    argv = [
        "devto-worker",
        "--cell", cell_name,
        "--tags", ",".join(tags),
        "--since", since,
        "--limit", str(limit),
        "--comment-limit", str(comment_limit),
        "--description", MODULE["description"],
        "--refresh-interval", str(MODULE["refresh_interval"]),
        "--append",
    ]
    if authors:
        argv.extend(["--authors", ",".join(authors)])
    if no_comments:
        argv.append("--no-comments")
    if getattr(args, "devto_graph", False):
        argv.append("--graph")

    from flex.modules.devto.compile import worker

    original_argv = sys.argv
    try:
        sys.argv = argv
        worker.main()
    finally:
        sys.argv = original_argv

    panel_content = Text()
    panel_content.append("Dev.to cell ready.\n\n", style="cyan")
    panel_content.append("Cell                  ", style="")
    panel_content.append(f"{cell_name}\n", style="green")
    panel_content.append("Scope                 ", style="")
    panel_content.append(", ".join(tags[:8]), style="green")
    if len(tags) > 8:
        panel_content.append(f" (+{len(tags) - 8} more)", style="green")
    panel_content.append("\n\n")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@orient"\n', style="bold")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@scope"\n', style="bold")
    panel_content.append("  flex core search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@tags"\n', style="bold")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
