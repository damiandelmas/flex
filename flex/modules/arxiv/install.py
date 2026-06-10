"""arXiv install hook for public no-auth research-paper cells."""

from __future__ import annotations

from flex.modules.arxiv.compile.worker import (
    DEFAULT_CELL_NAME,
    DEFAULT_DESCRIPTION,
    DEFAULT_MAX_PAPERS,
    DEFAULT_REFRESH_INTERVAL,
    PUBLIC_SEED_QUERIES,
    REFRESH_MODULE,
    build_cell,
)


CLI_NAME = "arxiv"
MODULE_SUMMARY = "index public arXiv papers with polite no-auth API limits"

MODULE = {
    "cell_type": "arxiv",
    "maturity": "public",
    "license_intent": "MIT-compatible public source module",
    "release_posture": "public",
    "auth": "none required",
    "description": DEFAULT_DESCRIPTION,
    "default_cell_name": DEFAULT_CELL_NAME,
    "refresh_module": REFRESH_MODULE,
    "refresh_interval": DEFAULT_REFRESH_INTERVAL,
    "views_from": ("arxiv",),
    "presets_from": ("arxiv",),
    "instructions_from": ("arxiv",),
    "query_examples": ("@orient", "@landscape", "SELECT paper_title, section_type, abs_url FROM papers LIMIT 5"),
}


def _has_option(parser, option: str) -> bool:
    return any(option in action.option_strings for action in parser._actions)


def register_args(parser) -> None:
    """Register arXiv init flags on the shared init parser."""
    if not _has_option(parser, "--arxiv-query"):
        parser.add_argument(
            "--arxiv-query",
            default=",".join(PUBLIC_SEED_QUERIES),
            help="Comma-separated arXiv API search queries for --module arxiv.",
        )
    if not _has_option(parser, "--arxiv-ids"):
        parser.add_argument(
            "--arxiv-ids",
            default=None,
            help="Comma-separated exact arXiv IDs for --module arxiv.",
        )
    if not _has_option(parser, "--arxiv-max-papers"):
        parser.add_argument(
            "--arxiv-max-papers",
            type=int,
            default=DEFAULT_MAX_PAPERS,
            help=f"Max papers per query for --module arxiv (default: {DEFAULT_MAX_PAPERS}).",
        )
    if not _has_option(parser, "--arxiv-cell"):
        parser.add_argument(
            "--arxiv-cell",
            default=DEFAULT_CELL_NAME,
            help=f"Cell name for --module arxiv (default: {DEFAULT_CELL_NAME}).",
        )
    if not _has_option(parser, "--arxiv-with-source"):
        parser.add_argument(
            "--arxiv-with-source",
            action="store_true",
            help="Download arXiv LaTeX source during initial ingest.",
        )


def run(args, console) -> None:
    """Install arXiv cell: small seed pull -> views/presets -> refresh lifecycle."""
    from flex.cli import _install_claude_assets
    _install_claude_assets(("flex:arxiv",))
    query_text = getattr(args, "arxiv_query", "") or ""
    queries = [q.strip() for q in query_text.split(",") if q.strip()]
    id_text = getattr(args, "arxiv_ids", None)
    ids = [i.strip() for i in id_text.split(",") if i.strip()] if id_text else None
    cell = getattr(args, "arxiv_cell", DEFAULT_CELL_NAME) or DEFAULT_CELL_NAME
    max_papers = getattr(args, "arxiv_max_papers", DEFAULT_MAX_PAPERS)
    with_source = bool(getattr(args, "arxiv_with_source", False))

    console.print(f"  arXiv cell          [bold]{cell}[/bold]")
    console.print(f"  per-query cap       {max_papers}")
    console.print("  auth                none required")
    console.print("  rate limit          3 seconds between arXiv API requests")
    console.print("  Semantic Scholar    optional SEMANTIC_SCHOLAR_API_KEY for expansion tools")
    console.print()

    build_cell(
        cell=cell,
        queries=queries or PUBLIC_SEED_QUERIES,
        ids=ids,
        max_papers=max_papers,
        with_source=with_source,
        graph=False,
        append=False,
        description=DEFAULT_DESCRIPTION,
    )

    console.print()
    console.print("  Query: [bold]flex core search --cell %s \"@orient\"[/bold]" % cell)
    console.print("  Refresh: [bold]python -m flex.refresh --cells %s --dry-run[/bold]" % cell)
