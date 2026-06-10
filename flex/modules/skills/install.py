"""tools install hook.

The public cell name is ``tools``. The implementation package remains
``flex.modules.skills`` because the schema and compiler were originally built
around Claude skill artifacts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


CLI_NAME = "tools"
MODULE_SUMMARY = "register the public tools catalog cell"

DEFAULT_CELL_NAME = "tools"
DEFAULT_REFRESH_INTERVAL = 6 * 60 * 60

MODULE = {
    "cell_type": "tools",
    "maturity": "canonical",
    "license_intent": "MIT-compatible public source module",
    "release_posture": "public",
    "description": (
        "Public catalog of AI development tools, MCP servers, Claude Code "
        "skills, agents, hooks, commands, and plugin manifests."
    ),
    "default_cell_name": DEFAULT_CELL_NAME,
    "refresh_module": "flex.modules.skills.compile.refresh",
    "refresh_interval": DEFAULT_REFRESH_INTERVAL,
    "views_from": ("skills",),
    "presets_from": ("skills",),
    "instructions_from": ("skills",),
    "query_examples": ("@orient", "SELECT tool_name, stars FROM tools ORDER BY stars DESC LIMIT 10"),
}


def register_args(parser) -> None:
    """Register tools-specific init flags without colliding with other modules."""
    existing = {opt for action in parser._actions for opt in action.option_strings}
    if "--tools-cell" not in existing:
        parser.add_argument(
            "--tools-cell",
            default=DEFAULT_CELL_NAME,
            help="Cell name for the tools catalog (--module tools only).",
        )
    if "--tools-refresh" not in existing:
        parser.add_argument(
            "--tools-refresh",
            action="store_true",
            help="Run one bounded tools refresh after bootstrapping the cell.",
        )


def run(args, console) -> None:
    """Bootstrap a queryable tools cell without doing network work by default."""
    from flex.cli import _install_claude_assets
    _install_claude_assets(("flex:tools",))
    from flex.core import log_op, open_cell, set_meta, validate_cell
    from flex.registry import CELLS_DIR, register_cell
    from flex.retrieve.presets import install_presets
    from flex.views import install_views, regenerate_views
    from flex.modules.skills.compile.worker import SCHEMA_DDL

    cell_name = getattr(args, "tools_cell", None) or DEFAULT_CELL_NAME
    cell_path = CELLS_DIR / f"{cell_name}.db"
    cell_path.parent.mkdir(parents=True, exist_ok=True)

    db = open_cell(cell_path)
    db.executescript(SCHEMA_DDL)

    now = datetime.now(timezone.utc).isoformat()
    set_meta(db, "cell_type", "tools")
    set_meta(db, "substrate", "skills")
    set_meta(db, "surface", "tools")
    set_meta(db, "implementation_module", "flex.modules.skills")
    set_meta(db, "description", MODULE["description"])
    set_meta(db, "created_at", now)
    set_meta(db, "last_pull_ts", "0")
    set_meta(db, "retrieval:public_name", "tools")
    set_meta(db, "retrieval:implementation", "skills schema and compiler")

    module_root = Path(__file__).resolve().parent
    views_dir = module_root / "stock" / "views"
    if views_dir.exists():
        install_views(db, views_dir)

    general_presets = module_root.parents[1] / "retrieve" / "presets" / "general"
    if general_presets.exists():
        install_presets(db, general_presets)

    regenerate_views(db)
    validate_cell(db)

    log_op(
        db,
        "tools_init",
        "_meta",
        params={"cell": cell_name, "surface": "tools", "implementation": "skills"},
        rows_affected=1,
        source="skills/install.py",
    )
    db.commit()
    db.close()

    register_cell(
        name=cell_name,
        path=cell_path,
        cell_type="tools",
        description=MODULE["description"],
        lifecycle="refresh",
        refresh_interval=DEFAULT_REFRESH_INTERVAL,
        refresh_module=MODULE["refresh_module"],
    )

    if getattr(args, "tools_refresh", False):
        from flex.modules.skills.compile.refresh import refresh

        refresh(str(cell_path), dry_run=False, modes=["search"])

    console.print(f"  tools cell          [green]{cell_name}[/green]")
    console.print("  query               [bold]flex core search --cell tools \"@orient\"[/bold]")
