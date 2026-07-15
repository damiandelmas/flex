"""Install hook for the unified mixed-filesystem compiler."""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path


CLI_NAME = "filesystem"
MODULE_SUMMARY = "compile a mixed folder into one embedded, watched filesystem cell"
MODULE = {
    "cell_type": "filesystem",
    "maturity": "stable",
    "auth": "none",
    "description": "Mixed Markdown, code, and text filesystem compiler",
    "views_from": ("filesystem",),
    "presets_from": ("filesystem",),
    "instructions_from": ("filesystem",),
}


def _add_arg(parser, *flags, **kwargs) -> None:
    existing = {option for action in parser._actions for option in action.option_strings}
    if not any(flag in existing for flag in flags):
        parser.add_argument(*flags, **kwargs)


def register_args(parser) -> None:
    _add_arg(parser, "--path", default=None, help="Folder to compile")
    _add_arg(parser, "--name", default=None, help="Cell name (default: folder name)")
    _add_arg(parser, "--description", default=None, help="Cell description")
    _add_arg(parser, "--obsidian", action="store_true",
             help="Add Obsidian tags, aliases, Dataview, and wikilink semantics")
    _add_arg(parser, "--exclude", action="append", default=[],
             help="Exclude a path or filename pattern (repeatable)")
    _add_arg(parser, "--no-watch", action="store_true",
             help="Build a static cell instead of keeping it current")


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", value).strip("-").lower() or "filesystem"


def _resolve_root(args) -> Path:
    path_value = getattr(args, "path", None)
    vault_value = getattr(args, "vault", None)
    if path_value and vault_value:
        if Path(path_value).expanduser().resolve() != Path(vault_value).expanduser().resolve():
            raise SystemExit("filesystem: --path and --vault identify different roots")
    value = path_value or vault_value
    if not value:
        raise SystemExit("filesystem: --path is required")
    root = Path(value).expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"filesystem: not a readable directory: {root}")
    return root


def run(args, console) -> None:
    """Create, populate, register, and teach one mixed filesystem cell."""
    from flex.cli import (
        _install_claude_assets, _install_launchd, _install_systemd,
        _patch_claude_json, _start_services_direct, _verify_services,
    )
    from flex.modules.fs.compile.schema import FILESYSTEM_SCHEMA_DDL
    from flex.modules.fs.compile.worker import reconcile_cell
    from flex.registry import resolve_cell
    from flex.retrieve.embeddings import set_active_model
    from flex.sdk import create, register

    root = _resolve_root(args)
    if getattr(args, "embed", False) and getattr(args, "no_embed", False):
        raise SystemExit("filesystem: --embed and --no-embed conflict")
    embed_enabled = not bool(getattr(args, "no_embed", False))
    if embed_enabled and not getattr(args, "_model_ok", True):
        raise SystemExit(
            "filesystem: the Nomic model is unavailable; rerun `flex init` online "
            "or choose --no-embed"
        )
    name = getattr(args, "name", None) or _slug(root.name)
    if resolve_cell(name) is not None:
        raise SystemExit(
            f"filesystem: cell {name!r} already exists; choose --name for a new cell"
        )
    description = getattr(args, "description", None) or f"{name} — filesystem at {root}"
    obsidian = bool(getattr(args, "obsidian", False))
    exclude = list(getattr(args, "exclude", None) or [])
    kinds_value = getattr(args, "_filesystem_file_kinds", None)
    file_kinds = tuple(kinds_value) if kinds_value else None
    lifecycle = "static" if getattr(args, "no_watch", False) else "watch"

    db = create(name, description, cell_type="filesystem", schema=FILESYSTEM_SCHEMA_DDL)
    db_path = Path(db.execute("PRAGMA database_list").fetchone()[2])
    try:
        metadata = {
            "profile": "filesystem",
            "cell_type": "filesystem",
            "source_path": str(root),
            "selections": json.dumps([str(root)]),
            "exclude": json.dumps(exclude),
            "obsidian": "true" if obsidian else "false",
            "embed": "true" if embed_enabled else "false",
            "lifecycle": lifecycle,
        }
        if file_kinds:
            metadata["file_kinds"] = json.dumps(list(file_kinds))
        for key, value in metadata.items():
            db.execute("INSERT OR REPLACE INTO _meta(key,value) VALUES(?,?)", (key, value))
        if embed_enabled:
            set_active_model(db, "nomic-v1.5-fp32", 256)
            db.execute(
                "INSERT OR REPLACE INTO _meta(key,value) VALUES('embedding_model',?)",
                ("nomic-embed-text-v1.5-fp32",),
            )
            db.execute("INSERT OR REPLACE INTO _meta VALUES('embedding_dim','768')")
        db.commit()

        stats = reconcile_cell(
            db, root, obsidian=obsidian, exclude=exclude, file_kinds=file_kinds,
            process_cache={},
        )
        sources = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
        chunks = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        presets = Path(__file__).resolve().parent / "stock" / "presets"
        register(
            db, name, description, cell_type="filesystem",
            presets_dirs=[presets], lifecycle=lifecycle,
            watch_path=None if lifecycle == "static" else str(root),
            watch_pattern="**/*" if lifecycle == "watch" else None,
        )
    except Exception:
        db.close()
        try:
            db_path.unlink()
        except OSError:
            pass
        raise
    db.close()

    _install_claude_assets(("flex", "flex:filesystem", "flex:markdown", "flex:codegraph"))
    console.print(
        f"  filesystem          [green]{sources} files, {chunks} chunks[/green]"
    )
    console.print(
        f"  embeddings          {'[green]Nomic fp32 @ serve-256[/green]' if embed_enabled else '[yellow]off[/yellow]'}"
    )
    if obsidian:
        console.print("  Obsidian            [green]enabled for Markdown files[/green]")

    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
    _patch_claude_json()
    console.print(f"  Query               [bold]flex core search --cell {name} \"@orient\"[/bold]")
