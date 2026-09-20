"""Shared install runner for coding-agent cells.

This is lifecycle glue around the Claude Code substrate, not a parallel
substrate. Agent modules still own only their source parser/transpiler and
declare the rest through a small spec.
"""

from __future__ import annotations

import importlib
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable


def _load_ref(ref: str) -> Callable[..., Any]:
    module_name, attr = ref.split(":", 1)
    return getattr(importlib.import_module(module_name), attr)


def _record_signature(conn: sqlite3.Connection, spec: dict[str, Any], source: Path) -> None:
    keys = spec.get("signature_meta_keys") or ()
    if not keys:
        return

    values: tuple[Any, ...]
    signature_ref = spec.get("signature")
    if signature_ref:
        result = _load_ref(signature_ref)(source)
        values = tuple(result) if isinstance(result, tuple) else (result,)
    else:
        values = (source.stat().st_size,)

    for key, value in zip(keys, values):
        conn.execute(
            "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
            (key, str(value)),
        )
    conn.commit()


def _registration_lifecycle_kwargs(spec: dict[str, Any], source: Path) -> dict[str, Any]:
    """Return registry lifecycle fields for coding-agent source tracking."""
    lifecycle = spec.get("lifecycle", "watch")
    watch_path = spec.get("watch_path") or source
    if lifecycle == "watch" and Path(watch_path).is_file():
        watch_path = Path(watch_path).parent
    fields = {
        "lifecycle": lifecycle,
        "refresh_interval": (
            int(spec["refresh_interval"])
            if lifecycle == "refresh" and spec.get("refresh_interval") is not None
            else None
        ),
        "refresh_module": spec.get("refresh_module"),
        "watch_path": watch_path,
        "watch_pattern": spec.get("watch_pattern"),
    }
    if spec.get("detector") is not None:
        fields["detector"] = spec["detector"]
    if spec.get("detector_config") is not None:
        fields["detector_config"] = spec["detector_config"]
    return fields


def register_common_args(parser, *, source_flag: str, source_help: str, default_name: str) -> None:
    """Register source + name args without colliding with other module hooks."""
    existing = {opt for action in parser._actions for opt in action.option_strings}
    if source_flag not in existing:
        parser.add_argument(source_flag, default=None, help=source_help)
    if "--name" not in existing:
        parser.add_argument("--name", default=None, help=f"Flex cell name (default: {default_name})")


def run_from_spec(args, console, spec: dict[str, Any]) -> None:
    """Install a coding-agent cell from a declarative module spec."""
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
    from rich.text import Text

    from flex.modules.claude_code import ENRICHMENT_STUBS
    from flex.modules.claude_code.compile.worker import bootstrap_claude_code_cell
    from flex.modules.claude_code.contract import validate_coding_agent_cell
    from flex.registry import register_cell
    from flex.cli import (
        _install_claude_assets,
        _install_launchd,
        _install_systemd,
        _patch_claude_json,
        _start_services_direct,
        _verify_services,
    )

    cell_type = spec["cell_type"]
    name = getattr(args, "name", None) or spec.get("default_cell_name") or cell_type
    description = spec.get("description") or f"{cell_type} coding-agent session provenance."
    source_attr = spec["source_arg"].lstrip("-").replace("-", "_")
    source_arg = getattr(args, source_attr, None)
    source = (Path(source_arg) if source_arg else Path(spec["default_source"])).expanduser()

    if spec.get("skill"):
        _install_claude_assets((spec["skill"],))

    console.print(f"  {spec.get('source_label', cell_type + ' source'):<20} {source}")
    if not source.exists():
        console.print(f"  [yellow]not found[/yellow] — {spec.get('missing_hint', 'run the source agent at least once.')}")
        return

    db_path = bootstrap_claude_code_cell(name=name, cell_type=cell_type)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    for ddl in ENRICHMENT_STUBS:
        conn.execute(ddl)
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES ('description', ?)",
        (description,),
    )
    conn.commit()

    failed: list[str] = []
    transpile = _load_ref(spec["transpile"])

    with Progress(
        TextColumn("  {task.description:<20}"),
        SpinnerColumn(spinner_name="dots", style="white", finished_text="[green]✓[/green]"),
        BarColumn(bar_width=20, complete_style="white", finished_style="green"),
        TextColumn("{task.fields[info]}"),
        console=console,
        transient=False,
    ) as progress:
        t_ingest = progress.add_task("Ingesting sessions", total=None, info="", visible=True)
        t_embed = progress.add_task("Queueing vectors", total=None, info="", visible=False)
        t_graph = progress.add_task("Publishing surface", total=None, info="", visible=False)

        def _p_cb(i, total, n_sessions, n_chunks, elapsed):
            progress.update(
                t_ingest,
                total=total,
                completed=i,
                info=f"{n_sessions} sessions / {n_chunks} chunks",
            )

        stats = transpile(source, conn, progress_cb=_p_cb)
        progress.update(
            t_ingest,
            completed=progress.tasks[t_ingest].total or 1,
            total=progress.tasks[t_ingest].total or 1,
            info=f"{stats.get('sessions', 0)} sessions / {stats.get('chunks', 0)} chunks",
        )

        progress.update(
            t_embed, visible=True, total=1, completed=1,
            info=f"{stats.get('chunks', 0):,} chunks queued",
        )
        progress.update(t_graph, visible=True, info="publishing structural surface")
        try:
            from flex.cli import _find_view_dirs
            from flex.manage.install_presets import ensure_cell_presets
            from flex.views import install_views, regenerate_views

            for view_dir in _find_view_dirs("claude_code", cell_type):
                install_views(conn, view_dir)
            regenerate_views(conn)
            ensure_cell_presets(conn, cell_type)
            conn.commit()
        except Exception as exc:
            failed.append("structural surface")
            console.print(f"  [yellow]surface: {exc}[/yellow]")
        semantic_pending = bool(stats.get("chunks", 0))
        model_available = bool(getattr(args, "_model_ok", True))
        conn.execute(
            "INSERT OR REPLACE INTO _meta(key,value) VALUES('semantic_status',?)",
            (
                "ready" if not semantic_pending
                else "pending" if model_available
                else "unavailable",
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO _meta(key,value) VALUES('semantic_pending',?)",
            ("1" if semantic_pending else "0",),
        )
        conn.commit()
        progress.update(
            t_graph,
            visible=True,
            total=1,
            completed=1,
            info="queued for background convergence",
        )

    try:
        _record_signature(conn, spec, source)
        if spec.get("source_meta_key"):
            conn.execute(
                "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
                (spec["source_meta_key"], str(source)),
            )
            conn.commit()
    except OSError:
        pass

    report = validate_coding_agent_cell(conn, cell_type=cell_type)
    if not report.ok or report.warnings:
        console.print()
        console.print(f"  [yellow]{report.summary()}[/yellow]")

    conn.close()

    register_cell(
        name=name,
        path=str(db_path),
        cell_type=cell_type,
        description=description,
        **_registration_lifecycle_kwargs(spec, source),
    )

    if sys.platform != "win32":
        managed = _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        if not managed:
            failed.append("service manager registration could not be verified")
        if not worker_ok:
            failed.append("worker service is not running")
        if not mcp_ok:
            failed.append("MCP service is not running")
    _patch_claude_json()

    console.print()
    console.print(
        f"  [bold]{stats.get('sessions', 0):,} sessions[/bold] · "
        f"[bold]{stats.get('chunks', 0):,} chunks[/bold]"
    )
    console.print()

    panel = Text()
    panel.append(f"{cell_type} cell ready.\n\n", style="cyan")
    panel.append("Query examples:\n", style="bold")
    for example in spec.get("query_examples") or ("@orient", "@digest", "@file path='src/foo.py'"):
        panel.append(f'  flex search --cell {name} "{example}"\n', style="dim")
    console.print(Panel(panel, padding=(1, 2), highlight=False))
    console.print()

    if failed:
        console.print(f"  [yellow]Completed with {len(failed)} warning(s):[/yellow]")
        for warning in failed:
            console.print(f"    [dim]- {warning}[/dim]")
        console.print()
