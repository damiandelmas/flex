"""Shared install runner for coding-agent cells.

This is lifecycle glue around the Claude Code substrate, not a parallel
substrate. Agent modules still own only their source parser/transpiler and
declare the rest through a small spec.
"""

from __future__ import annotations

import importlib
import sqlite3
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
    return {
        "lifecycle": lifecycle,
        "refresh_interval": (
            int(spec["refresh_interval"])
            if lifecycle == "refresh" and spec.get("refresh_interval") is not None
            else None
        ),
        "refresh_module": spec.get("refresh_module"),
        "watch_path": spec.get("watch_path") or source,
        "watch_pattern": spec.get("watch_pattern"),
    }


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

    from flex.modules.claude_code import ENRICHMENT_STUBS, run_enrichment
    from flex.modules.claude_code.compile.worker import (
        _batch_embed_chunks,
        bootstrap_claude_code_cell,
    )
    from flex.modules.claude_code.contract import validate_coding_agent_cell
    from flex.registry import register_cell

    cell_type = spec["cell_type"]
    name = getattr(args, "name", None) or spec.get("default_cell_name") or cell_type
    description = spec.get("description") or f"{cell_type} coding-agent session provenance."
    source_attr = spec["source_arg"].lstrip("-").replace("-", "_")
    source_arg = getattr(args, source_attr, None)
    source = (Path(source_arg) if source_arg else Path(spec["default_source"])).expanduser()

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

    n_comm = 0
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
        t_embed = progress.add_task("Building vectors", total=None, info="", visible=False)
        t_graph = progress.add_task("Building graph", total=None, info="", visible=False)

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

        progress.update(t_embed, visible=True, info="encoding")
        if stats.get("chunks", 0) > 0:
            def _e_cb(done, total):
                progress.update(
                    t_embed,
                    completed=done,
                    total=total,
                    info=f"{done:,} / {total:,} chunks",
                )
            try:
                _batch_embed_chunks(conn, quiet=True, progress_cb=_e_cb)
            except Exception as e:
                console.print(f"  [yellow]embed: {e}[/yellow]")
                conn.commit()
        progress.update(t_embed, visible=True, total=1, completed=1, info="done")

        progress.update(t_graph, visible=True, info="analyzing")

        def _g_cb(step):
            progress.update(t_graph, info=step)

        n_comm, failed = run_enrichment(conn, cell_type=cell_type, progress_cb=_g_cb)
        progress.update(
            t_graph,
            visible=True,
            total=1,
            completed=1,
            info=f"{n_comm} topic clusters found" if n_comm else "done",
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

    console.print()
    console.print(
        f"  [bold]{stats.get('sessions', 0):,} sessions[/bold] · "
        f"[bold]{stats.get('chunks', 0):,} chunks[/bold]"
        + (f" · [bold]{n_comm}[/bold] topic clusters" if n_comm else "")
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
