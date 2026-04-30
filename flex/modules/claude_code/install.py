"""claude-code install hook.

Called by the CLI dispatcher when `flex init --module claude-code` runs.
Responsibilities: Claude assets → cell bootstrap → enrichment stubs →
initial backfill → enrichment pipeline → services → MCP wiring → panel.
"""

import contextlib
import io
import sys
import time
from pathlib import Path


MODULE_SUMMARY = "scan sessions, start worker, register MCP"


def register_args(parser) -> None:
    """claude-code has no module-specific flags."""
    pass


def _run_enrichment_quiet(conn, progress_cb=None) -> tuple[int, list[str]]:
    """Back-compat shim — delegates to the public entry point.

    Kept so existing callers keep working. New callers should import
    `flex.modules.claude_code.run_enrichment` directly.
    """
    from flex.modules.claude_code.enrichment import run_enrichment
    return run_enrichment(conn, cell_type='claude-code', progress_cb=progress_cb)


def run(args, console) -> None:
    """Install claude-code module.

    Assumes the CLI dispatcher already ran preflight, storage creation, and
    model download. This function handles steps 3-7 from the old cmd_init.
    """
    import sqlite3 as _sqlite3
    from rich.panel import Panel
    from rich.progress import (
        BarColumn, Progress, SpinnerColumn, TextColumn,
    )
    from rich.text import Text

    from flex.cli import (
        FLEX_HOME, _ENRICHMENT_STUBS, _find_view_dirs,
        _install_claude_assets, _install_launchd, _install_systemd,
        _patch_claude_json, _start_services_direct, _verify_services,
    )

    _warnings: list[str] = []
    _model_ok = getattr(args, '_model_ok', True)

    # 3. Claude assets
    _install_claude_assets()
    console.print("  capture             [green]ok[/green]")
    console.print()

    # 4. Sessions
    try:
        from flex.modules.claude_code.compile.worker import (
            CLAUDE_PROJECTS, bootstrap_claude_code_cell, initial_backfill,
        )
    except ImportError:
        console.print("[yellow]Claude Code module not available. Skipping session setup.[/yellow]")
        return

    jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))
    _enrich_failures: list[str] = []

    cell_path = bootstrap_claude_code_cell()

    # Install enrichment stubs + views on every init (even empty cells)
    _stub_conn = _sqlite3.connect(str(cell_path), timeout=30.0)
    try:
        _stub_conn.execute("PRAGMA journal_mode=WAL")
        _stub_conn.execute("PRAGMA busy_timeout=30000")
        for ddl in _ENRICHMENT_STUBS.get('claude-code', []):
            _stub_conn.execute(ddl)
        _stub_conn.commit()
        try:
            from flex.views import install_views as _siv, regenerate_views as _srv
            from flex.manage.install_presets import install_cell as _sip
            for _svd in _find_view_dirs('claude_code', 'claude-code'):
                _siv(_stub_conn, _svd)
            _srv(_stub_conn)
            _stub_conn.commit()
            _sip('claude_code')
        except Exception:
            pass
    finally:
        _stub_conn.close()

    n_clusters = 0
    stats: dict = {'sessions': 0, 'chunks': 0, 'elapsed': 0, 'embed_ok': True}

    if not jsonls:
        console.print("  [dim]No Claude Code sessions found yet.[/dim]")
        console.print("  [dim]Start using Claude Code — sessions index automatically in the background.[/dim]")
        console.print("  [dim]Ask Claude: [bold]\"Use flex: what did we work on today?\"[/bold][/dim]")
        console.print()
    else:
        console.print(f"  Indexing [bold]{len(jsonls):,}[/bold] sessions")
        console.print()

        conn = _sqlite3.connect(str(cell_path), timeout=30.0)
        try:
            conn.row_factory = _sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")

            for ddl in _ENRICHMENT_STUBS.get('claude-code', []):
                conn.execute(ddl)
            conn.commit()

            try:
                from flex.views import install_views as _iv, regenerate_views as _rv
                from flex.manage.install_presets import install_cell as _install_presets_cell
                for _vd in _find_view_dirs('claude_code', 'claude-code'):
                    _iv(conn, _vd)
                _rv(conn)
                _install_presets_cell('claude_code')
                conn.commit()
            except Exception as e:
                print(f"[init] Views/presets install failed: {e}", file=sys.stderr)
                _warnings.append(f"Views/presets: {e}")

            _already = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
            if _already > 0:
                console.print(f"  [dim]({_already:,} already indexed, resuming)[/dim]")
                console.print()
            _existing_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
            _already_embedded = conn.execute(
                "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
            ).fetchone()[0]
            _phase = {"sessions": _already, "chunks": _existing_chunks}

            with Progress(
                TextColumn("  {task.description:<20}"),
                SpinnerColumn(spinner_name="dots", style="white", finished_text="[green]✓[/green]"),
                BarColumn(bar_width=20, complete_style="white", finished_style="green"),
                TextColumn("{task.fields[info]}"),
                console=console,
                transient=False,
            ) as progress:
                t_read  = progress.add_task("Scanning sessions", total=len(jsonls), info="",
                                            completed=_already)
                t_index = progress.add_task("Building vectors",  total=None,        info="", visible=False)
                t_graph = progress.add_task("Building graph",    total=None,        info="", visible=False)

                _scan_start = [None]

                def _eta_str(done, total, start):
                    if start is None or done < 1:
                        return "calculating..."
                    rate = done / (time.time() - start)
                    if rate <= 0:
                        return "calculating..."
                    secs = (total - done) / rate
                    if secs < 60:
                        return f"~{secs:.0f}s left"
                    elif secs < 3600:
                        return f"~{secs/60:.0f}m left"
                    else:
                        return f"~{secs/3600:.1f}h left"

                def _progress(i, total, sessions, chunks, elapsed):
                    if _scan_start[0] is None:
                        _scan_start[0] = time.time()
                    eta = _eta_str(_already + i, len(jsonls), _scan_start[0]) if i >= 5 else "calculating..."
                    progress.update(t_read, completed=_already + i,
                                    info=f"{_already + i:,} / {len(jsonls):,} sessions   {eta}")
                    _phase["sessions"] = sessions
                    _phase["chunks"]   = chunks

                def _phase2(sessions, chunks, elapsed):
                    progress.update(t_read, completed=len(jsonls),
                                    info=f"{len(jsonls):,} sessions scanned")
                    _phase["sessions"] = sessions
                    _phase["chunks"]   = chunks
                    progress.update(t_index, visible=True,
                                    completed=_already_embedded, total=_existing_chunks,
                                    info=f"{_already_embedded:,} / {_existing_chunks:,} chunks   calculating...")

                _embed_start = [None]

                def _embed_progress(done, total):
                    if _embed_start[0] is None and done > 0:
                        _embed_start[0] = time.time()
                    abs_done  = _already_embedded + done
                    abs_total = _already_embedded + total
                    if _embed_start[0] and (time.time() - _embed_start[0]) >= 15:
                        eta = _eta_str(done, total, _embed_start[0])
                    else:
                        eta = "calculating..."
                    progress.update(t_index, completed=abs_done, total=abs_total,
                                    info=f"{abs_done:,} / {abs_total:,} chunks   {eta}")

                buf2 = io.StringIO()
                with contextlib.redirect_stderr(buf2):
                    try:
                        stats = initial_backfill(conn, progress_cb=_progress, phase2_cb=_phase2,
                                                 quiet_embed=True, embed_progress_cb=_embed_progress,
                                                 skip_embed=not _model_ok)
                    except Exception as e:
                        console.print(f"  [yellow]Backfill error: {e}[/yellow]")
                        _warnings.append(f"Backfill: {e}")
                        stats = {'sessions': _phase.get('sessions', 0),
                                 'chunks': _phase.get('chunks', 0),
                                 'elapsed': 0, 'embed_ok': False}

                if not stats.get('embed_ok', True):
                    _warnings.append("Embedding incomplete — vec_ops disabled until re-embedded")
                    progress.update(t_index, completed=stats['chunks'], total=stats['chunks'],
                                    info=f"{stats['chunks']:,} chunks (embedding skipped)")
                else:
                    progress.update(t_index, completed=stats['chunks'], total=stats['chunks'],
                                    info=f"{stats['chunks']:,} chunks embedded")

                progress.update(t_graph, visible=True, info="analyzing")
                def _graph_cb(label):
                    progress.update(t_graph, info=label)
                try:
                    n_clusters, _enrich_failures = _run_enrichment_quiet(conn, progress_cb=_graph_cb)
                except Exception as e:
                    console.print(f"  [yellow]Enrichment error: {e}[/yellow]")
                    _warnings.append(f"Enrichment: {e}")
                    n_clusters, _enrich_failures = 0, []
                cluster_info = f"{n_clusters} topic clusters found" if n_clusters else "done"
                progress.update(t_graph, total=1, completed=1, info=cluster_info)

            console.print()
            console.print(
                f"  [bold]{stats['sessions']:,} sessions[/bold] · "
                f"[bold]{stats['chunks']:,} chunks[/bold]"
                + (f" · [bold]{n_clusters}[/bold] topic clusters" if n_clusters else "")
            )
            if _enrich_failures:
                for _f in _enrich_failures:
                    _warnings.append(f"Enrichment: {_f} skipped")
            console.print()
            try:
                from flex.core import log_op
                log_op(conn, 'init_complete', 'claude_code', rows_affected=stats['chunks'])
            except Exception as e:
                print(f"[init] log_op: {e}", file=sys.stderr)

            try:
                from tzlocal import get_localzone
                _tz = str(get_localzone())
            except Exception:
                import datetime as _dt
                _tz = _dt.datetime.now().astimezone().tzname() or 'UTC'
            conn.execute("INSERT OR REPLACE INTO _meta(key, value) VALUES ('timezone', ?)", [_tz])
            conn.commit()
        finally:
            conn.close()

    # 5. Services
    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        _status = lambda ok: "[green]running[/green]" if ok else "[red]failed[/red]"
        console.print(f"  worker             {_status(worker_ok)}")
        console.print(f"  MCP                {_status(mcp_ok)}")

    # 6. Claude Code wiring
    _patch_claude_json()
    console.print()

    # 7. Final panel
    panel_content = Text()
    panel_content.append("Flex is ready.\n\n", style="cyan")
    panel_content.append("Claude Code            ")
    panel_content.append("MCP server installed\n", style="green")
    panel_content.append("restart or open a new session to connect\n\n", style="dim")
    panel_content.append("MCP Server Endpoint    ")
    panel_content.append("http://localhost:7134/mcp\n", style="green")
    panel_content.append("use with claude.ai, Cursor, or any MCP client", style="dim")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
    console.print()
    console.print("  Ask:", highlight=False)
    console.print('    "Use flex: What did we accomplish today?"', highlight=False)
    console.print('    "Use flex: What\'s the lineage of this file?"', highlight=False)
    console.print()
    console.print("  Agent:", highlight=False)
    console.print('    "Use trace: What projects am I working on?"', highlight=False)
    console.print("    [dim]Spawns a dedicated retrieval sub-agent for deeper searches.[/dim]")
    console.print()
    console.print("  Slash commands:", highlight=False)
    console.print("    /flex:local — search with the current agent", highlight=False)
    console.print("    /flex:agent — delegate to trace", highlight=False)
    console.print()
    console.print("  Control depth by ending your slash command with:", highlight=False)
    console.print("    go           quick", highlight=False)
    console.print("    goo          moderate", highlight=False)
    console.print("    gooo         deep", highlight=False)
    console.print("    goooooooo    exhaustive", highlight=False)
    console.print()

    # Surface warnings back to caller
    if _warnings:
        console.print(f"  [yellow]Completed with {len(_warnings)} warning(s):[/yellow]")
        for w in _warnings:
            console.print(f"    [dim]- {w}[/dim]")
        console.print()
        console.print("  [dim]Run[/dim] [bold]flex sync[/bold] [dim]to repair, or[/dim] [bold]flex init[/bold] [dim]to retry.[/dim]")
        console.print()
        # Hard failures → raise so the dispatcher signals a non-zero exit
        _soft_prefixes = ("Model download:", "Embedding incomplete")
        _hard = [w for w in _warnings if not any(w.startswith(p) for p in _soft_prefixes)]
        if _hard:
            raise RuntimeError(f"init completed with {len(_hard)} hard warning(s)")
