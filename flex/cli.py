#!/usr/bin/env python3
"""
Flex CLI — flex init + flex search.

pip install getflex
flex init              # storage + model + MCP wiring
flex search "query"    # query your sessions
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
CLAUDE_DIR = Path.home() / ".claude"
CLAUDE_JSON = Path.home() / ".claude.json"
SYSTEMD_DIR = Path.home() / ".config" / "systemd" / "user"
LAUNCHD_DIR = Path.home() / "Library" / "LaunchAgents"


def _safe_write(path: Path, content: str):
    """Write content to path, replacing symlinks with regular files."""
    if path.is_symlink():
        path.unlink()
    path.write_text(content)


# Package data locations (relative to this file)
PKG_ROOT = Path(__file__).parent

# ============================================================
# flex init
# ============================================================


def _install_claude_assets():
    """Copy agents, commands, and skills from package ai/ dir to ~/.claude/."""
    _INSTALL_FILES = {
        "agents/trace.md",
        "commands/flex/agent.md",
        "commands/flex/search.md",
        "skills/flex/",                 # session history search skill
    }

    # Subordinate: what we actually want to install right now
    _INSTALL_ASSETS = [
        "agents/trace.md",
        "commands/flex/agent.md",
        "commands/flex/search.md",
        "skills/flex/",
    ]

    _claude_src = PKG_ROOT / "ai"
    if not _claude_src.exists():
        return

    def _is_allowed(rel_str):
        """Check if a relative path is in the install set."""
        for entry in _INSTALL_FILES:
            if entry.endswith("/"):
                if rel_str.startswith(entry) or rel_str == entry.rstrip("/"):
                    return True
            elif rel_str == entry:
                return True
        return False

    for asset in _INSTALL_ASSETS:
        if not _is_allowed(asset):
            print(f"  [warn] {asset} skipped")
            continue

        src_path = _claude_src / asset
        if src_path.is_dir():
            # Copy entire directory (skills)
            for src in src_path.rglob("*"):
                if src.is_file():
                    rel = src.relative_to(_claude_src)
                    dest = CLAUDE_DIR / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.is_symlink():
                        dest.unlink()
                    if src.resolve() != dest.resolve():
                        shutil.copy2(src, dest)
        elif src_path.is_file():
            rel = src_path.relative_to(_claude_src)
            dest = CLAUDE_DIR / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.is_symlink():
                dest.unlink()
            if src_path.resolve() != dest.resolve():
                shutil.copy2(src_path, dest)


def _install_systemd():
    """Generate and install systemd user units. Returns True if installed."""
    if sys.platform != "linux":
        return False

    # Check systemctl is available and functional before writing unit files
    try:
        result = subprocess.run(
            ["systemctl", "--user", "--no-pager", "status"],
            capture_output=True, timeout=5,
        )
        # Exit codes 0-3 are all "systemctl worked" (3 = no units, still available)
        if result.returncode > 3:
            raise subprocess.CalledProcessError(result.returncode, "systemctl")
    except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False

    SYSTEMD_DIR.mkdir(parents=True, exist_ok=True)
    python = sys.executable

    _SYSTEMD_UNITS = {
        "flex-worker.service": (
            "[Unit]\n"
            "Description=Flex Live Capture Worker\n"
            "After=network.target\n\n"
            "[Service]\n"
            "Type=simple\n"
            f"ExecStart={python} -m flex.daemon\n"
            "Restart=on-failure\n"
            "RestartSec=5\n"
            "Environment=PYTHONUNBUFFERED=1\n\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        ),
        "flex-mcp.service": (
            "[Unit]\n"
            "Description=Flex MCP Server\n"
            "After=network.target\n\n"
            "[Service]\n"
            "Type=simple\n"
            f"ExecStart={python} -m flex.serve --http --port 7134\n"
            "Restart=always\n"
            "RestartSec=5\n"
            "Environment=PYTHONUNBUFFERED=1\n\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        ),
    }
    for service_name, content in _SYSTEMD_UNITS.items():
        (SYSTEMD_DIR / service_name).write_text(content)

    for cmd, label in [
        (["systemctl", "--user", "daemon-reload"], "daemon-reload"),
        (["systemctl", "--user", "enable", "--now", "flex-worker", "flex-mcp"], "enable services"),
    ]:
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=15)
        except subprocess.CalledProcessError as e:
            print(f"  [warn] systemd {label}: {e.stderr.decode().strip()}", file=sys.stderr)
            return False
        except subprocess.TimeoutExpired:
            print(f"  [warn] systemd {label}: timeout", file=sys.stderr)
            return False
    return True


def _install_launchd():
    """Generate and install launchd user agents (macOS). Returns True if installed."""
    if sys.platform != "darwin":
        return False

    LAUNCHD_DIR.mkdir(parents=True, exist_ok=True)
    python = sys.executable

    _LAUNCHD_PLISTS = {
        "dev.getflex.worker.plist": f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.getflex.worker</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>-m</string>
        <string>flex.daemon</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{FLEX_HOME / "logs" / "worker.log"}</string>
    <key>StandardErrorPath</key>
    <string>{FLEX_HOME / "logs" / "worker.err"}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>
</dict>
</plist>""",
        "dev.getflex.mcp.plist": f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.getflex.mcp</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>-m</string>
        <string>flex.mcp_server</string>
        <string>--http</string>
        <string>--port</string>
        <string>7134</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{FLEX_HOME / "logs" / "mcp.log"}</string>
    <key>StandardErrorPath</key>
    <string>{FLEX_HOME / "logs" / "mcp.err"}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>
</dict>
</plist>""",
    }

    (FLEX_HOME / "logs").mkdir(exist_ok=True)

    uid = os.getuid()
    for name, content in _LAUNCHD_PLISTS.items():
        plist_path = LAUNCHD_DIR / name
        label = name.replace(".plist", "")
        # Check if already loaded — skip bootstrap if so
        probe = subprocess.run(
            ["launchctl", "print", f"user/{uid}/{label}"],
            capture_output=True, timeout=10,
        )
        already_loaded = probe.returncode == 0
        if already_loaded:
            # Update plist in place, kickstart to pick up changes
            plist_path.write_text(content)
            subprocess.run(
                ["launchctl", "kickstart", "-k", f"user/{uid}/{label}"],
                capture_output=True, timeout=10,
            )
        else:
            plist_path.write_text(content)
            result = subprocess.run(
                ["launchctl", "bootstrap", f"user/{uid}", str(plist_path)],
                capture_output=True, timeout=10,
            )
            if result.returncode != 0:
                print(f"  [warn] launchctl bootstrap {name}: {result.stderr.decode().strip()}", file=sys.stderr)

    return True


def _is_port_open(port: int) -> bool:
    """Check if a TCP port is accepting connections on localhost."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _is_worker_alive() -> bool:
    """Check if worker daemon is running via PID file or process scan."""
    pid_file = FLEX_HOME / "worker.pid"
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)  # signal 0 = existence check
            return True
        except (ValueError, OSError):
            pid_file.unlink(missing_ok=True)
    # Fallback: scan process table
    try:
        r = subprocess.run(
            ["pgrep", "-f", "flex.daemon"],
            capture_output=True, timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


def _verify_services() -> tuple:
    """Check if worker and MCP are actually running. Returns (worker_ok, mcp_ok)."""
    # MCP: retry port check (service may still be starting)
    mcp_ok = False
    for _ in range(6):  # 3 seconds max
        if _is_port_open(7134):
            mcp_ok = True
            break
        time.sleep(0.5)

    worker_ok = _is_worker_alive()
    return worker_ok, mcp_ok


def _start_services_direct():
    """Start worker + MCP as background processes. Last resort fallback.
    Skipped on Windows — stdio MCP transport handles it per-session."""
    if sys.platform == "win32":
        return
    python = sys.executable
    log_dir = FLEX_HOME / "logs"
    log_dir.mkdir(exist_ok=True)

    def _start(name, cmd, pid_file, log_file):
        # Don't start if already running
        if pid_file.exists():
            try:
                os.kill(int(pid_file.read_text().strip()), 0)
                return  # already running
            except (ValueError, OSError):
                pid_file.unlink(missing_ok=True)

        with open(log_file, "a") as out, open(log_file.with_suffix(".err"), "a") as err:
            proc = subprocess.Popen(cmd, stdout=out, stderr=err, start_new_session=True)
        pid_file.write_text(str(proc.pid))

    if not _is_worker_alive():
        _start("worker",
               [python, "-m", "flex.daemon"],
               FLEX_HOME / "worker.pid", log_dir / "worker.log")

    if not _is_port_open(7134):
        _start("mcp",
               [python, "-m", "flex.mcp_server", "--http", "--port", "7134"],
               FLEX_HOME / "mcp.pid", log_dir / "mcp.log")


def _kill_pid_services():
    """Kill any directly-started services by PID file. Ensures clean handoff to service manager."""
    if sys.platform == "win32":
        return
    import signal
    for pid_name in ["worker.pid", "mcp.pid"]:
        pid_file = FLEX_HOME / pid_name
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                os.kill(pid, signal.SIGTERM)
            except (ValueError, OSError):
                pass
            pid_file.unlink(missing_ok=True)


def _patch_claude_json():
    """Add MCP server entry to ~/.claude.json.

    Streamable HTTP transport — stateless per-request, supports unlimited
    concurrent Claude Code sessions. One process, warm cache, shared across
    sessions. Services (systemd/launchd) keep it running.
    """
    if CLAUDE_JSON.exists():
        try:
            data = json.loads(CLAUDE_JSON.read_text())
        except (json.JSONDecodeError, ValueError):
            backup = CLAUDE_JSON.with_suffix('.json.bak')
            CLAUDE_JSON.rename(backup)
            print(f"  [warn] Corrupt {CLAUDE_JSON.name} — backed up to {backup.name}", file=sys.stderr)
            data = {}
    else:
        data = {}

    servers = data.setdefault("mcpServers", {})
    entry = {"type": "http", "url": "http://localhost:7134/mcp/"}
    if "flex" not in servers or servers["flex"] != entry:
        servers["flex"] = entry
        _safe_write(CLAUDE_JSON, json.dumps(data, indent=2) + "\n")
        return True
    return False


def _run_enrichment_quiet(conn, progress_cb=None) -> tuple[int, list[str]]:
    """Run enrichment silently. Returns (cluster_count, failed_step_names)."""
    import io
    import contextlib

    try:
        from flex.modules.claude_code.manage.rebuild_all import (
            rebuild_warmup_types, reembed_sources, rebuild_source_graph,
            rebuild_community_labels, rebuild_file_graph, rebuild_delegation_graph,
        )
        from flex.modules.claude_code.manage.enrich_summary import run as run_fingerprints
        from flex.modules.claude_code.manage.enrich_soma_repos import run as _register_soma_repos
        from flex.modules.claude_code.manage.enrich_repo_project import run as run_repo_project
        from flex.views import regenerate_views, install_views
        from flex.manage.install_presets import install_cell as install_presets_cell
    except ImportError:
        return 0, []

    failures: list[str] = []
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        for step, fn in [
            ("warmup types",       lambda: rebuild_warmup_types(conn)),
            ("source pooling",     lambda: reembed_sources(conn)),
            ("source graph",       lambda: rebuild_source_graph(conn)),
            ("file graph",         lambda: rebuild_file_graph(conn)),
            ("delegation graph",   lambda: rebuild_delegation_graph(conn)),
            ("fingerprints",       lambda: run_fingerprints(conn)),
            ("repo registry",      lambda: _register_soma_repos(conn)),
            ("repo attribution",   lambda: run_repo_project(conn)),
            ("community labels",   lambda: rebuild_community_labels(conn)),
        ]:
            if progress_cb:
                progress_cb(step)
            try:
                fn()
            except Exception:
                failures.append(step)

        if progress_cb:
            progress_cb("presets")
        try:
            # Use existing conn directly to avoid a second connection fighting for
            # the write lock while conn is still open.
            from flex.manage.install_presets import install_presets
            from flex.manage.install_presets import GENERAL_DIR, MODULE_PRESETS, MODULE_ROOT
            for pd in [GENERAL_DIR] + MODULE_PRESETS.get('claude-code', []):
                if pd.exists():
                    install_presets(conn, pd)
            conn.commit()
            n_presets = conn.execute("SELECT COUNT(*) FROM _presets").fetchone()[0]
            if n_presets == 0:
                failures.append("presets (0 installed)")
        except Exception:
            failures.append("presets")

        if progress_cb:
            progress_cb("views")
        try:
            view_dir = _find_view_dir('claude_code', 'claude-code')
            if view_dir:
                install_views(conn, view_dir)
            regenerate_views(conn)
            conn.commit()
        except Exception:
            failures.append("views")

    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph"
            " WHERE community_id IS NOT NULL"
        ).fetchone()
        return (row[0] if row else 0), failures
    except Exception:
        return 0, failures




def cmd_init(args):
    """Initialize flex. With --module claude-code, also indexes sessions and starts services."""
    import io
    import contextlib
    from rich.console import Console
    from rich.progress import (
        Progress, TextColumn, BarColumn, TimeElapsedColumn, SpinnerColumn,
    )
    from rich.panel import Panel
    from rich.text import Text
    import sqlite3 as _sqlite3

    console = Console()
    _warnings: list[str] = []  # accumulate phase failures for exit code
    _module = getattr(args, 'module', None)
    _is_claude_code = _module == 'claude-code'

    # Stop running worker/MCP before init to avoid DB lock contention
    if _is_claude_code:
        _kill_pid_services()

    console.print()

    # 0. Pre-flight: check system deps before doing anything
    _sudo = "" if getattr(os, 'geteuid', lambda: 1)() == 0 else "sudo "
    _missing_sys = [b for b in ("git",) if not shutil.which(b)]
    if _missing_sys:
        console.print("  [yellow]Missing system dependencies — run first:[/yellow]")
        console.print()
        if shutil.which("brew"):
            console.print(f"  [bold]brew install {' '.join(_missing_sys)}[/bold]")
        else:
            console.print(f"  [bold]{_sudo}apt install {' '.join(_missing_sys)}[/bold]")
        console.print()
        console.print("  Then re-run: [bold]flex init[/bold]")
        console.print()
        return

    console.print("  [cyan]Setting up flex[/cyan]...", highlight=False)
    console.print()

    # 1. Storage
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    (FLEX_HOME / "cells").mkdir(exist_ok=True)

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        try:
            from flex.modules.soma.lib.identity.file_identity import FileIdentity
            from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
            from flex.modules.soma.lib.identity.url_identity import URLIdentity
            from flex.modules.soma.lib.identity.content_identity import ContentIdentity
            FileIdentity(); RepoIdentity(); URLIdentity(); ContentIdentity()
            from flex.modules.soma.lib.eternity.eternity import Eternity
            Eternity()
        except ImportError:
            pass  # SOMA optional — keep silent
        except Exception as e:
            print(f"[init] SOMA init: {e}", file=sys.stderr)

    console.print("  storage             [green]ok[/green]")

    # 2. Model
    from flex.onnx.fetch import download_model, model_ready
    _model_ok = True
    _model_valid = model_ready()
    if not _model_valid:
        console.print("  model               downloading")
        try:
            download_model()
        except RuntimeError as e:
            console.print(f"  [yellow]model[/yellow]               [yellow]failed[/yellow]")
            console.print(f"  [dim]Semantic search disabled. SQL and keyword search still work.[/dim]")
            console.print(f"  [dim]Rerun [bold]flex init[/bold] when online to download the embedding model.[/dim]")
            _model_ok = False
            _warnings.append(f"Model download: {e}")
    if _model_ok:
        console.print("  model               [green]ok[/green]")

    # 3. Claude assets (claude-code only)
    if _is_claude_code:
        _install_claude_assets()
        console.print("  capture             [green]ok[/green]")
    console.print()

    if not _is_claude_code:
        # Base install done — no MCP wiring (no cell/service to serve)
        console.print()
        panel_content = Text()
        panel_content.append("Flex is ready.\n\n", style="cyan")
        panel_content.append("  flex search          ", style="bold")
        panel_content.append("query from terminal\n", style="dim")
        panel_content.append("For Claude Code session search:\n", style="dim")
        panel_content.append("  curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code\n", style="bold")
        console.print(Panel(panel_content, padding=(1, 2), highlight=False))
        console.print()
        return

    # 4. Sessions (claude-code only)
    try:
        from flex.modules.claude_code.compile.worker import (
            bootstrap_claude_code_cell, initial_backfill, CLAUDE_PROJECTS,
        )
    except ImportError:
        console.print("[yellow]Claude Code module not available. Skipping session setup.[/yellow]")
        return

    jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))
    _enrich_failures: list[str] = []

    # Always bootstrap cell (even if empty) so flex search works immediately
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
            _svd = _find_view_dir('claude_code', 'claude-code')
            if _svd:
                _siv(_stub_conn, _svd)
            _srv(_stub_conn)
            _stub_conn.commit()
            _sip('claude_code')
        except Exception:
            pass
    finally:
        _stub_conn.close()

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

            # Install enrichment stubs — ensures tables exist even if enrichment fails
            for ddl in _ENRICHMENT_STUBS.get('claude-code', []):
                conn.execute(ddl)
            conn.commit()

            # Install views + presets early (only need stub tables to exist)
            try:
                from flex.views import install_views as _iv, regenerate_views as _rv
                from flex.manage.install_presets import install_cell as _install_presets_cell
                _vd = _find_view_dir('claude_code', 'claude-code')
                if _vd:
                    _iv(conn, _vd)
                _rv(conn)
                _install_presets_cell('claude_code')
                conn.commit()
            except Exception as e:
                print(f"[init] Views/presets install failed: {e}", file=sys.stderr)
                _warnings.append(f"Views/presets: {e}")

            # Seed from existing cell for resume display — show resume hint
            _already = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
            if _already > 0:
                console.print(f"  [dim]({_already:,} already indexed, resuming)[/dim]")
                console.print()
            _existing_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
            _already_embedded = conn.execute(
                "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
            ).fetchone()[0]
            _phase = {"sessions": _already, "chunks": _existing_chunks}

            # Embedding uses flex-embed (Rust) or Python ONNX fallback.
            # No cloud API needed.

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

                # Graph + enrichment (spinner, fully silent)
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

    # 5. Services (skip on Windows — stdio MCP transport handles it per-session)
    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)  # give service manager a moment
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        _status = lambda ok: "[green]running[/green]" if ok else "[red]failed[/red]"
        console.print(f"  worker             {_status(worker_ok)}")
        console.print(f"  MCP                {_status(mcp_ok)}")

    # 6. Claude Code wiring — streamable HTTP to localhost:7134
    _patch_claude_json()
    console.print()

    # Final box
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

    # Exit code: 0 = success (possibly with soft warnings), 1 = hard failure
    _soft_prefixes = ("Model download:", "Embedding incomplete")
    _hard = [w for w in _warnings if not any(w.startswith(p) for p in _soft_prefixes)]
    if _warnings:
        console.print(f"  [yellow]Completed with {len(_warnings)} warning(s):[/yellow]")
        for w in _warnings:
            console.print(f"    [dim]- {w}[/dim]")
        console.print()
        console.print("  [dim]Run[/dim] [bold]flex sync[/bold] [dim]to repair, or[/dim] [bold]flex init[/bold] [dim]to retry.[/dim]")
        console.print()
        if _hard:
            sys.exit(1)



# ============================================================
# flex search
# ============================================================

def _open_cell_for_search(cell_name: str):
    """Open a cell with vec_ops UDF registered. Returns (db, cleanup) or exits."""
    from flex.registry import resolve_cell
    from flex.core import open_cell

    path = resolve_cell(cell_name)
    if path is None:
        print(f"Cell '{cell_name}' not found.", file=sys.stderr)
        print("Run 'flex init' first, then use Claude Code to build your index.", file=sys.stderr)
        sys.exit(1)

    db = open_cell(str(path))

    # Try to register vec_ops (needs ONNX + embeddings)
    try:
        from flex.retrieve.vec_ops import VectorCache, register_vec_ops
        from flex.onnx import get_model

        embedder = get_model()
        caches = {}
        for table, id_col in [("_raw_chunks", "id"), ("_raw_sources", "source_id")]:
            try:
                cache = VectorCache()
                cache.load_from_db(db, table, "embedding", id_col)
                if cache.size > 0:
                    cache.load_columns(db, table, id_col)
                    caches[table] = cache
            except Exception:
                pass

        if caches and embedder:
            # Read vec config from _meta
            config = {}
            try:
                rows = db.execute(
                    "SELECT key, value FROM _meta WHERE key LIKE 'vec:%'"
                ).fetchall()
                config = {r[0]: r[1] for r in rows}
            except Exception:
                pass
            embed_query = lambda text: embedder.encode(text, prefix='search_query: ')
            embed_doc   = lambda text: embedder.encode(text, prefix='search_document: ')
            register_vec_ops(db, caches, embed_query, config, embed_doc_fn=embed_doc)
    except ImportError:
        pass  # vec_ops won't work but plain SQL is fine

    return db


def _format_results(result_json: str, as_json: bool = False) -> str:
    """Format query results for terminal output."""
    if as_json:
        return result_json

    data = json.loads(result_json)
    if isinstance(data, dict) and "error" in data:
        return f"Error: {data['error']}"
    if not isinstance(data, list) or len(data) == 0:
        return "No results."

    # Simple table format
    keys = list(data[0].keys())
    # Compute column widths
    widths = {k: len(k) for k in keys}
    for row in data:
        for k in keys:
            val = str(row.get(k, ""))
            if len(val) > 80:
                val = val[:77] + "..."
            widths[k] = max(widths[k], len(val))

    # Cap total width
    lines = []
    header = "  ".join(k.ljust(widths[k]) for k in keys)
    lines.append(header)
    lines.append("  ".join("-" * widths[k] for k in keys))
    for row in data:
        vals = []
        for k in keys:
            val = str(row.get(k, ""))
            if len(val) > 80:
                val = val[:77] + "..."
            vals.append(val.ljust(widths[k]))
        lines.append("  ".join(vals))

    return "\n".join(lines)


def cmd_search(args):
    """Execute a query against a cell."""
    # Lazy import — avoids pulling in mcp deps at CLI startup
    from flex.mcp_server import execute_query

    db = _open_cell_for_search(args.cell)
    try:
        result = execute_query(db, args.query)
        print(result)
    finally:
        db.close()


# ============================================================
# flex sync
# ============================================================

# Normalize cell_type aliases to module directory names
_TYPE_TO_MODULE = {
    'claude-code': 'claude_code',
}


def _find_view_dir(cell_name: str, cell_type: str | None) -> Path | None:
    """Resolve the curated view directory for a cell.

    ~/.flex/views/ takes precedence (user library — editable, git-tracked).
    Falls back to module stock/views/ (auto-discovered from PKG_ROOT/modules/).
    No hardcoded module list — any module with stock/views/ is found.
    """
    key = cell_type or cell_name
    # Normalize aliases (e.g. 'claude-code' → 'claude_code')
    module_name = _TYPE_TO_MODULE.get(key, key)

    # User library takes precedence
    user_dir = Path.home() / '.flex' / 'views' / module_name
    if user_dir.exists() and any(user_dir.glob('*.sql')):
        return user_dir

    # Stock library fallback — auto-discover from module directory
    stock = PKG_ROOT / 'modules' / module_name / 'stock' / 'views'
    if stock.exists() and any(stock.glob('*.sql')):
        return stock

    return None


# Enrichment stub DDL — curated views LEFT JOIN these tables.
# Empty stubs let views work before enrichments run.
_ENRICHMENT_STUBS = {
    'claude-code': [
        """CREATE TABLE IF NOT EXISTS _enrich_source_graph (
            source_id TEXT PRIMARY KEY, centrality REAL, is_hub INTEGER DEFAULT 0,
            is_bridge INTEGER DEFAULT 0, community_id INTEGER, community_label TEXT)""",
        """CREATE TABLE IF NOT EXISTS _types_source_warmup (
            source_id TEXT PRIMARY KEY, is_warmup_only INTEGER DEFAULT 0)""",
        """CREATE TABLE IF NOT EXISTS _enrich_session_summary (
            source_id TEXT PRIMARY KEY, fingerprint_index TEXT)""",
        """CREATE TABLE IF NOT EXISTS _enrich_repo_identity (
            repo_root TEXT PRIMARY KEY, repo_path TEXT, project TEXT, git_remote TEXT)""",
        """CREATE TABLE IF NOT EXISTS _enrich_file_graph (
            source_id TEXT PRIMARY KEY, file_community_id INTEGER, file_centrality REAL,
            file_is_hub INTEGER DEFAULT 0, shared_file_count INTEGER)""",
        """CREATE TABLE IF NOT EXISTS _enrich_delegation_graph (
            source_id TEXT PRIMARY KEY, agents_spawned INTEGER,
            is_orchestrator INTEGER DEFAULT 0, delegation_depth INTEGER,
            parent_session TEXT)""",
        """CREATE TABLE IF NOT EXISTS _ops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER DEFAULT (strftime('%s','now')),
            operation TEXT, target TEXT, sql TEXT, params TEXT,
            rows_affected INTEGER, source TEXT)""",
        """CREATE TABLE IF NOT EXISTS _views (
            name TEXT PRIMARY KEY, sql TEXT NOT NULL,
            description TEXT, created_at INTEGER)""",
    ],
}



def _check_fts(conn, cell_name: str):
    """Check FTS5 consistency for a cell connection."""
    chunks_count = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    # chunks_fts
    try:
        fts_count = conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
        if fts_count != chunks_count:
            print(f"    chunks_fts drift ({fts_count} vs {chunks_count}) — rebuilding")
            conn.execute("INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')")
            conn.commit()
        else:
            print(f"    chunks_fts ok ({fts_count} rows)")
    except Exception:
        print(f"    chunks_fts not present (skip)")
    # content_fts
    try:
        content_count = conn.execute("SELECT COUNT(*) FROM _raw_content").fetchone()[0]
        content_fts_count = conn.execute("SELECT COUNT(*) FROM content_fts").fetchone()[0]
        if content_fts_count != content_count:
            print(f"    content_fts drift ({content_fts_count} vs {content_count}) — rebuilding")
            conn.execute("INSERT INTO content_fts(content_fts) VALUES('rebuild')")
            conn.commit()
    except Exception:
        pass  # content_fts may not exist on all cell types


def cmd_sync(args):
    """Bring all three layers (code, data, services) into parity."""
    import sqlite3
    import time

    from flex.registry import list_cells, resolve_cell
    from flex.views import regenerate_views, install_views
    from flex.manage.install_presets import install_cell as install_presets_cell

    cells = list_cells()
    if not cells:
        print("No cells registered. Run 'flex init' first.")
        return

    target = args.cell  # None = all cells

    print("flex sync")
    print()

    # ---- Phase 1: Presets ----
    print("[1/5] Presets")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        try:
            install_presets_cell(name)
        except Exception as e:
            print(f"  {name}: FAILED ({e})")

    # ---- Phase 2: Cell sync (stubs + curated views + auto views + FTS5) ----
    print()
    print("[2/5] Cell sync")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        db_path = resolve_cell(name)
        if not db_path or not db_path.exists():
            print(f"  {name}: SKIP (not found)")
            continue
        conn = None
        try:
            conn = sqlite3.connect(str(db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")

            # Stubs
            cell_type = cell.get('cell_type')
            for ddl in _ENRICHMENT_STUBS.get(cell_type, []):
                conn.execute(ddl)
            conn.commit()

            # Curated views
            view_dir = _find_view_dir(name, cell_type)
            if view_dir:
                install_views(conn, view_dir)

            # Auto views
            regenerate_views(conn)
            conn.commit()

            # FTS5 consistency
            _check_fts(conn, name)

            # Summary
            try:
                views = [r[0] for r in conn.execute(
                    "SELECT name FROM _views ORDER BY name"
                ).fetchall()]
                print(f"  {name}: ok ({len(views)} views [{', '.join(views)}])")
            except Exception:
                print(f"  {name}: ok")
        except Exception as e:
            print(f"  {name}: FAILED ({e})")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ---- Phase 3: Services ----
    print()
    print("[3/5] Services")

    # Install service units if missing (recovery from partial init)
    if sys.platform == "linux":
        worker_unit = SYSTEMD_DIR / "flex-worker.service"
        if not worker_unit.exists():
            print("  Installing missing systemd units")
            try:
                _install_systemd()
            except Exception as e:
                print(f"  systemd install FAILED: {e}")
        elif "flex.daemon" not in worker_unit.read_text():
            pass  # custom unit — don't overwrite

        for service in ["flex-worker", "flex-mcp"]:
            try:
                subprocess.run(
                    ["systemctl", "--user", "restart", service],
                    check=True, capture_output=True, timeout=10,
                )
                print(f"  {service}: restarted")
            except subprocess.CalledProcessError as e:
                print(f"  {service}: FAILED ({e.stderr.decode().strip()})")
            except FileNotFoundError:
                print(f"  {service}: SKIP (systemctl not found)")

    elif sys.platform == "darwin":
        worker_plist = LAUNCHD_DIR / "dev.getflex.worker.plist"
        if not worker_plist.exists():
            print("  Installing missing launchd agents")
            try:
                _install_launchd()
            except Exception as e:
                print(f"  launchd install FAILED: {e}")
        else:
            # Kill any PID-managed processes before launchd takes over
            _kill_pid_services()
            uid = os.getuid()
            for label in ["dev.getflex.worker", "dev.getflex.mcp"]:
                try:
                    subprocess.run(
                        ["launchctl", "kickstart", "-k",
                         f"user/{uid}/{label}"],
                        capture_output=True, timeout=10,
                    )
                    print(f"  {label}: restarted")
                except Exception as e:
                    print(f"  {label}: FAILED ({e})")

    # Verify services actually started (all platforms)
    time.sleep(1)
    worker_ok, mcp_ok = _verify_services()
    if not worker_ok or not mcp_ok:
        _start_services_direct()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
    if not worker_ok:
        print("  worker: FAILED (could not start)")
    if not mcp_ok:
        print("  MCP: FAILED (could not start)")

    # ---- Phase 4: MCP wiring ----
    print()
    print("[4/5] MCP wiring")
    try:
        if CLAUDE_JSON.exists():
            _cfg = json.loads(CLAUDE_JSON.read_text())
            if "flex" not in _cfg.get("mcpServers", {}):
                print("  adding (missing from init)")
                _patch_claude_json()
            else:
                print("  ok")
        else:
            print("  creating ~/.claude.json")
            _patch_claude_json()
    except Exception as e:
        print(f"  FAILED ({e})")

    # ---- Phase 5: Optional enrichment rebuild ----
    if args.full:
        print()
        print("[5/5] Enrichment rebuild (claude_code)")
        try:
            t0 = time.time()
            result = subprocess.run(
                [sys.executable, "-m", "flex.modules.claude_code.manage.rebuild_all"],
                capture_output=True, text=True, timeout=600,
                cwd=str(PKG_ROOT.parent),
            )
            elapsed = time.time() - t0
            if result.returncode == 0:
                # Print last few lines of output
                lines = result.stdout.strip().splitlines()
                for line in lines[-10:]:
                    print(f"  {line}")
                print(f"  done in {elapsed:.1f}s")
            else:
                print(f"  FAILED (exit {result.returncode})")
                for line in result.stderr.strip().splitlines()[-5:]:
                    print(f"  {line}")
        except subprocess.TimeoutExpired:
            print("  TIMEOUT (>600s)")
        except Exception as e:
            print(f"  ERROR: {e}")

    print()
    print("Sync complete.")


# ============================================================
# flex remove
# ============================================================

def _human_size(n: int) -> str:
    """Format bytes as human-readable."""
    if n < 1024:
        return f"{n}B"
    elif n < 1048576:
        return f"{n / 1024:.0f}KB"
    elif n < 1073741824:
        return f"{n / 1048576:.0f}MB"
    return f"{n / 1073741824:.1f}GB"




def cmd_remove(args):
    """Delete and unregister a cell."""
    from flex.registry import resolve_cell, unregister_cell

    for name in args.cells:
        cell_path = resolve_cell(name)
        if cell_path and cell_path.exists():
            cell_path.unlink()
            print(f"  {name}: deleted {cell_path}")

        if unregister_cell(name):
            print(f"  {name}: unregistered")
        else:
            print(f"  {name}: not found in registry")


def cmd_status(args):
    """Show cell health, lifecycle, and refresh status."""
    from flex.registry import list_cells
    import sqlite3 as _sqlite3

    cells = list_cells()
    if not cells:
        print("No cells registered. Run 'flex init' first.")
        return

    # Filter unlisted unless --all
    if not args.all:
        cells = [c for c in cells if not c.get('unlisted')]

    if args.json:
        import json
        out = []
        for c in cells:
            entry = {
                'name': c['name'],
                'cell_type': c.get('cell_type'),
                'lifecycle': c.get('lifecycle', 'static'),
                'refresh_status': c.get('refresh_status'),
                'last_refresh_at': c.get('last_refresh_at'),
                'refresh_interval': c.get('refresh_interval'),
            }
            try:
                db = _sqlite3.connect(c['path'], timeout=5)
                entry['chunks'] = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
                entry['sources'] = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
                db.close()
            except Exception:
                entry['chunks'] = entry['sources'] = None
            out.append(entry)
        print(json.dumps(out, indent=2))
        return

    # Table format
    print(f"{'CELL':<24} {'TYPE':<14} {'LIFECYCLE':<10} {'LAST REFRESH':<22} {'STATUS':<8} {'CHUNKS':>8} {'SOURCES':>8}")
    print("─" * 104)

    for c in cells:
        name = c['name'][:23]
        ct = (c.get('cell_type') or '—')[:13]
        lc = (c.get('lifecycle') or 'static')[:9]
        lr = (c.get('last_refresh_at') or '—')[:21]
        rs = (c.get('refresh_status') or '—')[:7]

        try:
            db = _sqlite3.connect(c['path'], timeout=5)
            chunks = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
            sources = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
            db.close()
        except Exception:
            chunks = sources = '?'

        print(f"{name:<24} {ct:<14} {lc:<10} {lr:<22} {rs:<8} {chunks:>8} {sources:>8}")

    # Summary
    total_refresh = sum(1 for c in cells if c.get('lifecycle') == 'refresh')
    total_watch = sum(1 for c in cells if c.get('lifecycle') == 'watch')
    errors = sum(1 for c in cells if (c.get('refresh_status') or '').startswith('error'))
    print(f"\n{len(cells)} cells ({total_refresh} refresh, {total_watch} watch, {errors} errors)")



# ============================================================
# Main
# ============================================================

def _gnu_flex_proxy():
    """Detect GNU flex usage and transparently forward to the real lexer.

    GNU flex (the lexer generator) ships with Xcode on macOS and is common
    on Linux dev machines.  Our `flex` binary can shadow it.  Instead of
    breaking the user's C build, we detect GNU-style invocations and exec
    the real thing.
    """
    argv = sys.argv[1:]
    if not argv:
        return  # bare `flex` — ours

    # Our commands — definitely not GNU flex
    our_commands = {"init", "search", "sync", "remove", "status", "-h", "--help"}
    if argv[0] in our_commands:
        return

    # Heuristics: does this look like GNU flex?
    gnu_extensions = {".l", ".lex", ".ll", ".l++", ".lxx"}
    gnu_flags = {
        "-o", "--outfile", "--header-file", "--header",
        "-C", "-Ca", "-Ce", "-Cf", "-CF", "-Cm", "-Cr",
        "-d", "--debug",
        "-i", "--case-insensitive",
        "-l", "--lex-compat",
        "-L", "--noline",
        "-s", "--nodefault",
        "-t", "--stdout",
        "-v", "--verbose",
        "-V", "--version",
        "-w", "--nowarn",
        "-B", "--batch",
        "-I", "--interactive",
        "-P", "--prefix",
        "-S", "--skel",
        "--nounistd", "--bison-bridge", "--bison-locations",
        "--posix", "--noansi-definitions", "--noansi-prototypes",
    }

    looks_gnu = False
    for arg in argv:
        if any(arg.endswith(ext) for ext in gnu_extensions):
            looks_gnu = True
            break
        if arg in gnu_flags or any(arg.startswith(f + "=") for f in gnu_flags):
            looks_gnu = True
            break

    if not looks_gnu:
        return

    # Find the real GNU flex — skip ourselves
    my_path = os.path.realpath(shutil.which("flex") or "")
    real_flex = None

    # Check well-known paths first
    for candidate in ["/usr/bin/flex", "/usr/local/opt/flex/bin/flex"]:
        if os.path.isfile(candidate) and os.path.realpath(candidate) != my_path:
            real_flex = candidate
            break

    # Fall back to which -a
    if not real_flex:
        try:
            all_paths = subprocess.check_output(
                ["which", "-a", "flex"], text=True, stderr=subprocess.DEVNULL
            ).strip().split("\n")
            for p in all_paths:
                if os.path.realpath(p) != my_path:
                    real_flex = p
                    break
        except subprocess.CalledProcessError:
            pass

    if real_flex:
        os.execv(real_flex, [real_flex] + argv)
        # execv replaces the process — never returns
    else:
        print(
            "This is getflex (AI knowledge engine), not GNU flex (lexer generator).\n"
            "GNU flex doesn't appear to be installed on this system.\n"
            "\n"
            "  Install GNU flex:  apt install flex  /  brew install flex\n"
            "  Use getflex:       flex init  |  flex search  |  flex sync",
            file=sys.stderr,
        )
        sys.exit(1)


def main():
    _gnu_flex_proxy()
    from flex.registry import load_plugins
    load_plugins()

    parser = argparse.ArgumentParser(
        prog="flex",
        description="Your AI sessions, searchable forever.",
    )
    sub = parser.add_subparsers(dest="command")

    # flex init
    init_p = sub.add_parser("init", help="Initialize flex (base or with a module)")
    init_p.add_argument("--module", default=None, help="Module to install (e.g. claude-code). Without this, installs base flex only.")

    # flex search
    search_p = sub.add_parser("search", help="Search your sessions")
    search_p.add_argument("query", help="SQL query, @preset, or vec_ops expression")
    search_p.add_argument("--cell", default="claude_code", help="Cell to query (default: claude_code)")
    search_p.add_argument("--json", action="store_true", help="Output raw JSON")

    # flex sync
    sync_p = sub.add_parser("sync", help="Bring code, data, and services into parity")
    sync_p.add_argument("--cell", default=None, help="Sync specific cell only (default: all)")
    sync_p.add_argument("--full", action="store_true", help="Also rebuild enrichments (~2min)")

    # Plugin commands — modules register their own subcommands via hooks
    from flex.registry import get_hook
    _register_commands = get_hook("register_cli_commands")
    if _register_commands:
        _register_commands(sub)

    # flex status
    stat_p = sub.add_parser("status", help="Show cell health and refresh status")
    stat_p.add_argument("--json", action="store_true", help="Machine-readable output")
    stat_p.add_argument("--all", action="store_true", help="Include unlisted cells")

    _register_extra = get_hook("register_extra_commands")
    if _register_extra:
        _register_extra(sub)

    args = parser.parse_args()
    try:
        if args.command == "init":
            cmd_init(args)
        elif args.command == "search":
            cmd_search(args)
        elif args.command == "sync":
            cmd_sync(args)
        elif args.command == "remove":
            cmd_remove(args)
        elif args.command == "status":
            cmd_status(args)
        elif hasattr(args, 'func'):
            args.func(args)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        print("\n  Interrupted. Run flex init again to resume.")
        sys.exit(130)


if __name__ == "__main__":
    main()
