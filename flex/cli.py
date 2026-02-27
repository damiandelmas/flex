#!/usr/bin/env python3
"""
Flex CLI — flex init + flex search.

pip install getflex
flex init              # hooks + daemon + MCP wiring
flex search "query"    # query your sessions
"""

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
CLAUDE_DIR = Path.home() / ".claude"
CLAUDE_JSON = Path.home() / ".claude.json"
HOOKS_DIR = CLAUDE_DIR / "hooks"
SYSTEMD_DIR = Path.home() / ".config" / "systemd" / "user"

# Package data locations (relative to this file)
PKG_ROOT = Path(__file__).parent
CAPTURE_HOOKS_DIR = PKG_ROOT / "modules" / "claude_code" / "compile" / "hooks"
DOCPAC_HOOKS_DIR  = PKG_ROOT / "modules" / "docpac" / "compile" / "hooks"

# PostToolUse matcher — all tool types that produce indexable events
POST_TOOL_MATCHER = (
    "Write|Edit|Read|MultiEdit|NotebookEdit|Grep|Glob|Bash|"
    "WebFetch|WebSearch|Task|TaskOutput|mcp__.*"
)

# Hooks to install
HOOKS = {
    "PostToolUse": [
        {"src": CAPTURE_HOOKS_DIR / "claude-code-capture.sh", "name": "claude-code-capture.sh"},
        {"src": DOCPAC_HOOKS_DIR / "flex-index.sh", "name": "flex-index.sh"},
    ],
    "UserPromptSubmit": [
        {"src": CAPTURE_HOOKS_DIR / "user-prompt-capture.sh", "name": "user-prompt-capture.sh"},
    ],
}


# ============================================================
# flex init
# ============================================================

def _install_hooks():
    """Copy hook scripts to ~/.claude/hooks/ and set executable."""
    HOOKS_DIR.mkdir(parents=True, exist_ok=True)
    installed = []
    for event, hooks in HOOKS.items():
        for hook in hooks:
            if not hook["src"].exists():
                continue  # module not present in this distribution
            dest = HOOKS_DIR / hook["name"]
            shutil.copy2(hook["src"], dest)
            dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            installed.append(hook["name"])
    return installed


def _install_claude_assets():
    """Copy agents and commands from package ai/ dir to ~/.claude/."""
    _claude_src = PKG_ROOT / "ai"
    if not _claude_src.exists():
        return
    for src in _claude_src.rglob("*.md"):
        rel = src.relative_to(_claude_src)
        dest = CLAUDE_DIR / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)


def _patch_settings_json():
    """Non-destructively add hook entries to ~/.claude/settings.json."""
    settings_path = CLAUDE_DIR / "settings.json"
    if settings_path.exists():
        settings = json.loads(settings_path.read_text())
    else:
        CLAUDE_DIR.mkdir(parents=True, exist_ok=True)
        settings = {}

    hooks = settings.setdefault("hooks", {})

    # --- PostToolUse ---
    post_hooks = hooks.setdefault("PostToolUse", [])
    our_commands = {str(HOOKS_DIR / h["name"]) for h in HOOKS["PostToolUse"]}
    # Check if our hooks are already registered
    already = set()
    for group in post_hooks:
        for h in group.get("hooks", []):
            if h.get("command") in our_commands:
                already.add(h["command"])
    missing = our_commands - already
    if missing:
        new_group = {
            "matcher": POST_TOOL_MATCHER,
            "hooks": [
                {"type": "command", "command": cmd, "timeout": 5}
                for cmd in sorted(missing)
            ],
        }
        post_hooks.append(new_group)

    # --- UserPromptSubmit ---
    user_hooks = hooks.setdefault("UserPromptSubmit", [])
    our_commands = {str(HOOKS_DIR / h["name"]) for h in HOOKS["UserPromptSubmit"]}
    already = set()
    for group in user_hooks:
        for h in group.get("hooks", []):
            if h.get("command") in our_commands:
                already.add(h["command"])
    missing = our_commands - already
    if missing:
        new_group = {
            "hooks": [
                {"type": "command", "command": cmd, "timeout": 5}
                for cmd in sorted(missing)
            ],
        }
        user_hooks.append(new_group)

    settings_path.write_text(json.dumps(settings, indent=2) + "\n")


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
            f"ExecStart={python} -m flex.modules.claude_code.compile.worker --daemon\n"
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
            f"ExecStart={python} -m flex.mcp_server --http --port 7134\n"
            "Restart=always\n"
            "RestartSec=5\n"
            "Environment=PYTHONUNBUFFERED=1\n\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        ),
    }
    for service_name, content in _SYSTEMD_UNITS.items():
        (SYSTEMD_DIR / service_name).write_text(content)

    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    subprocess.run(
        ["systemctl", "--user", "enable", "--now", "flex-worker", "flex-mcp"],
        check=True,
    )
    return True


def _patch_claude_json():
    """Add MCP server entry to ~/.claude.json."""
    if CLAUDE_JSON.exists():
        data = json.loads(CLAUDE_JSON.read_text())
    else:
        data = {}

    servers = data.setdefault("mcpServers", {})
    if "flex" not in servers:
        servers["flex"] = {"type": "sse", "url": "http://localhost:7134/sse"}
        CLAUDE_JSON.write_text(json.dumps(data, indent=2) + "\n")
        return True
    return False


def _run_enrichment(conn):
    """Run post-backfill enrichment pipeline.

    Each phase is independent — a failure in graph building does not prevent
    presets and views from being installed (which are required for @orient).
    """
    import time as _time
    t0 = time.time()

    try:
        from flex.modules.claude_code.manage.rebuild_all import (
            rebuild_warmup_types, reembed_sources, rebuild_source_graph,
        )
        from flex.modules.claude_code.manage.enrich_summary import run as run_fingerprints
        from flex.modules.claude_code.manage.enrich_repo_project import run as run_repo_project
        from flex.views import regenerate_views, install_views
        from flex.manage.install_presets import install_cell as install_presets_cell
    except ImportError as e:
        print(f"  [skip] enrichment unavailable: {e}")
        return

    print("  Enriching...")

    # Phase 1: Graph enrichments — each step independent
    for step, fn, label in [
        ("warmup",  lambda: rebuild_warmup_types(conn),     "warmup detection"),
        ("reembed", lambda: reembed_sources(conn),          "source pooling"),
        ("graph",   lambda: rebuild_source_graph(conn),     "graph built"),
    ]:
        try:
            fn()
            print(f"  [ok] {label}")
        except Exception as e:
            print(f"  [warn] {label} skipped: {e}")

    # Phase 2: Session fingerprints
    try:
        n_fp = run_fingerprints(conn)
        print(f"  [ok] {n_fp} sessions fingerprinted")
    except Exception as e:
        print(f"  [warn] fingerprints skipped: {e}")

    # Phase 3: Repo attribution
    try:
        n_rp = run_repo_project(conn)
        print(f"  [ok] {n_rp} sources attributed")
    except Exception as e:
        print(f"  [warn] repo attribution skipped: {e}")

    # Phase 4: Presets + views — always runs, required for @orient
    try:
        install_presets_cell('claude_code')
        print("  [ok] presets installed")
    except Exception as e:
        print(f"  [warn] presets install failed: {e}")

    try:
        view_dir = _find_view_dir('claude_code', 'claude-code')
        if view_dir:
            install_views(conn, view_dir)
            print("  [ok] curated views installed")
        regenerate_views(conn)
        conn.commit()
        print("  [ok] views generated")
    except Exception as e:
        print(f"  [warn] views install failed: {e}")

    print(f"  [ok] enrichment done in {time.time()-t0:.0f}s")


def _run_enrichment_quiet(conn) -> tuple[int, list[str]]:
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
            try:
                fn()
            except Exception:
                failures.append(step)

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
    """Wire hooks, daemon, and MCP for Claude Code capture."""
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

    console.print()
    console.print("  [bold]Setting up flex...[/bold]")
    console.print()

    # 1. Storage
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    (FLEX_HOME / "cells").mkdir(exist_ok=True)

    # Auto-migrate queue.db from 2-col to 3-col schema (pre-0.2.0 installs)
    _qdb = FLEX_HOME / "queue.db"
    if _qdb.exists():
        try:
            _qconn = _sqlite3.connect(str(_qdb), timeout=5)
            _qcols = [r[1] for r in _qconn.execute("PRAGMA table_info(claude_code_pending)")]
            if _qcols and "payload" not in _qcols:
                _qconn.execute("DROP TABLE claude_code_pending")
                _qconn.commit()
            _qconn.close()
        except Exception:
            pass  # queue.db is transient — worst case recreated by next hook

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

    console.print("  [dim]storage[/dim]             [green]ok[/green]")

    # 2. Model
    from flex.onnx.fetch import download_model, model_ready
    _model_ok = True
    if not model_ready():
        console.print("  [dim]model[/dim]               [yellow]downloading...[/yellow]")
        try:
            download_model()
        except RuntimeError as e:
            console.print(f"  [yellow]model[/yellow]               [yellow]failed: {e}[/yellow]")
            console.print(f"  [dim]SQL and FTS will work. Rerun flex init to retry download.[/dim]")
            _model_ok = False
            _warnings.append(f"Model download: {e}")
    if _model_ok:
        console.print("  [dim]model[/dim]               [green]ok[/green]")

    # 3. Pre-flight: check system deps before doing anything
    import shutil as _shutil, os as _os
    _sudo = "" if _os.geteuid() == 0 else "sudo "
    _missing_sys = [b for b in ("jq", "git") if not _shutil.which(b)]
    if _missing_sys:
        console.print()
        console.print("  [yellow]Missing system dependencies — run first:[/yellow]")
        console.print()
        if _shutil.which("brew"):
            console.print(f"  [bold]brew install {' '.join(_missing_sys)}[/bold]")
        else:
            console.print(f"  [bold]{_sudo}apt install {' '.join(_missing_sys)}[/bold]")
        console.print()
        console.print("  Then re-run: [bold]flex init[/bold]")
        console.print()
        return

    # 3b. Hooks + settings + claude assets
    _install_hooks()
    _patch_settings_json()
    _install_claude_assets()
    console.print("  [dim]capture[/dim]             [green]ok[/green]")
    console.print()

    # 4. Sessions
    from flex.modules.claude_code.compile.worker import (
        bootstrap_claude_code_cell, initial_backfill, CLAUDE_PROJECTS,
    )

    jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))
    _enrich_failures: list[str] = []

    if not jsonls:
        console.print("  [dim]No Claude Code sessions found.[/dim]")
        console.print("  [dim]Sessions index automatically as you use Claude Code.[/dim]")
        console.print()
    else:
        console.print(f"  Indexing [bold]{len(jsonls):,}[/bold] sessions")
        console.print()

        cell_path = bootstrap_claude_code_cell()
        conn = _sqlite3.connect(str(cell_path), timeout=30.0)
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

        _nomic_embedder = [None]  # set below if user provides a Nomic API key

        # Prompt for Nomic key BEFORE scanning so user can walk away.
        # Skip entirely if model download failed — no point prompting.
        # Estimate unembedded chunks from unprocessed session count (avg ~50 chunks/session).
        _est_unembedded = (_existing_chunks - _already_embedded) + (len(jsonls) - _already) * 50
        try:
            from flex.onnx.embed import has_gpu as _has_gpu
            if _model_ok and not _has_gpu() and _est_unembedded > 10_000:
                import threading as _threading
                from flex.onnx.nomic_embed import NomicEmbedder as _NomicEmbedder
                _flex_cfg = FLEX_HOME / "secrets"
                _saved_key = None
                if _flex_cfg.exists():
                    for _line in _flex_cfg.read_text().splitlines():
                        if _line.startswith("NOMIC_API_KEY="):
                            _saved_key = _line.split("=", 1)[1].strip()
                            break
                _env_key = os.environ.get("NOMIC_API_KEY", "").strip()
                _flag_key = getattr(args, 'nomic_key', None) or ''
                _force_local = getattr(args, 'local', False)
                key = _saved_key or _flag_key or _env_key

                if _force_local:
                    key = ''  # explicit --local: skip Nomic entirely
                elif key:
                    _ne = _NomicEmbedder(key)
                    _err = _ne.validate()
                    if _err:
                        console.print(f"  [yellow]Nomic key invalid: {_err}[/yellow]")
                        console.print("  Falling back to local CPU.")
                        key = ''
                    else:
                        _nomic_embedder[0] = _ne
                elif not sys.stdin.isatty():
                    # Non-interactive (Docker, CI, PTY tools) — fall through to CPU
                    console.print("  [dim]Non-interactive terminal detected, using local CPU.[/dim]")
                    console.print("  [dim]For faster embeddings: flex init --nomic-key <key>[/dim]")
                    key = ''

                if not key and not _force_local and not _nomic_embedder[0] and sys.stdin.isatty():
                    est_secs = _est_unembedded / 27
                    est_str = f"~{est_secs / 60:.0f}m" if est_secs < 3600 else f"~{est_secs / 3600:.1f}h"
                    console.print("  No GPU detected.")
                    console.print(f"  Estimated time to build your vectors on CPU: {est_str}.")
                    console.print()
                    console.print("  For faster indexing, use the Nomic API (~2m, free tier):")
                    console.print("  [bold blue][link=https://atlas.nomic.ai/cli-login]atlas.nomic.ai/cli-login[/link][/bold blue]")
                    console.print()
                    console.print("  [dim]Ctrl+C is safe —[/dim] [blue]flex init[/blue] [dim]picks up where it left off.[/dim]")
                    console.print()
                    key = ''.join(c for c in input("  Enter Nomic API key (or press Enter to use local CPU): ") if c.isprintable()).strip()
                    if key:
                        _ne = _NomicEmbedder(key)
                        import sys as _sys
                        _done_event = _threading.Event()
                        def _spin():
                            frames = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
                            i = 0
                            while not _done_event.is_set():
                                _sys.stdout.write(f"\r  {frames[i % len(frames)]} Validating key...")
                                _sys.stdout.flush()
                                _done_event.wait(0.1)
                                i += 1
                        _spin_thread = _threading.Thread(target=_spin, daemon=True)
                        _spin_thread.start()
                        _err = _ne.validate()
                        _done_event.set()
                        _spin_thread.join()
                        if _err:
                            _sys.stdout.write(f"\r  ⚠  Key invalid: {_err}\n")
                            _sys.stdout.write("  Falling back to local CPU.\n")
                            _sys.stdout.flush()
                        else:
                            _nomic_embedder[0] = _ne
                            _sys.stdout.write("\r  ✓  Nomic API ready.     \n")
                            _sys.stdout.flush()
                            _lines = [l for l in (_flex_cfg.read_text().splitlines() if _flex_cfg.exists() else []) if not l.startswith("NOMIC_API_KEY=")]
                            _lines.append(f"NOMIC_API_KEY={key}")
                            _flex_cfg.write_text("\n".join(_lines) + "\n")
                    console.print()
        except Exception as e:
            print(f"[init] Nomic setup: {e}", file=sys.stderr)
            # fall through to local ONNX

        with Progress(
            TextColumn("  [dim]{task.description:<20}[/dim]"),
            SpinnerColumn(spinner_name="dots", finished_text="[green]✓[/green]"),
            BarColumn(bar_width=20, complete_style="green", finished_style="green"),
            TextColumn("[dim]{task.fields[info]}[/dim]"),
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
                                             embedder_ref=_nomic_embedder,
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
            progress.update(t_graph, visible=True, info="analyzing...")
            try:
                n_clusters, _enrich_failures = _run_enrichment_quiet(conn)
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

        conn.close()

    # 5. Services
    systemd_ok = _install_systemd()
    if systemd_ok:
        console.print("  [dim]worker[/dim]             [green]running[/green]")
        console.print("  [dim]MCP[/dim]                [green]running[/green]")

    # 6. Claude Code wiring (localhost — direct, no relay)
    _patch_claude_json()
    console.print()

    # Final box
    panel_content = Text()
    panel_content.append("Flex is ready.\n\n", style="bold magenta")
    panel_content.append("Claude Code            ", style="white")
    panel_content.append("MCP server installed\n", style="green")
    panel_content.append("restart or open a new session to connect\n\n", style="dim")
    panel_content.append("MCP Server Endpoint    ", style="white")
    panel_content.append("http://localhost:7134\n", style="green")
    panel_content.append("use with claude.ai, Cursor, or any MCP client", style="dim")
    console.print(Panel(panel_content, padding=(1, 2)))
    console.print()
    console.print("  Ask:")
    console.print('    [blue]"Use flex: What did we accomplish today?"[/blue]')
    console.print('    [blue]"Use flex: What\'s the lineage of this file?"[/blue]')
    console.print()
    console.print("  Agent:")
    console.print('    [blue]"Use flx-trace: What projects am I working on?"[/blue]')
    console.print("    [dim]Spawns a dedicated retrieval sub-agent for deeper searches.[/dim]")
    console.print()
    console.print("  Slash commands:")
    console.print("    [blue]/flex:local[/blue] [dim]— search with the current agent[/dim]")
    console.print("    [blue]/flex:agent[/blue] [dim]— delegate to flx-trace[/dim]")
    console.print()
    console.print("  Control depth by ending your slash command with:")
    console.print("    [dim]go           quick[/dim]")
    console.print("    [dim]goo          moderate[/dim]")
    console.print("    [dim]gooo         deep[/dim]")
    console.print("    [dim]goooooooo    exhaustive[/dim]")
    console.print()

    # Exit code: 0 = full success, 1 = partial completion
    if _warnings:
        console.print(f"  [yellow]Completed with {len(_warnings)} warning(s):[/yellow]")
        for w in _warnings:
            console.print(f"    [dim]- {w}[/dim]")
        console.print()
        console.print("  [dim]Run[/dim] [bold]flex sync[/bold] [dim]to repair, or[/dim] [bold]flex init[/bold] [dim]to retry.[/dim]")
        console.print()
        sys.exit(1)


# ============================================================
# flex index
# ============================================================

def cmd_index(args):
    """Index sessions or corpus."""
    import sqlite3

    if args.source == "claude-code":
        from flex.modules.claude_code.compile.worker import (
            bootstrap_claude_code_cell, initial_backfill, CLAUDE_PROJECTS,
        )

        jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))
        if not jsonls:
            print("No Claude Code sessions found in ~/.claude/projects/")
            return

        print(f"Found {len(jsonls)} Claude Code sessions.")
        cell_path = bootstrap_claude_code_cell()

        conn = sqlite3.connect(str(cell_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")

        def _progress(i, total, sessions, chunks, elapsed):
            sys.stdout.write(
                f"\r  Indexing... {i}/{total} files "
                f"({sessions} sessions, {chunks:,} chunks) [{elapsed:.0f}s]"
            )
            sys.stdout.flush()

        stats = initial_backfill(conn, progress_cb=_progress)
        print()
        print(
            f"Indexed {stats['sessions']} sessions, "
            f"{stats['chunks']:,} chunks in {stats['elapsed']:.0f}s"
        )

        _run_enrichment(conn)
        conn.close()

    elif args.source == "docpac":
        if not args.path:
            print("docpac requires a path: flex index docpac /path/to/corpus")
            return
        subprocess.run(
            [sys.executable, "-m", "flex.modules.docpac.compile.init", args.path],
            check=True,
        )


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

# Map cell_type/name → module library/views/ path (stock views ship with module)
_MODULE_VIEWS = {
    'claude-code':  PKG_ROOT / 'modules' / 'claude_code' / 'stock' / 'views',
    'claude_code':  PKG_ROOT / 'modules' / 'claude_code' / 'stock' / 'views',
    'claude_chat':  PKG_ROOT / 'modules' / 'claude_chat' / 'stock' / 'views',
    'docpac':       PKG_ROOT / 'modules' / 'docpac'      / 'stock' / 'views',
}
# User-owned view subdirectory names (relative to ~/.flex/views/)
_USER_VIEW_DIRS = {
    'claude-code': 'claude_code',
    'claude_code': 'claude_code',
    'claude_chat': 'claude_chat',
    'docpac':      'docpac',
}


def _find_view_dir(cell_name: str, cell_type: str | None) -> Path | None:
    """Resolve the curated view directory for a cell.

    ~/.flex/views/ takes precedence (user library — editable, git-tracked).
    Falls back to module library/views/ (stock views shipped with the module).
    """
    key = cell_type or cell_name
    # User library takes precedence
    sub = _USER_VIEW_DIRS.get(key)
    if sub:
        user_dir = Path.home() / '.flex' / 'views' / sub
        if user_dir.exists():
            return user_dir
    # Stock library fallback (ships with module)
    stock = _MODULE_VIEWS.get(key)
    if stock and stock.exists():
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


def cmd_relay(args):
    """Generate machine_id, start services, print relay URL."""
    try:
        import websockets  # noqa: F401
    except ImportError:
        from rich.console import Console
        Console().print("[red]websockets not installed.[/red] Run: [bold]pip install websockets[/bold]")
        return

    import uuid as _uuid
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text
    console = Console()

    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    machine_id_path = FLEX_HOME / "machine_id"
    if not machine_id_path.exists():
        machine_id_path.write_text(_uuid.uuid4().hex[:8])
    machine_id = machine_id_path.read_text().strip()

    # Start services
    _install_systemd()
    flex_serve = Path("/usr/local/bin/flex-serve")
    if flex_serve.exists():
        import subprocess
        subprocess.run([str(flex_serve)], check=False)

    url = f"https://{machine_id}.getflex.dev/sse"
    panel_content = Text()
    panel_content.append("MCP Server Endpoint\n\n", style="white")
    panel_content.append("  ")
    panel_content.append(url, style="bold blue")
    console.print(Panel(panel_content, padding=(0, 1)))


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
    print("[1/4] Presets")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        install_presets_cell(name)

    # ---- Phase 1.5: Enrichment stubs ----
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        cell_type = cell.get('cell_type')
        stubs = _ENRICHMENT_STUBS.get(cell_type, [])
        if not stubs:
            continue
        db_path = resolve_cell(name)
        if not db_path or not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(str(db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            for ddl in stubs:
                conn.execute(ddl)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"  {name}: STUB FAILED ({e})")

    # ---- Phase 2: Curated views ----
    print()
    print("[2/4] Curated views")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        view_dir = _find_view_dir(name, cell.get('cell_type'))
        if not view_dir:
            print(f"  {name}: SKIP (no curated views)")
            continue
        db_path = resolve_cell(name)
        if not db_path or not db_path.exists():
            print(f"  {name}: SKIP (not found)")
            continue
        try:
            conn = sqlite3.connect(str(db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            install_views(conn, view_dir)
            # Count installed views
            try:
                views = [r[0] for r in conn.execute(
                    "SELECT name FROM _views ORDER BY name"
                ).fetchall()]
                print(f"  {name}: {len(views)} views [{', '.join(views)}]")
            except Exception:
                print(f"  {name}: views installed")
            conn.close()
        except sqlite3.OperationalError as e:
            print(f"  {name}: LOCKED ({e})")

    # ---- Phase 3: Auto-generated views ----
    print()
    print("[3/4] Auto-generated views (regenerate)")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        db_path = resolve_cell(name)
        if not db_path or not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(str(db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            regenerate_views(conn)
            conn.commit()
            conn.close()
            print(f"  {name}: ok")
        except sqlite3.OperationalError as e:
            print(f"  {name}: LOCKED ({e})")

    # ---- Phase 4: Services ----
    print()
    print("[4/4] Services")

    # Install systemd units if missing (recovery from partial init)
    if sys.platform == "linux":
        worker_unit = SYSTEMD_DIR / "flex-worker.service"
        if not worker_unit.exists():
            print("  Installing missing systemd units...")
            try:
                _install_systemd()
            except Exception as e:
                print(f"  systemd install FAILED: {e}")
        elif "flex.modules.claude_code.compile.worker" not in worker_unit.read_text():
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

    # Install MCP wiring if missing (recovery from partial init)
    try:
        if CLAUDE_JSON.exists():
            _cfg = json.loads(CLAUDE_JSON.read_text())
            if "flex" not in _cfg.get("mcpServers", {}):
                print("  MCP wiring: adding (missing from init)")
                _patch_claude_json()
        else:
            print("  MCP wiring: creating ~/.claude.json")
            _patch_claude_json()
    except Exception as e:
        print(f"  MCP wiring: FAILED ({e})")

    # ---- Optional: Full enrichment rebuild ----
    if args.full:
        print()
        print("[5/4] Enrichment rebuild (claude_code)")
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
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        prog="flex",
        description="Your AI sessions, searchable forever.",
    )
    sub = parser.add_subparsers(dest="command")

    # flex init
    init_p = sub.add_parser("init", help="Wire hooks, daemon, and MCP for Claude Code")
    init_p.add_argument("--local", action="store_true", help="Use local CPU for embeddings, skip Nomic prompt")
    init_p.add_argument("--nomic-key", help="Nomic API key (skips interactive prompt)")

    # flex index
    idx = sub.add_parser("index", help="Index sessions or corpus")
    idx.add_argument("source", choices=["claude-code", "docpac"])
    idx.add_argument("path", nargs="?", help="Corpus path (docpac only)")

    # flex search
    search_p = sub.add_parser("search", help="Search your sessions")
    search_p.add_argument("query", help="SQL query, @preset, or vec_ops expression")
    search_p.add_argument("--cell", default="claude_code", help="Cell to query (default: claude_code)")
    search_p.add_argument("--json", action="store_true", help="Output raw JSON")

    # flex sync
    sync_p = sub.add_parser("sync", help="Bring code, data, and services into parity")
    sync_p.add_argument("--cell", default=None, help="Sync specific cell only (default: all)")
    sync_p.add_argument("--full", action="store_true", help="Also rebuild enrichments (~2min)")

    args = parser.parse_args()
    if args.command == "init":
        cmd_init(args)
    elif args.command == "index":
        cmd_index(args)
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "sync":
        cmd_sync(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
