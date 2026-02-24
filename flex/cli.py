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
            dest = HOOKS_DIR / hook["name"]
            shutil.copy2(hook["src"], dest)
            dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            installed.append(hook["name"])
    return installed


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
        print("  [skip] systemd not available (not Linux)")
        print("         macOS launchd support coming in v0.1")
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
        print("  [skip] systemd not available (container or non-systemd Linux)")
        print("         Start worker manually: python -m flex.modules.claude_code.compile.worker --daemon")
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
            f"ExecStart={python} -m flex.mcp_server --http --port 8081\n"
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
        servers["flex"] = {"type": "sse", "url": "http://localhost:8081/sse"}
        CLAUDE_JSON.write_text(json.dumps(data, indent=2) + "\n")
        return True
    return False


def _run_enrichment(conn):
    """Run post-backfill enrichment pipeline.

    Each phase is independent — a failure in graph building does not prevent
    presets and views from being installed (which are required for @orient).
    """
    import time as _time
    t0 = _time.time()

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

    print(f"  [ok] enrichment done in {_time.time()-t0:.0f}s")


def cmd_init(args):
    """Wire hooks, daemon, and MCP for Claude Code capture."""
    print("flex init")
    print()

    # 1. Create ~/.flex/
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    (FLEX_HOME / "cells").mkdir(exist_ok=True)
    print("  [ok] ~/.flex/ created")

    # 1b. Initialize SOMA identity (~/.soma/)
    try:
        from flex.modules.soma.lib.identity.file_identity import FileIdentity
        from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
        from flex.modules.soma.lib.identity.url_identity import URLIdentity
        from flex.modules.soma.lib.identity.content_identity import ContentIdentity
        FileIdentity()
        RepoIdentity()
        URLIdentity()
        ContentIdentity()
        print("  [ok] ~/.soma/ ready (file, repo, url, content identity)")
        from flex.modules.soma.lib.eternity.eternity import Eternity
        Eternity()  # creates ~/.soma/backups/
        print("  [ok] eternity ready (backup, git versioning, cloud sync)")
    except ImportError:
        print("  [warn] soma identity unavailable — file tracking disabled")

    # 2. Install model (copy from bundled package, or download from GitHub)
    from flex.onnx.fetch import download_model, model_ready
    if model_ready():
        print("  [ok] model ready")
    else:
        print("  Installing embedding model...")
        try:
            download_model()
            print("  [ok] model installed")
        except RuntimeError as e:
            print(f"  [FAIL] {e}")
            print("  Continuing without model — backfill will be skipped.")
            print()
            # Fall through to hooks/services without backfill
            _install_hooks()
            print("  [ok] hooks installed")
            _patch_settings_json()
            print("  [ok] settings.json patched")
            if _install_systemd():
                print("  [ok] services started")
            _patch_claude_json()
            print()
            print("Done (partial — model download failed).")
            return

    # 3. Install hooks
    installed = _install_hooks()
    print(f"  [ok] hooks installed: {', '.join(installed)}")

    # 4. Patch settings.json
    _patch_settings_json()
    print("  [ok] ~/.claude/settings.json patched")

    # 5. Detect sessions + bootstrap cell
    from flex.modules.claude_code.compile.worker import (
        bootstrap_claude_code_cell, initial_backfill, CLAUDE_PROJECTS,
    )
    import sqlite3

    jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))
    if jsonls:
        print(f"  Found {len(jsonls)} Claude Code sessions.")
        cell_path = bootstrap_claude_code_cell()
        print(f"  [ok] cell ready ({cell_path.name})")

        # 6. Backfill
        conn = sqlite3.connect(str(cell_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")

        # Live progress display — background thread repaints every 250ms so
        # the elapsed counter and spinner keep moving during slow embeds.
        import threading

        _state = {"i": 0, "sessions": 0, "chunks": 0, "done": False}
        _t0 = time.time()
        _spinner = "|/-\\"

        def _ticker():
            tick = 0
            while not _state["done"]:
                elapsed = time.time() - _t0
                i, total_ = _state["i"], len(jsonls)
                pct = i / total_ * 100 if total_ else 0
                spin = _spinner[tick % 4]
                sys.stdout.write(
                    f"\r  {spin} {i}/{total_} ({pct:.0f}%)  "
                    f"{_state['sessions']:,} sessions  "
                    f"{_state['chunks']:,} chunks  "
                    f"{elapsed:.0f}s      "
                )
                sys.stdout.flush()
                tick += 1
                time.sleep(0.25)

        _thread = threading.Thread(target=_ticker, daemon=True)
        _thread.start()

        def _progress(i, total, sessions, chunks, elapsed):
            _state["i"] = i
            _state["sessions"] = sessions
            _state["chunks"] = chunks

        stats = initial_backfill(conn, progress_cb=_progress)
        _state["done"] = True
        _thread.join()
        sys.stdout.write("\r" + " " * 72 + "\r")  # clear line
        sys.stdout.flush()
        print(
            f"  [ok] indexed {stats['sessions']} sessions, "
            f"{stats['chunks']:,} chunks in {stats['elapsed']:.0f}s"
        )

        # 7. Enrichment
        _run_enrichment(conn)
        conn.close()
    else:
        print("  No Claude Code sessions found. Cell will be created on first use.")

    # 8. Install systemd services (AFTER cell exists)
    if _install_systemd():
        print("  [ok] flex-worker + flex-mcp services started")

    # 9. Patch .claude.json
    if _patch_claude_json():
        print("  [ok] MCP server wired (localhost:8081)")
    else:
        print("  [ok] MCP server already wired")

    print()
    print("Done. Your sessions are searchable.")
    print("Use 'flex search \"your query\"' or ask Claude directly.")


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
            is_bridge INTEGER DEFAULT 0, community_id INTEGER)""",
        """CREATE TABLE IF NOT EXISTS _types_source_warmup (
            source_id TEXT PRIMARY KEY, is_warmup_only INTEGER DEFAULT 0)""",
        """CREATE TABLE IF NOT EXISTS _enrich_session_summary (
            source_id TEXT PRIMARY KEY, fingerprint_index TEXT)""",
    ],
}


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
        except Exception:
            pass  # best-effort

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
    sub.add_parser("init", help="Wire hooks, daemon, and MCP for Claude Code")

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
