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
from pathlib import Path

FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
CLAUDE_DIR = Path.home() / ".claude"
CLAUDE_JSON = Path.home() / ".claude.json"
HOOKS_DIR = CLAUDE_DIR / "hooks"
SYSTEMD_DIR = Path.home() / ".config" / "systemd" / "user"

# Package data locations (relative to this file)
PKG_ROOT = Path(__file__).parent
CAPTURE_HOOKS_DIR = PKG_ROOT / "modules" / "claude_code" / "compile" / "hooks"
DATA_HOOKS_DIR = PKG_ROOT / "data" / "hooks"
SYSTEMD_TMPL_DIR = PKG_ROOT / "data" / "systemd"

# PostToolUse matcher — all tool types that produce indexable events
POST_TOOL_MATCHER = (
    "Write|Edit|Read|MultiEdit|NotebookEdit|Grep|Glob|Bash|"
    "WebFetch|WebSearch|Task|TaskOutput|mcp__.*"
)

# Hooks to install
HOOKS = {
    "PostToolUse": [
        {"src": CAPTURE_HOOKS_DIR / "claude-code-capture.sh", "name": "claude-code-capture.sh"},
        {"src": DATA_HOOKS_DIR / "flex-index.sh", "name": "flex-index.sh"},
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

    SYSTEMD_DIR.mkdir(parents=True, exist_ok=True)
    python = sys.executable

    for tmpl_name, service_name in [
        ("flex-worker.service.tmpl", "flex-worker.service"),
        ("flex-mcp.service.tmpl", "flex-mcp.service"),
    ]:
        tmpl = (SYSTEMD_TMPL_DIR / tmpl_name).read_text()
        rendered = tmpl.replace("{{PYTHON}}", python)
        (SYSTEMD_DIR / service_name).write_text(rendered)

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


def cmd_init(args):
    """Wire hooks, daemon, and MCP for Claude Code capture."""
    print("flex init")
    print()

    # 1. Create ~/.flex/
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    (FLEX_HOME / "cells").mkdir(exist_ok=True)
    print("  [ok] ~/.flex/ created")

    # 2. Install hooks
    installed = _install_hooks()
    print(f"  [ok] hooks installed: {', '.join(installed)}")

    # 3. Patch settings.json
    _patch_settings_json()
    print("  [ok] ~/.claude/settings.json patched")

    # 4. Install systemd services
    if _install_systemd():
        print("  [ok] flex-worker + flex-mcp services started")

    # 5. Patch .claude.json
    if _patch_claude_json():
        print("  [ok] MCP server wired (localhost:8081)")
    else:
        print("  [ok] MCP server already wired")

    print()
    print("Done. Claude Code will now capture your sessions automatically.")
    print("Use 'flex search \"your query\"' to search, or ask Claude directly.")


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
            register_vec_ops(db, caches, embedder.encode, config)
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

# Map cell_type → curated view directory (relative to repo root)
_VIEW_DIRS = {
    'claude-code': 'views/claude_code',
    'claude_chat': 'views/claude_chat',    # legacy name in registry
    'docpac': 'views/docpac',
}
# Also map by cell name for cells whose cell_type doesn't match
_VIEW_DIRS_BY_NAME = {
    'claude_code': 'views/claude_code',
    'claude_chat': 'views/claude_chat',
}


def _find_view_dir(cell_name: str, cell_type: str | None) -> Path | None:
    """Resolve the curated view directory for a cell."""
    repo_root = PKG_ROOT.parent  # flex/ -> main/
    # Try by cell_type first, then by name
    rel = _VIEW_DIRS.get(cell_type) or _VIEW_DIRS_BY_NAME.get(cell_name)
    if rel:
        d = repo_root / rel
        if d.exists():
            return d
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
    from flex.utils.install_presets import install_cell as install_presets_cell

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
    elif args.command == "search":
        cmd_search(args)
    elif args.command == "sync":
        cmd_sync(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
