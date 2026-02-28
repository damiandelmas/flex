"""
Dirty environment test runner — validates flx in hostile/unusual environments.

Scenarios:
  devtools  — GNU flex collision (/usr/bin/flex is the lexer)
  conda     — Python path resolution inside conda env
  upgrade   — 0.1.43 -> current upgrade path
  minimal   — missing git/jq, expect graceful failure

Usage:
    python3 /run_dirty.py --scenario devtools
    python3 /run_dirty.py --scenario conda
    python3 /run_dirty.py --scenario upgrade
    python3 /run_dirty.py --scenario minimal

Exit 0 = pass, exit 1 = fail.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from harness import Harness

FLEX_HOME = Path.home() / ".flex"
CLAUDE_JSON = Path.home() / ".claude.json"


def _mcp_config():
    """Load MCP config from ~/.claude.json, return the flex server entry or None."""
    if not CLAUDE_JSON.exists():
        return None
    try:
        d = json.loads(CLAUDE_JSON.read_text())
        return d.get("mcpServers", {}).get("flex")
    except Exception:
        return None


def _run(cmd, **kwargs):
    """Run a command and return the CompletedProcess."""
    defaults = {"capture_output": True, "text": True, "timeout": 120}
    defaults.update(kwargs)
    return subprocess.run(cmd, **defaults)


# ── Scenario: devtools ────────────────────────────────────────────────────────

def scenario_devtools():
    """GNU flex collision — /usr/bin/flex is the lexer, flx is getflex."""
    h = Harness("dirty-devtools")

    # ── Verify GNU flex occupies /usr/bin/flex ─────────────────────────────
    h.phase("GNU flex collision")

    which_flex = shutil.which("flex")
    h.check("which flex returns /usr/bin/flex",
            which_flex == "/usr/bin/flex",
            f"got: {which_flex}")

    which_flx = shutil.which("flx")
    h.check("which flx returns getflex binary",
            which_flx is not None,
            f"got: {which_flx}")

    # ── flx CLI works ────────────────────────────────────────────────────
    h.phase("flx CLI")

    r = _run(["flx", "--help"])
    h.check("flx --help exit 0", r.returncode == 0,
            r.stderr[:200] if r.stderr else "")

    # ── flx init --local ─────────────────────────────────────────────────
    h.phase("flx init")

    r = subprocess.run(["flx", "init", "--local"],
                       capture_output=False, timeout=600)
    h.check("flx init --local exit 0", r.returncode == 0,
            f"exit code {r.returncode}")
    h.check("~/.flex/ created", FLEX_HOME.exists())

    # ── flx search ───────────────────────────────────────────────────────
    h.phase("flx search")

    r = _run(["flx", "search", "SELECT COUNT(*) FROM sessions"])
    h.check("flx search exit 0", r.returncode == 0,
            r.stderr[:200] if r.stderr else "")

    # ── MCP config: stdio transport ──────────────────────────────────────
    h.phase("MCP config")

    h.check("~/.claude.json exists", CLAUDE_JSON.exists())
    srv = _mcp_config()
    h.check("flex MCP entry exists", srv is not None)
    if srv:
        h.check("MCP has 'command' key (stdio)",
                "command" in srv,
                f"keys: {list(srv.keys())}")
        h.check("MCP has no 'url' key (not SSE)",
                "url" not in srv,
                f"keys: {list(srv.keys())}")

    return h


# ── Scenario: conda ──────────────────────────────────────────────────────────

def scenario_conda():
    """Python path resolution inside conda env."""
    h = Harness("dirty-conda")

    # ── sys.executable contains conda path ────────────────────────────────
    h.phase("Conda Python path")

    exe = sys.executable
    has_conda_path = "conda" in exe or "envs" in exe
    h.check("sys.executable contains conda/envs",
            has_conda_path,
            f"sys.executable = {exe}")

    # ── flx init --local ─────────────────────────────────────────────────
    h.phase("flx init")

    r = subprocess.run(["flx", "init", "--local"],
                       capture_output=False, timeout=600)
    h.check("flx init --local exit 0", r.returncode == 0,
            f"exit code {r.returncode}")

    # ── MCP command matches sys.executable ────────────────────────────────
    h.phase("MCP config")

    h.check("~/.claude.json exists", CLAUDE_JSON.exists())
    srv = _mcp_config()
    h.check("flex MCP entry exists", srv is not None)
    if srv:
        cmd = srv.get("command", "")
        h.check("MCP command matches sys.executable",
                exe in cmd,
                f"sys.executable={exe}, command={cmd}")

    # ── flx search ───────────────────────────────────────────────────────
    h.phase("flx search")

    r = _run(["flx", "search", "@orient"])
    h.check("flx search @orient exit 0", r.returncode == 0,
            r.stderr[:200] if r.stderr else "")

    # ── import flex works ────────────────────────────────────────────────
    h.phase("Import check")

    try:
        import flex
        h.check("import flex works", True)
    except ImportError as e:
        h.check("import flex works", False, str(e))

    return h


# ── Scenario: upgrade ────────────────────────────────────────────────────────

def scenario_upgrade():
    """0.1.43 -> current upgrade path."""
    h = Harness("dirty-upgrade")

    # ── flx init --local on top of any existing ~/.flex/ ──────────────────
    h.phase("flx init after upgrade")

    r = subprocess.run(["flx", "init", "--local"],
                       capture_output=False, timeout=600)
    h.check("flx init --local exit 0", r.returncode == 0,
            f"exit code {r.returncode}")
    h.check("~/.flex/ exists", FLEX_HOME.exists())

    # ── Cell data exists ─────────────────────────────────────────────────
    h.phase("Cell data")

    import flex.registry as reg
    cell_path = reg.resolve_cell("claude_code")
    h.check("cell registered", cell_path is not None)

    if cell_path and cell_path.exists():
        import sqlite3
        conn = sqlite3.connect(str(cell_path))
        n_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        n_sessions = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
        conn.close()

        h.check("chunks indexed after upgrade", n_chunks > 0, f"got {n_chunks}")
        h.check("sessions indexed after upgrade", n_sessions > 0, f"got {n_sessions}")

    # ── flx search works ─────────────────────────────────────────────────
    h.phase("flx search")

    r = _run(["flx", "search", "--json",
              "SELECT COUNT(*) as n FROM sessions"])
    h.check("flx search exit 0", r.returncode == 0,
            r.stderr[:200] if r.stderr else "")

    # ── MCP config: stdio, not SSE ───────────────────────────────────────
    h.phase("MCP config")

    h.check("~/.claude.json exists", CLAUDE_JSON.exists())
    srv = _mcp_config()
    h.check("flex MCP entry exists", srv is not None)
    if srv:
        h.check("MCP has stdio config ('command' key)",
                "command" in srv,
                f"keys: {list(srv.keys())}")
        h.check("MCP has no SSE config ('url' key)",
                "url" not in srv,
                f"keys: {list(srv.keys())}")

    # ── flx sync ─────────────────────────────────────────────────────────
    h.phase("flx sync")

    r = _run(["flx", "sync"], timeout=60)
    h.check("flx sync exit 0", r.returncode == 0,
            r.stderr[:200] if r.stderr else "")

    return h


# ── Scenario: minimal ────────────────────────────────────────────────────────

def scenario_minimal():
    """No git, no jq — expect graceful failure."""
    h = Harness("dirty-minimal")

    # ── Verify git/jq are absent ─────────────────────────────────────────
    h.phase("Missing dependencies")

    h.check("git not installed", shutil.which("git") is None,
            f"found at: {shutil.which('git')}")
    h.check("jq not installed", shutil.which("jq") is None,
            f"found at: {shutil.which('jq')}")

    # ── flx init --local should fail with clear error ────────────────────
    h.phase("flx init failure")

    r = _run(["flx", "init", "--local"])
    h.check("flx init exit non-zero", r.returncode != 0,
            f"exit code {r.returncode}")

    # Check that error message mentions missing deps
    output = (r.stdout or "") + (r.stderr or "")
    output_lower = output.lower()
    mentions_git = "git" in output_lower
    mentions_jq = "jq" in output_lower
    h.check("error mentions git or jq",
            mentions_git or mentions_jq,
            f"output: {output[:500]}")

    return h


# ── Main ──────────────────────────────────────────────────────────────────────

SCENARIOS = {
    "devtools": scenario_devtools,
    "conda": scenario_conda,
    "upgrade": scenario_upgrade,
    "minimal": scenario_minimal,
}


def main():
    parser = argparse.ArgumentParser(description="Dirty environment test runner")
    parser.add_argument("--scenario", required=True, choices=list(SCENARIOS.keys()),
                        help="Scenario to run")
    args = parser.parse_args()

    h = SCENARIOS[args.scenario]()
    sys.exit(h.finish())


if __name__ == "__main__":
    main()
