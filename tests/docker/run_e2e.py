"""
E2E assertion script — runs inside the Docker container.

Calls `flex init` as a real subprocess (no monkeypatching),
then queries the resulting cell to assert all invariants.
Exit 0 = pass, exit 1 = fail.
"""
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

FLEX_HOME = Path.home() / ".flex"
PASS = "\033[32m[PASS]\033[0m"
FAIL = "\033[31m[FAIL]\033[0m"

failures = []


def check(name, condition, detail=""):
    if condition:
        print(f"  {PASS} {name}")
    else:
        msg = f"{name}" + (f": {detail}" if detail else "")
        print(f"  {FAIL} {msg}")
        failures.append(msg)


# ── Run flex init ─────────────────────────────────────────────────────────────
print("=" * 60)
print("Running: flex init")
print("=" * 60)

result = subprocess.run(
    ["flex", "init"],
    capture_output=False,   # let output stream to terminal
    timeout=600,
)

print()
print("=" * 60)
print("Asserting invariants")
print("=" * 60)

check("flex init exit 0", result.returncode == 0,
      f"exit code {result.returncode}")

# ── Filesystem ────────────────────────────────────────────────────────────────
check("~/.flex/ created",        FLEX_HOME.exists())
check("~/.flex/cells/ exists",   (FLEX_HOME / "cells").exists())
check("registry.db exists",      (FLEX_HOME / "registry.db").exists())

settings = Path.home() / ".claude" / "settings.json"
check("settings.json exists",    settings.exists())
claude_json = Path.home() / ".claude.json"
check(".claude.json exists",     claude_json.exists())

# ── Settings.json hooks ───────────────────────────────────────────────────────
if settings.exists():
    s = json.loads(settings.read_text())
    hooks = s.get("hooks", {})
    check("PostToolUse hooked",    "PostToolUse" in hooks)
    check("UserPromptSubmit hooked", "UserPromptSubmit" in hooks)
    all_cmds = []
    for group in hooks.get("PostToolUse", []):
        for h in group.get("hooks", []):
            all_cmds.append(h.get("command", ""))
    check("capture hook registered",
          any("claude-code-capture" in c for c in all_cmds),
          f"cmds: {all_cmds}")

# ── claude.json MCP ──────────────────────────────────────────────────────────
if claude_json.exists():
    d = json.loads(claude_json.read_text())
    servers = d.get("mcpServers", {})
    check("flex MCP entry",      "flex" in servers)
    check("MCP URL correct",
          servers.get("flex", {}).get("url") == "http://localhost:7532/sse")

# ── Cell contents ─────────────────────────────────────────────────────────────
import flex.registry as reg
cell_path = reg.resolve_cell("claude_code")
check("cell registered", cell_path is not None)
check("cell db on disk",  cell_path is not None and cell_path.exists())

if cell_path and cell_path.exists():
    conn = sqlite3.connect(str(cell_path))

    n_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    n_sessions = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    n_embedded = conn.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]

    check("chunks indexed",      n_chunks > 0,     f"got {n_chunks}")
    check("sessions indexed",    n_sessions == 2,  f"got {n_sessions}")
    check("embeddings present",  n_embedded > 0,   f"got {n_embedded}")

    tool_names = {r[0] for r in conn.execute(
        "SELECT DISTINCT tool_name FROM _edges_tool_ops"
    ).fetchall()}
    check("tool_ops extracted",
          bool({"Read", "Edit", "Write"} & tool_names),
          f"got {tool_names}")

    msg_types = {r[0] for r in conn.execute(
        "SELECT DISTINCT type FROM _types_message"
    ).fetchall()}
    check("message types classified",
          bool({"user_prompt", "tool_call"} & msg_types),
          f"got {msg_types}")

    preset_names = {r[0] for r in conn.execute(
        "SELECT name FROM _presets"
    ).fetchall()}
    check("@orient installed",   "orient" in preset_names,
          f"presets: {sorted(preset_names)}")

    view_names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'"
    ).fetchall()}
    check("messages view",   "messages" in view_names, f"views: {view_names}")
    check("sessions view",   "sessions" in view_names, f"views: {view_names}")

    # embedding dimension — all must be uniform (guards against mixed-model artifacts)
    dims = {len(r[0]) // 4 for r in conn.execute(
        "SELECT embedding FROM _raw_chunks WHERE embedding IS NOT NULL LIMIT 500"
    ).fetchall()}
    check("uniform embedding dims", len(dims) == 1, f"got dims: {dims}")

    # enrichment has rows — not just table existence
    n_graph = conn.execute(
        "SELECT COUNT(*) FROM _enrich_source_graph"
    ).fetchone()[0]
    check("source graph has rows", n_graph > 0, f"got {n_graph}")

    conn.close()

# ── flex-serve + vec_ops ───────────────────────────────────────────────────────
import shutil, time

if shutil.which("flex-serve"):
    print()
    print("=" * 60)
    print("Starting flex-serve and testing vec_ops")
    print("=" * 60)

    subprocess.Popen(["flex-serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)  # wait for VectorCache to warm

    r = subprocess.run(
        ["flex", "search", "--json",
         "SELECT v.id, v.score FROM vec_ops('_raw_chunks', 'test query') v LIMIT 3"],
        capture_output=True, text=True, timeout=30,
    )
    check("vec_ops exit 0", r.returncode == 0, r.stderr[:200] if r.stderr else "")
    if r.returncode == 0:
        try:
            rows = json.loads(r.stdout)
            check("vec_ops returns rows", len(rows) > 0, f"got {len(rows)}")
            check("vec_ops score field",
                  all("score" in row for row in rows), "missing score")
        except Exception as e:
            check("vec_ops json parse", False, str(e))

# ── Summary ───────────────────────────────────────────────────────────────────
print()
print("=" * 60)
if failures:
    print(f"\033[31mFAILED — {len(failures)} checks failed:\033[0m")
    for f in failures:
        print(f"  • {f}")
    sys.exit(1)
else:
    print("\033[32mAll checks passed — flex init E2E OK\033[0m")
    sys.exit(0)
