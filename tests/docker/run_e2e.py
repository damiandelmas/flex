"""
E2E assertion script — runs inside the Docker container.

Calls `flex init` as a real subprocess (no monkeypatching),
then queries the resulting cell to assert all invariants.
Exit 0 = pass, exit 1 = fail.
"""
import json
import os
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

init_cmd = ["flex", "init"]
nomic_key = os.environ.get("NOMIC_API_KEY", "")
if nomic_key:
    init_cmd.extend(["--nomic-key", nomic_key])
else:
    init_cmd.append("--local")

result = subprocess.run(
    init_cmd,
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
          servers.get("flex", {}).get("url") == "http://localhost:7134/sse")

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
    check("sessions indexed",    n_sessions > 0,   f"got {n_sessions}")
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

    # enrichment table exists (graph needs ≥20 chunks/session to populate — seed data is small)
    has_graph_table = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = '_enrich_source_graph'"
    ).fetchone()[0]
    check("source graph table exists", has_graph_table > 0)
    # If sessions are large enough to pass the noise floor, graph should have rows
    big_sessions = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT source_id FROM _edges_source GROUP BY source_id HAVING COUNT(*) >= 20"
        ")"
    ).fetchone()[0]
    if big_sessions >= 3:
        n_graph = conn.execute(
            "SELECT COUNT(*) FROM _enrich_source_graph"
        ).fetchone()[0]
        check("source graph has rows", n_graph > 0, f"got {n_graph}")

    # ── Delegation edges ──────────────────────────────────────────────────────
    n_deleg = conn.execute(
        "SELECT COUNT(*) FROM _edges_delegations"
    ).fetchone()[0]
    n_deleg_valid = conn.execute(
        "SELECT COUNT(*) FROM _edges_delegations d "
        "JOIN _raw_chunks r ON d.chunk_id = r.id"
    ).fetchone()[0]
    check("delegation edges exist", n_deleg > 0, f"got {n_deleg}")
    check("delegation 100% JOIN rate",
          n_deleg == n_deleg_valid,
          f"total={n_deleg} valid={n_deleg_valid}")

    n_agent_type = conn.execute(
        "SELECT COUNT(*) FROM _edges_delegations WHERE agent_type IS NOT NULL"
    ).fetchone()[0]
    check("delegation agent_type populated",
          n_agent_type == n_deleg,
          f"nonnull={n_agent_type} total={n_deleg}")

    # no duplicate delegation edges
    n_dupes = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT chunk_id, child_session_id, COUNT(*) as c"
        "  FROM _edges_delegations GROUP BY chunk_id, child_session_id HAVING c > 1"
        ")"
    ).fetchone()[0]
    check("delegation no duplicates", n_dupes == 0, f"got {n_dupes} dupes")

    # ── Preset param validation ───────────────────────────────────────────────
    # @story with no session param should return error, not crash
    r_preset = subprocess.run(
        ["flex", "search", "--json", "@story"],
        capture_output=True, text=True, timeout=15,
    )
    check("preset missing param no crash", r_preset.returncode == 0)
    if r_preset.returncode == 0:
        try:
            out = json.loads(r_preset.stdout)
            has_error = (isinstance(out, dict) and "error" in out) or (
                isinstance(out, list) and len(out) > 0
                and isinstance(out[0], dict) and "error" in out[0])
            check("preset missing param returns error", has_error,
                  f"got: {r_preset.stdout[:200]}")
        except Exception:
            # non-JSON is also acceptable (error message)
            check("preset missing param returns error", True)

    # ── View exclusions (graceful with empty _meta) ───────────────────────────
    # Views should work even without exclude_paths/_meta keys seeded
    conn.row_factory = sqlite3.Row
    try:
        n_msg_view = conn.execute("SELECT COUNT(*) as n FROM messages").fetchone()[0]
        check("messages view queryable", n_msg_view > 0, f"got {n_msg_view}")
    except Exception as e:
        check("messages view queryable", False, str(e))

    try:
        n_sess_view = conn.execute("SELECT COUNT(*) as n FROM sessions").fetchone()[0]
        check("sessions view queryable", n_sess_view > 0, f"got {n_sess_view}")
    except Exception as e:
        check("sessions view queryable", False, str(e))

    conn.close()

# ── flex-serve + vec_ops ───────────────────────────────────────────────────────
import shutil, time

if shutil.which("flex-serve"):
    print()
    print("=" * 60)
    print("Starting flex-serve and testing vec_ops")
    print("=" * 60)

    # Kill any leftover daemons from prior runs to avoid non-deterministic side effects
    subprocess.run(["flex-serve", "--stop"], capture_output=True)
    time.sleep(1)

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

    # ── Authorizer — write operations blocked ─────────────────────────────────
    for label, sql in [
        ("DELETE blocked",  "DELETE FROM _raw_chunks WHERE 1=0"),
        ("DROP blocked",    "DROP TABLE _raw_chunks"),
        ("comment bypass blocked", "/* hi */ DROP TABLE _raw_chunks"),
    ]:
        r_auth = subprocess.run(
            ["flex", "search", "--json", sql],
            capture_output=True, text=True, timeout=15,
        )
        if r_auth.returncode == 0:
            try:
                out = json.loads(r_auth.stdout)
                blocked = isinstance(out, dict) and "error" in out
                check(label, blocked, f"got: {r_auth.stdout[:200]}")
            except Exception:
                check(label, True)  # non-JSON error output = blocked
        else:
            check(label, True)  # non-zero exit = blocked

    # ── Orient structure ─────────────────────────────────────────────────────
    r_orient = subprocess.run(
        ["flex", "search", "--json", "@orient"],
        capture_output=True, text=True, timeout=15,
    )
    check("orient exit 0", r_orient.returncode == 0,
          r_orient.stderr[:200] if r_orient.stderr else "")
    if r_orient.returncode == 0:
        try:
            orient_data = json.loads(r_orient.stdout)
            # orient returns {query: name, results: [...]} blocks
            query_names = set()
            all_results = []
            if isinstance(orient_data, list):
                for block in orient_data:
                    if isinstance(block, dict) and "query" in block:
                        query_names.add(block["query"])
                        all_results.extend(block.get("results", []))

            # No spurious "default" block
            check("orient no default block",
                  "default" not in query_names,
                  f"query blocks: {sorted(query_names)}")

            # No empty "retrieval" block
            check("orient no retrieval block",
                  "retrieval" not in query_names,
                  f"query blocks: {sorted(query_names)}")

            # query_surface has the three tiers: view, table_function, edge_table
            surface_kinds = set()
            for block in orient_data if isinstance(orient_data, list) else []:
                if block.get("query") == "query_surface":
                    for row in block.get("results", []):
                        if isinstance(row, dict) and "kind" in row:
                            surface_kinds.add(row["kind"])
            check("orient query_surface has views",
                  "view" in surface_kinds,
                  f"kinds: {sorted(surface_kinds)}")
            check("orient query_surface has table_function",
                  "table_function" in surface_kinds,
                  f"kinds: {sorted(surface_kinds)}")
            check("orient query_surface has edge_table",
                  "edge_table" in surface_kinds,
                  f"kinds: {sorted(surface_kinds)}")

            # about block has description (not empty)
            about_has_desc = False
            for block in orient_data if isinstance(orient_data, list) else []:
                if block.get("query") == "about":
                    for row in block.get("results", []):
                        if isinstance(row, dict):
                            # May be {description: "..."} or {key: "description", value: "..."}
                            desc = row.get("description") or (
                                row.get("value") if row.get("key") == "description" else None)
                            if desc and desc.strip():
                                about_has_desc = True
            check("orient about has description", about_has_desc)

        except json.JSONDecodeError:
            check("orient json parse", False, f"raw: {r_orient.stdout[:300]}")

# ── Claude Code MCP integration ───────────────────────────────────────────────
# If OAuth credentials are available, test the full chain:
# Claude Code → flex MCP → cell query → real result
creds = Path.home() / ".claude" / ".credentials.json"
if creds.exists() and shutil.which("claude"):
    print()
    print("=" * 60)
    print("Testing Claude Code → flex MCP integration")
    print("=" * 60)

    r_claude = subprocess.run(
        ["claude", "-p",
         "--output-format", "json",
         "--allowedTools", "mcp__flex__flex_search",
         "--max-turns", "3",
         'Use the flex MCP tool (mcp__flex__flex_search) to run this exact SQL query: '
         'SELECT COUNT(*) as n FROM sessions. '
         'The cell parameter should be "claude_code". '
         'Return ONLY the number from the result, nothing else.'],
        capture_output=True, text=True, timeout=120,
    )

    try:
        claude_out = json.loads(r_claude.stdout) if r_claude.stdout else {}
    except json.JSONDecodeError:
        claude_out = {}

    is_error = claude_out.get("is_error", True)
    result_text = str(claude_out.get("result", ""))
    num_turns = claude_out.get("num_turns", 0)

    check("claude -p exit 0", r_claude.returncode == 0,
          r_claude.stderr[:200] if r_claude.stderr else "")
    check("claude MCP no error", not is_error,
          f"result: {result_text[:200]}")
    check("claude MCP used tool", num_turns >= 2,
          f"num_turns={num_turns} (1 = no tool call)")
    # Result should contain a number (the session count)
    import re
    numbers = re.findall(r'\d+', result_text)
    check("claude MCP returned count",
          any(int(n) > 0 for n in numbers) if numbers else False,
          f"result: {result_text[:200]}")
else:
    print()
    print("=" * 60)
    print("Skipping Claude Code MCP test (no credentials)")
    print("=" * 60)

# ── Cleanup ────────────────────────────────────────────────────────────────────
if shutil.which("flex-serve"):
    subprocess.run(["flex-serve", "--stop"], capture_output=True)

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
