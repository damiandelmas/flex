"""
E2E assertion script — runs inside the Docker container.

Calls `flex init` as a real subprocess (no monkeypatching),
then queries the resulting cell to assert all invariants.
Exit 0 = pass, exit 1 = fail.
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

from harness import Harness

h = Harness("e2e")
FLEX_HOME = Path.home() / ".flex"


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

# ── Filesystem ────────────────────────────────────────────────────────────────
h.phase("Filesystem")

h.check("flex init exit 0", result.returncode == 0,
        f"exit code {result.returncode}")
h.check("~/.flex/ created",        FLEX_HOME.exists())
h.check("~/.flex/cells/ exists",   (FLEX_HOME / "cells").exists())
h.check("registry.db exists",      (FLEX_HOME / "registry.db").exists())

settings = Path.home() / ".claude" / "settings.json"
h.check("settings.json exists",    settings.exists())
claude_json = Path.home() / ".claude.json"
h.check(".claude.json exists",     claude_json.exists())

# ── Settings.json hooks ───────────────────────────────────────────────────────
h.phase("Hooks wiring")

if settings.exists():
    s = json.loads(settings.read_text())
    hooks = s.get("hooks", {})
    h.check("PostToolUse hooked",    "PostToolUse" in hooks)
    h.check("UserPromptSubmit hooked", "UserPromptSubmit" in hooks)
    all_cmds = []
    for group in hooks.get("PostToolUse", []):
        for hook in group.get("hooks", []):
            all_cmds.append(hook.get("command", ""))
    h.check("capture hook registered",
            any("claude-code-capture" in c for c in all_cmds),
            f"cmds: {all_cmds}")

# ── claude.json MCP ──────────────────────────────────────────────────────────
h.phase("MCP wiring")

if claude_json.exists():
    d = json.loads(claude_json.read_text())
    servers = d.get("mcpServers", {})
    h.check("flex MCP entry",      "flex" in servers)
    h.check("MCP URL correct",
            servers.get("flex", {}).get("url") == "http://localhost:7134/sse")

# ── Cell contents ─────────────────────────────────────────────────────────────
h.phase("Cell contents")

import flex.registry as reg
cell_path = reg.resolve_cell("claude_code")
h.check("cell registered", cell_path is not None)
h.check("cell db on disk",  cell_path is not None and cell_path.exists())

if cell_path and cell_path.exists():
    conn = sqlite3.connect(str(cell_path))

    n_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    n_sessions = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    n_embedded = conn.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]

    h.check("chunks indexed",      n_chunks > 0,     f"got {n_chunks}")
    h.check("sessions indexed",    n_sessions > 0,   f"got {n_sessions}")
    h.check("embeddings present",  n_embedded > 0,   f"got {n_embedded}")

    tool_names = {r[0] for r in conn.execute(
        "SELECT DISTINCT tool_name FROM _edges_tool_ops"
    ).fetchall()}
    h.check("tool_ops extracted",
            bool({"Read", "Edit", "Write"} & tool_names),
            f"got {tool_names}")

    msg_types = {r[0] for r in conn.execute(
        "SELECT DISTINCT type FROM _types_message"
    ).fetchall()}
    h.check("message types classified",
            bool({"user_prompt", "tool_call"} & msg_types),
            f"got {msg_types}")

    preset_names = {r[0] for r in conn.execute(
        "SELECT name FROM _presets"
    ).fetchall()}
    h.check("@orient installed",   "orient" in preset_names,
            f"presets: {sorted(preset_names)}")

    view_names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'"
    ).fetchall()}
    h.check("messages view",   "messages" in view_names, f"views: {view_names}")
    h.check("sessions view",   "sessions" in view_names, f"views: {view_names}")

    # embedding dimension — all must be uniform
    dims = {len(r[0]) // 4 for r in conn.execute(
        "SELECT embedding FROM _raw_chunks WHERE embedding IS NOT NULL LIMIT 500"
    ).fetchall()}
    h.check("uniform embedding dims", len(dims) == 1, f"got dims: {dims}")

    # enrichment table exists
    has_graph_table = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = '_enrich_source_graph'"
    ).fetchone()[0]
    h.check("source graph table exists", has_graph_table > 0)

    big_sessions = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT source_id FROM _edges_source GROUP BY source_id HAVING COUNT(*) >= 20"
        ")"
    ).fetchone()[0]
    if big_sessions >= 3:
        n_graph = conn.execute(
            "SELECT COUNT(*) FROM _enrich_source_graph"
        ).fetchone()[0]
        h.check("source graph has rows", n_graph > 0, f"got {n_graph}")

    # ── Delegation edges ──────────────────────────────────────────────────────
    h.phase("Delegation edges")

    n_deleg = conn.execute(
        "SELECT COUNT(*) FROM _edges_delegations"
    ).fetchone()[0]
    n_deleg_valid = conn.execute(
        "SELECT COUNT(*) FROM _edges_delegations d "
        "JOIN _raw_chunks r ON d.chunk_id = r.id"
    ).fetchone()[0]
    h.check("delegation edges exist", n_deleg > 0, f"got {n_deleg}")
    h.check("delegation 100% JOIN rate",
            n_deleg == n_deleg_valid,
            f"total={n_deleg} valid={n_deleg_valid}")

    n_agent_type = conn.execute(
        "SELECT COUNT(*) FROM _edges_delegations WHERE agent_type IS NOT NULL"
    ).fetchone()[0]
    h.check("delegation agent_type populated",
            n_agent_type == n_deleg,
            f"nonnull={n_agent_type} total={n_deleg}")

    n_dupes = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT chunk_id, child_session_id, COUNT(*) as c"
        "  FROM _edges_delegations GROUP BY chunk_id, child_session_id HAVING c > 1"
        ")"
    ).fetchone()[0]
    h.check("delegation no duplicates", n_dupes == 0, f"got {n_dupes} dupes")

    # ── Preset param validation ───────────────────────────────────────────────
    h.phase("Preset validation")

    r_preset = subprocess.run(
        ["flex", "search", "--json", "@story"],
        capture_output=True, text=True, timeout=15,
    )
    h.check("preset missing param no crash", r_preset.returncode == 0)
    if r_preset.returncode == 0:
        try:
            out = json.loads(r_preset.stdout)
            has_error = (isinstance(out, dict) and "error" in out) or (
                isinstance(out, list) and len(out) > 0
                and isinstance(out[0], dict) and "error" in out[0])
            h.check("preset missing param returns error", has_error,
                    f"got: {r_preset.stdout[:200]}")
        except Exception:
            h.check("preset missing param returns error", True)

    # ── View exclusions ───────────────────────────────────────────────────────
    conn.row_factory = sqlite3.Row
    try:
        n_msg_view = conn.execute("SELECT COUNT(*) as n FROM messages").fetchone()[0]
        h.check("messages view queryable", n_msg_view > 0, f"got {n_msg_view}")
    except Exception as e:
        h.check("messages view queryable", False, str(e))

    try:
        n_sess_view = conn.execute("SELECT COUNT(*) as n FROM sessions").fetchone()[0]
        h.check("sessions view queryable", n_sess_view > 0, f"got {n_sess_view}")
    except Exception as e:
        h.check("sessions view queryable", False, str(e))

    conn.close()

# ── flex-serve + vec_ops ───────────────────────────────────────────────────────
import shutil, time

if shutil.which("flex-serve"):
    h.phase("flex-serve + vec_ops")

    subprocess.run(["flex-serve", "--stop"], capture_output=True)
    time.sleep(1)

    subprocess.Popen(["flex-serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)

    r = subprocess.run(
        ["flex", "search", "--json",
         "SELECT v.id, v.score FROM vec_ops('_raw_chunks', 'test query') v LIMIT 3"],
        capture_output=True, text=True, timeout=30,
    )
    h.check("vec_ops exit 0", r.returncode == 0, r.stderr[:200] if r.stderr else "")
    if r.returncode == 0:
        try:
            rows = json.loads(r.stdout)
            h.check("vec_ops returns rows", len(rows) > 0, f"got {len(rows)}")
            h.check("vec_ops score field",
                    all("score" in row for row in rows), "missing score")
        except Exception as e:
            h.check("vec_ops json parse", False, str(e))

    # ── Authorizer ─────────────────────────────────────────────────────────
    h.phase("Authorizer")

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
                h.check(label, blocked, f"got: {r_auth.stdout[:200]}")
            except Exception:
                h.check(label, True)
        else:
            h.check(label, True)

    # ── Orient structure ─────────────────────────────────────────────────────
    h.phase("Orient structure")

    r_orient = subprocess.run(
        ["flex", "search", "--json", "@orient"],
        capture_output=True, text=True, timeout=15,
    )
    h.check("orient exit 0", r_orient.returncode == 0,
            r_orient.stderr[:200] if r_orient.stderr else "")
    if r_orient.returncode == 0:
        try:
            orient_data = json.loads(r_orient.stdout)
            query_names = set()
            all_results = []
            if isinstance(orient_data, list):
                for block in orient_data:
                    if isinstance(block, dict) and "query" in block:
                        query_names.add(block["query"])
                        all_results.extend(block.get("results", []))

            h.check("orient no default block",
                    "default" not in query_names,
                    f"query blocks: {sorted(query_names)}")

            h.check("orient no retrieval block",
                    "retrieval" not in query_names,
                    f"query blocks: {sorted(query_names)}")

            surface_kinds = set()
            for block in orient_data if isinstance(orient_data, list) else []:
                if block.get("query") == "query_surface":
                    for row in block.get("results", []):
                        if isinstance(row, dict) and "kind" in row:
                            surface_kinds.add(row["kind"])
            h.check("orient query_surface has views",
                    "view" in surface_kinds,
                    f"kinds: {sorted(surface_kinds)}")
            h.check("orient query_surface has table_function",
                    "table_function" in surface_kinds,
                    f"kinds: {sorted(surface_kinds)}")
            h.check("orient query_surface has edge_table",
                    "edge_table" in surface_kinds,
                    f"kinds: {sorted(surface_kinds)}")

            about_has_desc = False
            for block in orient_data if isinstance(orient_data, list) else []:
                if block.get("query") == "about":
                    for row in block.get("results", []):
                        if isinstance(row, dict):
                            desc = row.get("description") or (
                                row.get("value") if row.get("key") == "description" else None)
                            if desc and desc.strip():
                                about_has_desc = True
            h.check("orient about has description", about_has_desc)

        except json.JSONDecodeError:
            h.check("orient json parse", False, f"raw: {r_orient.stdout[:300]}")

    # ── Query surface — presets, raw SQL, vec_ops with tokens ─────────────
    h.phase("Query surface (paradigmatic)")

    def _query(label, sql, expect_rows=True):
        """Run a flex search query and check it returns valid results."""
        r_q = subprocess.run(
            ["flex", "search", "--json", sql],
            capture_output=True, text=True, timeout=30,
        )
        if r_q.returncode != 0:
            h.check(label, False, f"exit {r_q.returncode}: {r_q.stderr[:200]}")
            return None
        try:
            data = json.loads(r_q.stdout)
            if expect_rows:
                is_list = isinstance(data, list)
                has_rows = is_list and len(data) > 0
                h.check(label, has_rows,
                        f"type={type(data).__name__} len={len(data) if is_list else 'N/A'}")
                return data if has_rows else None
            else:
                h.check(label, True)
                return data
        except json.JSONDecodeError:
            h.check(label, False, f"invalid json: {r_q.stdout[:200]}")
            return None

    # ── Structural queries (no embeddings needed) ──────────────────────
    _query("query: sessions view",
           "SELECT session_id, title, project FROM sessions LIMIT 5")
    _query("query: messages by type",
           "SELECT id, type FROM messages WHERE type='user_prompt' LIMIT 5")
    _query("query: project distribution",
           "SELECT project, COUNT(*) as sessions "
           "FROM sessions GROUP BY project ORDER BY sessions DESC")

    # ── vec_ops — nearest neighbors ──────────────────────────────────
    _query("query: nearest neighbors",
           "SELECT v.id, v.score, m.content "
           "FROM vec_ops('_raw_chunks', 'fix bug in authentication') v "
           "JOIN messages m ON v.id = m.id "
           "ORDER BY v.score DESC LIMIT 5")

    # ── vec_ops — modulation tokens ──────────────────────────────────
    _query("query: vec_ops diverse",
           "SELECT v.id, v.score, m.project "
           "FROM vec_ops('_raw_chunks', 'refactor code', 'diverse') v "
           "JOIN messages m ON v.id = m.id "
           "ORDER BY v.score DESC LIMIT 5")
    _query("query: vec_ops recent",
           "SELECT v.id, v.score "
           "FROM vec_ops('_raw_chunks', 'test coverage', 'recent:7') v "
           "LIMIT 5")

    # ── vec_ops — pre-filtered (user intent only) ────────────────────
    _query("query: user intent only",
           "SELECT v.id, v.score, m.content "
           "FROM vec_ops('_raw_chunks', 'debugging', '', "
           "'SELECT id FROM messages WHERE type = ''user_prompt''') v "
           "JOIN messages m ON v.id = m.id LIMIT 5")

    # ── Presets ──────────────────────────────────────────────────────
    _query("query: @health preset", "@health")
    _query("query: @digest preset", "@digest days=30")

    # ── FTS (exact term match) ───────────────────────────────────────
    _query("query: FTS exact term",
           "SELECT c.id, substr(c.content, 1, 100) as preview "
           "FROM chunks_fts JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid "
           "WHERE chunks_fts MATCH 'test OR code OR bug' "
           "ORDER BY bm25(chunks_fts) LIMIT 5")

    # ── Incremental update — add sessions + re-init ───────────────────────
    h.phase("Incremental update (new sessions)")

    # Record baseline counts
    cell_path_inc = reg.resolve_cell("claude_code")
    conn_inc = sqlite3.connect(str(cell_path_inc))
    baseline_sessions = conn_inc.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    baseline_chunks = conn_inc.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    conn_inc.close()

    # Generate 10 new sessions with a distinct project name
    new_proj_dir = Path.home() / ".claude" / "projects" / "-home-testuser-projects-newfeature"
    new_proj_dir.mkdir(parents=True, exist_ok=True)

    import random as _rnd
    _rnd.seed(9999)  # different seed from original
    for i in range(10):
        sid = f"newsess-{i:04d}-0000-0000-0000-{i:012d}"
        entries = []
        ts_base = "2026-02-24T10:00:00Z"
        entries.append({
            "type": "user",
            "uuid": f"new-uuid-{i}-0",
            "timestamp": ts_base,
            "message": {"role": "user", "content": f"Implement feature {i} for the new module"},
            "cwd": "/home/testuser/projects/newfeature",
            "parentUuid": None,
        })
        # A couple tool calls
        for j in range(3):
            tid = f"toolu_new_{i}_{j}"
            entries.append({
                "type": "assistant",
                "uuid": f"new-uuid-{i}-{j+1}",
                "timestamp": ts_base,
                "message": {"role": "assistant", "content": [
                    {"type": "text", "text": f"Working on step {j+1}."},
                    {"type": "tool_use", "id": tid, "name": "Read",
                     "input": {"file_path": f"/home/testuser/projects/newfeature/module_{i}.py"}},
                    {"type": "tool_result", "tool_use_id": tid,
                     "content": f"def feature_{i}():\n    return {i}\n"},
                ]},
                "cwd": "/home/testuser/projects/newfeature",
            })
        p = new_proj_dir / f"{sid}.jsonl"
        with open(p, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

    h.check("incremental: 10 new sessions written",
            len(list(new_proj_dir.glob("*.jsonl"))) == 10)

    # Re-run flex init — INSERT OR IGNORE skips existing, picks up new
    r_inc = subprocess.run(
        ["flex", "init", "--local"],
        capture_output=True, text=True, timeout=600,
    )
    h.check("incremental: re-init exit 0", r_inc.returncode == 0,
            f"exit {r_inc.returncode}")

    # Verify counts increased
    conn_inc2 = sqlite3.connect(str(cell_path_inc))
    new_sessions = conn_inc2.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    new_chunks = conn_inc2.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    new_embedded = conn_inc2.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    conn_inc2.close()

    h.check("incremental: sessions increased",
            new_sessions > baseline_sessions,
            f"before={baseline_sessions} after={new_sessions}")
    h.check("incremental: chunks increased",
            new_chunks > baseline_chunks,
            f"before={baseline_chunks} after={new_chunks}")
    h.check("incremental: new chunks embedded",
            new_embedded == new_chunks,
            f"{new_embedded}/{new_chunks} embedded")

    # Verify new sessions queryable
    _query("incremental: new sessions in vec_ops",
           "SELECT v.id, v.score FROM vec_ops('_raw_chunks', 'implement feature new module') v LIMIT 3")

    # Run @orient again — should still be valid after incremental update
    r_orient2 = subprocess.run(
        ["flex", "search", "--json", "@orient"],
        capture_output=True, text=True, timeout=15,
    )
    h.check("incremental: orient still valid", r_orient2.returncode == 0)

    # ── flex sync after incremental ───────────────────────────────────────
    h.phase("flex sync (post-incremental)")

    # Add 5 more sessions
    for i in range(10, 15):
        sid = f"syncsess-{i:04d}-0000-0000-0000-{i:012d}"
        entries = []
        ts_base = "2026-02-25T14:00:00Z"
        entries.append({
            "type": "user",
            "uuid": f"sync-uuid-{i}-0",
            "timestamp": ts_base,
            "message": {"role": "user", "content": f"Fix bug {i} in production"},
            "cwd": "/home/testuser/projects/newfeature",
            "parentUuid": None,
        })
        for j in range(2):
            tid = f"toolu_sync_{i}_{j}"
            entries.append({
                "type": "assistant",
                "uuid": f"sync-uuid-{i}-{j+1}",
                "timestamp": ts_base,
                "message": {"role": "assistant", "content": [
                    {"type": "text", "text": f"Fixing issue {j+1}."},
                    {"type": "tool_use", "id": tid, "name": "Edit",
                     "input": {"file_path": f"/home/testuser/projects/newfeature/hotfix_{i}.py",
                               "old_string": "pass", "new_string": "return True"}},
                    {"type": "tool_result", "tool_use_id": tid,
                     "content": "File edited successfully."},
                ]},
                "cwd": "/home/testuser/projects/newfeature",
            })
        p = new_proj_dir / f"{sid}.jsonl"
        with open(p, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

    pre_sync_sessions = new_sessions  # from last check

    # Run flex init to pick up new sessions, then flex sync for views/presets
    r_init2 = subprocess.run(
        ["flex", "init", "--local"],
        capture_output=True, text=True, timeout=600,
    )
    h.check("sync: re-init exit 0", r_init2.returncode == 0,
            f"exit {r_init2.returncode}")

    r_sync = subprocess.run(
        ["flex", "sync"],
        capture_output=True, text=True, timeout=60,
    )
    h.check("sync: flex sync exit 0", r_sync.returncode == 0,
            f"exit {r_sync.returncode}: {r_sync.stderr[:200]}")

    # Verify everything still works after sync
    conn_sync = sqlite3.connect(str(cell_path_inc))
    post_sync_sessions = conn_sync.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    post_sync_chunks = conn_sync.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    post_sync_embedded = conn_sync.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]

    # Views still work
    try:
        msg_count = conn_sync.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        views_ok = msg_count > 0
    except Exception:
        views_ok = False

    # Presets still work
    preset_names = {r[0] for r in conn_sync.execute("SELECT name FROM _presets").fetchall()}
    conn_sync.close()

    h.check("sync: sessions increased",
            post_sync_sessions > pre_sync_sessions,
            f"before={pre_sync_sessions} after={post_sync_sessions}")
    h.check("sync: all embedded",
            post_sync_embedded == post_sync_chunks,
            f"{post_sync_embedded}/{post_sync_chunks}")
    h.check("sync: views queryable", views_ok)
    h.check("sync: presets intact", "orient" in preset_names,
            f"presets: {sorted(preset_names)}")

    # Final query validation — full stack still works
    _query("sync: vec_ops after sync",
           "SELECT v.id, v.score FROM vec_ops('_raw_chunks', 'fix bug production') v LIMIT 3")
    _query("sync: sessions view after sync",
           "SELECT session_id, project FROM sessions LIMIT 5")

# ── Claude Code MCP integration ───────────────────────────────────────────────
creds = Path.home() / ".claude" / ".credentials.json"
if creds.exists() and shutil.which("claude"):
    h.phase("Claude Code MCP integration")

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

    h.check("claude -p exit 0", r_claude.returncode == 0,
            r_claude.stderr[:200] if r_claude.stderr else "")
    h.check("claude MCP no error", not is_error,
            f"result: {result_text[:200]}")
    h.check("claude MCP used tool", num_turns >= 2,
            f"num_turns={num_turns} (1 = no tool call)")
    numbers = re.findall(r'\d+', result_text)
    h.check("claude MCP returned count",
            any(int(n) > 0 for n in numbers) if numbers else False,
            f"result: {result_text[:200]}")
else:
    h.phase("Claude Code MCP integration (SKIPPED)")
    h.skip("claude MCP checks", "no credentials or claude binary")

# ── Cleanup ────────────────────────────────────────────────────────────────────
if shutil.which("flex-serve"):
    subprocess.run(["flex-serve", "--stop"], capture_output=True)

# ── Summary ───────────────────────────────────────────────────────────────────
sys.exit(h.finish())
