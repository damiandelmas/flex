"""
Degraded Install E2E — tests flex init resilience when model is unavailable.

Validates:
  1. Infrastructure survives model failure (hooks, services, MCP wiring)
  2. Query surface works without embeddings (views, presets, FTS)
  3. Exit code is non-zero on partial completion
  4. flex sync recovers missing layers
  5. Authorizer still blocks writes
  6. Truncated model file (passes model_ready but crashes on ONNX load)
  7. SIGINT mid-embedding + resume completes

Runs in Docker after seed_sessions.py has populated ~/.claude/projects/.

Exit 0 = pass, exit 1 = fail.
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from harness import Harness

h = Harness("degraded")
FLEX_HOME = Path.home() / ".flex"

# ── Backup model before any corruption ────────────────────────────────────────
import shutil

model_data = FLEX_HOME / "models" / "model.onnx.data"
bundled_data = Path("/flex/flex/onnx/model.onnx.data")
_model_backup = Path("/tmp/model.onnx.data.backup")

for p in [model_data, bundled_data]:
    if p.exists() and p.stat().st_size > (1 << 20):
        shutil.copy2(p, _model_backup)
        print(f"  Model backed up from {p} ({p.stat().st_size} bytes)")
        break


# ── Phase 1: Normal init (baseline) ──────────────────────────────────────────
h.phase("Phase 1: Normal init (baseline)")

r = subprocess.run(["flex", "init", "--local"], capture_output=False, timeout=600)
h.check("baseline init exit 0", r.returncode == 0, f"exit code {r.returncode}")

# ── Phase 2: Corrupt model, delete cell, re-init ─────────────────────────────
h.phase("Phase 2: Corrupt model + re-init")

# Corrupt model — check both ~/.flex/models/ and bundled location
corrupted = False
for p in [model_data, bundled_data]:
    if p.exists():
        p.write_bytes(b"corrupt")
        print(f"  Model corrupted: {p}")
        corrupted = True
if not corrupted:
    print("  WARNING: no model.onnx.data found to corrupt")

# Delete cell to force re-init
registry = FLEX_HOME / "registry.db"
if registry.exists():
    conn = sqlite3.connect(str(registry))
    row = conn.execute("SELECT path FROM cells WHERE name='claude_code'").fetchone()
    if row:
        cell_path = Path(row[0])
        conn.execute("DELETE FROM cells WHERE name='claude_code'")
        conn.commit()
        cell_path.unlink(missing_ok=True)
        Path(str(cell_path) + "-wal").unlink(missing_ok=True)
        Path(str(cell_path) + "-shm").unlink(missing_ok=True)
        print("  Cell deleted")
    conn.close()

# Re-init with corrupt model
print("  Running flex init --local with corrupt model...")
r = subprocess.run(
    ["flex", "init", "--local"],
    capture_output=True, text=True, timeout=600,
)

# ── Phase 3: Infrastructure survives ─────────────────────────────────────────
h.phase("Phase 3: Infrastructure checks")

h.check("non-zero exit on corrupt model", r.returncode != 0,
        f"exit code {r.returncode}")

h.check("~/.flex/ exists", FLEX_HOME.exists())
h.check("registry.db exists", registry.exists())

settings = Path.home() / ".claude" / "settings.json"
h.check("settings.json exists", settings.exists())

claude_json = Path.home() / ".claude.json"
h.check("~/.claude.json exists", claude_json.exists())

if claude_json.exists():
    cfg = json.loads(claude_json.read_text())
    h.check("MCP entry present", "flex" in cfg.get("mcpServers", {}))

if settings.exists():
    s = json.loads(settings.read_text())
    hooks = s.get("hooks", {})
    h.check("PostToolUse hooked", "PostToolUse" in hooks)
    h.check("UserPromptSubmit hooked", "UserPromptSubmit" in hooks)

# ── Phase 4: Query surface without embeddings ────────────────────────────────
h.phase("Phase 4: Query surface (no embeddings)")

sys.path.insert(0, "/test")
import flex.registry as reg

cell_path = reg.resolve_cell("claude_code")
h.check("cell registered", cell_path is not None)
h.check("cell db on disk", cell_path is not None and cell_path.exists())

if cell_path and cell_path.exists():
    conn = sqlite3.connect(str(cell_path))

    n_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    n_sessions = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]

    h.check("chunks indexed", n_chunks > 0, f"got {n_chunks}")
    h.check("sessions indexed", n_sessions > 0, f"got {n_sessions}")

    n_embedded = conn.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    h.check("embeddings absent or partial", n_embedded < n_chunks,
            f"{n_embedded}/{n_chunks} embedded")

    preset_names = {r[0] for r in conn.execute(
        "SELECT name FROM _presets"
    ).fetchall()}
    h.check("@orient installed", "orient" in preset_names,
            f"presets: {sorted(preset_names)}")

    view_names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'"
    ).fetchall()}
    h.check("messages view exists", "messages" in view_names)
    h.check("sessions view exists", "sessions" in view_names)

    conn.row_factory = sqlite3.Row
    try:
        n_msg = conn.execute("SELECT COUNT(*) as n FROM messages").fetchone()[0]
        h.check("messages view queryable", n_msg > 0, f"got {n_msg}")
    except Exception as e:
        h.check("messages view queryable", False, str(e))

    try:
        n_sess = conn.execute("SELECT COUNT(*) as n FROM sessions").fetchone()[0]
        h.check("sessions view queryable", n_sess > 0, f"got {n_sess}")
    except Exception as e:
        h.check("sessions view queryable", False, str(e))

    try:
        fts_count = conn.execute(
            "SELECT COUNT(*) FROM chunks_fts WHERE chunks_fts MATCH 'test OR auth OR code'"
        ).fetchone()[0]
        h.check("FTS works", fts_count >= 0)
    except Exception as e:
        h.check("FTS table exists", False, str(e))

    conn.close()

# ── Phase 5: flex sync recovery ──────────────────────────────────────────────
h.phase("Phase 5: flex sync recovery")

r = subprocess.run(
    ["flex", "sync"],
    capture_output=True, text=True, timeout=60,
)
h.check("flex sync exit 0", r.returncode == 0, f"exit {r.returncode}: {r.stderr[:200]}")

if cell_path and cell_path.exists():
    conn = sqlite3.connect(str(cell_path))

    preset_names = {r[0] for r in conn.execute("SELECT name FROM _presets").fetchall()}
    h.check("presets still present after sync", "orient" in preset_names)

    view_names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'"
    ).fetchall()}
    h.check("views still present after sync", "messages" in view_names)

    conn.close()

# ── Phase 6: Security ────────────────────────────────────────────────────────
h.phase("Phase 6: Security")

for label, sql in [
    ("DELETE blocked", "DELETE FROM _raw_chunks WHERE 1=0"),
    ("DROP blocked", "DROP TABLE _raw_chunks"),
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

# ── Phase 7: Truncated model (passes model_ready, crashes on load) ────────────
h.phase("Phase 7: Truncated model (realistic)")

# Delete cell again for fresh init
if registry.exists():
    conn = sqlite3.connect(str(registry))
    row = conn.execute("SELECT path FROM cells WHERE name='claude_code'").fetchone()
    if row:
        cell_path_7 = Path(row[0])
        conn.execute("DELETE FROM cells WHERE name='claude_code'")
        conn.commit()
        cell_path_7.unlink(missing_ok=True)
        Path(str(cell_path_7) + "-wal").unlink(missing_ok=True)
        Path(str(cell_path_7) + "-shm").unlink(missing_ok=True)
    conn.close()

# Write a 1MB truncated file — passes os.path.exists() + model_ready()
# but fails onnxruntime.InferenceSession() with protobuf/format error
for p in [model_data, bundled_data]:
    if p.parent.exists():
        p.write_bytes(b"\x00" * (1 << 20))  # 1MB of zeros
        print(f"  Model truncated (1MB): {p}")

r7 = subprocess.run(
    ["flex", "init", "--local"],
    capture_output=True, text=True, timeout=600,
)

h.check("truncated model: non-zero exit", r7.returncode != 0,
        f"exit code {r7.returncode}")

# Infrastructure must still land
claude_json_7 = Path.home() / ".claude.json"
h.check("truncated model: MCP wiring survives",
        claude_json_7.exists() and "flex" in json.loads(claude_json_7.read_text()).get("mcpServers", {}))

settings_7 = Path.home() / ".claude" / "settings.json"
if settings_7.exists():
    s7 = json.loads(settings_7.read_text())
    h.check("truncated model: hooks survive",
            "PostToolUse" in s7.get("hooks", {}))

# Cell should exist with chunks but no/partial embeddings
cell_path_7 = reg.resolve_cell("claude_code")
if cell_path_7 and cell_path_7.exists():
    conn7 = sqlite3.connect(str(cell_path_7))
    n_chunks_7 = conn7.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    n_embedded_7 = conn7.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    h.check("truncated model: chunks parsed", n_chunks_7 > 0, f"got {n_chunks_7}")
    h.check("truncated model: embeddings absent",
            n_embedded_7 < n_chunks_7,
            f"{n_embedded_7}/{n_chunks_7} embedded")
    conn7.close()

# Error should surface in stdout (Rich console) or stderr
_combined_output = (r7.stdout + r7.stderr).lower()
has_model_err = any(kw in _combined_output for kw in [
    "onnx", "model", "embed", "invalid", "protobuf", "runtime",
    "backfill", "warning", "failed"
])
h.check("truncated model: error surfaced in output",
        has_model_err,
        f"stdout: {r7.stdout[:200]} | stderr: {r7.stderr[:200]}")


# ── Phase 8: SIGINT mid-embedding + resume ────────────────────────────────────
h.phase("Phase 8: SIGINT + resume")

# Restore valid model from backup (saved before Phase 2 corrupted it)
_have_model = _model_backup.exists() and _model_backup.stat().st_size > (1 << 20)
if _have_model:
    for p in [model_data, bundled_data]:
        if p.parent.exists():
            shutil.copy2(_model_backup, p)
            print(f"  Model restored to {p}")

if _have_model:
    # Delete cell for fresh init
    if registry.exists():
        conn = sqlite3.connect(str(registry))
        row = conn.execute("SELECT path FROM cells WHERE name='claude_code'").fetchone()
        if row:
            cp8 = Path(row[0])
            conn.execute("DELETE FROM cells WHERE name='claude_code'")
            conn.commit()
            cp8.unlink(missing_ok=True)
            Path(str(cp8) + "-wal").unlink(missing_ok=True)
            Path(str(cp8) + "-shm").unlink(missing_ok=True)
            print("  Cell deleted for interrupt test")
        conn.close()

    # Start flex init in a subprocess, send SIGINT after 5s
    # (should be mid-embedding for 930 sessions)
    print("  Starting flex init --local (will SIGINT in 5s)...")
    proc = subprocess.Popen(
        ["flex", "init", "--local"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    time.sleep(5)
    proc.send_signal(signal.SIGINT)

    try:
        stdout_8, stderr_8 = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout_8, stderr_8 = proc.communicate()

    exit_code_8 = proc.returncode
    print(f"  Interrupted init exited with code {exit_code_8}")

    # Check partial state — some chunks should exist with some embedded
    cell_path_8 = reg.resolve_cell("claude_code")
    partial_chunks = 0
    partial_embedded = 0

    if cell_path_8 and cell_path_8.exists():
        conn8 = sqlite3.connect(str(cell_path_8))
        partial_chunks = conn8.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        partial_embedded = conn8.execute(
            "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
        ).fetchone()[0]
        conn8.close()

    h.check("interrupt: partial chunks persisted",
            partial_chunks > 0,
            f"got {partial_chunks}")

    # Now resume — run flex init again, should complete
    print(f"  Resuming init ({partial_chunks} chunks, {partial_embedded} embedded)...")
    r_resume = subprocess.run(
        ["flex", "init", "--local"],
        capture_output=True, text=True, timeout=600,
    )

    h.check("resume: exit 0", r_resume.returncode == 0,
            f"exit code {r_resume.returncode}")

    # All chunks should now be embedded
    cell_path_8r = reg.resolve_cell("claude_code")
    if cell_path_8r and cell_path_8r.exists():
        conn8r = sqlite3.connect(str(cell_path_8r))
        final_chunks = conn8r.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        final_embedded = conn8r.execute(
            "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
        ).fetchone()[0]
        conn8r.close()

        h.check("resume: all chunks present",
                final_chunks >= partial_chunks,
                f"before={partial_chunks} after={final_chunks}")
        h.check("resume: all chunks embedded",
                final_embedded == final_chunks,
                f"{final_embedded}/{final_chunks} embedded")
    else:
        h.check("resume: cell exists after resume", False, "cell not found")

else:
    h.skip("SIGINT + resume", "model not available for interrupt test")


# ── Summary ───────────────────────────────────────────────────────────────────
sys.exit(h.finish())
