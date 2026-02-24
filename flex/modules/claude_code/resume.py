"""
flex resume — jump back into any Claude Code session.

Finds the session JSONL, symlinks it into the current project dir
if needed, and execs `claude -r <session-id>`.

Usage:
    flex resume <session-id>        # full or prefix match
    flex resume abc123              # partial ID works
"""

import json
import os
import subprocess
import sys
from pathlib import Path

CLAUDE_PROJECTS = Path.home() / ".claude" / "projects"


def find_session(session_id: str) -> Path | None:
    """Find a session JSONL by full or prefix match."""
    if not CLAUDE_PROJECTS.is_dir():
        return None
    for jsonl in CLAUDE_PROJECTS.rglob(f"{session_id}*.jsonl"):
        if jsonl.is_file() and jsonl.stat().st_size > 0:
            return jsonl
    return None


def extract_cwd(session_file: Path) -> str | None:
    """Extract working directory from session JSONL metadata."""
    try:
        with open(session_file) as f:
            for i, line in enumerate(f):
                if i >= 20:
                    break
                try:
                    data = json.loads(line)
                    cwd = data.get("cwd")
                    if cwd and Path(cwd).is_dir():
                        return cwd
                except (json.JSONDecodeError, KeyError):
                    continue
    except OSError:
        pass
    return None


def ensure_symlink(session_file: Path, target_dir: Path) -> None:
    """Symlink session into target project dir if it lives elsewhere."""
    if session_file.parent == target_dir:
        return
    target_dir.mkdir(parents=True, exist_ok=True)
    link = target_dir / session_file.name
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(session_file)


def cmd_resume(session_id: str) -> None:
    """Find session, symlink if needed, exec claude -r."""
    session_file = find_session(session_id)
    if session_file is None:
        print(f"Session '{session_id}' not found in ~/.claude/projects/", file=sys.stderr)
        sys.exit(1)

    full_id = session_file.stem
    cwd = extract_cwd(session_file)

    if cwd:
        # Symlink into the session's project dir so claude can find it
        project_encoded = cwd.replace("/", "-").lstrip("-")
        claude_project_dir = CLAUDE_PROJECTS / f"-{project_encoded}"
        ensure_symlink(session_file, claude_project_dir)
        os.chdir(cwd)
        print(f"  session: {full_id}")
        print(f"  cwd:     {cwd}")
    else:
        print(f"  session: {full_id}")
        print(f"  cwd:     (using current directory)")

    print()
    os.execvp("claude", ["claude", "-r", full_id])
