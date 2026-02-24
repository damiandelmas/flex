"""
Git utilities — generic, adapter-agnostic.

Used by any module that needs to resolve git repo identity from paths.
These are defaults; user-specific naming lives in _enrich_repo_identity
(via SOMA alias).
"""

import subprocess
from pathlib import Path

GENERIC_DIR_NAMES = {'main', 'master', 'dev', 'staging', 'prod', 'context', 'sandbox'}


def project_from_git_root(git_root: str) -> str:
    """
    Derive a meaningful project name from a git root path.

    Rules (in order):
      1. Worktree: .../aura/worktrees/sql-first → aura
      2. Generic basename: .../flex/main  → flex
      3. Default: basename of git_root
    """
    p = Path(git_root)
    parts = p.parts

    if 'worktrees' in parts:
        idx = parts.index('worktrees')
        if idx > 0:
            return parts[idx - 1]

    if p.name in GENERIC_DIR_NAMES and p.parent.name:
        return p.parent.name

    return p.name


def git_root_from_path(path: str) -> str | None:
    """
    Find git root for a path by shelling out to git.
    Works on files or directories. Returns None if not in a git repo.
    """
    p = Path(path)
    check = p if p.is_dir() else p.parent
    if not check.exists():
        return None
    try:
        r = subprocess.run(
            ["git", "-C", str(check), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=3
        )
        return r.stdout.strip() or None if r.returncode == 0 else None
    except Exception:
        return None
