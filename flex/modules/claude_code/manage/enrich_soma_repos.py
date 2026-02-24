"""
SOMA Repo Registration — cell-driven, authorship-filtered.

Derives the user's own git repos from what's already captured in the cell:
  1. git config --global user.email  → author identity
  2. _edges_tool_ops.target_file     → every file they've touched in Claude Code
  3. git rev-parse --show-toplevel   → collapse files → distinct git roots
  4. git log --author=<email> -1     → filter: only repos they've committed to
  5. ri.register(root)               → write to ~/.soma/repo-identity.db

No filesystem crawl. No downloaded/vendored repos. Cell is the source of truth.
"""

import subprocess
from pathlib import Path

from flex.modules.soma.lib.git import git_root_from_path


def _git_author_email() -> str | None:
    """Get user's git email from global config."""
    try:
        r = subprocess.run(
            ["git", "config", "--global", "user.email"],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip() or None
    except Exception:
        return None


def _git_roots_from_cell(db) -> set[str]:
    """
    Extract distinct git roots from tool ops captured in the cell.

    Groups by parent directory first to minimize git subprocess calls —
    1,000 files in one repo produce one git call, not 1,000.
    """
    rows = db.execute(
        "SELECT DISTINCT target_file FROM _edges_tool_ops"
        " WHERE target_file IS NOT NULL AND target_file != ''"
    ).fetchall()

    # Unique parent dirs — many files share a dir, many dirs share a git root
    dirs: set[str] = set()
    for (path,) in rows:
        p = Path(path)
        dirs.add(str(p.parent if not p.is_dir() else p))

    roots: set[str] = set()
    for d in dirs:
        if Path(d).exists():
            root = git_root_from_path(d)
            if root:
                roots.add(root)

    return roots


def _has_author_commits(git_root: str, author_email: str) -> bool:
    """Return True if the user has at least one commit in this repo."""
    try:
        r = subprocess.run(
            ["git", "-C", git_root, "log",
             f"--author={author_email}", "--oneline", "-1"],
            capture_output=True, text=True, timeout=5,
        )
        return bool(r.stdout.strip())
    except Exception:
        return False


def run(db) -> int:
    """
    Register user's own git repos in SOMA from cell tool ops.

    Safe to call on every enrichment cycle — ri.register() is idempotent.
    Returns count of repos registered (new + updated).
    """
    try:
        from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
    except ImportError:
        return 0

    author = _git_author_email()
    if not author:
        return 0

    git_roots = _git_roots_from_cell(db)
    if not git_roots:
        return 0

    ri = RepoIdentity()
    registered = 0

    for root in sorted(git_roots):
        if _has_author_commits(root, author):
            repo = ri.register(root)
            if repo:
                registered += 1

    return registered
