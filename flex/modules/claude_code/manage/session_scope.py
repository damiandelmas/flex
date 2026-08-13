"""Session launch context recovery for Claude Code cells."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Iterable


CLAUDE_PROJECTS = Path.home() / ".claude" / "projects"


def first_cwd_from_lines(lines: Iterable[str]) -> str | None:
    """Return the first explicit cwd captured in one Claude JSONL session."""
    for line in lines:
        try:
            entry = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        cwd = entry.get("cwd")
        if isinstance(cwd, str) and cwd.strip():
            return os.path.normpath(os.path.expanduser(cwd.strip()))
    return None


def first_cwd_from_jsonl(path: str | Path) -> str | None:
    """Read only as far into a JSONL session as needed to recover its cwd."""
    try:
        with Path(path).open("r", encoding="utf-8", errors="replace") as handle:
            return first_cwd_from_lines(handle)
    except OSError:
        return None


def backfill_primary_cwds(
    conn: sqlite3.Connection,
    projects_root: str | Path = CLAUDE_PROJECTS,
) -> int:
    """Backfill missing launch cwd values from canonical Claude JSONLs.

    The operation is additive and idempotent. Symlinked aliases are ignored so a
    moved session is read from its canonical JSONL rather than counted twice.
    """
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(_raw_sources)")
    }
    if "primary_cwd" not in columns:
        return 0

    missing = {
        row[0]
        for row in conn.execute(
            """
            SELECT source_id
            FROM _raw_sources
            WHERE primary_cwd IS NULL OR primary_cwd = ''
            """
        )
    }
    if not missing:
        return 0

    root = Path(projects_root)
    if not root.exists():
        return 0

    canonical: dict[str, Path] = {}
    for path in root.rglob("*.jsonl"):
        if path.is_symlink() or path.stem not in missing:
            continue
        previous = canonical.get(path.stem)
        if previous is None or path.stat().st_mtime > previous.stat().st_mtime:
            canonical[path.stem] = path

    updated = 0
    for session_id, path in canonical.items():
        cwd = first_cwd_from_jsonl(path)
        if not cwd:
            continue
        result = conn.execute(
            """
            UPDATE _raw_sources
            SET primary_cwd = ?
            WHERE source_id = ?
              AND (primary_cwd IS NULL OR primary_cwd = '')
            """,
            (cwd, session_id),
        )
        updated += result.rowcount
    return updated
