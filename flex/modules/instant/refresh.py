"""instant cell refresh — size-signature watch + regen-on-change.

An instant cell is wipe-and-recompile (it carries its own recipe in `_meta`).
This exposes the worker's `refresh(cell_path, graph, dry_run)` contract so a
`lifecycle='watch'` instant cell stays fresh: the worker dry-runs the source
signature on a short cadence and only triggers a real regen when the selections
actually changed. The signature is a coarse stat aggregate (total size + file
count + newest mtime) over the cell's selection trees — the "msize" watch —
pruning the same noise dirs the walk excludes so big repos stay cheap to poll.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

_SIGNATURE_KEY = "instant_sources_signature"

try:  # share the compiler's exact enumeration so the watch tracks what's indexed
    from flex.modules.instant.install import _iter_indexed_paths
except Exception:  # pragma: no cover - defensive (degraded: stat nothing, force regen)
    def _iter_indexed_paths(folder, code=False, include_config=False):
        import os
        from pathlib import Path
        for dirpath, dirnames, filenames in os.walk(folder):
            dirnames[:] = [d for d in dirnames if d not in {".git", "node_modules"}]
            for fn in filenames:
                yield Path(dirpath) / fn


def _selections(conn: sqlite3.Connection) -> list[str]:
    row = conn.execute("SELECT value FROM _meta WHERE key='selections'").fetchone()
    if not row or not row[0]:
        return []
    try:
        return [s for s in json.loads(row[0]) if s]
    except (ValueError, TypeError):
        return []


def _code_flag(conn: sqlite3.Connection) -> bool:
    """The cell's stored code-only flag — so the signature enumerates the same ext set
    the compile did (a code cell watches code exts; a doc cell watches docs)."""
    row = conn.execute("SELECT value FROM _meta WHERE key='chunking'").fetchone()
    if row and row[0]:
        try:
            return bool(json.loads(row[0]).get("code", False))
        except (ValueError, TypeError):
            return False
    return False


def _signature(selections: list[str], code: bool = False) -> str:
    """Coarse stat aggregate over EXACTLY the files instant indexes (`_iter_indexed_paths`),
    so the watch tracks the same set the compiler does — including .context/.work/.teams
    dot-dir content, which the old self-rolled walk pruned (and so went blind to most of
    a corpus with that layout). Changes iff an indexed file is added/removed/edited."""
    total_size = 0
    count = 0
    newest = 0.0
    for sel in selections:
        root = Path(sel).expanduser()
        if not root.exists():
            continue
        if root.is_file():
            try:
                st = root.stat()
                total_size += st.st_size
                count += 1
                newest = max(newest, st.st_mtime)
            except OSError:
                pass
            continue
        for p in _iter_indexed_paths(root, code=code, include_config=True):
            try:
                st = p.stat()
            except OSError:
                continue
            total_size += st.st_size
            count += 1
            if st.st_mtime > newest:
                newest = st.st_mtime
    return f"{total_size}:{count}:{int(newest)}"


def _last_signature(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT value FROM _meta WHERE key=?", (_SIGNATURE_KEY,)).fetchone()
    return str(row[0]) if row and row[0] else None


def _cell_name(cell_path: str) -> str | None:
    """Resolve the cell name from the registry by path (regen is name-keyed)."""
    try:
        from flex.registry import FLEX_HOME
        reg = sqlite3.connect(str(FLEX_HOME / "registry.db"))
        base = Path(cell_path).name
        row = reg.execute(
            "SELECT name FROM cells WHERE path=? OR path LIKE ?",
            (str(cell_path), f"%{base}"),
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False) -> dict:
    uri = f"file:{Path(cell_path)}?mode=ro" if dry_run else None
    conn = sqlite3.connect(uri or str(cell_path), uri=bool(uri), timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        selections = _selections(conn)
        sig = _signature(selections, _code_flag(conn))
        last = _last_signature(conn)
        changed = sig != last
        if dry_run:
            return {"changed": changed, "signature": sig, "sources": len(selections)}
    finally:
        conn.close()

    if not changed:
        return {"changed": False, "signature": sig}

    name = _cell_name(cell_path)
    if not name:
        return {"changed": True, "error": "cell name not resolvable from registry"}

    # regen = wipe-and-recompile from the stored recipe (restores nest/code mode)
    proc = subprocess.run(
        [sys.executable, "-m", "flex", "init", "--module", "instant", "--regen", name],
        capture_output=True, text=True, timeout=900,
    )
    if proc.returncode != 0:
        return {"changed": True, "error": f"regen failed: {proc.stderr[-300:]}"}

    # stamp the new signature into the freshly recompiled cell
    rw = sqlite3.connect(str(cell_path), timeout=30.0)
    try:
        rw.execute("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
                   (_SIGNATURE_KEY, sig))
        rw.commit()
    finally:
        rw.close()
    return {"changed": True, "signature": sig, "regenerated": name}
