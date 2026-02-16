"""
FlexSearch Cell Registry — centralized cell catalog.

Single source of truth for cell name → path resolution.
All consumers import from here instead of defining CELLS_ROOT locally.

Registry location: ~/.flexsearch/registry.db
Cells stay at their current paths (no file moves).
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Legacy default root — cells live here until explicit migration
CELLS_ROOT = Path.home() / ".qmem/cells/projects"

# Registry location
FLEXSEARCH_DIR = Path.home() / ".flexsearch"
REGISTRY_DB = FLEXSEARCH_DIR / "registry.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS cells (
    name        TEXT PRIMARY KEY,
    path        TEXT NOT NULL,
    cell_type   TEXT,
    description TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
"""


def _open_registry() -> sqlite3.Connection:
    """Open registry.db, creating ~/.flexsearch/ if needed."""
    FLEXSEARCH_DIR.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(REGISTRY_DB), timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(_SCHEMA)
    return db


def register_cell(
    name: str,
    path: str | Path,
    cell_type: str | None = None,
    description: str | None = None,
) -> None:
    """Register or update a cell in the registry.

    Auto-reads description from cell's _meta table if not provided.
    """
    db = _open_registry()
    now = datetime.now(timezone.utc).isoformat()
    path_str = str(Path(path).resolve())

    # Auto-detect description from cell's _meta if not provided
    if description is None:
        try:
            cell_db = sqlite3.connect(path_str, timeout=5)
            row = cell_db.execute(
                "SELECT value FROM _meta WHERE key = 'description'"
            ).fetchone()
            if row:
                description = row[0]
            cell_db.close()
        except Exception:
            pass

    # Auto-detect cell_type from schema if not provided
    if cell_type is None:
        try:
            cell_db = sqlite3.connect(path_str, timeout=5)
            tables = {r[0] for r in cell_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            cell_db.close()
            if '_types_message' in tables:
                cell_type = 'claude-code'
            elif '_types_docpac' in tables:
                cell_type = 'docpac'
        except Exception:
            pass

    db.execute("""
        INSERT INTO cells (name, path, cell_type, description, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            path = excluded.path,
            cell_type = COALESCE(excluded.cell_type, cells.cell_type),
            description = COALESCE(excluded.description, cells.description),
            updated_at = excluded.updated_at
    """, (name, path_str, cell_type, description, now, now))
    db.commit()
    db.close()


def unregister_cell(name: str) -> bool:
    """Remove a cell from the registry. Returns True if it existed."""
    db = _open_registry()
    cursor = db.execute("DELETE FROM cells WHERE name = ?", (name,))
    db.commit()
    deleted = cursor.rowcount > 0
    db.close()
    return deleted


def resolve_cell(name: str) -> Optional[Path]:
    """Resolve cell name to db path. Registry first, filesystem fallback.

    Returns Path to main.db or None if cell doesn't exist anywhere.
    """
    # 1. Try registry
    try:
        db = _open_registry()
        row = db.execute(
            "SELECT path FROM cells WHERE name = ?", (name,)
        ).fetchone()
        db.close()
        if row:
            p = Path(row[0])
            if p.exists():
                return p
    except Exception:
        pass

    # 2. Filesystem fallback
    fallback = CELLS_ROOT / name / "main.db"
    if fallback.exists():
        return fallback

    return None


def list_cells() -> list[dict]:
    """List all registered cells with metadata."""
    try:
        db = _open_registry()
        rows = db.execute(
            "SELECT name, path, cell_type, description, created_at, updated_at "
            "FROM cells ORDER BY name"
        ).fetchall()
        db.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def discover_cells() -> list[str]:
    """Unified discovery: registry cells + filesystem scan for unregistered.

    Returns sorted list of cell names. Merges both sources.
    """
    names = set()

    # From registry
    for cell in list_cells():
        p = Path(cell['path'])
        if p.exists():
            names.add(cell['name'])

    # Filesystem fallback for unregistered cells
    if CELLS_ROOT.exists():
        for d in CELLS_ROOT.iterdir():
            if d.is_dir() and (d / "main.db").exists():
                names.add(d.name)

    return sorted(names)
