"""
Flex Cell Registry — centralized cell catalog.

Single source of truth for cell name → path resolution.
All consumers import from here instead of defining paths locally.

Registry location: ~/.flex/registry.db
Cells live at ~/.flex/cells/{uuid}.db
"""

import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# All cells live under ~/.flex/ (override with FLEX_HOME env var)
FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
CELLS_DIR = FLEX_HOME / "cells"
REGISTRY_DB = FLEX_HOME / "registry.db"

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS cells (
    id          TEXT PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    path        TEXT NOT NULL,
    corpus_path TEXT,
    cell_type   TEXT,
    description TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
"""

# Migration: add columns if upgrading from old schema
_MIGRATIONS = [
    "ALTER TABLE cells ADD COLUMN id TEXT",
    "ALTER TABLE cells ADD COLUMN corpus_path TEXT",
    "ALTER TABLE cells ADD COLUMN unlisted INTEGER DEFAULT 0",
]


def _open_registry() -> sqlite3.Connection:
    """Open registry.db, creating ~/.flex/ if needed."""
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(REGISTRY_DB), timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(_SCHEMA)

    # Run migrations for schema upgrades
    for sql in _MIGRATIONS:
        try:
            db.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists

    return db


def _auto_detect(path_str: str) -> tuple[str | None, str | None]:
    """Auto-detect cell_type and description from a cell's schema/meta."""
    cell_type = None
    description = None
    try:
        with sqlite3.connect(path_str, timeout=5) as cell_db:
            # Description from _meta
            row = cell_db.execute(
                "SELECT value FROM _meta WHERE key = 'description'"
            ).fetchone()
            if row:
                description = row[0]

            # Type from _types_* tables
            tables = {r[0] for r in cell_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            if '_types_message' in tables:
                cell_type = 'claude-code'
            elif '_types_docpac' in tables:
                cell_type = 'docpac'
    except Exception:
        pass
    return cell_type, description


def register_cell(
    name: str,
    path: str | Path,
    cell_type: str | None = None,
    description: str | None = None,
    corpus_path: str | Path | None = None,
) -> str:
    """Register or update a cell in the registry.

    Auto-detects cell_type and description from cell's schema if not provided.
    Returns the cell's UUID id.
    """
    db = _open_registry()
    now = datetime.now(timezone.utc).isoformat()
    path_str = str(Path(path).resolve())
    corpus_str = str(Path(corpus_path).resolve()) if corpus_path else None

    # Auto-detect if not provided
    if cell_type is None or description is None:
        detected_type, detected_desc = _auto_detect(path_str)
        if cell_type is None:
            cell_type = detected_type
        if description is None:
            description = detected_desc

    # Check if cell already has an id
    existing = db.execute(
        "SELECT id FROM cells WHERE name = ?", (name,)
    ).fetchone()
    cell_id = existing['id'] if existing and existing['id'] else str(uuid.uuid4())

    db.execute("""
        INSERT INTO cells (id, name, path, corpus_path, cell_type, description,
                           created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            id = COALESCE(cells.id, excluded.id),
            path = excluded.path,
            corpus_path = COALESCE(excluded.corpus_path, cells.corpus_path),
            cell_type = COALESCE(excluded.cell_type, cells.cell_type),
            description = COALESCE(excluded.description, cells.description),
            updated_at = excluded.updated_at
    """, (cell_id, name, path_str, corpus_str, cell_type, description, now, now))
    db.commit()
    db.close()
    return cell_id


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

    Returns Path to .db file or None if cell doesn't exist anywhere.
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

    return None


def resolve_cell_for_path(file_path: str | Path) -> tuple[str, Path] | None:
    """Resolve a file path to its owning cell via longest corpus_path match.

    Used by the worker to determine which cell a file belongs to.
    Returns (cell_name, db_path) or None.
    """
    file_str = str(Path(file_path).resolve())
    try:
        db = _open_registry()
        row = db.execute("""
            SELECT name, path FROM cells
            WHERE corpus_path IS NOT NULL AND ? LIKE corpus_path || '%'
            ORDER BY LENGTH(corpus_path) DESC
            LIMIT 1
        """, (file_str,)).fetchone()
        db.close()
        if row:
            p = Path(row['path'])
            if p.exists():
                return row['name'], p
    except Exception:
        pass
    return None


def list_cells() -> list[dict]:
    """List all registered cells with metadata."""
    try:
        db = _open_registry()
        rows = db.execute(
            "SELECT id, name, path, corpus_path, cell_type, description, "
            "created_at, updated_at, COALESCE(unlisted, 0) as unlisted "
            "FROM cells ORDER BY name"
        ).fetchall()
        db.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def discover_cells() -> list[str]:
    """Discover listed cells from registry.

    Returns sorted list of cell names. Skips unlisted cells.
    """
    names = set()

    # From registry (skip unlisted)
    for cell in list_cells():
        if cell.get('unlisted'):
            continue
        p = Path(cell['path'])
        if p.exists():
            names.add(cell['name'])

    return sorted(names)
