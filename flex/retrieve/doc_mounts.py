"""Named document mounts for query materialization.

This module exposes allowed local Markdown documents as temp-table rows.
SQL presets can read ``_flex_docs`` without gaining arbitrary filesystem
access.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flex.core import get_meta
from flex.modules.specs import (
    asset_modules_for,
    flex_home,
    module_spec_for,
    normalize_cell_type,
)

DOCS_TABLE = "_flex_docs"
MAX_DOC_CHARS = 40000


def materialize_docs(db: sqlite3.Connection, sql: str) -> str:
    """Populate _flex_docs when a query references it."""
    if DOCS_TABLE not in sql:
        return sql
    install_docs_table(db)
    return sql


def install_docs_table(db: sqlite3.Connection) -> None:
    """Create the temp document table for the current cell connection."""
    db.execute(f"DROP TABLE IF EXISTS temp.{DOCS_TABLE}")
    db.execute(
        f"""
        CREATE TEMP TABLE {DOCS_TABLE} (
            scope TEXT NOT NULL,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            mtime TEXT,
            chars INTEGER NOT NULL,
            content TEXT NOT NULL
        )
        """
    )
    rows = list(resolve_doc_rows(db))
    if rows:
        db.executemany(
            f"""
            INSERT INTO {DOCS_TABLE}
                (scope, name, path, mtime, chars, content)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def resolve_doc_rows(db: sqlite3.Connection) -> list[tuple[str, str, str, str, int, str]]:
    """Return allowed document rows for the current cell."""
    registry_meta = _registry_metadata_for_connection(db) or {}
    cell_type = normalize_cell_type(
        get_meta(db, "cell_type") or registry_meta.get("cell_type")
    )
    cell_name = registry_meta.get("name")

    docs: list[tuple[str, str, Path, Path]] = []
    docs.extend(_packaged_instruction_paths(cell_type))
    docs.extend(_local_note_paths(cell_name, cell_type))

    out: list[tuple[str, str, str, str, int, str]] = []
    seen: set[Path] = set()
    for scope, name, path, root in docs:
        loaded = _load_allowed_doc(scope, name, path, root)
        if loaded is None:
            continue
        resolved = Path(loaded[2])
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(loaded)
    return out


def _packaged_instruction_paths(cell_type: str | None) -> list[tuple[str, str, Path, Path]]:
    if not cell_type:
        return []
    module_names: list[str] = []
    direct = normalize_cell_type(cell_type)
    if direct:
        module_names.append(direct)
    for module_name in asset_modules_for(cell_type, "instructions_from"):
        if module_name not in module_names:
            module_names.append(module_name)

    docs = []
    for module_name in module_names:
        spec = module_spec_for(module_name)
        root = Path(spec["_module_root"]) if spec and spec.get("_module_root") else (
            Path(__file__).resolve().parents[1] / "modules" / module_name
        )
        path = root / "stock" / "instructions.md"
        docs.append(("cell_instructions", module_name, path, root))
    return docs


def _local_note_paths(cell_name: str | None, cell_type: str | None) -> list[tuple[str, str, Path, Path]]:
    root = flex_home() / "instructions"
    names = []
    for value in (cell_name, cell_type):
        if value and value not in names:
            names.append(value)
    return [("local_notes", name, root / f"{name}.md", root) for name in names]


def _load_allowed_doc(
    scope: str,
    name: str,
    path: Path,
    root: Path,
) -> tuple[str, str, str, str, int, str] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        resolved = path.resolve()
        root_resolved = root.resolve()
        resolved.relative_to(root_resolved)
    except (OSError, ValueError):
        return None
    try:
        stat = resolved.stat()
        content = resolved.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    if len(content) > MAX_DOC_CHARS:
        content = content[:MAX_DOC_CHARS]
    mtime = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
    return (scope, name, str(resolved), mtime, len(content), content)


def _registry_metadata_for_connection(db: sqlite3.Connection) -> dict[str, str] | None:
    try:
        db_path = db.execute("PRAGMA database_list").fetchone()[2]
    except Exception:
        return None
    if not db_path:
        return None
    try:
        resolved = Path(db_path).resolve()
        from flex.registry import list_cells

        for cell in list_cells():
            try:
                if Path(cell["path"]).resolve() == resolved:
                    return {
                        "name": str(cell["name"]),
                        "cell_type": str(cell.get("cell_type") or ""),
                    }
            except Exception:
                continue
    except Exception:
        return None
    return None
