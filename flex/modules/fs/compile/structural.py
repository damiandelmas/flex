"""Filesystem-owned structural (no-embed, multi-root) profile.

This is the replacement contract for the former Instant projection.  It uses
the ordinary Filesystem schema and atomic writer; only selection discovery is
special.  Source ids are rooted at ``/`` so two selected directories cannot
collide on the same relative filename.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flex.modules.fs.compile.walker import walk_files


STRUCTURAL_PROFILE = "filesystem-structural"
STRUCTURAL_RESOLVER = "filesystem-structural@v1"
STRUCTURAL_SIGNATURE_KEY = "structural_sources_signature"
_STRUCTURAL_PRESETS = Path(__file__).resolve().parent / "stock" / "structural" / "presets"


class StructuralRecipeError(RuntimeError):
    """A structural cell has no safe, complete reconstruction recipe."""


def selections_from_meta(conn: sqlite3.Connection) -> tuple[Path, ...]:
    row = conn.execute("SELECT value FROM _meta WHERE key='selections'").fetchone()
    try:
        raw = json.loads(row[0]) if row and row[0] else []
    except (TypeError, ValueError) as exc:
        raise StructuralRecipeError("invalid structural selections recipe") from exc
    if not isinstance(raw, list) or not raw:
        raise StructuralRecipeError("structural cell has no selections recipe")
    selections = tuple(Path(value).expanduser().resolve() for value in raw)
    missing = [str(path) for path in selections if not path.is_dir()]
    if missing:
        raise StructuralRecipeError("structural selection(s) missing: " + ", ".join(missing))
    return selections


def structural_paths(selections: tuple[Path, ...], *, exclude=()) -> tuple[Path, ...]:
    """Return the complete, de-duplicated selected source set.

    The normal Filesystem worker uses paths relative to one root.  Structural
    cells intentionally preserve a union of roots, so this function declares
    the full set before publication and lets the shared writer own deletes.
    """
    paths: dict[str, Path] = {}
    for selection in selections:
        for entry in walk_files(selection, exclude=tuple(exclude)):
            resolved = entry.path.resolve()
            paths.setdefault(str(resolved), resolved)
    return tuple(paths[key] for key in sorted(paths))


def recipe_signature(paths: tuple[Path, ...]) -> str:
    """Stable content-independent freshness signature for the declared set."""
    digest = hashlib.sha256()
    for path in paths:
        try:
            stat = path.stat()
        except OSError:
            continue
        digest.update(str(path).encode())
        digest.update(b"\0")
        digest.update(str(stat.st_size).encode())
        digest.update(b":")
        digest.update(str(stat.st_mtime_ns).encode())
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def reconcile_structural_cell(conn: sqlite3.Connection, *, exclude=()) -> dict[str, int]:
    """Reconcile a complete structural selection union through Filesystem.

    ``build_explicit_candidate`` validates every discovered path before any
    publication, writes each source through the normal transaction, and removes
    only rows absent from this declared union.  A failed pass leaves the last
    accepted per-source state intact and never writes a freshness receipt.
    """
    from flex.modules.fs.compile.worker import build_explicit_candidate

    selections = selections_from_meta(conn)
    paths = structural_paths(selections, exclude=exclude)
    if not paths:
        raise StructuralRecipeError("structural selections contain no indexable files")
    result = build_explicit_candidate(
        conn,
        Path("/"),
        paths,
        embed_enabled=False,
        exclude=tuple(exclude),
        file_kinds=None,
    )
    conn.execute(
        "INSERT OR REPLACE INTO _meta(key,value) VALUES(?,?)",
        (STRUCTURAL_SIGNATURE_KEY, recipe_signature(paths)),
    )
    conn.commit()
    return result.stats


def install_recipe(conn: sqlite3.Connection, selections, *, chunking: dict | None = None,
                   exclude=()) -> None:
    """Persist the complete derivation before the first structural publication."""
    roots = tuple(Path(value).expanduser().resolve() for value in selections)
    if not roots:
        raise StructuralRecipeError("structural profile requires at least one selection")
    missing = [str(root) for root in roots if not root.is_dir()]
    if missing:
        raise StructuralRecipeError("structural selection(s) missing: " + ", ".join(missing))
    values = {
        "profile": STRUCTURAL_PROFILE,
        "resolver": STRUCTURAL_RESOLVER,
        "embed": "false",
        "selections": json.dumps([str(root) for root in roots]),
        "chunking": json.dumps(chunking or {"split_mode": "structural", "code": False}),
        "exclude": json.dumps(list(exclude)),
    }
    for key, value in values.items():
        conn.execute("INSERT OR REPLACE INTO _meta(key,value) VALUES(?,?)", (key, value))
    conn.commit()


def install_structural_presets(conn: sqlite3.Connection) -> None:
    """Install the structural @orient after generic/code-oriented presets."""
    from flex.retrieve.presets import install_presets

    install_presets(conn, _STRUCTURAL_PRESETS)


def install_cell(name: str, description: str, selections, *, lifecycle: str,
                 exclude=()) -> tuple[int, int]:
    """Create one public Filesystem structural cell from a declared root union."""
    from flex.core import set_meta
    from flex.modules.fs.compile.schema import FILESYSTEM_SCHEMA_DDL
    from flex.registry import register_cell
    from flex.sdk import create, register

    db = create(name, description, cell_type="filesystem", schema=FILESYSTEM_SCHEMA_DDL)
    try:
        install_recipe(db, selections, exclude=exclude)
        set_meta(db, "description", description)
        set_meta(db, "cell_type", "filesystem")
        set_meta(db, "lifecycle", lifecycle)
        stats = reconcile_structural_cell(db, exclude=exclude)
        set_meta(db, "compiled_at", datetime.now(timezone.utc).isoformat())
        register(db, name, description, cell_type="filesystem")
        install_structural_presets(db)
        db.commit()
        db_path = Path(db.execute("PRAGMA database_list").fetchone()[2])
        roots = selections_from_meta(db)
        register_cell(
            name, db_path, cell_type="filesystem", description=description,
            corpus_path=roots[0], unlisted=True, lifecycle=lifecycle,
            refresh_module=None,
            watch_path=None if lifecycle == "static" else roots[0],
            watch_pattern="**/*" if lifecycle == "watch" else None,
        )
        return stats["indexed"] + stats["unchanged"], stats["deleted"]
    finally:
        db.close()
