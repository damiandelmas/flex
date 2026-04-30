"""
Flex Cell Registry — centralized cell management.

Single source of truth for cell name → path resolution.
All consumers import from here instead of defining paths locally.

Registry location: ~/.flex/registry.db
Cells live at ~/.flex/cells/{uuid}.db
"""

import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# All cells live under ~/.flex/ (override with FLEX_HOME env var)
FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
CELLS_DIR = FLEX_HOME / "cells"
REGISTRY_DB = FLEX_HOME / "registry.db"

# ── Hook registry ────────────────────────────────────────
# Extensible hook system for optional modules and plugins.
_hooks: dict[str, object] = {}

def register_hook(name: str, fn) -> None:
    """Register a callable hook by name."""
    _hooks[name] = fn

def get_hook(name: str):
    """Get a registered hook, or None if not installed."""
    return _hooks.get(name)


_plugins_loaded = False

# H4 — plugin module names must match ^flex(\.[a-zA-Z0-9_]+)+$.
# Each segment must be a valid Python identifier, preventing consecutive
# dots and arbitrary code execution via a poisoned plugins.txt.
import re as _re
_PLUGIN_NAME_RE = _re.compile(r'^flex(\.[a-zA-Z0-9_]+)+$')


def load_plugins():
    """Load optional plugins listed in ~/.flex/plugins.txt (one module per line).

    Each line must be a ``flex.*`` importable module — anything else is
    rejected with a stderr warning and skipped. Comments (``#``) and blank
    lines are allowed.
    """
    global _plugins_loaded
    if _plugins_loaded:
        return
    _plugins_loaded = True
    import importlib
    plugins_file = FLEX_HOME / "plugins.txt"
    if not plugins_file.exists():
        return
    for line in plugins_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if '..' in line or not _PLUGIN_NAME_RE.match(line):
            import sys as _sys
            print(f"[plugins] Invalid module name: {line!r} (must match flex.*)", file=_sys.stderr)
            continue
        try:
            importlib.import_module(line)
        except ImportError:
            pass


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
    "ALTER TABLE cells ADD COLUMN source_url TEXT",
    "ALTER TABLE cells ADD COLUMN checksum TEXT",
    # Lifecycle management (SDK cells can register their refresh)
    "ALTER TABLE cells ADD COLUMN lifecycle TEXT DEFAULT 'static'",
    "ALTER TABLE cells ADD COLUMN refresh_interval INTEGER",
    "ALTER TABLE cells ADD COLUMN refresh_script TEXT",
    "ALTER TABLE cells ADD COLUMN refresh_module TEXT",
    "ALTER TABLE cells ADD COLUMN last_refresh_at TEXT",
    "ALTER TABLE cells ADD COLUMN refresh_status TEXT",
    "ALTER TABLE cells ADD COLUMN watch_path TEXT",
    "ALTER TABLE cells ADD COLUMN watch_pattern TEXT",
    # Active/inactive: active cells get VectorCache at startup, inactive are lazy-loaded on first query
    "ALTER TABLE cells ADD COLUMN active INTEGER DEFAULT 1",
]


def _open_registry() -> sqlite3.Connection:
    """Open registry.db, creating ~/.flex/ if needed."""
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    os.chmod(FLEX_HOME, 0o700)
    db = sqlite3.connect(str(REGISTRY_DB), timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_autocheckpoint=100")

    # Init schema + migrations
    _has_cells = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cells'"
    ).fetchone()
    if not _has_cells:
        db.executescript(_SCHEMA)

    # Always run migrations — idempotent (duplicate column errors are expected)
    for sql in _MIGRATIONS:
        try:
            db.execute(sql)
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
                pass
            else:
                print(f"[registry] migration warning: {e}", file=sys.stderr)

    # Ensure restrictive permissions on registry and WAL files
    for suffix in ('', '-wal', '-shm'):
        p = Path(str(REGISTRY_DB) + suffix)
        if p.exists():
            try:
                os.chmod(p, 0o600)
            except OSError:
                pass

    return db


def _open_registry_readonly() -> sqlite3.Connection:
    """Open registry.db for reads without requiring directory write access.

    MCP clients can run under a filesystem sandbox where ``~/.flex`` is
    readable but not writable. A normal sqlite open still tries to create
    lock/journal side files, so read-only discovery must fall back to an
    immutable URI instead of reporting an empty registry.
    """
    try:
        db = sqlite3.connect(f"file:{REGISTRY_DB}?mode=ro", uri=True, timeout=5)
        db.execute("PRAGMA schema_version").fetchone()
    except sqlite3.OperationalError:
        try:
            db.close()
        except Exception:
            pass
        db = sqlite3.connect(
            f"file:{REGISTRY_DB}?mode=ro&immutable=1",
            uri=True,
            timeout=5,
        )
        db.execute("PRAGMA schema_version").fetchone()
    db.row_factory = sqlite3.Row
    return db


# Dict-driven legacy type detection: _types_* table name → cell_type.
# First match wins. Order matters for shared-substrate cells with multiple
# _types_ tables (e.g. claude_code has _types_message, _types_file_body,
# _types_source_warmup). New cells should write _meta.cell_type instead of
# extending this map.
_TYPE_TABLE_MAP = {
    '_types_codex_turn': 'codex',
    '_types_message':   'claude_code',
    '_types_markdown':  'markdown',
}

try:
    from flex.modules.registry_ext import _EXT_TABLES
    _TYPE_TABLE_MAP.update(_EXT_TABLES)
except ImportError:
    pass


_TYPE_TABLE_RE = _re.compile(r'^_types_([a-zA-Z0-9_]+)$')


def _meta_value(cell_db: sqlite3.Connection, key: str) -> str | None:
    row = cell_db.execute(
        "SELECT value FROM _meta WHERE key = ?", (key,)
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _generic_type_from_tables(tables: set[str]) -> str | None:
    """Infer a cell type only when exactly one unknown _types_* table exists."""
    unknown_types = []
    for table_name in sorted(tables):
        if table_name in _TYPE_TABLE_MAP:
            continue
        match = _TYPE_TABLE_RE.match(table_name)
        if match:
            unknown_types.append(match.group(1))
    return unknown_types[0] if len(unknown_types) == 1 else None


def _auto_detect(path_str: str) -> tuple[str | None, str | None]:
    """Auto-detect cell_type and description from a cell's schema/meta.

    Metadata is the primary extension mechanism. Table sniffing remains only
    for legacy cells and simple ad-hoc cells that have one obvious _types_ table.
    """
    cell_type = None
    description = None
    try:
        with sqlite3.connect(path_str, timeout=5) as cell_db:
            tables = {r[0] for r in cell_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}

            # Metadata first. This lets new cells become live/queryable without
            # registry.py edits for every new module or runtime.
            if "_meta" in tables:
                description = _meta_value(cell_db, "description")
                cell_type = _meta_value(cell_db, "cell_type")

            # Legacy/shared-substrate fallback — explicit map first.
            for table_name, detected_type in _TYPE_TABLE_MAP.items():
                if cell_type is None and table_name in tables:
                    cell_type = detected_type
                    break
            if cell_type is None:
                cell_type = _generic_type_from_tables(tables)
    except Exception:
        pass
    return cell_type, description


def register_cell(
    name: str,
    path: str | Path,
    cell_type: str | None = None,
    description: str | None = None,
    corpus_path: str | Path | None = None,
    source_url: str | None = None,
    checksum: str | None = None,
    lifecycle: str | None = None,
    refresh_interval: int | None = None,
    refresh_script: str | None = None,
    refresh_module: str | None = None,
    watch_path: str | Path | None = None,
    watch_pattern: str | None = None,
) -> str:
    """Register or update a cell in the registry.

    Auto-detects cell_type and description from cell's schema if not provided.
    Lifecycle params control how the daemon keeps the cell fresh:
      lifecycle='static'  — no refresh (default)
      lifecycle='refresh' — re-run refresh_script or refresh_module on interval
      lifecycle='watch'   — daemon monitors watch_path for file changes

    Returns the cell's UUID id.
    """
    db = _open_registry()
    now = datetime.now(timezone.utc).isoformat()
    path_str = str(Path(path).resolve())

    # Validate cell path is within safe boundaries
    if not _is_safe_cell_path(Path(path_str)):
        raise ValueError(f"Cell path must be within {FLEX_HOME}: {path_str}")

    corpus_str = str(Path(corpus_path).resolve()) if corpus_path else None
    watch_str = str(Path(watch_path).resolve()) if watch_path else None

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
                           source_url, checksum,
                           lifecycle, refresh_interval, refresh_script, refresh_module,
                           watch_path, watch_pattern,
                           created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            id = COALESCE(cells.id, excluded.id),
            path = excluded.path,
            corpus_path = COALESCE(excluded.corpus_path, cells.corpus_path),
            cell_type = COALESCE(excluded.cell_type, cells.cell_type),
            description = COALESCE(excluded.description, cells.description),
            source_url = COALESCE(excluded.source_url, cells.source_url),
            checksum = COALESCE(excluded.checksum, cells.checksum),
            lifecycle = COALESCE(excluded.lifecycle, cells.lifecycle),
            refresh_interval = CASE
                WHEN excluded.lifecycle IS NOT NULL AND excluded.lifecycle != 'refresh'
                    THEN excluded.refresh_interval
                ELSE COALESCE(excluded.refresh_interval, cells.refresh_interval)
            END,
            refresh_script = COALESCE(excluded.refresh_script, cells.refresh_script),
            refresh_module = COALESCE(excluded.refresh_module, cells.refresh_module),
            watch_path = COALESCE(excluded.watch_path, cells.watch_path),
            watch_pattern = COALESCE(excluded.watch_pattern, cells.watch_pattern),
            updated_at = excluded.updated_at
    """, (cell_id, name, path_str, corpus_str, cell_type, description,
          source_url, checksum,
          lifecycle, refresh_interval, refresh_script, refresh_module,
          watch_str, watch_pattern,
          now, now))
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


def _is_safe_cell_path(p: Path) -> bool:
    """Validate that a cell path is within FLEX_HOME or a known safe location."""
    try:
        resolved = p.resolve()
        # Allow paths within FLEX_HOME (standard) or /tmp (testing)
        if str(resolved).startswith(str(FLEX_HOME.resolve())):
            return True
        if str(resolved).startswith("/tmp/"):
            return True
        return False
    except (OSError, ValueError):
        return False


def resolve_cell(name: str) -> Optional[Path]:
    """Resolve cell name to db path. Registry first, filesystem fallback.

    Returns Path to .db file or None if cell doesn't exist anywhere.
    Validates that resolved paths are within FLEX_HOME.
    """
    # 1. Try registry
    try:
        db = _open_registry_readonly()
        row = db.execute(
            "SELECT path FROM cells WHERE name = ?", (name,)
        ).fetchone()
        db.close()
        if row:
            p = Path(row[0])
            if p.exists() and _is_safe_cell_path(p):
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
        db = _open_registry_readonly()
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
        db = _open_registry_readonly()
        rows = db.execute(
            "SELECT id, name, path, corpus_path, cell_type, description, "
            "source_url, checksum, "
            "lifecycle, refresh_interval, refresh_script, refresh_module, "
            "last_refresh_at, refresh_status, watch_path, watch_pattern, "
            "created_at, updated_at, COALESCE(unlisted, 0) as unlisted, "
            "COALESCE(active, 1) as active "
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


def discover_active_cells() -> list[str]:
    """Discover listed cells that should be warmed at startup.

    Returns sorted list of cell names. Skips unlisted and inactive cells.
    Inactive cells are still discoverable (in the enum) but their VectorCaches
    are lazy-loaded on first query instead of pre-warmed.
    """
    names = set()
    for cell in list_cells():
        if cell.get('unlisted'):
            continue
        if not cell.get('active', 1):
            continue
        p = Path(cell['path'])
        if p.exists():
            names.add(cell['name'])
    return sorted(names)


def set_active(name: str, active: bool) -> bool:
    """Set a cell's active flag. Returns True if cell existed.

    Active cells get VectorCache warmed at startup (instant queries).
    Inactive cells are lazy-loaded on first query.
    """
    db = _open_registry()
    now = datetime.now(timezone.utc).isoformat()
    cursor = db.execute(
        "UPDATE cells SET active = ?, updated_at = ? WHERE name = ?",
        (1 if active else 0, now, name)
    )
    db.commit()
    updated = cursor.rowcount > 0
    db.close()
    return updated


def _parse_registry_ts(value: object) -> datetime | None:
    """Parse registry ISO timestamps, tolerating trailing Z."""
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return None


def classify_refresh_state(cell: dict, now: datetime | None = None) -> dict:
    """Return derived lifecycle status for a registry cell.

    Registry fields store only the last transition. This helper turns those
    raw fields into operational state for status displays and refresh policy:
    due, running age, and stale-running detection.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    lifecycle = cell.get('lifecycle') or 'static'
    status = cell.get('refresh_status')
    interval = cell.get('refresh_interval') or 0
    try:
        interval_s = int(interval or 0)
    except (TypeError, ValueError):
        interval_s = 0

    last_dt = _parse_registry_ts(cell.get('last_refresh_at'))
    age_s = None
    if last_dt:
        age_s = max(0, int((now - last_dt).total_seconds()))

    # Give a running refresh at least 10 minutes, and otherwise two full
    # intervals, before declaring the registry state stale.
    stale_after_s = max(interval_s * 2, 600) if interval_s else 600
    is_running = status == 'running'
    is_error = (status or '').startswith('error')
    stale_running = bool(is_running and age_s is not None and age_s > stale_after_s)

    due = False
    if lifecycle == 'refresh':
        if stale_running:
            due = True
        elif is_running:
            due = False
        elif not last_dt:
            due = True
        elif interval_s:
            due = age_s is not None and age_s >= interval_s

    overdue = bool(
        lifecycle == 'refresh'
        and due
        and last_dt is not None
        and not is_running
        and not is_error
    )
    never_run = bool(
        lifecycle == 'refresh'
        and due
        and last_dt is None
        and not is_running
        and not is_error
    )

    if stale_running:
        effective_status = 'stale-running'
    elif overdue:
        effective_status = 'overdue'
    elif never_run:
        effective_status = 'never-run'
    else:
        effective_status = status or 'idle'

    return {
        'effective_refresh_status': effective_status,
        'refresh_due': due,
        'refresh_stale': stale_running,
        'refresh_overdue': overdue,
        'refresh_never_run': never_run,
        'refresh_age_s': age_s,
        'refresh_stale_after_s': stale_after_s if is_running else None,
        'refresh_running_for_s': age_s if is_running else None,
    }


def discover_refreshable() -> list[dict]:
    """Discover cells that need periodic refresh.

    Returns list of dicts with name, path, refresh_script, refresh_module,
    refresh_interval. Used by the daemon to know what to refresh and how.
    """
    results = []
    try:
        db = _open_registry()
        rows = db.execute("""
            SELECT name, path, lifecycle, refresh_script, refresh_module,
                   refresh_interval, last_refresh_at, refresh_status,
                   COALESCE(unlisted, 0) as unlisted,
                   COALESCE(active, 1) as active
            FROM cells
            WHERE lifecycle = 'refresh'
              AND refresh_interval IS NOT NULL
              AND (refresh_script IS NOT NULL OR refresh_module IS NOT NULL)
              AND COALESCE(active, 1) = 1
              AND COALESCE(unlisted, 0) = 0
        """).fetchall()
        db.close()
        for r in rows:
            p = Path(r['path'])
            if p.exists():
                results.append(dict(r))
    except Exception:
        pass
    return results


def discover_watched() -> list[dict]:
    """Discover cells that watch file directories.

    Returns list of dicts with name, path, watch_path, watch_pattern.
    Used by the daemon to know what directories to monitor.
    """
    results = []
    try:
        db = _open_registry()
        rows = db.execute("""
            SELECT name, path, cell_type, refresh_module, refresh_script,
                   watch_path, watch_pattern,
                   COALESCE(unlisted, 0) as unlisted,
                   COALESCE(active, 1) as active
            FROM cells
            WHERE lifecycle = 'watch'
              AND watch_path IS NOT NULL
              AND COALESCE(active, 1) = 1
              AND COALESCE(unlisted, 0) = 0
        """).fetchall()
        db.close()
        for r in rows:
            if Path(r['path']).exists() and Path(r['watch_path']).exists():
                results.append(dict(r))
    except Exception:
        pass
    return results


def update_refresh_status(name: str, status: str, timestamp: str | None = None):
    """Update refresh status and timestamp for a cell."""
    try:
        db = _open_registry()
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        db.execute(
            "UPDATE cells SET refresh_status = ?, last_refresh_at = ?, updated_at = ? WHERE name = ?",
            (status, ts, ts, name)
        )
        db.commit()
        db.close()
    except Exception:
        pass
