"""Config-driven source visibility for coding-agent cells.

Raw tables remain the recovery surface. This sidecar marks source-level
visibility for ordinary views and retrieval primitives.
"""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import sqlite3
import time
from pathlib import Path
from typing import Iterable


VISIBILITY_TABLE = "_coding_agent_source_visibility"
EXCLUDED_ROOT_KEYS = (
    "coding_agent_excluded_session_roots",
    "source_visibility_excluded_roots",
)


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _has_table(conn, table):
        return False
    # Avoid PRAGMA table_info: the MCP materialization authorizer deliberately
    # blocks broad PRAGMAs. LIMIT 0 exposes the schema without reading rows.
    cursor = conn.execute(f"SELECT * FROM [{table}] LIMIT 0")
    return any(item[0] == column for item in (cursor.description or ()))


def normalize_session_root(value: object) -> str | None:
    """Normalize a configured local source/session root without resolving it."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "://" in text or text.startswith(("codex:", "claude_code:", "goose:")):
        return None
    text = os.path.expanduser(text)
    if not os.path.isabs(text):
        text = os.path.abspath(text)
    return posixpath.normpath(Path(text).as_posix())


def _load_json_list(raw: str | None) -> list[object]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = [raw]
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def excluded_session_roots(conn: sqlite3.Connection) -> list[str]:
    roots: list[str] = []
    seen: set[str] = set()
    configured: list[object] = []
    if _has_table(conn, "_meta"):
        for key in EXCLUDED_ROOT_KEYS:
            row = conn.execute("SELECT value FROM _meta WHERE key=?", (key,)).fetchone()
            configured.extend(_load_json_list(row[0] if row else None))

    # Installation policy belongs in the user config so rebuilt cells inherit it.
    # Cell-local _meta remains a portable override for standalone cells/tests.
    config_path = Path.home() / ".flex" / "config.json"
    try:
        scope = json.loads(config_path.read_text()).get("scope", {})
        configured.extend(scope.get("exclude_session_roots", []))
    except (OSError, json.JSONDecodeError, AttributeError, TypeError):
        pass

    for item in configured:
        root = normalize_session_root(item)
        if root and root not in seen:
            seen.add(root)
            roots.append(root)
    return roots


def _generation(roots: Iterable[str]) -> int:
    payload = json.dumps(list(roots), separators=(",", ":"), sort_keys=True)
    # Keep inside signed sqlite INTEGER range.
    return int(hashlib.sha256(payload.encode("utf-8")).hexdigest()[:15], 16)


def ensure_source_visibility_schema(conn: sqlite3.Connection) -> None:
    # Individual execute calls are savepoint-safe. executescript() performs an
    # implicit COMMIT and would destroy Codex's active-append publication fence.
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {VISIBILITY_TABLE} (
            source_id TEXT PRIMARY KEY,
            visible INTEGER NOT NULL DEFAULT 1,
            reason TEXT,
            basis TEXT NOT NULL,
            generation INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_ca_source_visibility_visible
            ON {VISIBILITY_TABLE}(visible)
    """)


def _source_locators(
    conn: sqlite3.Connection, source_id: str | None = None
) -> dict[str, set[str]]:
    locators: dict[str, set[str]] = {}
    if not _has_table(conn, "_raw_sources"):
        return locators

    raw_cols = {row[1] for row in conn.execute("PRAGMA table_info(_raw_sources)")}
    select_cols = ["source_id"]
    for col in ("primary_cwd", "source", "source_path"):
        if col in raw_cols:
            select_cols.append(col)
    where = " WHERE source_id=?" if source_id is not None else ""
    params = (source_id,) if source_id is not None else ()
    for row in conn.execute(
        f"SELECT {', '.join(select_cols)} FROM _raw_sources{where}", params
    ).fetchall():
        sid = str(row[0])
        bucket = locators.setdefault(sid, set())
        for value in row[1:]:
            norm = normalize_session_root(value)
            if norm:
                bucket.add(norm)

    if _has_table(conn, "_types_codex_source"):
        cols = {
            row[1] for row in conn.execute("PRAGMA table_info(_types_codex_source)")
        }
        wanted = [c for c in ("rollout_path", "sessions_dir", "codex_home") if c in cols]
        if wanted and "session_id" in cols:
            sidecar_where = " WHERE session_id=?" if source_id is not None else ""
            for row in conn.execute(
                f"SELECT session_id, {', '.join(wanted)} FROM _types_codex_source"
                f"{sidecar_where}",
                params,
            ).fetchall():
                bucket = locators.setdefault(str(row[0]), set())
                for value in row[1:]:
                    norm = normalize_session_root(value)
                    if norm:
                        bucket.add(norm)

    if _has_table(conn, "_codex_source_state"):
        cols = {
            row[1] for row in conn.execute("PRAGMA table_info(_codex_source_state)")
        }
        if {"session_id", "source_path"} <= cols:
            state_where = " AND session_id=?" if source_id is not None else ""
            for sid, source_path in conn.execute(
                "SELECT session_id, source_path FROM _codex_source_state "
                f"WHERE session_id IS NOT NULL{state_where}",
                params,
            ).fetchall():
                norm = normalize_session_root(source_path)
                if norm:
                    locators.setdefault(str(sid), set()).add(norm)

    return locators


def _under_root(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def refresh_source_visibility(conn: sqlite3.Connection) -> int:
    """Backfill/update visibility rows from the current config.

    Returns the number of source rows evaluated.
    """
    ensure_source_visibility_schema(conn)
    if not _has_table(conn, "_raw_sources"):
        return 0

    roots = excluded_session_roots(conn)
    generation = _generation(roots)
    now = int(time.time())
    locators = _source_locators(conn)
    source_ids = [str(row[0]) for row in conn.execute(
        "SELECT source_id FROM _raw_sources"
    ).fetchall()]

    rows = []
    for source_id in source_ids:
        matched_root = None
        for locator in sorted(locators.get(source_id, ())):
            matched_root = next((root for root in roots if _under_root(locator, root)), None)
            if matched_root:
                break
        if matched_root:
            rows.append((
                source_id,
                0,
                f"source path under excluded session root: {matched_root}",
                "config:excluded_session_roots",
                generation,
                now,
            ))
        else:
            rows.append((source_id, 1, None, "config:default_visible", generation, now))

    conn.executemany(
        f"""
        INSERT INTO {VISIBILITY_TABLE}
            (source_id, visible, reason, basis, generation, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            visible=excluded.visible,
            reason=excluded.reason,
            basis=excluded.basis,
            generation=excluded.generation,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def refresh_source_visibility_for_source(
    conn: sqlite3.Connection, source_id: str
) -> bool:
    """Classify one newly inserted or updated source in the ingest transaction."""
    ensure_source_visibility_schema(conn)
    if not _has_table(conn, "_raw_sources"):
        return False
    if conn.execute(
        "SELECT 1 FROM _raw_sources WHERE source_id=?", (source_id,)
    ).fetchone() is None:
        conn.execute(
            f"DELETE FROM {VISIBILITY_TABLE} WHERE source_id=?", (source_id,)
        )
        return False

    roots = excluded_session_roots(conn)
    generation = _generation(roots)
    locators = _source_locators(conn, source_id).get(source_id, set())
    matched_root = next(
        (
            root
            for locator in sorted(locators)
            for root in roots
            if _under_root(locator, root)
        ),
        None,
    )
    visible = 0 if matched_root else 1
    reason = (
        f"source path under excluded session root: {matched_root}"
        if matched_root else None
    )
    basis = "config:excluded_session_roots" if matched_root else "config:default_visible"
    conn.execute(
        f"""
        INSERT INTO {VISIBILITY_TABLE}
            (source_id, visible, reason, basis, generation, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            visible=excluded.visible,
            reason=excluded.reason,
            basis=excluded.basis,
            generation=excluded.generation,
            updated_at=excluded.updated_at
        """,
        (source_id, visible, reason, basis, generation, int(time.time())),
    )
    return True


def source_visibility_is_current(conn: sqlite3.Connection) -> bool:
    """Prove every current source has a row for the active root policy."""
    if not (_has_table(conn, VISIBILITY_TABLE) and _has_table(conn, "_raw_sources")):
        return False
    roots = excluded_session_roots(conn)
    generation = _generation(roots)
    missing_or_stale = conn.execute(
        f"""
        SELECT 1
        FROM _raw_sources src
        LEFT JOIN {VISIBILITY_TABLE} vis ON vis.source_id = src.source_id
        WHERE vis.source_id IS NULL OR vis.generation != ?
        LIMIT 1
        """,
        (generation,),
    ).fetchone()
    stale_extra = conn.execute(
        f"""
        SELECT 1
        FROM {VISIBILITY_TABLE} vis
        LEFT JOIN _raw_sources src ON src.source_id = vis.source_id
        WHERE src.source_id IS NULL
        LIMIT 1
        """
    ).fetchone()
    return missing_or_stale is None and stale_extra is None


def hidden_ids(conn: sqlite3.Connection, table: str = "_raw_chunks") -> set[str] | None:
    """Return the comparatively small hidden-id set, or None if policy is absent."""
    if not (_has_table(conn, VISIBILITY_TABLE) and _has_table(conn, table)):
        return None
    if not source_visibility_is_current(conn):
        try:
            refresh_source_visibility(conn)
        except sqlite3.Error:
            # An immutable/read-only reader may not repair stale policy. Do not
            # discard an already-populated mask merely because refresh is barred.
            if not _has_table(conn, VISIBILITY_TABLE):
                return None
    if table == "_raw_sources":
        return {
            str(row[0])
            for row in conn.execute(
                f"""
                SELECT src.source_id
                FROM _raw_sources src
                LEFT JOIN {VISIBILITY_TABLE} vis ON vis.source_id=src.source_id
                WHERE COALESCE(vis.visible, 0)=0
                """
            ).fetchall()
        }
    if not _has_table(conn, "_edges_source"):
        return None
    id_col = "id" if _has_column(conn, table, "id") else "chunk_id"
    return {
        str(row[0])
        for row in conn.execute(
            f"""
            SELECT DISTINCT t.[{id_col}]
            FROM [{table}] t
            LEFT JOIN _edges_source es ON es.chunk_id = t.[{id_col}]
            LEFT JOIN {VISIBILITY_TABLE} vis ON vis.source_id = es.source_id
            WHERE es.chunk_id IS NOT NULL AND COALESCE(vis.visible, 0) = 0
            """
        ).fetchall()
    }


def visible_chunk_ids(conn: sqlite3.Connection, table: str = "_raw_chunks") -> set[str] | None:
    """Compatibility helper for bounded callers; retrieval uses hidden_ids()."""
    hidden = hidden_ids(conn, table)
    if hidden is None:
        return None
    id_col = "source_id" if table == "_raw_sources" else (
        "id" if _has_column(conn, table, "id") else "chunk_id"
    )
    return {
        str(row[0]) for row in conn.execute(f"SELECT [{id_col}] FROM [{table}]").fetchall()
    } - hidden
