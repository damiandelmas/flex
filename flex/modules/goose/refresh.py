"""Goose cell refresh — structural capture plus asynchronous semantics.

Registry hook: lifecycle='watch' + refresh_module='flex.modules.goose.refresh'.
The flex daemon dry-runs this on the local watch cadence and calls
`refresh(cell_path, ...)` when the source signature changes.

Source capture is WAL-aware and commits canonical rows before semantic work.
Embedding debt remains explicit and is drained by the separately scheduled
refresh lane.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from flex.modules.claude_code import run_enrichment
from flex.modules.claude_code.compile.worker import _batch_embed_chunks
from flex.modules.goose.compile.worker import (
    DEFAULT_GOOSE_DB,
    compute_source_signature,
    ensure_goose_cell_schema,
    transpile,
)


_SIZE_KEY = "goose_db_size"
_SIGNATURE_KEY = "goose_source_signature"
_LEGACY_SIZE_KEYS = ("coding_agent_source_size",)
_SOURCE_KEY = "goose_db_path"


def _source_from_meta(conn: sqlite3.Connection) -> Path:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_SOURCE_KEY,)).fetchone()
    if row and row[0]:
        return Path(row[0])
    return DEFAULT_GOOSE_DB


def _last_size(conn: sqlite3.Connection) -> int:
    for key in (_SIZE_KEY,) + _LEGACY_SIZE_KEYS:
        row = conn.execute("SELECT value FROM _meta WHERE key = ?", (key,)).fetchone()
        if row and row[0]:
            try:
                return int(row[0])
            except (TypeError, ValueError):
                continue
    return 0


def _last_signature(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?", (_SIGNATURE_KEY,)
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def _embedding_debt(conn: sqlite3.Connection) -> int:
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM _raw_chunks "
            "WHERE content IS NOT NULL AND embedding IS NULL"
        ).fetchone()[0])
    except sqlite3.OperationalError:
        return 0


def _record_source_state(
    conn: sqlite3.Connection,
    source: Path,
    signature: str,
    size: int,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (_SIZE_KEY, str(size)),
    )
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (_SOURCE_KEY, str(source)),
    )
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (_SIGNATURE_KEY, signature),
    )
    conn.commit()


def _is_source_event(source: Path, observed_path: Path) -> bool:
    source = source.expanduser().resolve()
    try:
        observed = observed_path.expanduser().resolve()
    except OSError:
        observed = observed_path.expanduser().absolute()
    return observed in (source, Path(f"{source}-wal"))


def sync_source_path(conn: sqlite3.Connection, observed_path: Path) -> int:
    """Publish one Goose source event structurally, without embedding.

    Returns a change count suitable for the shared event lifecycle.  A source
    signature change counts even when it only updates session metadata rather
    than appending a message.
    """
    ensure_goose_cell_schema(conn)
    source = _source_from_meta(conn)
    if not source.exists() or not _is_source_event(source, Path(observed_path)):
        return 0

    signature, size = compute_source_signature(source)
    if signature == _last_signature(conn):
        return 0

    stats = transpile(source, conn)
    # Record the signature observed before transpilation. If Goose writes
    # concurrently, the next WAL event/signature comparison retains the debt.
    _record_source_state(conn, source, signature, size)
    return max(1, int(stats.get("chunks", 0)))


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False) -> dict:
    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        if not dry_run:
            ensure_goose_cell_schema(conn)
        source = _source_from_meta(conn)
        if not source.exists():
            return {"chunks": 0, "sources": 0, "skipped": "source missing"}

        try:
            current_signature, current_size = compute_source_signature(source)
        except OSError:
            return {"chunks": 0, "sources": 0, "skipped": "stat failed"}

        structural_changed = current_signature != _last_signature(conn)
        semantic_debt = _embedding_debt(conn)
        if dry_run:
            return {
                "dry_run": True,
                "needs_resync": structural_changed or semantic_debt > 0,
            }

        if not structural_changed and semantic_debt == 0 and not graph:
            return {"chunks": 0, "sources": 0, "skipped": "size unchanged"}

        stats = {"sessions": 0, "chunks": 0}
        if structural_changed:
            stats = transpile(source, conn)
            # Structural truth is committed before any model work begins.
            _record_source_state(
                conn, source, current_signature, current_size,
            )

        embedded = 0
        semantic_debt = _embedding_debt(conn)
        if semantic_debt > 0 or graph:
            try:
                embedded = _batch_embed_chunks(conn, quiet=True)
            except Exception as e:
                print(f"[goose.refresh] embed failed: {e}", file=sys.stderr)
                conn.commit()
        if stats.get("chunks", 0) > 0 or graph:
            try:
                run_enrichment(conn, cell_type="goose")
            except Exception as e:
                print(f"[goose.refresh] enrichment failed: {e}", file=sys.stderr)

        result = {
            "sources": stats.get("sessions", 0),
            "chunks": stats.get("chunks", 0),
        }
        if embedded:
            result["embedded"] = embedded
        return result
    finally:
        conn.close()
