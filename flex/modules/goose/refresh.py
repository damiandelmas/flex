"""Goose cell refresh — size-poll against sessions.db.

Registry hook: lifecycle='watch' + refresh_module='flex.modules.goose.refresh'.
The flex daemon dry-runs this on the local watch cadence and calls
`refresh(cell_path, ...)` when the source signature changes.

Short-circuits when sessions.db hasn't grown, mirroring CC's stat()-based
daemon scan for JSONL files. All heavy lifting (embed, enrichment) is
stolen from claude_code.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from flex.modules.claude_code import run_enrichment
from flex.modules.claude_code.compile.worker import _batch_embed_chunks
from flex.modules.goose.compile.worker import DEFAULT_GOOSE_DB, transpile


_SIZE_KEY = "goose_db_size"
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


def _record_size(conn: sqlite3.Connection, source: Path, size: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (_SIZE_KEY, str(size)),
    )
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (_SOURCE_KEY, str(source)),
    )
    conn.commit()


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False) -> dict:
    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        source = _source_from_meta(conn)
        if not source.exists():
            return {"chunks": 0, "sources": 0, "skipped": "source missing"}

        try:
            current_size = source.stat().st_size
        except OSError:
            return {"chunks": 0, "sources": 0, "skipped": "stat failed"}

        if dry_run:
            return {"dry_run": True, "needs_resync": current_size > _last_size(conn)}

        if current_size <= _last_size(conn) and not graph:
            return {"chunks": 0, "sources": 0, "skipped": "size unchanged"}

        stats = transpile(source, conn)

        if stats.get("chunks", 0) > 0 or graph:
            try:
                _batch_embed_chunks(conn, quiet=True)
            except Exception as e:
                print(f"[goose.refresh] embed failed: {e}", file=sys.stderr)
                conn.commit()
            try:
                run_enrichment(conn, cell_type="goose")
            except Exception as e:
                print(f"[goose.refresh] enrichment failed: {e}", file=sys.stderr)

        _record_size(conn, source, current_size)
        return {
            "sources": stats.get("sessions", 0),
            "chunks": stats.get("chunks", 0),
        }
    finally:
        conn.close()
