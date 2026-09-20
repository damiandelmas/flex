"""DeepSeek Harness watch refresh."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from flex.modules.claude_code import run_enrichment
from flex.modules.claude_code.compile.worker import _batch_embed_chunks
from flex.modules.deepseek.compile.worker import (
    DEFAULT_DSH_HOME,
    compute_source_signature,
    ensure_deepseek_cell_schema,
    transpile,
)


_SIGNATURE_KEY = "deepseek_source_signature"
_SIZE_KEY = "deepseek_source_size"
_SOURCE_KEY = "deepseek_source_path"


def _source_from_meta(conn: sqlite3.Connection) -> Path:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_SOURCE_KEY,)).fetchone()
    return Path(row[0]).expanduser() if row and row[0] else DEFAULT_DSH_HOME


def _last_signature(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_SIGNATURE_KEY,)).fetchone()
    return str(row[0]) if row and row[0] else None


def _embedding_debt(conn: sqlite3.Connection) -> int:
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM _raw_chunks WHERE content IS NOT NULL AND embedding IS NULL"
        ).fetchone()[0])
    except sqlite3.OperationalError:
        return 0


def _record_source_state(conn: sqlite3.Connection, source: Path, signature: str, size: int) -> None:
    for key, value in ((_SOURCE_KEY, str(source)), (_SIGNATURE_KEY, signature), (_SIZE_KEY, str(size))):
        conn.execute("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def sync_source_path(conn: sqlite3.Connection, observed_path: Path) -> int:
    """Publish a changed DSH root structurally, without doing model work inline."""
    ensure_deepseek_cell_schema(conn)
    source = _source_from_meta(conn)
    observed = Path(observed_path).expanduser()
    if source.is_file():
        relevant = observed.resolve() == source.resolve()
    else:
        try:
            relevant = observed.resolve().is_relative_to(source.resolve())
        except (OSError, ValueError):
            relevant = False
    if not relevant:
        return 0
    signature, size = compute_source_signature(source)
    if signature == _last_signature(conn):
        return 0
    stats = transpile(source, conn)
    _record_source_state(conn, source, signature, size)
    return max(1, int(stats.get("chunks", 0)))


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False) -> dict:
    """Refresh one DeepSeek cell, separating structural and semantic work."""
    if dry_run:
        conn = sqlite3.connect(f"file:{Path(cell_path)}?mode=ro", uri=True, timeout=30.0)
    else:
        conn = sqlite3.connect(str(cell_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        if not dry_run:
            ensure_deepseek_cell_schema(conn)
        source = _source_from_meta(conn)
        if not source.exists():
            return {"chunks": 0, "sources": 0, "skipped": "source missing"}
        signature, size = compute_source_signature(source)
        structural_changed = signature != _last_signature(conn)
        semantic_debt = _embedding_debt(conn)
        if dry_run:
            return {"dry_run": True, "needs_resync": structural_changed or semantic_debt > 0}
        if not structural_changed and semantic_debt == 0 and not graph:
            return {"chunks": 0, "sources": 0, "skipped": "signature unchanged"}

        stats = {"sessions": 0, "chunks": 0}
        if structural_changed:
            stats = transpile(source, conn)
            # The receipt advances only after structural rows and FTS commit.
            _record_source_state(conn, source, signature, size)

        embedded = 0
        if _embedding_debt(conn) > 0 or graph:
            try:
                embedded = _batch_embed_chunks(conn, quiet=True)
            except Exception as exc:
                print(f"[deepseek.refresh] embed failed: {exc}", file=sys.stderr)
                conn.commit()
        if stats.get("chunks", 0) > 0 or graph:
            try:
                run_enrichment(conn, cell_type="deepseek")
            except Exception as exc:
                print(f"[deepseek.refresh] enrichment failed: {exc}", file=sys.stderr)

        result = {"sources": stats.get("sessions", 0), "chunks": stats.get("chunks", 0)}
        if embedded:
            result["embedded"] = embedded
        return result
    finally:
        conn.close()
