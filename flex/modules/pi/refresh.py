"""Pi coding-agent watch refresh."""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from pathlib import Path

from flex.modules.claude_code import run_enrichment
from flex.modules.pi.compile.worker import (
    DEFAULT_PI_SESSIONS,
    compute_source_signature,
    ensure_pi_cell_schema,
    transpile,
)


_SIGNATURE_KEY = "pi_source_signature"
_SIZE_KEY = "pi_source_size"
_SOURCE_KEY = "pi_source_path"


def _source_from_meta(conn: sqlite3.Connection) -> Path:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_SOURCE_KEY,)).fetchone()
    return Path(row[0]).expanduser() if row and row[0] else DEFAULT_PI_SESSIONS


def _last_signature(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_SIGNATURE_KEY,)).fetchone()
    return str(row[0]) if row and row[0] else None


def _embedding_debt(conn: sqlite3.Connection) -> int:
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM _raw_chunks WHERE content IS NOT NULL AND embedding IS NULL"
            ).fetchone()[0]
        )
    except sqlite3.OperationalError:
        return 0


def _record_source_state(
    conn: sqlite3.Connection, source: Path, signature: str, size: int
) -> None:
    for key, value in (
        (_SOURCE_KEY, str(source)),
        (_SIGNATURE_KEY, signature),
        (_SIZE_KEY, str(size)),
    ):
        conn.execute("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def _main_database_path(conn: sqlite3.Connection) -> Path | None:
    for _seq, name, path in conn.execute("PRAGMA database_list"):
        if name == "main" and path:
            return Path(path)
    return None


def _publish_structural_rebuild(
    conn: sqlite3.Connection,
    source: Path,
    signature: str,
    size: int,
) -> dict[str, int]:
    """Build privately, preserve stable vectors, then publish in one backup."""
    main_path = _main_database_path(conn)
    temp_path: Path | None = None
    if main_path is None:
        candidate = sqlite3.connect(":memory:")
    else:
        fd, raw_path = tempfile.mkstemp(
            prefix=f".{main_path.stem}.pi-candidate-",
            suffix=".db",
            dir=main_path.parent,
        )
        os.close(fd)
        temp_path = Path(raw_path)
        candidate = sqlite3.connect(temp_path)
    candidate.row_factory = sqlite3.Row
    try:
        conn.backup(candidate)
        candidate.execute(
            """
            CREATE TEMP TABLE _pi_prior_embeddings AS
            SELECT id,content,embedding
            FROM _raw_chunks
            WHERE embedding IS NOT NULL
            """
        )
        candidate.execute(
            "CREATE INDEX temp.idx_pi_prior_embedding_id ON _pi_prior_embeddings(id)"
        )
        stats = transpile(source, candidate)
        candidate.execute(
            """
            UPDATE _raw_chunks
            SET embedding=(
                SELECT prior.embedding
                FROM _pi_prior_embeddings AS prior
                WHERE prior.id=_raw_chunks.id
                  AND prior.content=_raw_chunks.content
            )
            WHERE EXISTS(
                SELECT 1
                FROM _pi_prior_embeddings AS prior
                WHERE prior.id=_raw_chunks.id
                  AND prior.content=_raw_chunks.content
            )
            """
        )
        reused = int(candidate.execute("SELECT changes()").fetchone()[0])
        _record_source_state(candidate, source, signature, size)
        integrity = candidate.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise RuntimeError(f"Pi candidate integrity check failed: {integrity}")
        candidate.backup(conn)
        conn.commit()
        return {**stats, "reused_embeddings": reused}
    finally:
        candidate.close()
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
            Path(str(temp_path) + "-wal").unlink(missing_ok=True)
            Path(str(temp_path) + "-shm").unlink(missing_ok=True)


def sync_source_path(conn: sqlite3.Connection, observed_path: Path) -> int:
    """Publish a changed Pi session root structurally, without model work inline."""
    ensure_pi_cell_schema(conn)
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
    stats = _publish_structural_rebuild(conn, source, signature, size)
    return max(1, int(stats.get("chunks", 0)))


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False) -> dict:
    """Refresh one Pi cell, separating structural and semantic work."""
    if dry_run:
        conn = sqlite3.connect(f"file:{Path(cell_path)}?mode=ro", uri=True, timeout=30.0)
    else:
        conn = sqlite3.connect(str(cell_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        if not dry_run:
            ensure_pi_cell_schema(conn)
        source = _source_from_meta(conn)
        if not source.exists():
            return {"chunks": 0, "sources": 0, "skipped": "source missing"}
        signature, size = compute_source_signature(source)
        structural_changed = signature != _last_signature(conn)
        semantic_debt = _embedding_debt(conn)
        if dry_run:
            return {
                "dry_run": True,
                "needs_resync": structural_changed,
                "embedding_debt": semantic_debt,
            }
        if not structural_changed and not graph:
            return {
                "chunks": 0,
                "sources": 0,
                "embedding_debt": semantic_debt,
                "skipped": "signature unchanged",
            }

        stats = {"sessions": 0, "chunks": 0}
        if structural_changed:
            stats = _publish_structural_rebuild(conn, source, signature, size)

        if graph:
            try:
                run_enrichment(conn, cell_type="pi")
            except Exception as exc:
                print(f"[pi.refresh] enrichment failed: {exc}", file=sys.stderr)

        return {
            "sources": stats.get("sessions", 0),
            "chunks": stats.get("chunks", 0),
            "reused_embeddings": stats.get("reused_embeddings", 0),
            "embedding_debt": _embedding_debt(conn),
        }
    finally:
        conn.close()
