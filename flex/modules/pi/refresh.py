"""Pi coding-agent structural refresh."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from flex.modules.claude_code import run_enrichment
from flex.modules.pi.compile.worker import (
    DEFAULT_PI_SESSIONS,
    _source_files,
    compute_source_signature,
    ensure_pi_cell_schema,
    remove_session,
    replace_session,
)


_SIGNATURE_KEY = "pi_source_signature"
_SIZE_KEY = "pi_source_size"
_SOURCE_KEY = "pi_source_path"
_REFRESH_BATCH = 20


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
    if not observed.is_file() or observed.suffix != ".jsonl":
        return 0
    _session_id, chunks, _entries = replace_session(observed, conn, ensure_schema=False)
    return max(1, chunks)


def _changed_sessions(
    conn: sqlite3.Connection, source: Path
) -> tuple[list[Path], list[Path]]:
    """Select changed native files using receipts for individual sessions."""
    previous = {
        row[0]: (row[1], row[2])
        for row in conn.execute(
            "SELECT source_path,size_bytes,mtime_ns FROM _pi_source_state"
        )
    }
    changed: list[tuple[int, Path]] = []
    present: set[str] = set()
    for path in _source_files(source):
        resolved = path.resolve()
        present.add(str(resolved))
        stat = resolved.stat()
        if previous.get(str(resolved)) != (stat.st_size, stat.st_mtime_ns):
            changed.append((stat.st_mtime_ns, resolved))
    # Active sessions go first; a historical catch-up cannot starve today's tail.
    changed.sort(key=lambda item: (-item[0], str(item[1])))
    deleted = [Path(path) for path in previous if path not in present]
    deleted.sort(key=str)
    return [path for _mtime, path in changed], deleted


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
            changed, deleted = _changed_sessions(conn, source)
            for path in changed[:_REFRESH_BATCH]:
                _session_id, chunks, _entries = replace_session(
                    path, conn, ensure_schema=False, stats=stats,
                )
                stats["sessions"] += 1
                stats["chunks"] += chunks
            for path in deleted[: max(0, _REFRESH_BATCH - stats["sessions"])]:
                remove_session(path, conn)
                stats["sessions"] += 1
            pending = max(0, len(changed) + len(deleted) - stats["sessions"])
            if pending == 0:
                # A source may grow during the bounded pass. Retain debt until
                # the next signature probe confirms the accepted generation.
                final_signature, final_size = compute_source_signature(source)
                if final_signature == signature:
                    _record_source_state(conn, source, signature, final_size)
                else:
                    pending = 1
            stats["pending"] = pending

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
            "pending": stats.get("pending", 0),
        }
    finally:
        conn.close()


def main() -> int:
    """Compatibility CLI over the common cell job, not another update owner."""
    from flex.registry import get_cell_metadata
    from flex.lifecycle_jobs import execute_job

    cell = get_cell_metadata("pi")
    if not cell or not cell.get("active", 1) or not cell.get("path"):
        print("[pi.refresh] active Pi cell unavailable", file=sys.stderr)
        return 1
    try:
        stats = execute_job("pi")
    except Exception as exc:
        print(f"[pi.refresh] {exc}", file=sys.stderr)
        return 1
    print(f"[pi.refresh] {stats}")
    return 75 if stats.get("deferred") else 0


if __name__ == "__main__":
    raise SystemExit(main())
