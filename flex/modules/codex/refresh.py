"""codex cell refresh — scan configured Codex session roots."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from flex.modules.codex.sources import (
    CodexSource,
    combined_signature,
    resolve_sources,
)


_SIZE_KEY = "codex_dir_total_size"
_COUNT_KEY = "codex_dir_file_count"
_SOURCE_KEY = "codex_source_path"
_SOURCES_SIGNATURE_KEY = "codex_sources_signature"
_SOURCES_COUNT_KEY = "codex_sources_count"


def _source_from_meta(conn: sqlite3.Connection) -> Path:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (_SOURCE_KEY,)).fetchone()
    if row and row[0]:
        return Path(row[0])
    return Path.home() / ".codex" / "sessions"


def _last_signature(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?", (_SOURCES_SIGNATURE_KEY,)
    ).fetchone()
    if row and row[0]:
        return str(row[0])

    # Back-compat for cells that only have the old aggregate signature.
    def _get(key: str) -> int:
        old = conn.execute("SELECT value FROM _meta WHERE key = ?", (key,)).fetchone()
        if old and old[0]:
            try:
                return int(old[0])
            except (TypeError, ValueError):
                return 0
        return 0

    size, count = _get(_SIZE_KEY), _get(_COUNT_KEY)
    return f"legacy:{size}:{count}" if size or count else None


def _record_meta(
    conn: sqlite3.Connection,
    primary_source: Path,
    sources: list[CodexSource],
    signature: str,
) -> None:
    total_size = 0
    file_count = 0
    for source in sources:
        if not source.sessions_dir.is_dir():
            continue
        for path in source.sessions_dir.rglob("rollout-*.jsonl"):
            try:
                total_size += path.stat().st_size
                file_count += 1
            except OSError:
                continue
    values = {
        _SIZE_KEY: str(total_size),
        _COUNT_KEY: str(file_count),
        _SOURCE_KEY: str(primary_source),
        _SOURCES_SIGNATURE_KEY: signature,
        _SOURCES_COUNT_KEY: str(len(sources)),
    }
    for key, value in values.items():
        conn.execute("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)", (key, value))
    conn.commit()


def _ensure_codex_tables(conn: sqlite3.Connection) -> None:
    from flex.modules.codex.compile.worker import ensure_codex_tables

    ensure_codex_tables(conn)


def _transpile_source(source: CodexSource, conn: sqlite3.Connection, meta: dict) -> dict:
    from flex.modules.codex.compile.worker import transpile

    return transpile(source.sessions_dir, conn, state_db=source.state_db, source_meta=meta)


def _embed_and_enrich(conn: sqlite3.Connection) -> None:
    from flex.modules.claude_code import run_enrichment
    from flex.modules.claude_code.compile.worker import _batch_embed_chunks

    try:
        _batch_embed_chunks(conn, quiet=True)
    except Exception as e:
        print(f"[codex.refresh] embed failed: {e}", file=sys.stderr)
        conn.commit()
    try:
        run_enrichment(conn, cell_type="codex")
    except Exception as e:
        print(f"[codex.refresh] enrichment failed: {e}", file=sys.stderr)


def refresh(cell_path: str, graph: bool = False, dry_run: bool = False) -> dict:
    if dry_run:
        uri = f"file:{Path(cell_path)}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    else:
        conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    if not dry_run:
        conn.execute("PRAGMA journal_mode=WAL")
    try:
        primary_source = _source_from_meta(conn)
        sources = resolve_sources(conn)

        if not sources:
            return {"chunks": 0, "sources": 0, "skipped": "source missing"}

        signature = combined_signature(sources)
        last_signature = _last_signature(conn)
        needs_resync = signature != last_signature

        if dry_run:
            return {
                "dry_run": True,
                "needs_resync": needs_resync,
                "sources": len([source for source in sources if source.usable]),
                "source_candidates": len(sources),
            }

        if not needs_resync and not graph:
            return {"chunks": 0, "sources": 0, "skipped": "signature unchanged"}

        _ensure_codex_tables(conn)
        ok_sources = [source for source in sources if source.usable]
        conn.execute("DELETE FROM _types_codex_source")

        total_sessions = 0
        total_chunks = 0
        for source in ok_sources:
            meta = {
                "source_kind": source.source_kind,
                "codex_home": str(source.codex_home),
                "sessions_dir": str(source.sessions_dir),
                "state_db": str(source.state_db),
                "source_order": source.source_order,
            }
            stats = _transpile_source(source, conn, meta)
            total_sessions += stats.get("sessions", 0)
            total_chunks += stats.get("chunks", 0)

        _record_meta(conn, primary_source, sources, signature)

        if total_chunks > 0 or graph:
            _embed_and_enrich(conn)

        return {
            "sources": len(ok_sources),
            "sessions": total_sessions,
            "chunks": total_chunks,
            "source_candidates": len(sources),
            "skipped_sources": len(sources) - len(ok_sources),
        }
    finally:
        conn.close()
