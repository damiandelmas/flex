"""Shared SOMA bridge for coding-agent adapters.

Coding-agent modules converge on the Claude Code substrate tables, especially
``_edges_tool_ops``. This helper turns those canonical tool-op rows into SOMA
identity edges without making every adapter know SOMA internals.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from flex.modules.soma import compile as soma_compile


def _resolve_file_path(target_file: str | None, cwd: str | None) -> str:
    if not target_file:
        return ""
    path = Path(target_file).expanduser()
    if not path.is_absolute() and cwd:
        path = Path(cwd).expanduser() / path
    return str(path)


def operation_to_chunk(op: dict[str, Any]) -> dict[str, Any]:
    """Convert a canonical tool operation into SOMA's chunk shape."""
    chunk_id = op.get("id") or op.get("chunk_id")
    tool = op.get("tool") or op.get("tool_name") or ""
    cwd = op.get("cwd") or ""
    target_file = _resolve_file_path(op.get("file") or op.get("target_file"), cwd)
    return {
        "id": chunk_id,
        "tool": tool,
        "file": target_file,
        "cwd": cwd,
        "url": op.get("url") or "",
        "web_content": op.get("web_content"),
        "web_status": op.get("web_status"),
        "session": op.get("session") or op.get("source_id") or "",
        "msg": op.get("msg") or op.get("position") or 0,
    }


def enrich_operation(conn: sqlite3.Connection, op: dict[str, Any]) -> bool:
    """Enrich and insert SOMA identity edges for one canonical tool op."""
    chunk = operation_to_chunk(op)
    if not chunk.get("id"):
        return False
    soma_compile.ensure_tables(conn)
    enriched = soma_compile.enrich(chunk)
    soma_compile.insert_edges(conn, enriched)
    return any(
        enriched.get(key)
        for key in (
            "file_uuid",
            "repo_root",
            "content_hash",
            "url_uuid",
        )
    )


def _tool_ops_sql(limit: int = 0) -> str:
    sql = """
        SELECT
            t.chunk_id,
            t.tool_name,
            t.target_file,
            t.cwd,
            es.source_id,
            es.position
        FROM _edges_tool_ops t
        LEFT JOIN _edges_source es ON es.chunk_id = t.chunk_id
        LEFT JOIN _edges_file_identity fi ON fi.chunk_id = t.chunk_id
        LEFT JOIN _edges_repo_identity ri ON ri.chunk_id = t.chunk_id
        LEFT JOIN _edges_content_identity ci ON ci.chunk_id = t.chunk_id
        LEFT JOIN _edges_url_identity ui ON ui.chunk_id = t.chunk_id
        WHERE t.tool_name IS NOT NULL
          AND (
            (
                t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
                AND fi.chunk_id IS NULL
            )
            OR (
                t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep','Bash')
                AND ri.chunk_id IS NULL
            )
            OR (
                t.tool_name IN ('Write','Edit','MultiEdit')
                AND ci.chunk_id IS NULL
            )
            OR (
                t.tool_name = 'WebFetch'
                AND ui.chunk_id IS NULL
            )
          )
        ORDER BY es.source_id, es.position, t.chunk_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return sql


def backfill_tool_ops(
    conn: sqlite3.Connection,
    *,
    limit: int = 0,
    dry_run: bool = False,
) -> dict[str, int]:
    """Backfill SOMA identity edges from canonical ``_edges_tool_ops`` rows."""
    soma_compile.ensure_tables(conn)
    rows = conn.execute(_tool_ops_sql(limit)).fetchall()
    stats = {"scanned": len(rows), "enriched": 0, "errors": 0}
    if dry_run:
        return stats

    for row in rows:
        try:
            op = {
                "chunk_id": row["chunk_id"] if hasattr(row, "keys") else row[0],
                "tool_name": row["tool_name"] if hasattr(row, "keys") else row[1],
                "target_file": row["target_file"] if hasattr(row, "keys") else row[2],
                "cwd": row["cwd"] if hasattr(row, "keys") else row[3],
                "source_id": row["source_id"] if hasattr(row, "keys") else row[4],
                "position": row["position"] if hasattr(row, "keys") else row[5],
            }
            if enrich_operation(conn, op):
                stats["enriched"] += 1
        except Exception:
            stats["errors"] += 1
    conn.commit()
    return stats
