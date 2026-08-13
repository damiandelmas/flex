"""Canonical writable SQL surface for the Ledger cell."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid


ANNOTATION_NAMESPACE = uuid.UUID("733c2562-df4a-4dd1-b71c-e7c6711f4418")

_MUTATION_RE = re.compile(
    r"^\s*(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"
    r"(?:main\.)?[\"`\[]?annotations[\"`\]]?\b",
    re.IGNORECASE,
)


def annotation_id(target_cell_id: str, target_chunk_id: str) -> str:
    """Return the stable annotation identity for one exact target object."""
    return str(uuid.uuid5(
        ANNOTATION_NAMESPACE, f"{target_cell_id}:{target_chunk_id}"
    ))


def mutation_operation(sql: str) -> str | None:
    """Recognize direct mutations of the public ``annotations`` relation."""
    match = _MUTATION_RE.match(sql)
    if match is None:
        return None
    return match.group(1).split()[0].lower()


def register_functions(
    conn: sqlite3.Connection,
    *,
    author_provider: str | None = None,
    author_session_id: str | None = None,
    author_source: str | None = None,
) -> None:
    """Install deterministic identity and caller-scoped authorship functions."""
    conn.create_function("ledger_annotation_id", 2, annotation_id, deterministic=True)
    conn.create_function("ledger_author_provider", 0, lambda: author_provider)
    conn.create_function("ledger_author_session_id", 0, lambda: author_session_id)
    conn.create_function("ledger_author_source", 0, lambda: author_source)


def execute_mutation(conn: sqlite3.Connection, sql: str) -> str:
    """Execute one canonical Ledger mutation transaction and return JSON."""
    operation = mutation_operation(sql)
    if operation is None:
        return json.dumps({
            "error": "Ledger writes must target the annotations relation directly"
        })

    try:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(sql)
        columns = [item[0] for item in cursor.description or ()]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return json.dumps({
        "status": "ok",
        "operation": operation,
        "results": rows,
    }, indent=2, default=str)
