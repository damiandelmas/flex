"""Create the dedicated Ledger commentary cell.

Ledger references objects in other cells by durable cell identity and chunk
identity. Installing or using Ledger never changes a target cell's schema.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3

from flex.core import log_op, open_cell
from flex.registry import CELLS_DIR, register_cell, resolve_cell


LEDGER_CELL = "ledger"
EXT_DIR = Path(__file__).resolve().parent
SCHEMA = EXT_DIR / "schema.sql"
SCHEMA_VERSION = 7


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (name,),
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = 'ledger_schema_version'"
    ).fetchone()
    if row is None:
        return 1 if _table_exists(conn, "_types_annotation") else 0
    try:
        return int(row[0])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid Ledger schema version: {row[0]!r}") from exc


def _validate_schema(conn: sqlite3.Connection) -> None:
    required = {
        "_raw_chunks", "_types_annotation", "_edges_annotation_target",
        "annotation_revisions", "chunks_fts", "annotations", "annotation_history",
    }
    present = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise RuntimeError("Ledger schema is incomplete: " + ", ".join(missing))
    author_columns = {"author_provider", "author_session_id", "author_source"}
    missing_columns = sorted(author_columns - _columns(conn, "_types_annotation"))
    if missing_columns:
        raise RuntimeError(
            "Ledger annotation provenance is incomplete: " + ", ".join(missing_columns)
        )
    for table in ("_types_annotation", "annotation_revisions",
                  "annotations", "annotation_history"):
        if "metadata" not in _columns(conn, table):
            raise RuntimeError(f"Ledger metadata column is missing from {table}")
    for table in ("_edges_annotation_target", "annotation_revisions", "annotations", "annotation_history"):
        if "target_end_chunk_id" not in _columns(conn, table):
            raise RuntimeError(f"Ledger range endpoint is missing from {table}")
    required_triggers = {
        "annotations_insert", "annotations_update", "annotations_delete",
    }
    present_triggers = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        )
    }
    missing_triggers = sorted(required_triggers - present_triggers)
    if missing_triggers:
        raise RuntimeError(
            "Ledger writable contract is incomplete: "
            + ", ".join(missing_triggers)
        )


def range_target_migration_sql() -> str:
    """Rebuild the v6 target edge without changing annotation identities."""
    return """
CREATE TABLE _edges_annotation_target_v7 (
    chunk_id TEXT PRIMARY KEY REFERENCES _raw_chunks(id),
    target_cell_id TEXT NOT NULL,
    target_chunk_id TEXT NOT NULL,
    target_end_chunk_id TEXT CHECK (
        target_end_chunk_id IS NULL OR
        (length(target_end_chunk_id)>0 AND target_end_chunk_id<>target_chunk_id)
    )
);
INSERT INTO _edges_annotation_target_v7(chunk_id,target_cell_id,target_chunk_id)
SELECT chunk_id,target_cell_id,target_chunk_id FROM _edges_annotation_target;
DROP TABLE _edges_annotation_target;
ALTER TABLE _edges_annotation_target_v7 RENAME TO _edges_annotation_target;
"""


def schema_migration_sql(conn: sqlite3.Connection) -> str:
    """Build migration SQL without writing; the caller owns production transactions."""
    current = _schema_version(conn) if _table_exists(conn, "_meta") else 0
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"Ledger schema {current} is newer than supported {SCHEMA_VERSION}"
        )
    if current == SCHEMA_VERSION:
        _validate_schema(conn)
        return ""

    existing = _table_exists(conn, "_types_annotation")
    existing_columns = _columns(conn, "_types_annotation") if existing else set()
    statements = [
        "DROP TRIGGER IF EXISTS annotations_insert",
        "DROP TRIGGER IF EXISTS annotations_update",
        "DROP TRIGGER IF EXISTS annotations_delete",
        "DROP VIEW IF EXISTS annotation_history",
        "DROP VIEW IF EXISTS annotations",
    ]
    if existing:
        for column in ("author_provider", "author_session_id", "author_source"):
            if column not in existing_columns:
                statements.append(
                    f"ALTER TABLE _types_annotation ADD COLUMN {column} TEXT"
                )
    for table in ("_types_annotation", "annotation_revisions"):
        if _table_exists(conn, table) and "metadata" not in _columns(conn, table):
            statements.append(
                f"ALTER TABLE {table} ADD COLUMN metadata TEXT "
                "CHECK (metadata IS NULL OR json_valid(metadata))"
            )
    if _table_exists(conn, "_edges_annotation_target") and "target_end_chunk_id" not in _columns(conn, "_edges_annotation_target"):
        statements.append(range_target_migration_sql())
    if _table_exists(conn, "annotation_revisions") and "target_end_chunk_id" not in _columns(conn, "annotation_revisions"):
        statements.append("ALTER TABLE annotation_revisions ADD COLUMN target_end_chunk_id TEXT")
    # Distinguish trigger END from transaction control for a wrapper that manages transactions.
    statements.append(SCHEMA.read_text().replace("\nEND;", "\n-- Close trigger body, not a transaction.\nEND;"))
    statements.extend([
        "INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')",
        "INSERT OR REPLACE INTO _meta(key,value) VALUES "
        "('cell_type','ledger'),"
        "('description','Annotations referencing objects in other Flex cells.'),"
        "('schema','ledger.v7'),"
        "('ledger_schema_version','7'),"
        "('embed','false'),"
        "('lifecycle','authored')",
    ])
    return ";\n".join(statements) + ";"


def ensure_schema(conn: sqlite3.Connection) -> bool:
    """Apply the migration on isolated/local connections; production callers manage their own transactions."""
    from flex.modules.extension.ledger.sql import register_functions
    register_functions(conn)
    script = schema_migration_sql(conn)
    if not script:
        return False
    try:
        conn.executescript("BEGIN IMMEDIATE;\n" + script + "\nCOMMIT;")
    except Exception:
        conn.rollback()
        raise
    _validate_schema(conn)
    return True


def ensure_presets(conn: sqlite3.Connection) -> bool:
    """Seed missing stock SQL programs without replacing database-owned rows.

    A name listed in _presets_retired is never re-added (as in
    flex.manage.install_presets). No compile check here: @index and @hydrate
    read tables the engine materializes per query, so a plain connection
    cannot judge them.
    """
    from flex.manage.install_presets import _retired
    from flex.retrieve.presets import parse_preset

    existing = {
        str(row[0]) for row in conn.execute("SELECT name FROM _presets")
    }
    retired = _retired(conn)
    installed = False
    for path in sorted((EXT_DIR / "stock" / "presets").glob("*.sql")):
        text = path.read_text()
        preset = parse_preset(text, path.stem)
        if preset["name"] in existing or preset["name"] in retired:
            continue
        cursor = conn.execute(
            "INSERT OR IGNORE INTO _presets(name,description,params,sql,source) "
            "VALUES (?,?,?,?, 'stock')",
            (
                preset["name"],
                preset["description"],
                preset.get("params", ""),
                text,
            ),
        )
        installed = cursor.rowcount > 0 or installed
    if installed:
        conn.commit()
    return installed


def ledger_path() -> Path:
    existing = resolve_cell(LEDGER_CELL)
    if existing:
        return existing
    CELLS_DIR.mkdir(parents=True, exist_ok=True)
    return CELLS_DIR / "ledger.db"


def open_ledger() -> sqlite3.Connection:
    """Open the Ledger cell, creating and registering it on first use."""
    path = ledger_path()
    conn = open_cell(str(path))
    ensure_schema(conn)
    ensure_presets(conn)
    if resolve_cell(LEDGER_CELL) is None:
        register_cell(
            LEDGER_CELL,
            path,
            cell_type="ledger",
            description="Annotations referencing objects in other Flex cells.",
            publish=True,
        )
    return conn


def install() -> dict:
    conn = open_ledger()
    try:
        log_op(
            conn,
            "ledger_install",
            "ledger",
            rows_affected=0,
            source="flex/modules/extension/ledger/install.py",
        )
        conn.commit()
        return {
            "cell": LEDGER_CELL,
            "path": str(ledger_path()),
            "schema": "ledger.v7",
            "presets": [p.stem for p in sorted((EXT_DIR / "stock" / "presets").glob("*.sql"))],
        }
    finally:
        conn.close()


def main() -> None:
    import json

    print(json.dumps(install()))


if __name__ == "__main__":
    main()
