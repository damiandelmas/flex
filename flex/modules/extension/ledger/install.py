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
SCHEMA_VERSION = 4


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


def ensure_schema(conn: sqlite3.Connection) -> bool:
    """Apply the additive Ledger schema migration once.

    Returns ``True`` only when a migration was applied. Legacy annotations are
    retained with unknown (NULL) authorship and receive searchable FTS rows.
    """
    from flex.modules.extension.ledger.sql import register_functions
    register_functions(conn)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)"
    )
    current = _schema_version(conn)
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"Ledger schema {current} is newer than supported {SCHEMA_VERSION}"
        )
    if current == SCHEMA_VERSION:
        _validate_schema(conn)
        return False

    existing = _table_exists(conn, "_types_annotation")
    existing_columns = _columns(conn, "_types_annotation") if existing else set()
    statements = [
        "BEGIN IMMEDIATE",
        "DROP VIEW IF EXISTS annotation_history",
        "DROP VIEW IF EXISTS annotations",
    ]
    if existing:
        for column in ("author_provider", "author_session_id", "author_source"):
            if column not in existing_columns:
                statements.append(
                    f"ALTER TABLE _types_annotation ADD COLUMN {column} TEXT"
                )
    statements.append(SCHEMA.read_text())
    statements.extend([
        "INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')",
        "INSERT OR REPLACE INTO _meta(key,value) VALUES "
        "('cell_type','ledger'),"
        "('description','Annotations referencing objects in other Flex cells.'),"
        "('schema','ledger.v4'),"
        "('ledger_schema_version','4'),"
        "('embed','false'),"
        "('lifecycle','authored')",
        "COMMIT",
    ])
    try:
        conn.executescript(";\n".join(statements) + ";")
    except Exception:
        conn.rollback()
        raise
    _validate_schema(conn)
    return True


def ensure_presets(conn: sqlite3.Connection) -> bool:
    """Compatibility no-op for pre-file-backed Ledger installations.

    Ledger's checked-in SQL files are resolved at query time by the shared
    preset resolver.  Existing ``_presets`` rows remain untouched as
    recoverable legacy state, but opening Ledger never copies or rewrites them.
    """
    del conn
    return False


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
    if resolve_cell(LEDGER_CELL) is None:
        register_cell(
            LEDGER_CELL,
            path,
            cell_type="ledger",
            description="Annotations referencing objects in other Flex cells.",
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
            "schema": "ledger.v4",
            "presets": [p.stem for p in sorted((EXT_DIR / "stock" / "presets").glob("*.sql"))],
        }
    finally:
        conn.close()


def main() -> None:
    import json

    print(json.dumps(install()))


if __name__ == "__main__":
    main()
