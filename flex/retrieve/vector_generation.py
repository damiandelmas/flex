"""Transactional generation markers for derived vector artifacts."""

from __future__ import annotations

import sqlite3


DDL = """
CREATE TABLE IF NOT EXISTS _vector_generations (
    relation TEXT PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
)
"""


def ensure_vector_generations(db: sqlite3.Connection) -> None:
    db.execute(DDL)
    tables = {
        str(row[0]) for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('_raw_chunks','_raw_sources')"
        )
    }
    for table in sorted(tables):
        prefix = "flex_vec_" + table.removeprefix("_raw_")
        relation = table.replace("'", "''")
        columns = {
            str(row[1]) for row in db.execute(f"PRAGMA table_info([{table}])")
        }
        id_column = "id" if "id" in columns else "source_id"
        update_columns = ["embedding", id_column]
        update_conditions = [
            "OLD.embedding IS NOT NEW.embedding",
            f"(NEW.embedding IS NOT NULL AND OLD.[{id_column}] IS NOT NEW.[{id_column}])",
        ]
        if "timestamp" in columns:
            update_columns.append("timestamp")
            update_conditions.append(
                "(NEW.embedding IS NOT NULL AND OLD.timestamp IS NOT NEW.timestamp)"
            )
        update_columns_sql = ",".join(update_columns)
        update_condition = " OR ".join(update_conditions)
        db.execute(
            "INSERT OR IGNORE INTO _vector_generations(relation,generation) "
            "VALUES(?,0)",
            (table,),
        )
        db.executescript(f"""
            DROP TRIGGER IF EXISTS {prefix}_ai;
            DROP TRIGGER IF EXISTS {prefix}_ad;
            DROP TRIGGER IF EXISTS {prefix}_au;
            CREATE TRIGGER {prefix}_ai
            AFTER INSERT ON [{table}]
            WHEN NEW.embedding IS NOT NULL
            BEGIN
                INSERT INTO _vector_generations(relation,generation)
                VALUES('{relation}',1)
                ON CONFLICT(relation) DO UPDATE SET
                    generation=generation+1,
                    updated_at=strftime('%s','now');
            END;
            CREATE TRIGGER {prefix}_ad
            AFTER DELETE ON [{table}]
            WHEN OLD.embedding IS NOT NULL
            BEGIN
                INSERT INTO _vector_generations(relation,generation)
                VALUES('{relation}',1)
                ON CONFLICT(relation) DO UPDATE SET
                    generation=generation+1,
                    updated_at=strftime('%s','now');
            END;
            CREATE TRIGGER {prefix}_au
            AFTER UPDATE OF {update_columns_sql} ON [{table}]
            WHEN {update_condition}
            BEGIN
                INSERT INTO _vector_generations(relation,generation)
                VALUES('{relation}',1)
                ON CONFLICT(relation) DO UPDATE SET
                    generation=generation+1,
                    updated_at=strftime('%s','now');
            END;
        """)


def vector_generation(db: sqlite3.Connection, relation: str) -> int | None:
    try:
        row = db.execute(
            "SELECT generation FROM _vector_generations WHERE relation=?",
            (relation,),
        ).fetchone()
    except sqlite3.DatabaseError:
        return None
    return int(row[0]) if row else None


def bump_vector_generation(db: sqlite3.Connection, relation: str) -> int:
    """Advance a relation in the caller's current write transaction."""
    ensure_vector_generations(db)
    db.execute(
        "INSERT INTO _vector_generations(relation,generation) VALUES(?,1) "
        "ON CONFLICT(relation) DO UPDATE SET "
        "generation=generation+1,updated_at=strftime('%s','now')",
        (relation,),
    )
    return int(db.execute(
        "SELECT generation FROM _vector_generations WHERE relation=?",
        (relation,),
    ).fetchone()[0])
