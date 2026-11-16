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


def _trigger_specs(db: sqlite3.Connection, table: str) -> dict:
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
    bump = f"""
        BEGIN
            INSERT INTO _vector_generations(relation,generation)
            VALUES('{relation}',1)
            ON CONFLICT(relation) DO UPDATE SET
                generation=generation+1,
                updated_at=strftime('%s','now');
        END"""
    return {
        f"{prefix}_ai": (
            f"CREATE TRIGGER {prefix}_ai AFTER INSERT ON [{table}] "
            f"WHEN NEW.embedding IS NOT NULL {bump}"
        ),
        f"{prefix}_ad": (
            f"CREATE TRIGGER {prefix}_ad AFTER DELETE ON [{table}] "
            f"WHEN OLD.embedding IS NOT NULL {bump}"
        ),
        f"{prefix}_au": (
            f"CREATE TRIGGER {prefix}_au AFTER UPDATE OF "
            f"{update_columns_sql} ON [{table}] WHEN {update_condition} {bump}"
        ),
    }


def _stale_triggers(db: sqlite3.Connection, specs: dict) -> list:
    """(table, name, sql) for every trigger that is missing or differs."""
    stale = []
    for table, wanted in specs.items():
        existing = {
            str(row[0]): " ".join(str(row[1] or "").split())
            for row in db.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger' "
                "AND tbl_name=?", (table,)
            )
        }
        stale.extend(
            (table, name, sql) for name, sql in wanted.items()
            if existing.get(name) != " ".join(sql.split())
        )
    return stale


def ensure_vector_generations(db: sqlite3.Connection) -> None:
    db.execute(DDL)
    tables = sorted(
        str(row[0]) for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('_raw_chunks','_raw_sources')"
        )
    )
    specs = {table: _trigger_specs(db, table) for table in tables}
    # Only touch triggers that are missing or differ. Plain execute (never
    # executescript): DDL here must not commit the caller's transaction, and
    # an unchanged schema keeps the schema cookie stable for other connections.
    # The swap runs in one transaction so a concurrent writer never sees a
    # table without its triggers: our own when the caller has none open
    # (re-checked once the write lock is held), else the caller's.
    owned = False
    stale = _stale_triggers(db, specs)
    if stale and not db.in_transaction:
        db.execute("BEGIN IMMEDIATE")
        owned = True
        stale = _stale_triggers(db, specs)
    try:
        for table in tables:
            db.execute(
                "INSERT OR IGNORE INTO _vector_generations(relation,generation) "
                "VALUES(?,0)",
                (table,),
            )
        for _table, name, sql in stale:
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
            db.execute(sql)
    except BaseException:
        if owned:
            db.rollback()
        raise
    if owned:
        db.commit()


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
    """Advance a relation in the caller's current write transaction.

    Never commits. The explicit upsert is kept even though the per-row triggers
    also advance the counter: on a legacy cell the triggers are only installed
    by this call (after the caller's UPDATE), and a rewrite that leaves the
    embedding unchanged fires no trigger.
    """
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
