"""Session-seeded query materialization across registered Flex cells."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import sqlite3
from typing import Mapping

from flex.meta import MaterializedCell, attach_cell_ids
from flex.runtime_context import RuntimeSeed, resolve_runtime_seed


SELF_TABLE = "_flex_self_objects"
CONTENT_VIEW = "_flex_self_content"
RUNTIME_TABLE = "_flex_runtime"


@dataclass(frozen=True)
class MaterializationContext:
    primary_cell: str | None = None
    explicit_cells: tuple[str, ...] = ()
    available_cells: tuple[str, ...] = ()
    environ: Mapping[str, str] | None = None


def _json_error(message: str) -> str:
    return json.dumps({"error": message})


def _mask_sql_data(sql: str) -> str:
    """Blank strings and comments while preserving executable offsets."""
    masked = list(sql)
    i = 0
    while i < len(sql):
        if sql.startswith("--", i):
            end = sql.find("\n", i + 2)
            end = len(sql) if end < 0 else end
            masked[i:end] = " " * (end - i)
            i = end
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = len(sql) if end < 0 else end + 2
            masked[i:end] = " " * (end - i)
            i = end
            continue
        if sql[i] in {"'", '"'}:
            quote = sql[i]
            start = i
            i += 1
            while i < len(sql):
                if sql[i] == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            masked[start:i] = " " * (i - start)
            continue
        i += 1
    return "".join(masked)


def _find_self_call(sql: str) -> tuple[int, int, str] | None:
    code = _mask_sql_data(sql)
    match = re.search(r"\bself\s*\(", code, re.IGNORECASE)
    if match is None:
        return None
    before = code[:match.start()].rstrip().upper()
    if not (before.endswith("FROM") or before.endswith("JOIN") or before.endswith(",")):
        return None

    start = match.end() - 1
    depth = 0
    i = start
    while i < len(code):
        char = code[i]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return match.start(), i + 1, sql[start + 1:i].strip()
        i += 1
    return None


def _selection_sql(inner: str, seed: RuntimeSeed) -> tuple[str | None, str | None]:
    if not inner:
        return (
            "SELECT seed_cell_id AS target_cell_id, "
            "seed_session_id AS target_object_id, "
            "'session' AS target_object_type, 0 AS source_order, "
            "'runtime seed via ' || seed_source AS selection_reason "
            f"FROM {RUNTIME_TABLE}",
            None,
        )
    if not (len(inner) >= 2 and inner[0] == "'" and inner[-1] == "'"):
        return None, "self() accepts zero arguments or one quoted SELECT statement"
    scope = inner[1:-1].replace("''", "'").strip()
    if not scope.upper().startswith(("SELECT", "WITH")):
        return None, "self() selection must be read-only SELECT or WITH SQL"
    return scope, None


def _install_runtime(db: sqlite3.Connection, seed: RuntimeSeed) -> None:
    db.execute(f"DROP TABLE IF EXISTS temp.{RUNTIME_TABLE}")
    db.execute(
        f"CREATE TEMP TABLE {RUNTIME_TABLE} ("
        "seed_cell_id TEXT NOT NULL, seed_cell_name TEXT NOT NULL, "
        "seed_session_id TEXT NOT NULL, seed_source TEXT NOT NULL)"
    )
    db.execute(
        f"INSERT INTO {RUNTIME_TABLE} VALUES (?, ?, ?, ?)",
        (seed.cell_id, seed.cell, seed.session_id, seed.source),
    )


def _scope_rows(db: sqlite3.Connection, sql: str) -> list[dict]:
    cursor = db.execute(sql)
    columns = [item[0] for item in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _normalize_rows(rows: list[dict]) -> tuple[list[dict], str | None]:
    required = {"target_cell_id", "target_object_id"}
    normalized = []
    for index, row in enumerate(rows):
        missing = required - set(row)
        if missing:
            return [], (
                "self() selection is missing required column(s): "
                + ", ".join(sorted(missing))
            )
        cell_id = str(row["target_cell_id"] or "").strip()
        object_id = str(row["target_object_id"] or "").strip()
        if not cell_id or not object_id:
            return [], "self() selection returned an empty cell or object identity"
        object_type = str(row.get("target_object_type") or "object").strip()
        if object_type not in {"session", "object"}:
            return [], f"self() unsupported target_object_type: {object_type}"
        source_order = row.get("source_order")
        try:
            source_order = int(source_order) if source_order is not None else index
        except (TypeError, ValueError):
            return [], "self() source_order must be an integer"
        normalized.append({
            "target_cell_id": cell_id,
            "target_object_id": object_id,
            "target_object_type": object_type,
            "source_order": source_order,
            "selection_reason": str(row.get("selection_reason") or "SQL selection"),
        })
    return normalized, None


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _has_relation(db: sqlite3.Connection, alias: str, name: str) -> bool:
    row = db.execute(
        f"SELECT 1 FROM {_q(alias)}.sqlite_master "
        "WHERE name = ? AND type IN ('table', 'view') LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _install_self_objects(
    db: sqlite3.Connection,
    rows: list[dict],
    cells: dict[str, MaterializedCell],
) -> None:
    relation = db.execute(
        "SELECT type FROM sqlite_temp_master WHERE name = ?", (CONTENT_VIEW,)
    ).fetchone()
    if relation:
        db.execute(
            f"DROP {'VIEW' if relation[0] == 'view' else 'TABLE'} "
            f"temp.{CONTENT_VIEW}"
        )
    db.execute(f"DROP TABLE IF EXISTS temp.{SELF_TABLE}")
    db.execute(
        f"CREATE TEMP TABLE {SELF_TABLE} ("
        "selection_id INTEGER PRIMARY KEY, "
        "target_cell_id TEXT NOT NULL, target_cell_name TEXT NOT NULL, "
        "cell_alias TEXT NOT NULL, target_object_id TEXT NOT NULL, "
        "target_object_type TEXT NOT NULL, source_order INTEGER NOT NULL, "
        "selection_reason TEXT NOT NULL)"
    )
    db.executemany(
        f"INSERT INTO {SELF_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                index,
                row["target_cell_id"],
                cells[row["target_cell_id"]].cell_name,
                cells[row["target_cell_id"]].alias,
                row["target_object_id"],
                row["target_object_type"],
                row["source_order"],
                row["selection_reason"],
            )
            for index, row in enumerate(rows)
        ],
    )


def _install_content_view(
    db: sqlite3.Connection,
    rows: list[dict],
    cells: dict[str, MaterializedCell],
) -> str | None:
    pieces: list[str] = []
    for cell_id in dict.fromkeys(row["target_cell_id"] for row in rows):
        cell = cells[cell_id]
        alias = _q(cell.alias)
        cell_id_sql = _literal(cell_id)
        cell_name_sql = _literal(cell.cell_name)
        kinds = {
            row["target_object_type"]
            for row in rows
            if row["target_cell_id"] == cell_id
        }
        if "session" in kinds:
            if not _has_relation(db, cell.alias, "messages"):
                return f"selected session cell has no messages relation: {cell.cell_name}"
            pieces.append(
                "SELECT s.selection_id, "
                f"{cell_id_sql} AS target_cell_id, {cell_name_sql} AS target_cell_name, "
                "m.id AS target_object_id, m.session_id AS parent_object_id, "
                "'message' AS target_object_type, m.position AS source_order, "
                "m.created_at AS created_at, m.type AS native_type, m.content AS content "
                f"FROM {SELF_TABLE} s JOIN {alias}.messages m "
                "ON s.target_object_type = 'session' "
                f"AND s.target_cell_id = {cell_id_sql} "
                "AND m.session_id = s.target_object_id"
            )
        if "object" in kinds:
            if not _has_relation(db, cell.alias, "_raw_chunks"):
                return f"selected object cell has no _raw_chunks relation: {cell.cell_name}"
            pieces.append(
                "SELECT s.selection_id, "
                f"{cell_id_sql} AS target_cell_id, {cell_name_sql} AS target_cell_name, "
                "c.id AS target_object_id, NULL AS parent_object_id, "
                "'object' AS target_object_type, s.source_order AS source_order, "
                "c.timestamp AS created_at, NULL AS native_type, c.content AS content "
                f"FROM {SELF_TABLE} s JOIN {alias}._raw_chunks c "
                "ON s.target_object_type = 'object' "
                f"AND s.target_cell_id = {cell_id_sql} "
                "AND c.id = s.target_object_id"
            )

    body = " UNION ALL ".join(pieces) if pieces else (
        "SELECT NULL AS selection_id, NULL AS target_cell_id, "
        "NULL AS target_cell_name, NULL AS target_object_id, "
        "NULL AS parent_object_id, NULL AS target_object_type, "
        "NULL AS source_order, NULL AS created_at, NULL AS native_type, "
        "NULL AS content WHERE 0"
    )
    # SQLite cannot persist a cross-database view in every supported build.
    # Materialize only the selected canonical rows into a query-local table;
    # the owning cells remain attached and authoritative.
    db.execute(f"CREATE TEMP TABLE {CONTENT_VIEW} AS {body}")
    return None


def materialize_self(
    db: sqlite3.Connection,
    sql: str,
    *,
    context: MaterializationContext | None = None,
    restore_authorizer=None,
) -> str:
    """Lower ``self()`` and self temp-relation references for one query."""
    call = _find_self_call(sql)
    if call is None and SELF_TABLE not in sql and CONTENT_VIEW not in sql:
        return sql
    if call is None:
        existing = {
            row[0] for row in db.execute(
                "SELECT name FROM sqlite_temp_master "
                "WHERE name IN (?, ?)", (SELF_TABLE, CONTENT_VIEW)
            )
        }
        if {SELF_TABLE, CONTENT_VIEW}.issubset(existing):
            return sql
    context = context or MaterializationContext()
    try:
        seed = resolve_runtime_seed(environ=context.environ)
    except ValueError as exc:
        return _json_error(str(exc))

    _install_runtime(db, seed)
    inner = call[2] if call else ""
    scope_sql, error = _selection_sql(inner, seed)
    if error:
        return _json_error(error)
    try:
        from flex.mcp_core import search_authorizer

        db.set_authorizer(search_authorizer)
        rows = _scope_rows(db, scope_sql or "")
    except sqlite3.DatabaseError as exc:
        return _json_error(f"self() selection failed: {exc}")
    finally:
        db.set_authorizer(restore_authorizer)

    rows, error = _normalize_rows(rows)
    if error:
        return _json_error(error)
    try:
        db.set_authorizer(None)
        cells, error = attach_cell_ids(
            db,
            (row["target_cell_id"] for row in rows),
            explicit_cells=context.explicit_cells,
        )
    finally:
        db.set_authorizer(restore_authorizer)
    if error:
        return _json_error(error)

    try:
        _install_self_objects(db, rows, cells)
        error = _install_content_view(db, rows, cells)
    except sqlite3.DatabaseError as exc:
        return _json_error(f"self materialization failed: {exc}")
    if error:
        return _json_error(error)
    if call is None:
        return sql
    return sql[:call[0]] + SELF_TABLE + sql[call[1]:]
