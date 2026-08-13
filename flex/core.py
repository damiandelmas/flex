"""
Flex Core — cell loading, SQL execution, metadata access.

Infrastructure plumbing. Every domain depends on core. No domain logic here.

Functions:
- open_cell()        -> load .db, return conn
- run_sql()          -> execute SQL, return list[dict]
- get_meta/set_meta  -> canonical metadata access with legacy fallback

View generation lives in views.py (same package).
"""

import datetime
import json
import sqlite3
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Re-export for backward compatibility — callers can import from either module
from flex.views import regenerate_views  # noqa: F401


def _zone_now(zone):
    """Return the current instant in an IANA zone, or None when unknown."""
    if not zone:
        return None
    try:
        return datetime.datetime.now(datetime.timezone.utc).astimezone(
            ZoneInfo(str(zone))
        )
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        return None


def _register_relational_udfs(db: sqlite3.Connection) -> None:
    """Install timezone helpers used by relational cell projections."""
    def local_time(zone):
        local = _zone_now(zone)
        return local.strftime("%I:%M %p") if local else None

    def offset(zone):
        local = _zone_now(zone)
        value = local.utcoffset() if local else None
        return int(value.total_seconds() // 60) if value is not None else None

    def call_window(zone):
        local = _zone_now(zone)
        if not local:
            return "unknown"
        return "open" if local.weekday() < 5 and 9 <= local.hour < 17 else "closed"

    db.create_function("tz_local_time", 1, local_time)
    db.create_function("tz_offset", 1, offset)
    db.create_function("tz_call_window", 1, call_window)


def open_cell(db_path: str) -> sqlite3.Connection:
    """Open a cell database with optimized settings."""
    db = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
    db.row_factory = sqlite3.Row
    _register_relational_udfs(db)
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-20000")
    db.execute("PRAGMA temp_store=MEMORY")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def open_cell_readonly(db_path: str | Path) -> sqlite3.Connection:
    """Open a cell for query reads, falling back to immutable SQLite URI.

    Some sandboxed agent seats can read a cell file but cannot create SQLite
    lock/journal side files next to it. Normal ``open_cell`` is still the
    write-capable runtime opener; this read path tolerates those sandboxes.
    """
    path = Path(db_path)
    try:
        return open_cell(str(path))
    except sqlite3.OperationalError:
        db = sqlite3.connect(
            f"file:{path}?mode=ro&immutable=1",
            uri=True,
            check_same_thread=False,
            timeout=10,
        )
        db.execute("PRAGMA schema_version").fetchone()
        db.row_factory = sqlite3.Row
        _register_relational_udfs(db)
        db.execute("PRAGMA query_only=ON")
        db.execute("PRAGMA cache_size=-20000")
        db.execute("PRAGMA temp_store=MEMORY")
        return db


def run_sql(db: sqlite3.Connection, query: str,
            params: tuple = ()) -> list[dict]:
    """Execute SQL, return list of dicts."""
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_meta(db: sqlite3.Connection, key: str) -> Optional[str]:
    """Read canonical metadata without migrating a legacy cell."""
    from flex.envelope import metadata_relation

    relation = metadata_relation(db)
    if relation is None:
        return None
    try:
        row = db.execute(
            f"SELECT value FROM {relation} WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def set_meta(db: sqlite3.Connection, key: str, value: str):
    """Write the cell's existing metadata authority; new cells use canonical."""
    from flex.envelope import ensure_metadata_surface, metadata_relation

    relation = metadata_relation(db)
    if relation is None:
        ensure_metadata_surface(db)
        relation = "_metadata"
    db.execute(
        f"INSERT INTO {relation}(key,value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    db.commit()


def ensure_ops_table(db: sqlite3.Connection):
    """Create _ops table if it doesn't exist. Idempotent."""
    db.execute("""CREATE TABLE IF NOT EXISTS _ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER DEFAULT (strftime('%s','now')),
        operation TEXT,
        target TEXT,
        sql TEXT,
        params TEXT,
        rows_affected INTEGER,
        source TEXT
    )""")


def log_op(db: sqlite3.Connection, operation: str, target: str,
           params: dict = None, rows_affected: int = None,
           source: str = None, sql: str = None):
    """Log a cell mutation to _ops. Self-logging — callers capture their own params."""
    ensure_ops_table(db)
    db.execute(
        "INSERT INTO _ops (operation, target, sql, params, rows_affected, source) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (operation, target, sql,
         json.dumps(params) if params else None,
         rows_affected, source))


def validate_cell(db: sqlite3.Connection):
    """Post-COMPILE sanity checks. Call after population, before embed.

    Catches invariant violations at ingest time, not when a view query
    returns wrong counts 3 months later.
    """
    errors = []

    # Source edge 1:1 invariant — each chunk belongs to exactly one source
    dupes = db.execute("""
        SELECT chunk_id, COUNT(*) as n FROM _edges_source
        GROUP BY chunk_id HAVING n > 1
    """).fetchall()
    if dupes:
        errors.append(f"{len(dupes)} chunks have multiple sources")

    # Every chunk should have a source edge
    orphans = db.execute("""
        SELECT c.id FROM _raw_chunks c
        LEFT JOIN _edges_source e ON c.id = e.chunk_id
        WHERE e.chunk_id IS NULL
    """).fetchall()
    if orphans:
        errors.append(f"{len(orphans)} chunks have no source edge")

    if errors:
        raise ValueError("Cell validation failed: " + "; ".join(errors))


def validate_tree_projection(
    db: sqlite3.Connection,
    *,
    known_nodes: set[str] | None = None,
    relations: set[str] | None = None,
    validate_depth: bool = False,
    validate_order: bool = False,
) -> None:
    """Validate current navigational projections in ``_edges_tree``.

    The table describes occurrences, not durable identities. Every emitted
    occurrence has at most one parent row; multiple appearances use aliases and
    resolve to referents through provider/SOMA surfaces elsewhere. ``relation``
    remains a provider-owned edge kind: one tree may legitimately mix relations.

    Existing Flex trees are edge-only, so a root need not have a synthetic row.
    Pass the provider's ``known_nodes`` when parent existence must be proven.
    ``relations`` can select a homogeneous module projection without changing
    the meaning of relation for heterogeneous provider trees. Depth and sibling
    order are provider hints by default; strict SummaryTree-like projections can
    opt into their derived invariants.
    """
    if not db.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type IN ('table','view') AND name='_edges_tree'"
    ).fetchone():
        return
    columns = {row[1] for row in db.execute("PRAGMA table_info(_edges_tree)")}
    required = {"id", "parent_id", "relation", "depth"}
    missing_columns = sorted(required - columns)
    if missing_columns:
        raise ValueError(
            "Tree projection validation failed: missing columns "
            + ", ".join(missing_columns)
        )
    position_sql = "position" if "position" in columns else "NULL AS position"
    params: tuple[str, ...] = ()
    where = ""
    if relations:
        placeholders = ",".join("?" for _ in relations)
        where = f" WHERE relation IN ({placeholders})"
        params = tuple(sorted(relations))
    rows = db.execute(
        f"SELECT id,parent_id,relation,depth,{position_sql} "
        f"FROM _edges_tree{where} ORDER BY relation,id,parent_id",
        params,
    ).fetchall()
    defects: list[str] = []
    occurrences: dict[str, tuple] = {}
    positions: set[tuple[str | None, int]] = set()
    for row in rows:
        occurrence, parent, relation, depth, position = row
        if occurrence in occurrences:
            defects.append(f"duplicate occurrence {occurrence}")
            continue
        occurrences[occurrence] = row
        if not occurrence or not str(occurrence).strip():
            defects.append("blank occurrence id")
        if known_nodes is not None and occurrence not in known_nodes:
            defects.append(f"unknown occurrence {occurrence}")
        if parent == occurrence:
            defects.append(f"self-parent {occurrence}")
        if not relation or not str(relation).strip():
            defects.append(f"blank relation for {occurrence}")
        if depth is not None and (type(depth) is not int or depth < 0):
            defects.append(f"invalid depth for {occurrence}")
        if position is not None:
            if type(position) is not int or position < 0:
                defects.append(f"invalid position for {occurrence}")
            else:
                key = (parent, position)
                if validate_order and key in positions:
                    defects.append(f"duplicate sibling position {parent}:{position}")
                positions.add(key)

    for occurrence, row in occurrences.items():
        parent, depth = row[1], row[3]
        if parent is None:
            if validate_depth and depth not in (None, 0):
                defects.append(f"root depth is not zero for {occurrence}")
            continue
        if parent not in occurrences:
            if known_nodes is not None and parent not in known_nodes:
                defects.append(f"missing parent {parent} for {occurrence}")
            continue
        parent_depth = occurrences[parent][3]
        if (validate_depth and depth is not None and parent_depth is not None
                and depth != parent_depth + 1):
            defects.append(f"invalid depth for {occurrence}")

    for start in occurrences:
        seen: set[str] = set()
        current: str | None = start
        while current in occurrences:
            if current in seen:
                defects.append(f"cycle at {current}")
                break
            seen.add(current)
            current = occurrences[current][1]

    if defects:
        raise ValueError("Tree projection validation failed: " + "; ".join(defects))
