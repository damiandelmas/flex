"""
Flexsearch Core — cell loading, SQL execution, view generation.

Functions:
- open_cell()        → load .db, return conn
- run_sql()          → execute SQL, return list[dict]
- get_meta/set_meta  → _meta table access
- regenerate_views() → discover tables from sqlite_master, read config
                       from _meta, emit CREATE VIEW for chunk-level and
                       source-level views with domain vocabulary renames.

View generator rules:
- Two view levels: chunk (base=_raw_chunks) and source (base=_raw_sources)
- _edges_source is always the bridge between chunks and sources
- Only tables with PK on FK (chunk_id or source_id) join views (1:1 rule)
- _types_* tables discovered alongside _edges_* and _enrich_*
- Column renames from _meta: view:{name}:rename:{col} -> domain
- View level from _meta: view:{name}:level -> chunk|source (default: chunk)
"""

import sqlite3
from typing import Optional


def open_cell(db_path: str) -> sqlite3.Connection:
    """Open a cell database with optimized settings."""
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-20000")
    db.execute("PRAGMA temp_store=MEMORY")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def run_sql(db: sqlite3.Connection, query: str,
            params: tuple = ()) -> list[dict]:
    """Execute SQL, return list of dicts."""
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_meta(db: sqlite3.Connection, key: str) -> Optional[str]:
    """Read a single value from _meta table."""
    try:
        row = db.execute(
            "SELECT value FROM _meta WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def set_meta(db: sqlite3.Connection, key: str, value: str):
    """Write a key-value pair to _meta table."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)"
    )
    db.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (key, value)
    )
    db.commit()


# ═══════════════════════════════════════════════
# View Generation
# ═══════════════════════════════════════════════

# Skip from view SELECT (binary/internal)
_SKIP_COLS = {'embedding', 'rowid'}
# FK columns (join keys, not data — except source_id in bridge)
_FK_COLS = {'chunk_id', 'source_id'}


def regenerate_views(db: sqlite3.Connection):
    """Discover tables, read config from _meta, emit CREATE VIEW."""
    all_tables = (
        _discover_tables(db, '_edges_%') +
        _discover_tables(db, '_types_%') +
        _discover_tables(db, '_enrich_%')
    )

    renames = _read_renames(db)
    levels = _read_view_levels(db)

    # Collect view names from all _meta keys
    view_names = set(renames.keys()) | set(levels.keys())
    if not view_names:
        view_names = {'chunks'}

    for view_name in view_names:
        level = levels.get(view_name, 'chunk')
        view_renames = renames.get(view_name, {})

        db.execute(f"DROP VIEW IF EXISTS [{view_name}]")

        if level == 'source':
            sql = _build_source_view(view_name, db, all_tables, view_renames)
        else:
            sql = _build_chunk_view(view_name, db, all_tables, view_renames)

        if sql:
            db.execute(sql)

    db.commit()


def _discover_tables(db: sqlite3.Connection, pattern: str) -> list[dict]:
    """Discover tables matching LIKE pattern with column and PK info."""
    tables = []
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
        (pattern,)
    ).fetchall()

    for (table_name,) in rows:
        cols = db.execute(f"PRAGMA table_info([{table_name}])").fetchall()
        col_info = []
        has_chunk_id, has_chunk_id_pk = False, False
        has_source_id, has_source_id_pk = False, False

        for c in cols:
            name, dtype, pk = c[1], c[2], bool(c[5])
            col_info.append({'name': name, 'type': dtype, 'pk': pk})
            if name == 'chunk_id':
                has_chunk_id = True
                if pk:
                    has_chunk_id_pk = True
            if name == 'source_id':
                has_source_id = True
                if pk:
                    has_source_id_pk = True

        tables.append({
            'name': table_name,
            'columns': col_info,
            'has_chunk_id': has_chunk_id,
            'has_chunk_id_pk': has_chunk_id_pk,
            'has_source_id': has_source_id,
            'has_source_id_pk': has_source_id_pk,
        })

    return tables


def _read_renames(db: sqlite3.Connection) -> dict[str, dict[str, str]]:
    """Read view:{name}:rename:{col} -> domain from _meta."""
    renames = {}
    try:
        rows = db.execute(
            "SELECT key, value FROM _meta WHERE key LIKE 'view:%:rename:%'"
        ).fetchall()
        for row in rows:
            parts = row[0].split(':')
            if len(parts) == 4:
                _, view_name, _, raw_col = parts
                renames.setdefault(view_name, {})[raw_col] = row[1]
    except sqlite3.OperationalError:
        pass
    return renames


def _read_view_levels(db: sqlite3.Connection) -> dict[str, str]:
    """Read view:{name}:level -> chunk|source from _meta."""
    levels = {}
    try:
        rows = db.execute(
            "SELECT key, value FROM _meta WHERE key LIKE 'view:%:level'"
        ).fetchall()
        for row in rows:
            parts = row[0].split(':')
            if len(parts) == 3:
                _, view_name, _ = parts
                levels[view_name] = row[1]
    except sqlite3.OperationalError:
        pass
    return levels


def _col_select(alias: str, col_name: str, renames: dict) -> str:
    """Build SELECT column with optional AS rename."""
    domain = renames.get(col_name, col_name)
    if domain == col_name:
        return f"{alias}.[{col_name}]"
    return f"{alias}.[{col_name}] AS [{domain}]"


def _has_table(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _build_chunk_view(view_name: str, db: sqlite3.Connection,
                      all_tables: list[dict],
                      renames: dict) -> Optional[str]:
    """
    Chunk-level view: _raw_chunks base, bridges to sources via _edges_source,
    joins all chunk_id PK tables directly and source_id PK tables via bridge.
    """
    if not _has_table(db, '_raw_chunks'):
        return None

    selects = []
    joins = []

    # 1. Base: _raw_chunks
    for c in db.execute("PRAGMA table_info(_raw_chunks)").fetchall():
        if c[1] not in _SKIP_COLS:
            selects.append(_col_select('r', c[1], renames))

    # 2. Bridge: _edges_source (if exists)
    has_bridge = _has_table(db, '_edges_source')
    if has_bridge:
        joins.append("LEFT JOIN _edges_source s ON r.id = s.chunk_id")
        for c in db.execute("PRAGMA table_info(_edges_source)").fetchall():
            col = c[1]
            if col == 'chunk_id':
                continue  # already the join key
            if col not in _SKIP_COLS:
                selects.append(_col_select('s', col, renames))

    # 3. _raw_sources (through bridge, if both exist)
    has_sources = has_bridge and _has_table(db, '_raw_sources')
    if has_sources:
        joins.append(
            "LEFT JOIN _raw_sources src ON s.source_id = src.source_id"
        )
        for c in db.execute("PRAGMA table_info(_raw_sources)").fetchall():
            col = c[1]
            if col == 'source_id':
                continue  # already included from bridge
            if col not in _SKIP_COLS:
                selects.append(_col_select('src', col, renames))

    # 4. All discovered tables with PK on chunk_id or source_id
    alias_idx = 0
    for table in all_tables:
        if table['name'] == '_edges_source':
            continue

        if table['has_chunk_id_pk']:
            # Direct join on chunk_id
            alias = f"t{alias_idx}"
            alias_idx += 1
            joins.append(
                f"LEFT JOIN [{table['name']}] {alias} "
                f"ON r.id = {alias}.chunk_id"
            )
            for col in table['columns']:
                if col['name'] not in _FK_COLS and col['name'] not in _SKIP_COLS:
                    selects.append(
                        _col_select(alias, col['name'], renames)
                    )

        elif table['has_source_id_pk'] and has_bridge:
            # Source-level table, join through bridge
            alias = f"t{alias_idx}"
            alias_idx += 1
            joins.append(
                f"LEFT JOIN [{table['name']}] {alias} "
                f"ON s.source_id = {alias}.source_id"
            )
            for col in table['columns']:
                if col['name'] not in _FK_COLS and col['name'] not in _SKIP_COLS:
                    selects.append(
                        _col_select(alias, col['name'], renames)
                    )

    select_str = ",\n    ".join(selects)
    join_str = "\n".join(joins)

    return f"""CREATE VIEW [{view_name}] AS
SELECT
    {select_str}
FROM _raw_chunks r
{join_str}"""


def _build_source_view(view_name: str, db: sqlite3.Connection,
                       all_tables: list[dict],
                       renames: dict) -> Optional[str]:
    """
    Source-level view: _raw_sources base, aggregates chunk count
    via _edges_source, joins source_id PK enrichment tables.
    """
    if not _has_table(db, '_raw_sources'):
        return None

    selects = []
    joins = []

    # 1. Base: _raw_sources
    for c in db.execute("PRAGMA table_info(_raw_sources)").fetchall():
        if c[1] not in _SKIP_COLS:
            selects.append(_col_select('src', c[1], renames))

    # 2. _edges_source for chunk count
    has_bridge = _has_table(db, '_edges_source')
    if has_bridge:
        joins.append("JOIN _edges_source s ON src.source_id = s.source_id")
        selects.append("COUNT(DISTINCT s.chunk_id) as chunk_count")

    # 3. Source-level enrichment tables (source_id PK)
    alias_idx = 0
    for table in all_tables:
        if table['name'] == '_edges_source':
            continue
        if table['has_source_id_pk']:
            alias = f"g{alias_idx}"
            alias_idx += 1
            joins.append(
                f"LEFT JOIN [{table['name']}] {alias} "
                f"ON src.source_id = {alias}.source_id"
            )
            for col in table['columns']:
                if col['name'] not in _FK_COLS and col['name'] not in _SKIP_COLS:
                    selects.append(
                        _col_select(alias, col['name'], renames)
                    )

    select_str = ",\n    ".join(selects)
    join_str = "\n".join(joins)
    group_by = "\nGROUP BY src.source_id" if has_bridge else ""

    return f"""CREATE VIEW [{view_name}] AS
SELECT
    {select_str}
FROM _raw_sources src
{join_str}{group_by}"""
