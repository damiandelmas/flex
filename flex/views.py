"""
Flex Views — self-describing view generation from sqlite_master.

Two tiers:
- Auto-generated raw views: mechanical LEFT JOIN, column passthrough, no renames
- Curated views: .sql files installed into _views table, carry domain vocabulary

Rules:
- Two view levels: chunk (base=_raw_chunks) and source (base=_raw_sources)
- _edges_source is always the bridge between chunks and sources
- Only tables with PK on FK (chunk_id or source_id) join views (1:1 rule)
- _types_* tables discovered alongside _edges_* and _enrich_*
- Curated views in _views table take precedence over auto-generated
"""

import re
import sqlite3
import time
from pathlib import Path
from typing import Optional


# Skip from view SELECT (binary/internal)
_SKIP_COLS = {'embedding', 'rowid'}
# FK columns (join keys, not data — except source_id in bridge)
_FK_COLS = {'chunk_id', 'source_id'}


# ═══════════════════════════════════════════════════════════════════════════════
# AUTO-GENERATED VIEWS (raw column passthrough)
# ═══════════════════════════════════════════════════════════════════════════════

def regenerate_views(db: sqlite3.Connection, views: dict = None):
    """Discover tables, emit CREATE VIEW. Raw column passthrough, no renames.

    Args:
        db: Cell connection
        views: Dict of {name: level} where level is 'chunk' or 'source'.
               If None, re-creates existing views by inspecting sqlite_master.
    """
    all_tables = (
        _discover_tables(db, '_edges_%') +
        _discover_tables(db, '_types_%') +
        _discover_tables(db, '_enrich_%')
    )

    if views is None:
        views = _detect_existing_views(db)
    if not views:
        views = {'chunks': 'chunk'}

    # Skip views owned by _views table (curated takes precedence)
    curated = set()
    if _has_table(db, '_views'):
        curated = {r[0] for r in db.execute(
            "SELECT name FROM _views"
        ).fetchall()}

    # Cache PRAGMA results for base tables
    base_cols = {}
    for tbl in ('_raw_chunks', '_raw_sources', '_edges_source'):
        if _has_table(db, tbl):
            base_cols[tbl] = db.execute(f"PRAGMA table_info([{tbl}])").fetchall()

    for view_name, level in views.items():
        if view_name in curated:
            continue  # curated view takes precedence

        db.execute(f"DROP VIEW IF EXISTS [{view_name}]")

        if level == 'source':
            sql = _build_source_view(view_name, db, all_tables, base_cols)
        else:
            sql = _build_chunk_view(view_name, db, all_tables, base_cols)

        if sql:
            db.execute(sql)

    db.commit()


def _detect_existing_views(db: sqlite3.Connection) -> dict:
    """Detect existing view names and levels from sqlite_master."""
    views = {}
    rows = db.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='view'"
    ).fetchall()
    for name, sql in rows:
        if sql and 'FROM _raw_sources' in sql and 'FROM _raw_chunks' not in sql:
            views[name] = 'source'
        else:
            views[name] = 'chunk'
    return views


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


def _col_select(alias: str, col_name: str, seen: set = None) -> Optional[str]:
    """Build SELECT column with dedup. Returns None if duplicate."""
    if seen is not None:
        if col_name in seen:
            return None  # skip duplicate column name
        seen.add(col_name)
    return f"{alias}.[{col_name}]"


def _has_table(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _build_chunk_view(view_name: str, db: sqlite3.Connection,
                      all_tables: list[dict],
                      base_cols: dict = None) -> Optional[str]:
    """
    Chunk-level view: _raw_chunks base, bridges to sources via _edges_source,
    joins all chunk_id PK tables directly and source_id PK tables via bridge.
    Raw column passthrough — no renames.
    """
    base_cols = base_cols or {}

    if '_raw_chunks' not in base_cols:
        return None

    selects = []
    joins = []
    seen = set()  # track emitted column names to skip duplicates

    # 1. Base: _raw_chunks
    for c in base_cols['_raw_chunks']:
        if c[1] not in _SKIP_COLS:
            s = _col_select('r', c[1], seen)
            if s:
                selects.append(s)

    # 2. Bridge: _edges_source (if exists)
    has_bridge = '_edges_source' in base_cols
    if has_bridge:
        joins.append("LEFT JOIN _edges_source s ON r.id = s.chunk_id")
        for c in base_cols['_edges_source']:
            col = c[1]
            if col == 'chunk_id':
                continue  # already the join key
            if col not in _SKIP_COLS:
                s = _col_select('s', col, seen)
                if s:
                    selects.append(s)

    # 3. _raw_sources (through bridge, if both exist)
    has_sources = has_bridge and '_raw_sources' in base_cols
    if has_sources:
        joins.append(
            "LEFT JOIN _raw_sources src ON s.source_id = src.source_id"
        )
        for c in base_cols['_raw_sources']:
            col = c[1]
            if col == 'source_id':
                continue  # already included from bridge
            if col not in _SKIP_COLS:
                s = _col_select('src', col, seen)
                if s:
                    selects.append(s)

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
                    s = _col_select(alias, col['name'], seen)
                    if s:
                        selects.append(s)

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
                    s = _col_select(alias, col['name'], seen)
                    if s:
                        selects.append(s)

    select_str = ",\n    ".join(selects)
    join_str = "\n".join(joins)

    return f"""CREATE VIEW [{view_name}] AS
SELECT
    {select_str}
FROM _raw_chunks r
{join_str}"""


def _build_source_view(view_name: str, db: sqlite3.Connection,
                       all_tables: list[dict],
                       base_cols: dict = None) -> Optional[str]:
    """
    Source-level view: _raw_sources base, aggregates chunk count
    via _edges_source, joins source_id PK enrichment tables.
    Raw column passthrough — no renames.
    """
    base_cols = base_cols or {}

    if '_raw_sources' not in base_cols:
        return None

    selects = []
    joins = []
    seen = set()  # track emitted column names to skip duplicates

    # 1. Base: _raw_sources
    for c in base_cols['_raw_sources']:
        if c[1] not in _SKIP_COLS:
            s = _col_select('src', c[1], seen)
            if s:
                selects.append(s)

    # 2. _edges_source for chunk count
    has_bridge = '_edges_source' in base_cols
    if has_bridge:
        joins.append("LEFT JOIN _edges_source s ON src.source_id = s.source_id")
        selects.append("COUNT(DISTINCT s.chunk_id) as chunk_count")
        seen.add('chunk_count')

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
                    s = _col_select(alias, col['name'], seen)
                    if s:
                        selects.append(s)

    select_str = ",\n    ".join(selects)
    join_str = "\n".join(joins)
    group_by = "\nGROUP BY src.source_id" if has_bridge else ""

    return f"""CREATE VIEW [{view_name}] AS
SELECT
    {select_str}
FROM _raw_sources src
{join_str}{group_by}"""


# ═══════════════════════════════════════════════════════════════════════════════
# CURATED VIEWS (.sql files → _views table)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_view_file(path: Path) -> tuple[str, str, str]:
    """Parse .sql file with @name, @description annotations.

    Returns (name, description, sql) where sql is the full file content.
    """
    content = path.read_text(encoding='utf-8')
    name = None
    description = None

    for line in content.splitlines():
        line = line.strip()
        if not line.startswith('--'):
            break
        text = line.lstrip('-').strip()
        if text.startswith('@name:'):
            name = text[len('@name:'):].strip()
        elif text.startswith('@description:'):
            description = text[len('@description:'):].strip()

    if not name:
        # Fallback: derive from filename
        name = path.stem

    return name, description, content


def install_views(db: sqlite3.Connection, view_dir: Path):
    """Read .sql files, execute CREATE VIEW, write metadata to _views."""
    db.execute("""CREATE TABLE IF NOT EXISTS _views (
        name TEXT PRIMARY KEY,
        sql TEXT NOT NULL,
        description TEXT,
        created_at INTEGER
    )""")

    installed = []
    for sql_file in sorted(view_dir.glob('*.sql')):
        name, desc, sql = parse_view_file(sql_file)
        db.execute(f"DROP VIEW IF EXISTS [{name}]")
        db.executescript(sql)
        db.execute(
            "INSERT OR REPLACE INTO _views (name, sql, description, created_at) "
            "VALUES (?, ?, ?, ?)",
            (name, sql, desc, int(time.time()))
        )
        installed.append(name)

    db.commit()

    if installed:
        from flex.core import log_op
        log_op(db, 'install_views', '_views',
               params={'views': installed, 'source_dir': str(view_dir)},
               rows_affected=len(installed), source='views.py')


def _validate_view(db: sqlite3.Connection, view_name: str,
                   base_table: str = '_raw_chunks') -> bool:
    """Check if view multiplies rows vs base table. Returns True if valid."""
    base = db.execute(f"SELECT COUNT(*) FROM {base_table}").fetchone()[0]
    view = db.execute(f"SELECT COUNT(*) FROM [{view_name}]").fetchone()[0]
    if view > base:
        raise ValueError(
            f"View {view_name} multiplies rows: {view} > {base}"
        )
    return True
