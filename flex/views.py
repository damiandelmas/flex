"""Self-describing view generation from sqlite_master."""

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
    # Health defects are part of the self-describing cell substrate. Keeping
    # this table at the schema-healing boundary lets old cells adopt newer
    # validators without making read-only orientation presets conditional.
    db.execute("""
        CREATE TABLE IF NOT EXISTS _health_defects (
            kind TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            observed_value TEXT,
            detail TEXT,
            PRIMARY KEY (kind, subject_id)
        )
    """)
    # _edges_tree is a base SDK edge, not a docpac extension. Older fixtures
    # and pre-SDK cells may lack it; an empty table means a flat hierarchy and
    # keeps depth-aware views valid without inventing rows.
    db.executescript("""
        CREATE TABLE IF NOT EXISTS _edges_tree (
            id TEXT NOT NULL,
            parent_id TEXT,
            branch_at TEXT,
            relation TEXT NOT NULL,
            depth INTEGER DEFAULT 0,
            position INTEGER,
            PRIMARY KEY (id, parent_id)
        );
        CREATE INDEX IF NOT EXISTS idx_tree_parent ON _edges_tree(parent_id);
        CREATE INDEX IF NOT EXISTS idx_tree_relation ON _edges_tree(relation);
    """)
    tree_columns = {
        row[1] for row in db.execute("PRAGMA table_info(_edges_tree)")
    }
    if "position" not in tree_columns:
        db.execute("ALTER TABLE _edges_tree ADD COLUMN position INTEGER")

    all_tables = (
        _discover_tables(db, '_edges_%') +
        _discover_tables(db, '_types_%') +
        _discover_tables(db, '_enrich_%')
    )

    if views is None:
        views = _detect_existing_views(db)
    if not views:
        views = {'chunks': 'chunk', 'sources': 'source'}

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

    _install_acp_views(db)
    db.commit()


def _detect_existing_views(db: sqlite3.Connection) -> dict:
    """Detect existing view names and levels from sqlite_master."""
    views = {}
    rows = db.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='view'"
    ).fetchall()
    for name, sql in rows:
        # Internal compatibility and contract views are not provider
        # projections. In particular, rebuilding canonical `_meta` as a
        # chunk view destroys the writable alias over `_metadata`.
        if name.startswith("_"):
            continue
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


def _has_column(db: sqlite3.Connection, table: str, column: str) -> bool:
    if not _has_table(db, table):
        return False
    return any(row[1] == column for row in db.execute(f"PRAGMA table_info([{table}])"))


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _provider_for_acp(db: sqlite3.Connection) -> str:
    # The cell's own self-description is authoritative when present.
    try:
        row = db.execute(
            "SELECT value FROM _meta WHERE key = 'cell_type'"
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except sqlite3.Error:
        pass
    # Fallback: provider-specific sidecars before the shared substrate table.
    # Non-Claude cells built on the claude_code substrate all carry
    # _types_message, so the generic table must be checked last.
    providers = [
        ("codex", "_types_codex_turn"),
        ("opencode", "_types_opencode_session"),
        ("goose", "_types_goose_session"),
        ("aider", "_types_aider_session"),
        ("claude_code", "_types_message"),
    ]
    for provider, table in providers:
        if _has_table(db, table):
            return provider
    return "coding_agent"


def _is_coding_agent_cell(db: sqlite3.Connection) -> bool:
    return any(
        _has_table(db, table)
        for table in (
            "_types_message",
            "_edges_tool_ops",
            "_types_codex_turn",
            "_types_opencode_session",
            "_types_goose_session",
            "_types_aider_session",
        )
    )


def _optional_col(db: sqlite3.Connection, alias: str, table: str, column: str,
                  fallback: str = "NULL") -> str:
    if _has_column(db, table, column):
        return f"{alias}.[{column}]"
    return fallback


def _install_acp_views(db: sqlite3.Connection) -> None:
    """Install ACP-shaped compatibility views for coding-agent cells.

    Native/provider tables remain the fidelity source. These views give coding
    agent cells a common recall surface with drillback pointers into native rows.
    """
    for name in ("acp_category_coverage", "acp_events", "acp_sessions"):
        db.execute(f"DROP VIEW IF EXISTS [{name}]")

    if not (
        _is_coding_agent_cell(db)
        and _has_table(db, "_raw_sources")
        and _has_table(db, "_raw_chunks")
        and _has_table(db, "_edges_source")
    ):
        return

    provider = _sql_literal(_provider_for_acp(db))
    native_cell = provider
    source_path = _optional_col(db, "src", "_raw_sources", "source_path")
    source_position = _optional_col(db, "s", "_edges_source", "position")
    chunk_timestamp = _optional_col(db, "r", "_raw_chunks", "timestamp")
    message_role = _optional_col(db, "m", "_types_message", "role")
    message_type = _optional_col(db, "m", "_types_message", "type")
    tool_name = _optional_col(db, "t", "_edges_tool_ops", "tool_name")
    target_file = _optional_col(db, "t", "_edges_tool_ops", "target_file")

    message_join = (
        "LEFT JOIN _types_message m ON r.id = m.chunk_id"
        if _has_table(db, "_types_message")
        else ""
    )
    tool_join = (
        "LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id"
        if _has_table(db, "_edges_tool_ops")
        else ""
    )

    db.execute(f"""
CREATE VIEW acp_sessions AS
SELECT
    {provider} AS provider,
    {native_cell} AS native_cell,
    src.source_id AS native_session_id,
    src.source_id AS native_record_id,
    src.source_id AS source_id,
    NULL AS chunk_id,
    {source_path} AS source_path,
    NULL AS source_offset_or_rowid,
    NULL AS raw_payload_ref,
    'direct' AS fidelity_status,
    0 AS inferred,
    'session' AS event_type,
    NULL AS event_role,
    COALESCE(src.title, src.summary, src.source_id) AS event_text,
    NULL AS tool_name,
    NULL AS target_file,
    MIN(r.timestamp) AS timestamp
FROM _raw_sources src
LEFT JOIN _edges_source s ON src.source_id = s.source_id
LEFT JOIN _raw_chunks r ON s.chunk_id = r.id
GROUP BY src.source_id
""")

    db.execute(f"""
CREATE VIEW acp_events AS
SELECT
    {provider} AS provider,
    {native_cell} AS native_cell,
    s.source_id AS native_session_id,
    r.id AS native_record_id,
    s.source_id AS source_id,
    r.id AS chunk_id,
    {source_path} AS source_path,
    {source_position} AS source_offset_or_rowid,
    NULL AS raw_payload_ref,
    CASE
        WHEN t.chunk_id IS NOT NULL OR m.chunk_id IS NOT NULL THEN 'direct'
        ELSE 'inferred'
    END AS fidelity_status,
    CASE
        WHEN t.chunk_id IS NOT NULL OR m.chunk_id IS NOT NULL THEN 0
        ELSE 1
    END AS inferred,
    CASE
        WHEN {tool_name} IS NOT NULL AND {tool_name} IN ('Bash', 'shell', 'local_shell')
            THEN 'terminal_create'
        WHEN {tool_name} IS NOT NULL THEN 'tool_call'
        WHEN {message_type} IS NOT NULL THEN {message_type}
        ELSE 'message'
    END AS event_type,
    {message_role} AS event_role,
    r.content AS event_text,
    {tool_name} AS tool_name,
    {target_file} AS target_file,
    {chunk_timestamp} AS timestamp
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
{message_join}
{tool_join}
""")

    db.execute("""
CREATE VIEW acp_category_coverage AS
SELECT
    event_type AS category,
    fidelity_status,
    inferred,
    COUNT(*) AS count
FROM acp_events
GROUP BY event_type, fidelity_status, inferred
""")


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


def _ensure_chunk_rollup_fresh(db: sqlite3.Connection) -> None:
    """Self-heal _enrich_chunk_rollup before a (re)installed view can read it.

    The claude_code/coding-agent chunks/messages views PK-probe
    _enrich_chunk_rollup instead of an inline aggregate subquery (perf fix —
    see flex.modules.claude_code.manage.chunk_rollup). Every known producer
    of that table (rebuild_all.py, enrichment.py, worker.py's daemon cycle,
    ingest-time upserts) keeps it fresh in the paths this module knows
    about — but install_views() is called from many places (install
    pipelines, `flex sync`, manual reinstalls, possibly future callers) and
    is the one choke point every one of them shares before a view goes
    live. Called unconditionally here so no caller can bypass it by
    forgetting to also call rebuild_chunk_rollup.

    No-op (cheap) for cells that were never claude_code-shaped: creates at
    most one small empty table and returns.

    Deliberately narrow scope — this guards against the OUTAGE case
    (table missing, empty, or on the old pre-type/ext schema — full
    rebuild_chunk_rollup()) plus a one-time PARTIAL-gap heal (individual
    chunks missing a rollup row — heal_missing_rollup_rows()), not
    ongoing staleness. One thing that looks tempting but is NOT done here:

    Comparing rollup row count against edge-table row/chunk_id counts to
    detect a PARTIAL rollup. Tried this — it doesn't work.
    rebuild_chunk_rollup() populates through a join against _raw_chunks,
    which correctly drops edge rows that can't join (orphaned
    _edges_file_identity rows referencing a chunk_id no longer in
    _raw_chunks; _edges_delegations rows with a NULL chunk_id). Real
    production cells have exactly such rows — a raw edge-table count and
    the rollup's count can legitimately and permanently differ by a
    handful of harmless orphan rows. Comparing raw counts makes
    install_views() see "stale" on every single call, forever, and pay a
    full rebuild every time instead of the cheap no-op this is
    supposed to be. heal_missing_rollup_rows() sidesteps this entirely by
    checking the actual invariant (LEFT JOIN _raw_chunks -> rollup, WHERE
    NULL) instead of comparing counts — orphans on the rollup side are
    invisible to a LEFT JOIN driven from _raw_chunks, so they can never
    trip it. It's gated behind a _meta flag so it only runs once per cell:
    after the AFTER INSERT trigger on _raw_chunks (see chunk_rollup.py) is
    in place, new gaps can't occur, so there's nothing to re-check.
    Ongoing routine freshness of file_uuids/child_session_id/agent_type
    (which the trigger does NOT populate — only ingest-time upserts and
    the periodic rebuild do) is still owned by
    worker.py::_run_enrichment_cycle (step 9.5), which rebuilds
    unconditionally every ~30 min.

    Schema note: since the type/ext computed-column fix, EVERY row of
    _raw_chunks gets a rollup row (not just edge-bearing ones — type/ext
    must be a real materialized value for every chunk for the index on
    `type` to be usable). So the outage check below is keyed off
    _raw_chunks directly rather than the edge tables — it also covers
    codex-shaped cells, which share this same table/view via
    insert_chunk_atom.
    """
    # Always ensure the table (CURRENT 6-column schema), its type index,
    # and the default-row trigger exist — CREATE VIEW succeeds even if a
    # referenced table doesn't, so without this a view installed ahead of
    # _ensure_core_tables would only fail later, at first SELECT, with a
    # confusing "no such table" instead of self-healing here. Shared with
    # worker.py::_ensure_core_tables so there is exactly one definition of
    # this schema (table + index + trigger) — see chunk_rollup.py.
    from flex.modules.claude_code.manage.chunk_rollup import (
        ensure_rollup_schema, heal_missing_rollup_rows,
    )
    # SHAPE GATE — must come BEFORE ensure_rollup_schema(). Every cell has
    # _raw_chunks, so that is not a discriminator. Only session-shaped cells
    # (claude_code/codex — the ones whose views PK-probe this table) may get
    # the rollup schema. Without this gate, ensure_rollup_schema() creates the
    # table AND its AFTER INSERT trigger on EVERY cell,
    # the rebuild below then fails on the missing _edges_* tables and is
    # swallowed by install_views()'s bare `except OperationalError: pass` — but
    # the trigger survives, so every future ingest into a social cell silently
    # leaks a junk row into a table no view or code path ever reads.
    # (The docstring's old "creates at most one small empty table" stopped
    # being true the moment the schema grew a trigger.)
    is_session_shaped = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_types_message'"
    ).fetchone() is not None
    if not is_session_shaped:
        return  # social/catalog/etc — no rollup view, no rollup schema

    cols_before = {r[1] for r in db.execute(
        "PRAGMA table_info(_enrich_chunk_rollup)").fetchall()}
    needs_migration = bool(cols_before) and not {'type', 'ext'} <= cols_before
    ensure_rollup_schema(db)  # creates table/migrates columns/index/trigger
    db.commit()

    has_raw_chunks = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_raw_chunks'"
    ).fetchone() is not None
    if not has_raw_chunks:
        return  # not a claude_code/codex-shaped cell — nothing to roll up

    has_rows = db.execute("SELECT 1 FROM _raw_chunks LIMIT 1").fetchone() is not None
    if not has_rows:
        return  # empty cell — rollup is correctly empty too

    rollup_empty = db.execute(
        "SELECT 1 FROM _enrich_chunk_rollup LIMIT 1"
    ).fetchone() is None

    if needs_migration or rollup_empty:
        try:
            from flex.modules.claude_code.manage.chunk_rollup import rebuild_chunk_rollup
            rebuild_chunk_rollup(db)
        except ImportError:
            pass
        return

    # Outage cases are handled above. What's left is the PARTIAL-gap case:
    # a handful of chunks with no rollup row at all — pre-trigger history,
    # a bulk backfill that used raw INSERTs before this trigger existed,
    # etc. Heal it with the exact invariant (LEFT JOIN ... WHERE
    # cr.chunk_id IS NULL), never a count comparison — see
    # heal_missing_rollup_rows' docstring for why a count-based version of
    # this check is permanently wrong (orphaned rollup rows for deleted
    # chunks make raw counts diverge forever). Gated on a _meta flag so
    # install_views stays the ~0ms no-op it's supposed to be on every call
    # after the first — the AFTER INSERT trigger (see chunk_rollup.py)
    # means new gaps can't occur going forward, so there's nothing to
    # re-check on subsequent calls.
    db.execute("""CREATE TABLE IF NOT EXISTS _meta (
        key TEXT PRIMARY KEY, value TEXT
    )""")
    already_healed = db.execute(
        "SELECT 1 FROM _meta WHERE key = 'rollup_healed_v1'"
    ).fetchone() is not None
    if already_healed:
        return

    healed = heal_missing_rollup_rows(db)
    db.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES ('rollup_healed_v1', ?)",
        (str(healed),)
    )
    db.commit()


def install_views(
    db: sqlite3.Connection,
    view_dir: Path,
    *,
    prepare_provider_state: bool = True,
    record_operation: bool = True,
):
    """Read .sql files, execute CREATE VIEW, write metadata to _views."""
    db.execute("""CREATE TABLE IF NOT EXISTS _views (
        name TEXT PRIMARY KEY,
        sql TEXT NOT NULL,
        description TEXT,
        created_at INTEGER
    )""")

    # Rebuild the rollup (if stale/empty/missing) BEFORE any view goes live,
    # so there is no window where a freshly (re)installed chunks/messages
    # view reads an empty rollup and silently returns NULL for the whole
    # historical backlog.
    if prepare_provider_state:
        try:
            _ensure_chunk_rollup_fresh(db)
        except sqlite3.OperationalError:
            pass

    # Same self-heal, for the social-module `_enrich_chunk_type` rollup
    # (see flex.manage.chunk_type) — no-op on cells without a supported
    # social-module type sidecar.
    if prepare_provider_state:
        try:
            from flex.manage.chunk_type import ensure_chunk_type_fresh
            ensure_chunk_type_fresh(db)
        except sqlite3.OperationalError:
            pass

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

    if installed and record_operation:
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
