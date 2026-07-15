"""Shared chunk-type enrichment rollup for social modules.

Every social module's ``chunks`` view used to expose its computed type as
``COALESCE(t.<col>, 'chunk') AS type`` over a LEFT JOIN against its
``_types_<module>`` sidecar. SQLite can't index a COALESCE expression, so
``WHERE type='x'`` full-scans ``_raw_chunks`` and left-joins the sidecar
per row, which becomes expensive on large cells.

``_enrich_chunk_type`` holds one row per ``_raw_chunks`` row with the
default already materialized ('chunk' baked in, not COALESCEd at query
time), so the view can select ``ect.type`` as a **plain column** —
that's what makes it indexable. Shared across social modules; each supplies
its own (type_table, type_column).
"""

from flex.core import log_op


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_chunk_type (
    chunk_id TEXT PRIMARY KEY,
    type TEXT
)
"""

CREATE_TYPE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_chunk_type ON _enrich_chunk_type(type)
"""

# Makes a missing _enrich_chunk_type row unrepresentable. The old social
# views read COALESCE(t.<col>, 'chunk') AS type — self-healing: a chunk
# missing its _types_<module> sidecar row still resolved to 'chunk'. The
# new view reads ect.type as a plain (indexable) column, so a chunk with
# no _enrich_chunk_type row resolves to NULL and is silently excluded from
# every WHERE type=... filter — no error, no self-heal. This trigger
# closes that: ANY insert into _raw_chunks (present, future, or a raw
# sqlite3 CLI backfill) gets a default ('chunk') row for free. The real
# ingest-time upsert (upsert_chunk_type, ON CONFLICT DO UPDATE) still wins
# afterward — INSERT OR IGNORE here never fights it. Mirrors
# trg_chunk_rollup_default in claude_code/manage/chunk_rollup.py exactly.
CREATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_chunk_type_default
AFTER INSERT ON _raw_chunks
BEGIN
    INSERT OR IGNORE INTO _enrich_chunk_type(chunk_id, type) VALUES (NEW.id, 'chunk');
END
"""


def ensure_chunk_type_schema(db) -> None:
    """Idempotently bring _enrich_chunk_type to the current schema: table,
    type index, and the default-row trigger. Single source of truth,
    called from both a social module's ingest-time table bootstrap and
    views.py's install-time self-heal (ensure_chunk_type_fresh) — so
    there's exactly one place this schema is defined, matching
    ensure_rollup_schema in chunk_rollup.py.
    """
    db.execute(CREATE_TABLE)
    db.execute(CREATE_TYPE_INDEX)
    db.execute(CREATE_TRIGGER)


def create_type_source_index(db, type_table: str, type_column: str) -> None:
    """Index the source sidecar column too — speeds the rebuild itself and
    the base-table path for anything that still queries ``_types_<mod>``
    directly."""
    idx_name = f"idx_{type_table.lstrip('_')}_{type_column}"
    db.execute(
        f"CREATE INDEX IF NOT EXISTS [{idx_name}] ON [{type_table}]([{type_column}])"
    )


def rebuild_chunk_type(db, type_table: str, type_column: str) -> int:
    """Wipe and rewrite ``_enrich_chunk_type`` from ``_raw_chunks`` LEFT
    JOIN the module's ``_types_<module>`` sidecar, with the 'chunk'
    default already materialized. One row per ``_raw_chunks`` row.
    """
    db.execute(CREATE_TABLE)
    db.execute(CREATE_TRIGGER)
    db.execute("DELETE FROM _enrich_chunk_type")

    db.execute(f"""
        INSERT INTO _enrich_chunk_type (chunk_id, type)
        SELECT r.id, COALESCE(t.[{type_column}], 'chunk')
        FROM _raw_chunks r
        LEFT JOIN [{type_table}] t ON r.id = t.chunk_id
    """)
    db.execute(CREATE_TYPE_INDEX)
    create_type_source_index(db, type_table, type_column)
    db.commit()

    row_count = db.execute("SELECT COUNT(*) FROM _enrich_chunk_type").fetchone()[0]
    log_op(db, 'rebuild_chunk_type', '_enrich_chunk_type',
           params={'type_table': type_table, 'type_column': type_column},
           rows_affected=row_count, source='chunk_type.py')
    return row_count


# Registry of social modules that expose a computed `type` column this way.
# Keyed off the module's `_types_<module>` sidecar table name (which a cell
# carries iff it's shaped like that module) -> the column on it holding the
# per-chunk type. A cell carries at most one of these tables in practice
# (one social module per cell) — arxiv is deliberately excluded (its `type`
# is a LIKE-based CASE, unindexable in principle, not filtered in the wild)
# as are tools/docpac/markdown/github/dm (already clean, no COALESCE `type`).
MODULE_TYPE_SOURCES = {
    '_types_reddit': 'post_type',
    '_types_hn': 'item_type',
}
try:
    from flex.modules.registry_ext import CHUNK_TYPE_SOURCES as _EXT_TYPE_SOURCES
except ImportError:
    _EXT_TYPE_SOURCES = {}
MODULE_TYPE_SOURCES.update(_EXT_TYPE_SOURCES)


def heal_missing_chunk_type_rows(db, batch_size: int = 5000) -> int:
    """Backfill rows for chunks that don't have an _enrich_chunk_type row —
    the exact invariant (LEFT JOIN ... WHERE ect.chunk_id IS NULL), never a
    count comparison. Mirrors heal_missing_rollup_rows in chunk_rollup.py:
    a count-based staleness guard has a permanent fencepost, because
    orphaned _enrich_chunk_type rows (chunk later deleted from
    _raw_chunks — harmless, expected, not an error) make COUNT(enrich) and
    COUNT(chunks) diverge forever, tripping a "stale" full rebuild on
    every single call. This checks the actual invariant instead: does
    every row in _raw_chunks have a matching row in _enrich_chunk_type?
    Orphan rows on the enrich side are invisible to this LEFT JOIN and
    never trip it.

    Returns the number of chunks healed (0 is the common case once the
    AFTER INSERT trigger is in place — this exists for cells created or
    bulk-loaded before the trigger existed).
    """
    healed = 0
    while True:
        missing = db.execute("""
            SELECT r.id FROM _raw_chunks r
            LEFT JOIN _enrich_chunk_type ect ON r.id = ect.chunk_id
            WHERE ect.chunk_id IS NULL
            LIMIT ?
        """, (batch_size,)).fetchall()
        if not missing:
            break
        db.executemany(
            "INSERT OR IGNORE INTO _enrich_chunk_type (chunk_id, type) VALUES (?, 'chunk')",
            [(row[0],) for row in missing]
        )
        db.commit()
        healed += len(missing)
        if len(missing) < batch_size:
            break
    if healed:
        log_op(db, 'heal_missing_chunk_type_rows', '_enrich_chunk_type',
               rows_affected=healed, source='chunk_type.py')
    return healed


def ensure_chunk_type_fresh(db) -> None:
    """Self-heal ``_enrich_chunk_type`` before a (re)installed view can read
    it. Mirrors claude_code's ``_ensure_chunk_rollup_fresh`` gate: an
    OUTAGE case (table missing/empty/index missing) triggers a full
    ``rebuild_chunk_type``, and a one-time PARTIAL-gap case (individual
    chunks with no row at all — pre-trigger history, a bulk backfill that
    used raw INSERTs before this trigger existed) is healed via the
    invariant-based ``heal_missing_chunk_type_rows``, gated behind a
    ``_meta`` flag so it runs once per cell, not every call — after the
    AFTER INSERT trigger is in place, new gaps can't occur, so there's
    nothing to re-check on subsequent calls. Never a row-count staleness
    comparison (see heal_missing_chunk_type_rows for why that's a
    permanent fencepost).

    No-op on cells that don't carry any of ``MODULE_TYPE_SOURCES`` (e.g.
    claude_code, docpac, arxiv) — one cheap ``sqlite_master`` lookup, no
    table created, no rebuild.
    """
    tables = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ({})".format(
            ",".join("?" for _ in MODULE_TYPE_SOURCES)
        ),
        list(MODULE_TYPE_SOURCES),
    ).fetchall()}
    if not tables:
        return  # not a social-shaped cell — nothing to roll up

    # In practice a cell carries exactly one of these; if more than one
    # somehow exists, take the first deterministically rather than guess.
    type_table = sorted(tables)[0]
    type_column = MODULE_TYPE_SOURCES[type_table]

    # Always ensure table/index/trigger exist first — CREATE VIEW succeeds
    # even if a referenced table doesn't, so without this a view installed
    # ahead of the module's schema bootstrap would only fail later, at
    # first SELECT, instead of self-healing here.
    ensure_chunk_type_schema(db)
    db.commit()

    has_raw_chunks = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_raw_chunks'"
    ).fetchone() is not None
    if not has_raw_chunks:
        return

    has_rows = db.execute("SELECT 1 FROM _raw_chunks LIMIT 1").fetchone() is not None
    if not has_rows:
        return  # empty cell — rollup is correctly empty too

    rollup_empty = db.execute(
        "SELECT 1 FROM _enrich_chunk_type LIMIT 1"
    ).fetchone() is None

    if rollup_empty:
        rebuild_chunk_type(db, type_table, type_column)
        return

    # Outage handled above. What's left is the PARTIAL-gap case. Gated on
    # a _meta flag so install_views stays a ~0ms no-op on every call after
    # the first.
    db.execute("""CREATE TABLE IF NOT EXISTS _meta (
        key TEXT PRIMARY KEY, value TEXT
    )""")
    already_healed = db.execute(
        "SELECT 1 FROM _meta WHERE key = 'chunk_type_healed_v1'"
    ).fetchone() is not None
    if already_healed:
        return

    healed = heal_missing_chunk_type_rows(db)
    db.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES ('chunk_type_healed_v1', ?)",
        (str(healed),)
    )
    db.commit()


def upsert_chunk_type(db, chunk_id: str, type_value: str) -> None:
    """Upsert a single row immediately after ingest writes the chunk's
    ``_types_<module>`` row, so a freshly ingested chunk shows the right
    ``type`` in the view without waiting for the next rebuild.

    ``type_value`` is the literal type string the caller just wrote to its
    own sidecar table (e.g. 'post'/'comment') — never NULL; callers that
    have no sidecar row for a chunk simply don't call this (the chunk
    keeps the 'chunk' default from the last rebuild, or gets it on the
    next one).
    """
    try:
        db.execute("""
            INSERT INTO _enrich_chunk_type (chunk_id, type)
            VALUES (?, ?)
            ON CONFLICT(chunk_id) DO UPDATE SET type = excluded.type
        """, (chunk_id, type_value))
    except Exception:
        # Missing-table (transitional cell state) or any other write
        # error here must never break ingest — the next rebuild self-heals.
        pass
