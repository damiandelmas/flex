"""Chunk-level enrichment rollup — precomputed 1:1 replacement for two
aggregate subqueries that used to live inline in the ``chunks``/``messages``
views.

The curated views need, per chunk: the JSON array of SOMA file UUIDs touched
(``_edges_file_identity`` is 1:many per chunk) and the delegation edge, if
any (``_edges_delegations`` is effectively 1:1 per chunk in practice). Both
used to be computed with a ``GROUP BY chunk_id`` subquery inlined into the
view. SQLite can't push a predicate into an aggregate subquery, so it
materializes the *entire* edge table before returning even one row —
13.6x slower on cells with a large ``_edges_delegations`` table.

``_enrich_chunk_rollup`` holds one row per chunk with the aggregate already
computed, so the view can PK-probe it like every other 1:1 join.

Freshness: this table is only as fresh as the last rebuild. New chunks
ingested between enrichment cycles are kept correct by an upsert at the
ingest write sites in ``flex.modules.claude_code.compile.worker`` — see
``_upsert_chunk_rollup`` there.
"""

from flex.core import log_op


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_chunk_rollup (
    chunk_id TEXT PRIMARY KEY,
    file_uuids TEXT,
    child_session_id TEXT,
    agent_type TEXT,
    type TEXT,
    ext TEXT
)
"""

CREATE_TYPE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_chunk_rollup_type ON _enrich_chunk_rollup(type)
"""

# Makes a missing rollup row unrepresentable: any INSERT into _raw_chunks —
# through the normal ingest path, a future ingest branch that forgets the
# upsert, or a raw `sqlite3` CLI backfill — gets a default ('chunk', all
# other columns NULL) rollup row for free. The real ingest-time upsert
# (_upsert_chunk_rollup in claude_code/compile/worker.py) then runs an
# ON CONFLICT DO UPDATE that overwrites this default with the real
# file_uuids/child_session_id/agent_type/type/ext — INSERT OR IGNORE here
# means it never fights that later write. Requires the 6-column schema
# (chunk_id, file_uuids, child_session_id, agent_type, type, ext) — only
# create this trigger after any old-shape 4-column table has been migrated.
CREATE_ROLLUP_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS trg_chunk_rollup_default
AFTER INSERT ON _raw_chunks
BEGIN
    INSERT OR IGNORE INTO _enrich_chunk_rollup(chunk_id, type) VALUES (NEW.id, 'chunk');
END
"""

# The ``type``/``ext`` CASE expressions below MUST stay byte-identical to
# the ones that used to live inline in chunks.sql (and that ext.sql/tests
# diff against). fb = _types_file_body, tp = _types_message, t =
# _edges_tool_ops. COALESCE(fb.target_file, t.target_file) is the "file"
# column the view derives ext from — file-body sub-chunks win over tool-op
# rows when both somehow join (they never do in practice: a chunk_id is
# either a file-body sub-chunk or a tool-op chunk, never both).
_TYPE_EXPR = """
        CASE
            WHEN fb.chunk_id IS NOT NULL THEN 'file'
            WHEN tp.type IS NOT NULL THEN tp.type
            ELSE 'chunk'
        END"""

_EXT_EXPR = """
        CASE
            WHEN COALESCE(fb.target_file, t.target_file) LIKE '%.%'
            THEN LOWER(SUBSTR(COALESCE(fb.target_file, t.target_file),
                LENGTH(RTRIM(COALESCE(fb.target_file, t.target_file),
                REPLACE(REPLACE(COALESCE(fb.target_file, t.target_file), '/', ''), '.', ''))) + 1))
            ELSE ''
        END"""


def ensure_rollup_schema(db):
    """Idempotently bring _enrich_chunk_rollup to the current schema: the
    6-column shape, its type index, and the default-row trigger.

    Safe to call against a table in any prior state — absent, legacy
    4-column (pre type/ext), or already current. Each step is its own
    guarded statement (not one executescript) specifically so that a
    legacy 4-column table doesn't blow up an unrelated CREATE INDEX before
    the ALTER TABLE below has had a chance to add the missing columns.

    Single source of truth for both callers that need this: the
    claude_code ingest schema bootstrap (worker.py::_ensure_core_tables)
    and the view-install self-heal (views.py::_ensure_chunk_rollup_fresh).
    """
    db.execute(CREATE_TABLE)
    cols = {r[1] for r in db.execute("PRAGMA table_info(_enrich_chunk_rollup)").fetchall()}
    if 'type' not in cols:
        db.execute("ALTER TABLE _enrich_chunk_rollup ADD COLUMN type TEXT")
    if 'ext' not in cols:
        db.execute("ALTER TABLE _enrich_chunk_rollup ADD COLUMN ext TEXT")
    db.execute(CREATE_TYPE_INDEX)
    db.execute(CREATE_ROLLUP_TRIGGER)


def heal_missing_rollup_rows(db, batch_size: int = 5000) -> int:
    """Backfill rollup rows for chunks that don't have one — the exact
    invariant (LEFT JOIN ... WHERE cr.chunk_id IS NULL), never a count
    comparison. A prior count-based staleness guard had a permanent
    fencepost: orphaned _enrich_chunk_rollup rows (rollup rows whose chunk
    was later deleted from _raw_chunks — harmless, expected, NOT an error)
    made COUNT(rollup) and COUNT(chunks) diverge forever, so a count check
    saw "stale" on every single call and paid for a full rebuild every
    daemon cycle. This checks the actual invariant instead: does every row
    in _raw_chunks have a matching row in _enrich_chunk_rollup? Rows on the
    rollup side with no matching chunk (orphans) are invisible to this
    query and never trip it.

    Returns the number of chunks healed (0 is the common case once the
    AFTER INSERT trigger is in place — this exists for cells created or
    bulk-loaded before the trigger existed).
    """
    healed = 0
    while True:
        missing = db.execute("""
            SELECT r.id FROM _raw_chunks r
            LEFT JOIN _enrich_chunk_rollup cr ON r.id = cr.chunk_id
            WHERE cr.chunk_id IS NULL
            LIMIT ?
        """, (batch_size,)).fetchall()
        if not missing:
            break
        db.executemany(
            "INSERT OR IGNORE INTO _enrich_chunk_rollup (chunk_id, type) VALUES (?, 'chunk')",
            [(row[0],) for row in missing]
        )
        db.commit()
        healed += len(missing)
        if len(missing) < batch_size:
            break
    if healed:
        log_op(db, 'heal_missing_rollup_rows', '_enrich_chunk_rollup',
               rows_affected=healed, source='chunk_rollup.py')
    return healed


def rebuild_chunk_rollup(db):
    """Wipe and rewrite ``_enrich_chunk_rollup`` from the edge tables.

    Column semantics:
      - file_uuids: json_group_array(file_uuid) per chunk_id, NULL if none.
      - child_session_id / agent_type: from _edges_delegations, NULL if none.
      - type / ext: computed for EVERY chunk (byte-identical to the CASE
        expressions that used to live inline in the chunks view), so
        ``type`` is a real materialized, indexable column and 'chunk' is a
        genuine value rather than something COALESCEd at query time.

    Every row of _raw_chunks gets a rollup row now (previously only rows
    with a file-identity or delegation edge did) — but file_uuids /
    child_session_id / agent_type stay NULL for edgeless chunks, exactly
    as before. Only type/ext are populated unconditionally.
    """
    db.execute(CREATE_TABLE)
    db.execute(CREATE_ROLLUP_TRIGGER)
    db.execute("DELETE FROM _enrich_chunk_rollup")

    db.execute(f"""
        INSERT INTO _enrich_chunk_rollup
            (chunk_id, file_uuids, child_session_id, agent_type, type, ext)
        SELECT
            r.id,
            fi.file_uuids,
            d.child_session_id,
            d.agent_type,
            {_TYPE_EXPR},
            {_EXT_EXPR}
        FROM _raw_chunks r
        LEFT JOIN (
            SELECT chunk_id, json_group_array(file_uuid) AS file_uuids
            FROM _edges_file_identity
            GROUP BY chunk_id
        ) fi ON r.id = fi.chunk_id
        LEFT JOIN (
            SELECT chunk_id, child_session_id, agent_type
            FROM _edges_delegations
            GROUP BY chunk_id
        ) d ON r.id = d.chunk_id
        LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
        LEFT JOIN _types_message tp ON r.id = tp.chunk_id
        LEFT JOIN _types_file_body fb ON r.id = fb.chunk_id
    """)
    db.execute(CREATE_TYPE_INDEX)
    db.commit()

    row_count = db.execute("SELECT COUNT(*) FROM _enrich_chunk_rollup").fetchone()[0]
    log_op(db, 'rebuild_chunk_rollup', '_enrich_chunk_rollup',
           rows_affected=row_count, source='chunk_rollup.py')
    return row_count
