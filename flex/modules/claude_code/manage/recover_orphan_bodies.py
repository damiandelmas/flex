#!/usr/bin/env python3
"""
Recovery: synthesize the missing `_raw_chunks` row for orphaned tool bodies.

## Cause (already fixed at ingest, see worker.py _store_content_raw callers)

Before the fix, `tool_result` content (and file-history-snapshot backup
content) was anchored to the *replying* line's chunk_id — a 'user' JSONL
line whose content is ONLY tool_result items. That line never gets a
`_raw_chunks` row: chunk materialization only happens for lines carrying
text/thinking/tool_use. The content landed safely in `_raw_content` and
`_edges_raw_content` (both keyed by that never-created chunk_id), but the
chunk itself — the thing every view (`messages`, `chunks`, `files`,
`@file-search`) joins through — was never inserted. Forward ingest now
anchors to the originating tool_use's chunk instead (see the comment at
worker.py:1189), so this defect is closed for new data. This script repairs
the ~1.39M historical edges left pointing at chunk_ids that don't exist.

## What this script does — additive only

For every `_edges_raw_content` row whose `chunk_id` has no matching
`_raw_chunks` row, synthesize:

  - `_raw_chunks(id, content, embedding, timestamp)` — id = the orphan
    chunk_id; content = a short synthetic tool signature (NOT the body —
    the body already lives in `_raw_content` and reaches `messages.file_body`
    through the existing, untouched `_edges_raw_content` row); embedding
    = NULL (picked up by the normal embed pass); timestamp = the earliest
    `_raw_content.first_seen` among that chunk's bodies.
  - `_edges_source(chunk_id, source_id, position)` — chunk_id format is
    always `<session_id>_<line_number>` (verified: 5000/5000 orphan sample
    match `^[0-9a-f-]+_\\d+$`); source_id = session_id, position = line
    number. source_type is left to its column default ('message').
  - `_types_message(chunk_id, type, role, chunk_number)` — type='tool_call'
    (matches the 475,202 existing body-carrying chunks that already use
    this type for tool bodies). role='user' for ordinary tool_result
    replies (that JSONL line is always a 'user' entry); role is left NULL
    for `_file_backup`/`_file_snapshot` groups, whose anchor line can be
    either role and isn't recoverable without re-reading the JSONL.
    parent_uuid/is_sidechain/entry_uuid/branch_id are left NULL/default —
    not recoverable from this table's data.

Every statement is `INSERT OR IGNORE`. Existing rows — in `_raw_chunks`,
`_edges_source`, `_types_message`, `_raw_content`, `_edges_raw_content`, or
anywhere else — are never UPDATEd or DELETEd. A chunk that already exists
(not orphaned) is never touched. Re-running is idempotent.

## Sessions with no `_raw_sources` row (~491 of ~22,500 owning sessions)

`_edges_source.source_id` is not FK-enforced and every view LEFT JOINs
`_raw_sources`, so a synthesized edge pointing at a source_id with no
`_raw_sources` row still surfaces the chunk fine (session_id populates,
project/title/etc. read NULL). Rather than fabricate project/title/summary
for these sessions, this script SKIPS creating a `_raw_sources` row for them
and reports the count — synthesizing plausible-looking metadata we don't
actually have would be a worse failure mode than a few NULL columns.

Usage:
    python -m flex.modules.claude_code.manage.recover_orphan_bodies [--dry-run] [--db-path PATH] [--batch-size N] [--limit N]
"""

import argparse
import re
import sys
import sqlite3
import time

from flex.registry import resolve_cell
from flex.core import open_cell, log_op

_CHUNK_ID_RE = re.compile(r'^(.+)_(\d+)$')

# Tools that aren't real tool_result replies — file-history-snapshot content,
# anchored to whichever line owned the snapshot (role unknown from this table).
_SNAPSHOT_TOOLS = {'_file_backup', '_file_snapshot'}


def _signature_for(tool_names: set) -> str:
    """Short, non-duplicating content signature for a synthesized chunk.

    Matches the existing tool_call convention of "<ToolName> <arg>" where an
    arg is knowable (Read/Edit/Write/Bash/...); falls back to a bare label
    when no arg is recoverable without re-reading JSONL/tool_input, which
    we deliberately don't do here (body must not be duplicated into content).
    """
    if '_file_snapshot' in tool_names:
        return '[file snapshot]'
    if tool_names == {'_file_backup'}:
        return '[file backup]'
    # Ordinary tool_result reply — exactly one tool_name expected.
    name = sorted(tool_names)[0] if tool_names else 'unknown'
    if name == 'unknown':
        return 'unknown tool result'
    return f'{name} result'


def find_orphans(conn: sqlite3.Connection, limit: int = 0):
    """One row per distinct orphaned chunk_id: (chunk_id, tool_names, first_seen)."""
    cur = conn.cursor()
    # Select chunk ids first.  This is deliberately a separate SQL relation: a
    # Python slice after fetching all body rows can still exhaust memory on a
    # large historical cell.  Once a selected chunk gets its _raw_chunks row,
    # it no longer matches this predicate, so successive limited runs advance
    # naturally in chunk_id order without state or mutation beyond recovery.
    selected_ids_sql = """
        SELECT erc.chunk_id
        FROM _edges_raw_content erc
        LEFT JOIN _raw_chunks rc ON rc.id = erc.chunk_id
        WHERE rc.id IS NULL
        GROUP BY erc.chunk_id
        ORDER BY erc.chunk_id
    """
    params = ()
    if limit:
        selected_ids_sql += " LIMIT ?"
        params = (limit,)

    rows = cur.execute(f"""
        SELECT erc.chunk_id, rc2.tool_name, rc2.first_seen
        FROM ({selected_ids_sql}) orphan_ids
        JOIN _edges_raw_content erc ON erc.chunk_id = orphan_ids.chunk_id
        JOIN _raw_content rc2 ON rc2.hash = erc.content_hash
        ORDER BY erc.chunk_id, rc2.tool_name, rc2.first_seen
    """, params).fetchall()

    grouped = {}
    for chunk_id, tool_name, first_seen in rows:
        g = grouped.setdefault(chunk_id, {'tools': set(), 'first_seen': None})
        g['tools'].add(tool_name or 'unknown')
        if first_seen is not None:
            if g['first_seen'] is None or first_seen < g['first_seen']:
                g['first_seen'] = first_seen

    for chunk_id, g in grouped.items():
        yield chunk_id, g['tools'], g['first_seen']


def recover(conn: sqlite3.Connection, dry_run: bool = False,
            batch_size: int = 20000, limit: int = 0):
    cur = conn.cursor()

    print("[recover_orphan_bodies] Scanning for orphaned edges...", file=sys.stderr)
    t0 = time.time()
    orphans = list(find_orphans(conn, limit=limit))
    print(f"[recover_orphan_bodies] {len(orphans)} distinct orphaned chunk_ids "
          f"in {time.time()-t0:.1f}s", file=sys.stderr)

    # Which owning sessions have no _raw_sources row? Report only — never
    # fabricate one.
    existing_sources = {r[0] for r in cur.execute("SELECT source_id FROM _raw_sources")}

    chunk_rows = []   # (id, content, embedding, timestamp)
    source_rows = []  # (chunk_id, source_id, position)
    type_rows = []    # (chunk_id, type, role, chunk_number)
    bad_format = 0
    sessions_seen = set()
    sessions_missing_source = set()

    for chunk_id, tools, first_seen in orphans:
        m = _CHUNK_ID_RE.match(chunk_id)
        if not m:
            bad_format += 1
            continue
        session_id, line_num_s = m.group(1), m.group(2)
        line_num = int(line_num_s)
        sessions_seen.add(session_id)
        if session_id not in existing_sources:
            sessions_missing_source.add(session_id)

        content = _signature_for(tools)
        chunk_rows.append((chunk_id, content, None, first_seen))
        source_rows.append((chunk_id, session_id, line_num))

        role = None if tools & _SNAPSHOT_TOOLS else 'user'
        type_rows.append((chunk_id, 'tool_call', role, line_num))

    print(f"[recover_orphan_bodies] {len(chunk_rows)} chunks to synthesize, "
          f"{bad_format} chunk_ids did not match <session>_<n> (skipped), "
          f"{len(sessions_seen)} owning sessions, "
          f"{len(sessions_missing_source)} of those have no _raw_sources row "
          f"(skipped — not fabricated)", file=sys.stderr)

    if dry_run:
        print("[recover_orphan_bodies] --dry-run: no writes. Sample:", file=sys.stderr)
        for row in chunk_rows[:20]:
            print(f"  {row}", file=sys.stderr)
        return {
            'orphans': len(orphans), 'to_insert': len(chunk_rows),
            'bad_format': bad_format, 'sessions': len(sessions_seen),
            'sessions_missing_source': len(sessions_missing_source),
        }

    inserted_chunks = 0
    inserted_edges = 0
    inserted_types = 0
    n = len(chunk_rows)
    for i in range(0, n, batch_size):
        cb = chunk_rows[i:i + batch_size]
        sb = source_rows[i:i + batch_size]
        tb = type_rows[i:i + batch_size]

        cur.executemany(
            "INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp) "
            "VALUES (?,?,?,?)", cb)
        inserted_chunks += cur.rowcount if cur.rowcount > 0 else 0

        cur.executemany(
            "INSERT OR IGNORE INTO _edges_source (chunk_id, source_id, position) "
            "VALUES (?,?,?)", sb)
        inserted_edges += cur.rowcount if cur.rowcount > 0 else 0

        cur.executemany(
            "INSERT OR IGNORE INTO _types_message (chunk_id, type, role, chunk_number) "
            "VALUES (?,?,?,?)", tb)
        inserted_types += cur.rowcount if cur.rowcount > 0 else 0

        conn.commit()
        done = min(i + batch_size, n)
        print(f"  {done}/{n} chunks committed", file=sys.stderr)

    t1 = time.time()
    actual_inserted = inserted_chunks + inserted_edges + inserted_types
    if actual_inserted:
        log_op(conn, 'recover_orphan_bodies', '_raw_chunks',
               params={'orphans': len(orphans),
                       'chunks_synthesized': inserted_chunks,
                       'edges_synthesized': inserted_edges,
                       'types_synthesized': inserted_types,
                       'planned_chunks': len(chunk_rows),
                       'bad_format': bad_format, 'sessions': len(sessions_seen),
                       'sessions_missing_source': len(sessions_missing_source)},
               rows_affected=actual_inserted, source='recover_orphan_bodies.py')
    conn.commit()

    print(f"\n[recover_orphan_bodies] Done in {t1-t0:.1f}s:", file=sys.stderr)
    print(f"  Orphaned edges (distinct chunk_ids): {len(orphans)}", file=sys.stderr)
    print(f"  Chunks synthesized: {inserted_chunks}", file=sys.stderr)
    print(f"  Source edges synthesized: {inserted_edges}", file=sys.stderr)
    print(f"  Message types synthesized: {inserted_types}", file=sys.stderr)
    print(f"  Bad chunk_id format (skipped): {bad_format}", file=sys.stderr)
    print(f"  Owning sessions: {len(sessions_seen)}", file=sys.stderr)
    print(f"  Sessions missing _raw_sources (edge points at absent source, "
          f"not fabricated): {len(sessions_missing_source)}", file=sys.stderr)

    return {
        'orphans': len(orphans), 'chunks_synthesized': inserted_chunks,
        'edges_synthesized': inserted_edges, 'types_synthesized': inserted_types,
        'bad_format': bad_format, 'sessions': len(sessions_seen),
        'sessions_missing_source': len(sessions_missing_source),
    }


def main():
    parser = argparse.ArgumentParser(
        description='Recover orphaned tool-result bodies by synthesizing their missing chunk rows')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--limit', type=int, default=0, help='Max orphan chunks to process (0=all)')
    parser.add_argument('--batch-size', type=int, default=20000, help='Rows per commit batch')
    parser.add_argument('--db-path', type=str, default=None,
                         help='Override cell path (for testing against a copy). '
                              'Defaults to the registered claude_code cell.')
    args = parser.parse_args()

    if args.db_path:
        db_path = args.db_path
        db = sqlite3.connect(db_path, timeout=60)
        db.execute("PRAGMA busy_timeout=60000")
        db.execute("PRAGMA journal_mode=WAL")
    else:
        resolved = resolve_cell('claude_code')
        if not resolved:
            print("[recover_orphan_bodies] FATAL: claude_code cell not found", file=sys.stderr)
            sys.exit(1)
        db_path = str(resolved)
        db = open_cell(db_path)
        db.execute("PRAGMA busy_timeout=60000")

    print(f"Opened: {db_path}", file=sys.stderr)

    t0 = time.time()
    recover(db, dry_run=args.dry_run, batch_size=args.batch_size, limit=args.limit)
    print(f"Total elapsed: {time.time()-t0:.1f}s", file=sys.stderr)

    db.close()


if __name__ == '__main__':
    main()
