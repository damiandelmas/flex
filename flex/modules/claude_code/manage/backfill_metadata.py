#!/usr/bin/env python3
"""
One-time backfill: is_sidechain + entry_uuid on existing chunks.

The unified write path captures these fields going forward, but already-indexed
entries (from the old sync path) need a retroactive UPDATE pass.

Usage:
    python -m flex.modules.claude_code.manage.backfill_metadata [--dry-run] [--limit N]
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from flex.registry import resolve_cell

CLAUDE_PROJECTS = Path.home() / ".claude/projects"


def backfill(conn, dry_run=False, limit=0):
    """Walk all JSONLs, UPDATE is_sidechain + entry_uuid.

    Chunk IDs are constructed directly as {session}_{line_num} since all
    sync-path chunks (both old and new) use this format. No chunk_number
    mapping needed — chunk_number != JSONL line_num for old-sync sessions.
    """
    cur = conn.cursor()

    # Build set of known chunk_ids for fast existence checks
    print("[backfill] Loading known chunk_ids...", file=sys.stderr)
    known_chunks = set()
    rows = cur.execute("SELECT chunk_id FROM _types_message").fetchall()
    for (chunk_id,) in rows:
        known_chunks.add(chunk_id)
    print(f"  {len(known_chunks)} chunks in _types_message", file=sys.stderr)

    # Find all JSONL files
    jsonl_files = sorted(CLAUDE_PROJECTS.rglob("*.jsonl"))
    print(f"  {len(jsonl_files)} JSONL files found", file=sys.stderr)

    updated_sc = 0
    updated_uuid = 0
    skipped = 0
    files_processed = 0

    for jsonl_path in jsonl_files:
        if not jsonl_path.exists():
            continue

        session_id = jsonl_path.stem
        try:
            lines = jsonl_path.read_text().splitlines()
        except Exception:
            continue

        session_updates = []

        for line_num, line in enumerate(lines, 1):
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            entry_type = entry.get('type')
            if entry_type not in ('user', 'assistant'):
                continue

            uuid = entry.get('uuid')
            is_sidechain = 1 if entry.get('isSidechain') else 0

            # Construct chunk_id directly — sync always uses {session}_{line_num}
            chunk_id = f"{session_id}_{line_num}"
            if chunk_id not in known_chunks:
                skipped += 1
                continue

            session_updates.append((is_sidechain, uuid, chunk_id))

        if session_updates:
            for is_sc, uuid, chunk_id in session_updates:
                if not dry_run:
                    cur.execute("""
                        UPDATE _types_message
                        SET is_sidechain = ?, entry_uuid = ?
                        WHERE chunk_id = ? AND (is_sidechain IS NULL OR entry_uuid IS NULL)
                    """, (is_sc, uuid, chunk_id))
                    if cur.rowcount > 0:
                        if is_sc:
                            updated_sc += 1
                        updated_uuid += 1
                else:
                    updated_uuid += 1
                    if is_sc:
                        updated_sc += 1

        files_processed += 1
        if files_processed % 500 == 0:
            if not dry_run:
                conn.commit()
            print(f"  {files_processed}/{len(jsonl_files)} files, "
                  f"sc={updated_sc} uuid={updated_uuid}",
                  file=sys.stderr)

        if limit and files_processed >= limit:
            break

    if not dry_run:
        conn.commit()

    print(f"\n[backfill] Done: {files_processed} files processed", file=sys.stderr)
    print(f"  is_sidechain set: {updated_sc}", file=sys.stderr)
    print(f"  entry_uuid set: {updated_uuid}", file=sys.stderr)
    print(f"  skipped (no chunk): {skipped}", file=sys.stderr)

    return {'files': files_processed, 'sidechain': updated_sc,
            'uuid': updated_uuid, 'skipped': skipped}


def main():
    parser = argparse.ArgumentParser(description="Backfill is_sidechain, entry_uuid")
    parser.add_argument("--dry-run", action="store_true", help="Don't write anything")
    parser.add_argument("--limit", type=int, default=0, help="Limit files processed")
    args = parser.parse_args()

    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[backfill] FATAL: claude_code cell not found", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    backfill(conn, dry_run=args.dry_run, limit=args.limit)
    conn.close()


if __name__ == "__main__":
    main()
