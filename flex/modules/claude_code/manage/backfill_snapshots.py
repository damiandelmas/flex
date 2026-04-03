#!/usr/bin/env python3
"""
One-time backfill: ingest file-history backup content into _raw_content.

Claude Code stores pre-edit file snapshots at ~/.claude/file-history/{session}/{hash}@v{N}.
The worker already stores pointer JSON (_file_snapshot) but not the actual content.
This pass reads the backup files from disk and stores them as _file_backup entries.

Forward path (worker.py) now captures these at sync time. This backfills historical data.

Usage:
    python -m flex.modules.claude_code.manage.backfill_snapshots [--dry-run] [--limit N]
"""

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

from flex.registry import resolve_cell

FILE_HISTORY = Path.home() / ".claude/file-history"


def backfill(conn, dry_run=False, limit=0):
    """Walk ~/.claude/file-history/ and store backup content in _raw_content."""
    cur = conn.cursor()

    # Build set of existing _file_backup hashes to skip duplicates
    print("[backfill_snapshots] Loading existing _file_backup hashes...", file=sys.stderr)
    existing = set()
    for (h,) in cur.execute(
        "SELECT hash FROM _raw_content WHERE tool_name = '_file_backup'"
    ):
        existing.add(h)
    print(f"  {len(existing)} existing _file_backup entries", file=sys.stderr)

    # Also load snapshot pointers to get chunk_id linkage
    # Map: (session_id, backupFileName) -> chunk_id
    print("[backfill_snapshots] Loading _file_snapshot pointers...", file=sys.stderr)
    pointer_map = {}  # (session_id, backup_name) -> (chunk_id, filepath)
    for (chunk_id, content) in cur.execute(
        "SELECT erc.chunk_id, rc.content FROM _edges_raw_content erc "
        "JOIN _raw_content rc ON erc.content_hash = rc.hash "
        "WHERE rc.tool_name = '_file_snapshot'"
    ):
        try:
            snap = json.loads(content)
            backups = snap.get('trackedFileBackups', {})
            # Extract session_id from chunk_id (format: session_id_linenum)
            parts = chunk_id.rsplit('_', 1)
            if len(parts) != 2:
                continue
            session_id = parts[0]
            for filepath, info in backups.items():
                if isinstance(info, dict):
                    bname = info.get('backupFileName', '')
                    if bname:
                        pointer_map[(session_id, bname)] = (chunk_id, filepath)
        except (json.JSONDecodeError, AttributeError):
            continue
    print(f"  {len(pointer_map)} snapshot pointers loaded", file=sys.stderr)

    if not FILE_HISTORY.exists():
        print("[backfill_snapshots] No file-history directory found", file=sys.stderr)
        return {'ingested': 0, 'skipped': 0, 'errors': 0, 'linked': 0}

    session_dirs = sorted(FILE_HISTORY.iterdir())
    ingested = 0
    skipped = 0
    errors = 0
    linked = 0
    total_bytes = 0

    for session_dir in session_dirs:
        if not session_dir.is_dir():
            continue
        session_id = session_dir.name

        for backup_file in sorted(session_dir.iterdir()):
            if not backup_file.is_file():
                continue

            try:
                content_bytes = backup_file.read_bytes()
                text = content_bytes.decode('utf-8', errors='replace')
                h = hashlib.sha256(text.encode('utf-8')).hexdigest()

                if h in existing:
                    skipped += 1
                    continue

                ts = int(backup_file.stat().st_mtime)

                if not dry_run:
                    cur.execute(
                        "INSERT OR IGNORE INTO _raw_content VALUES (?,?,?,?,?)",
                        (h, text, '_file_backup', len(text), ts)
                    )

                    # Link to snapshot pointer chunk_id if we have one
                    key = (session_id, backup_file.name)
                    if key in pointer_map:
                        chunk_id, _filepath = pointer_map[key]
                        cur.execute(
                            "INSERT OR IGNORE INTO _edges_raw_content VALUES (?,?)",
                            (chunk_id, h)
                        )
                        linked += 1

                existing.add(h)
                ingested += 1
                total_bytes += len(text)

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error reading {backup_file}: {e}", file=sys.stderr)

            if limit and (ingested + skipped) >= limit:
                break

        if not dry_run and ingested % 1000 == 0 and ingested > 0:
            conn.commit()
            print(f"  {ingested} ingested, {skipped} skipped, "
                  f"{total_bytes / 1024 / 1024:.1f}MB", file=sys.stderr)

        if limit and (ingested + skipped) >= limit:
            break

    if not dry_run:
        conn.commit()

    print(f"\n[backfill_snapshots] Done:", file=sys.stderr)
    print(f"  Ingested: {ingested} ({total_bytes / 1024 / 1024:.1f}MB)", file=sys.stderr)
    print(f"  Linked to pointers: {linked}", file=sys.stderr)
    print(f"  Skipped (already exists): {skipped}", file=sys.stderr)
    print(f"  Errors: {errors}", file=sys.stderr)

    return {'ingested': ingested, 'skipped': skipped,
            'errors': errors, 'linked': linked}


def main():
    parser = __import__('argparse').ArgumentParser(
        description="Backfill file-history snapshots into _raw_content")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[backfill_snapshots] FATAL: claude_code cell not found", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    backfill(conn, dry_run=args.dry_run, limit=args.limit)
    conn.close()


if __name__ == "__main__":
    main()
