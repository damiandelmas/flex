#!/usr/bin/env python3
"""
SOMA Identity Heal — Backfill missing identity edges.

Port of Thread's backfill-identity.py + backfill-content.py + backfill-url-uuid.py,
retargeted from flat columns to Flex edge tables.

Four passes:
  Pass 1: file_uuid + repo_root (from _edges_tool_ops with file ops)
  Pass 2: content_hash (from _edges_tool_ops with mutations)
  Pass 3: url_uuid (from _edges_tool_ops with WebFetch)
  Pass 4: old_blob_hash (from ~/.claude/file-history/ backup files)

Usage:
  python -m flex.modules.soma.manage.heal                    # run all passes
  python -m flex.modules.soma.manage.heal --dry-run           # report gaps only
  python -m flex.modules.soma.manage.heal --limit 100         # test subset
  python -m flex.modules.soma.manage.heal --pass file         # file_uuid only
  python -m flex.modules.soma.manage.heal --pass content
  python -m flex.modules.soma.manage.heal --pass url
  python -m flex.modules.soma.manage.heal --pass old_blob_hash
"""

import hashlib
import json
import re
import sys
import time
import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path

from flex.registry import resolve_cell
from flex.modules.soma.compile import ensure_tables


# ─────────────────────────────────────────────────────────────────────────────
# Gap queries
# ─────────────────────────────────────────────────────────────────────────────

FILE_GAP_SQL = """
    SELECT t.chunk_id, t.target_file
    FROM _edges_tool_ops t
    LEFT JOIN _edges_file_identity fi ON t.chunk_id = fi.chunk_id
    WHERE fi.chunk_id IS NULL
      AND t.target_file IS NOT NULL
      AND t.target_file NOT LIKE '/tmp/%'
      AND t.target_file NOT LIKE '/var/tmp/%'
      AND t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
"""

REPO_GAP_SQL = """
    SELECT t.chunk_id, t.target_file
    FROM _edges_tool_ops t
    LEFT JOIN _edges_repo_identity ri ON t.chunk_id = ri.chunk_id
    WHERE ri.chunk_id IS NULL
      AND t.target_file IS NOT NULL
      AND t.target_file NOT LIKE '/tmp/%'
      AND t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep','Bash')
"""

CONTENT_GAP_SQL = """
    SELECT t.chunk_id, t.target_file
    FROM _edges_tool_ops t
    LEFT JOIN _edges_content_identity ci ON t.chunk_id = ci.chunk_id
    WHERE ci.chunk_id IS NULL
      AND t.target_file IS NOT NULL
      AND t.tool_name IN ('Write','Edit','MultiEdit')
"""

URL_GAP_SQL = """
    SELECT c.id as chunk_id, c.content
    FROM _raw_chunks c
    JOIN _edges_tool_ops t ON c.id = t.chunk_id
    LEFT JOIN _edges_url_identity ui ON c.id = ui.chunk_id
    WHERE ui.chunk_id IS NULL
      AND t.tool_name = 'WebFetch'
"""

OLD_BLOB_GAP_SQL = """
    SELECT t.chunk_id, t.target_file, es.source_id
    FROM _edges_tool_ops t
    JOIN _edges_source es ON t.chunk_id = es.chunk_id
    LEFT JOIN _edges_content_identity ci ON t.chunk_id = ci.chunk_id
    WHERE t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
      AND t.target_file IS NOT NULL
      AND (ci.old_blob_hash IS NULL OR ci.chunk_id IS NULL)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Passes
# ─────────────────────────────────────────────────────────────────────────────

def _pass_file(conn, dry_run=False, limit=0):
    """Pass 1: file_uuid + repo_root backfill."""
    try:
        from flex.modules.soma.lib.identity.file_identity import FileIdentity
        from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
        file_id = FileIdentity()
        repo_id = RepoIdentity()
    except ImportError:
        print("[heal] SOMA identity not available — skipping file pass", file=sys.stderr)
        return

    # file_uuid
    sql = FILE_GAP_SQL + (f" LIMIT {limit}" if limit else "")
    gaps = conn.execute(sql).fetchall()
    print(f"\n[heal] Pass 1a: file_uuid", file=sys.stderr)
    print(f"  Gaps: {len(gaps)}", file=sys.stderr)

    if dry_run:
        return

    resolved = unresolvable = 0
    for chunk_id, target_file in gaps:
        try:
            file_uuid = file_id.assign(target_file)
            if file_uuid:
                conn.execute(
                    "INSERT OR IGNORE INTO _edges_file_identity (chunk_id, file_uuid) VALUES (?, ?)",
                    (chunk_id, file_uuid)
                )
                resolved += 1
            else:
                unresolvable += 1
        except Exception:
            unresolvable += 1

    conn.commit()
    print(f"  Resolved: {resolved}", file=sys.stderr)
    print(f"  Unresolvable: {unresolvable}", file=sys.stderr)

    # repo_root
    sql = REPO_GAP_SQL + (f" LIMIT {limit}" if limit else "")
    gaps = conn.execute(sql).fetchall()
    print(f"\n[heal] Pass 1b: repo_root", file=sys.stderr)
    print(f"  Gaps: {len(gaps)}", file=sys.stderr)

    if not gaps:
        return

    resolved = unresolvable = 0
    for chunk_id, target_file in gaps:
        try:
            result = repo_id.resolve_file(target_file)
            if result:
                _, repo = result
                if repo.root_commit:
                    conn.execute(
                        "INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked) VALUES (?, ?, 1)",
                        (chunk_id, repo.root_commit)
                    )
                    resolved += 1
                else:
                    unresolvable += 1
            else:
                unresolvable += 1
        except Exception:
            unresolvable += 1

    conn.commit()
    print(f"  Resolved: {resolved}", file=sys.stderr)
    print(f"  Unresolvable: {unresolvable}", file=sys.stderr)


def _pass_content(conn, dry_run=False, limit=0):
    """Pass 2: content_hash backfill."""
    try:
        from flex.modules.soma.lib.identity.content_identity import ContentIdentity
        content_id = ContentIdentity()
    except ImportError:
        print("[heal] SOMA ContentIdentity not available — skipping content pass", file=sys.stderr)
        return

    sql = CONTENT_GAP_SQL + (f" LIMIT {limit}" if limit else "")
    gaps = conn.execute(sql).fetchall()
    print(f"\n[heal] Pass 2: content_hash", file=sys.stderr)
    print(f"  Gaps: {len(gaps)}", file=sys.stderr)

    if dry_run:
        return

    resolved = unresolvable = 0
    for chunk_id, target_file in gaps:
        try:
            path = Path(target_file)
            if not path.is_file():
                unresolvable += 1
                continue
            content_hash = content_id.store(path.read_bytes())
            if content_hash:
                conn.execute(
                    "INSERT OR IGNORE INTO _edges_content_identity (chunk_id, content_hash) VALUES (?, ?)",
                    (chunk_id, content_hash)
                )
                resolved += 1
            else:
                unresolvable += 1
        except Exception:
            unresolvable += 1

    conn.commit()
    print(f"  Resolved: {resolved}", file=sys.stderr)
    print(f"  Unresolvable: {unresolvable} (file no longer exists)", file=sys.stderr)


def _pass_url(conn, dry_run=False, limit=0):
    """Pass 3: url_uuid backfill."""
    try:
        from flex.modules.soma.lib.identity.url_identity import URLIdentity
        url_id = URLIdentity()
    except ImportError:
        print("[heal] SOMA URLIdentity not available — skipping url pass", file=sys.stderr)
        return

    sql = URL_GAP_SQL + (f" LIMIT {limit}" if limit else "")
    gaps = conn.execute(sql).fetchall()
    print(f"\n[heal] Pass 3: url_uuid", file=sys.stderr)
    print(f"  Gaps: {len(gaps)}", file=sys.stderr)

    if dry_run:
        return

    resolved = unresolvable = 0
    for chunk_id, content in gaps:
        try:
            match = re.search(r'https?://[^\s]+', content or '')
            if not match:
                unresolvable += 1
                continue
            url = match.group(0)
            url_uuid = url_id.assign(url)
            if url_uuid:
                conn.execute(
                    "INSERT OR IGNORE INTO _edges_url_identity (chunk_id, url_uuid) VALUES (?, ?)",
                    (chunk_id, url_uuid)
                )
                resolved += 1
            else:
                unresolvable += 1
        except Exception:
            unresolvable += 1

    conn.commit()
    print(f"  Resolved: {resolved}", file=sys.stderr)
    print(f"  Unresolvable: {unresolvable} (no URL in content)", file=sys.stderr)


def _git_blob_hash(content: bytes) -> str:
    """Compute git blob hash (same as git hash-object)."""
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content).hexdigest()


def _build_snapshot_map(jsonl_path: Path, session_id: str) -> dict:
    """Scan JSONL and build {assistant_line_num: {filepath: blob_hash}} from file-history snapshots.

    Returns a dict mapping JSONL line numbers (1-based) of assistant entries
    to {filepath: old_blob_hash} computed from the backup files.
    """
    file_history_dir = Path.home() / '.claude' / 'file-history' / session_id

    # Phase 1: collect messageId → {filepath: blob_hash} from snapshot entries
    msg_hashes = {}  # messageId → {filepath: blob_hash}
    # Phase 2: collect line_num → uuid for assistant entries
    assistant_uuids = {}  # line_num → uuid

    try:
        with open(jsonl_path) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                entry_type = entry.get('type')

                if entry_type == 'file-history-snapshot':
                    snapshot = entry.get('snapshot', {})
                    msg_id = entry.get('messageId') or (
                        snapshot.get('messageId', '') if isinstance(snapshot, dict) else '')
                    backups = snapshot.get('trackedFileBackups', {}) if isinstance(snapshot, dict) else {}
                    if not msg_id or not backups:
                        continue
                    file_hashes = {}
                    for filepath, info in backups.items():
                        if not isinstance(info, dict):
                            continue
                        backup_name = info.get('backupFileName', '')
                        if not backup_name:
                            continue
                        backup_path = file_history_dir / backup_name
                        if backup_path.exists():
                            try:
                                content = backup_path.read_bytes()
                                file_hashes[filepath] = _git_blob_hash(content)
                            except Exception:
                                pass
                    if file_hashes:
                        msg_hashes.setdefault(msg_id, {}).update(file_hashes)

                elif entry_type == 'assistant':
                    uuid = entry.get('uuid', '')
                    if uuid:
                        assistant_uuids[line_num] = uuid
    except Exception:
        return {}

    # Phase 3: map line_num → {filepath: blob_hash} via uuid linkage
    result = {}
    for line_num, uuid in assistant_uuids.items():
        if uuid in msg_hashes:
            result[line_num] = msg_hashes[uuid]

    return result


def _pass_old_blob_hash(conn, dry_run=False, limit=0):
    """Pass 4: old_blob_hash backfill from file-history snapshots."""
    try:
        from flex.modules.claude_code.compile.worker import find_jsonl
    except ImportError:
        print("[heal] worker.find_jsonl not available — skipping old_blob_hash pass", file=sys.stderr)
        return

    sql = OLD_BLOB_GAP_SQL + (f" LIMIT {limit}" if limit else "")
    gaps = conn.execute(sql).fetchall()
    print(f"\n[heal] Pass 4: old_blob_hash", file=sys.stderr)
    print(f"  Gaps: {len(gaps)}", file=sys.stderr)

    if dry_run or not gaps:
        return

    # Group gaps by session
    session_gaps = defaultdict(list)
    for chunk_id, target_file, source_id in gaps:
        # Only new-format chunk_ids: {session}_{line_num}
        # Old formats like {session}_{ts}_{msg} would have parts[0] != source_id
        parts = chunk_id.rsplit('_', 1)
        if len(parts) == 2 and parts[1].isdigit() and parts[0] == source_id:
            session_gaps[source_id].append((chunk_id, target_file, int(parts[1])))

    resolved = unresolvable = skipped_sessions = 0

    for session_id, chunks in session_gaps.items():
        # Check file-history dir exists
        fh_dir = Path.home() / '.claude' / 'file-history' / session_id
        if not fh_dir.is_dir():
            skipped_sessions += 1
            unresolvable += len(chunks)
            continue

        # Find JSONL
        jsonl_path = find_jsonl(session_id)
        if not jsonl_path:
            skipped_sessions += 1
            unresolvable += len(chunks)
            continue

        # Build snapshot map for this session
        snapshot_map = _build_snapshot_map(jsonl_path, session_id)
        if not snapshot_map:
            skipped_sessions += 1
            unresolvable += len(chunks)
            continue

        for chunk_id, target_file, line_num in chunks:
            file_hashes = snapshot_map.get(line_num, {})
            blob_hash = file_hashes.get(target_file)
            if not blob_hash:
                unresolvable += 1
                continue

            # Check if row exists in _edges_content_identity
            existing = conn.execute(
                "SELECT chunk_id FROM _edges_content_identity WHERE chunk_id = ?",
                (chunk_id,)
            ).fetchone()

            if existing:
                conn.execute(
                    "UPDATE _edges_content_identity SET old_blob_hash = ? WHERE chunk_id = ?",
                    (blob_hash, chunk_id)
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO _edges_content_identity "
                    "(chunk_id, content_hash, blob_hash, old_blob_hash) VALUES (?, NULL, NULL, ?)",
                    (chunk_id, blob_hash)
                )
            resolved += 1

        conn.commit()

    print(f"  Resolved: {resolved}", file=sys.stderr)
    print(f"  Unresolvable: {unresolvable}", file=sys.stderr)
    if skipped_sessions:
        print(f"  Skipped sessions: {skipped_sessions} (no file-history or JSONL)", file=sys.stderr)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SOMA identity heal — backfill missing edges")
    parser.add_argument("--dry-run", action="store_true", help="Report gaps only")
    parser.add_argument("--limit", type=int, default=0, help="Limit per pass")
    parser.add_argument("--pass", dest="pass_name", choices=["file", "content", "url", "old_blob_hash"],
                        help="Run specific pass only")
    args = parser.parse_args()

    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[heal] FATAL: claude_code cell not found", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    ensure_tables(conn)

    t0 = time.time()
    print(f"[heal] SOMA Identity Heal — {cell_path}", file=sys.stderr)
    if args.dry_run:
        print("[heal] DRY RUN — no writes", file=sys.stderr)

    passes = {
        'file': _pass_file,
        'content': _pass_content,
        'url': _pass_url,
        'old_blob_hash': _pass_old_blob_hash,
    }

    if args.pass_name:
        passes[args.pass_name](conn, dry_run=args.dry_run, limit=args.limit)
    else:
        for fn in passes.values():
            fn(conn, dry_run=args.dry_run, limit=args.limit)

    elapsed = time.time() - t0
    print(f"\n[heal] Done in {elapsed:.1f}s", file=sys.stderr)
    conn.close()


def heal(conn):
    """Run all heal passes on an open connection. For use by the worker daemon."""
    t0 = time.time()
    print("[heal] SOMA identity heal...", file=sys.stderr)
    for fn in (_pass_file, _pass_content, _pass_url, _pass_old_blob_hash):
        fn(conn, dry_run=False, limit=0)
    print(f"[heal] Done in {time.time() - t0:.1f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
