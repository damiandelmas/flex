#!/usr/bin/env python3
"""
Backfill: generate file body sub-chunks from Read/Edit tool results.

Prior to this patch, only Write tool inputs were sub-chunked via
_ingest_file_body(). Read/Edit tool results (the actual file content
returned by Claude Code) were never captured for sub-chunking.

This backfill re-reads JSONL files, extracts tool_result content for
Read/Edit operations, strips line number prefixes (Read), and feeds
them through the file body chunker pipeline (tree-sitter/AST/markdown)
to produce :fb: sub-chunks.

Optimized: bulk extract → batch insert → single commit. Skips
_ingest_file_body() overhead (per-file DELETE + LIKE scan) by doing
content-hash dedup in-memory and batch-inserting directly.

Usage:
    python -m flex.modules.claude_code.manage.backfill_file_bodies [--dry-run] [--limit N]
"""

import hashlib
import json
import re
import sys
import time
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from flex.registry import resolve_cell
from flex.core import open_cell, log_op

_LINE_NUM_RE = re.compile(r'^\s*\d+\t', re.MULTILINE)

_TARGET_FILE_KEYS = {
    'Read': 'file_path', 'Edit': 'file_path',
}


def _strip_line_numbers(text: str) -> str:
    """Strip cat -n line number prefixes from Read tool output."""
    if not text or '\t' not in text[:50]:
        return text
    return _LINE_NUM_RE.sub('', text)


def _normalize_tool_result(content) -> str | None:
    """Normalize tool_result content to string."""
    if isinstance(content, str):
        return content if content else None
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get('type') == 'text':
                parts.append(item.get('text', ''))
            elif isinstance(item, str):
                parts.append(item)
        return '\n'.join(parts) if parts else None
    return None


def _extract_from_jsonl(args):
    """Extract Read/Edit tool results from a single JSONL file. (Process-safe.)"""
    session_id, jsonl_path = args
    try:
        with open(jsonl_path, 'r') as f:
            lines = f.readlines()
    except Exception:
        return []

    tool_use_map = {}  # tool_use.id → (tool_name, target_file, chunk_id)
    results = []       # (chunk_id, session_id, tool_name, target_file, raw, ts)

    for line_num, line in enumerate(lines, 1):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        entry_type = entry.get('type')
        if entry_type not in ('user', 'assistant'):
            continue

        chunk_id = f"{session_id}_{line_num}"
        message = entry.get('message', {})
        content = message.get('content', [])

        ts_int = 0
        timestamp = entry.get('timestamp')
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                ts_int = int(dt.timestamp())
            except Exception:
                pass

        if not isinstance(content, list):
            continue

        for item in content:
            if not isinstance(item, dict):
                continue
            item_type = item.get('type')

            if item_type == 'tool_use':
                tool_name = item.get('name', '')
                if tool_name not in ('Read', 'Edit'):
                    continue
                tool_input = item.get('input', {})
                tool_use_id = item.get('id', '')
                target_key = _TARGET_FILE_KEYS.get(tool_name)
                target_file = tool_input.get(target_key) if target_key else None
                if tool_use_id and target_file:
                    tool_use_map[tool_use_id] = (tool_name, target_file, chunk_id)

            elif item_type == 'tool_result':
                tool_use_id = item.get('tool_use_id', '')
                if tool_use_id not in tool_use_map:
                    continue
                tool_name, target_file, use_chunk_id = tool_use_map[tool_use_id]
                raw = _normalize_tool_result(item.get('content'))
                if raw and len(raw) > 50:
                    results.append((chunk_id, session_id, tool_name, target_file, raw, ts_int))

    return results


def backfill(conn: sqlite3.Connection, dry_run: bool = False, limit: int = 0):
    """Parallel extract from JSONLs, then batch insert sub-chunks."""
    from flex.modules.claude_code.compile.worker import find_jsonl
    from flex.compile.chunkers import chunk_file_body, MIN_BODY_SIZE, MAX_BODY_SIZE

    cur = conn.cursor()

    # Get all sessions + resolve JSONL paths
    sessions = cur.execute("SELECT source_id FROM _raw_sources").fetchall()
    print(f"[backfill] {len(sessions)} sessions", file=sys.stderr)

    # Build (session_id, jsonl_path) pairs
    jobs = []
    for (session_id,) in sessions:
        jsonl_path = find_jsonl(session_id)
        if jsonl_path and jsonl_path.exists():
            jobs.append((session_id, str(jsonl_path)))
    print(f"[backfill] {len(jobs)} JSONLs found on disk", file=sys.stderr)

    # --- Phase 1: Parallel extraction from JSONL files ---
    t0 = time.time()
    all_results = []
    with ProcessPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_extract_from_jsonl, job): job[0] for job in jobs}
        done = 0
        for future in as_completed(futures):
            items = future.result()
            all_results.extend(items)
            done += 1
            if done % 500 == 0:
                print(f"  Extracted {done}/{len(jobs)} sessions, "
                      f"{len(all_results)} results so far", file=sys.stderr)

    t1 = time.time()
    print(f"[backfill] Phase 1: {len(all_results)} tool results extracted "
          f"from {len(jobs)} sessions in {t1-t0:.1f}s", file=sys.stderr)

    if not all_results:
        return 0

    if limit:
        all_results = all_results[:limit]

    if dry_run:
        for chunk_id, session_id, tool_name, target_file, raw, ts in all_results[:20]:
            print(f"  [dry-run] {session_id[:8]}… {tool_name} → {target_file} "
                  f"({len(raw)} chars)", file=sys.stderr)
        print(f"  ... and {len(all_results) - 20} more", file=sys.stderr)
        return 0

    # --- Phase 2: Load existing fb: index for dedup ---
    existing_fb = set()
    for (tf, ch) in cur.execute("SELECT target_file, content_hash FROM _file_body_index"):
        existing_fb.add((tf, ch))
    print(f"[backfill] {len(existing_fb)} existing file body entries", file=sys.stderr)

    # --- Phase 3: Chunk and batch insert ---
    raw_content_rows = []   # (hash, content, tool_name, byte_len, ts)
    raw_content_edges = []  # (chunk_id, hash)
    fb_chunk_rows = []      # (id, content, ts)
    fb_source_rows = []     # (chunk_id, source_id, position)
    fb_type_rows = []       # (chunk_id, target_file, title, position)
    fb_index_rows = []      # (target_file, content_hash, parent_chunk_id, count, ts)

    skipped_dedup = 0
    skipped_size = 0
    chunked = 0

    for chunk_id, session_id, tool_name, target_file, raw, ts in all_results:
        # Store raw content (tool result) — SHA-256 dedup
        raw_clean = raw.encode('utf-8', errors='surrogatepass').decode('utf-8', errors='replace')
        h = hashlib.sha256(raw_clean.encode('utf-8')).hexdigest()
        raw_content_rows.append((h, raw_clean, tool_name, len(raw_clean), ts))
        raw_content_edges.append((chunk_id, h))

        # Strip line numbers for Read
        clean = _strip_line_numbers(raw_clean) if tool_name == 'Read' else raw_clean

        # Size gate
        if len(clean) < MIN_BODY_SIZE or len(clean) > MAX_BODY_SIZE:
            skipped_size += 1
            continue

        # Content-hash dedup
        content_hash = hashlib.sha256(clean.encode('utf-8')).hexdigest()[:16]
        if (target_file, content_hash) in existing_fb:
            skipped_dedup += 1
            continue
        existing_fb.add((target_file, content_hash))

        # Chunk
        try:
            chunks = chunk_file_body(clean, target_file)
        except Exception:
            continue

        for chunk in chunks:
            fb_id = f"{chunk_id}:fb:{chunk['position']}"
            fb_chunk_rows.append((fb_id, chunk['content'], ts))
            fb_source_rows.append((fb_id, session_id, chunk['position']))
            fb_type_rows.append((fb_id, target_file, chunk['title'], chunk['position']))

        fb_index_rows.append((target_file, content_hash, chunk_id, len(chunks), ts))
        chunked += 1

    t2 = time.time()
    print(f"[backfill] Phase 2: {chunked} files chunked, {len(fb_chunk_rows)} sub-chunks, "
          f"{skipped_dedup} deduped, {skipped_size} skipped (size) in {t2-t1:.1f}s",
          file=sys.stderr)

    # --- Phase 4: Batch insert ---
    cur.executemany("INSERT OR IGNORE INTO _raw_content VALUES (?,?,?,?,?)", raw_content_rows)
    cur.executemany("INSERT OR IGNORE INTO _edges_raw_content VALUES (?,?)", raw_content_edges)
    cur.executemany(
        "INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp) VALUES (?,?,NULL,?)",
        fb_chunk_rows)
    cur.executemany(
        "INSERT OR IGNORE INTO _edges_source (chunk_id, source_id, source_type, position) "
        "VALUES (?,?,'file-body',?)",
        fb_source_rows)
    cur.executemany("INSERT OR IGNORE INTO _types_file_body VALUES (?,?,?,?)", fb_type_rows)
    cur.executemany("INSERT OR REPLACE INTO _file_body_index VALUES (?,?,?,?,?)", fb_index_rows)

    conn.commit()

    t3 = time.time()
    log_op(conn, 'backfill_file_bodies', '_raw_chunks',
           params={'results': len(all_results), 'chunked': chunked,
                   'deduped': skipped_dedup, 'size_skipped': skipped_size},
           rows_affected=len(fb_chunk_rows), source='backfill_file_bodies.py')

    print(f"\n[backfill] Done in {t3-t0:.1f}s:", file=sys.stderr)
    print(f"  Tool results: {len(all_results)}", file=sys.stderr)
    print(f"  Raw content stored: {len(raw_content_rows)}", file=sys.stderr)
    print(f"  Files chunked: {chunked}", file=sys.stderr)
    print(f"  Sub-chunks inserted: {len(fb_chunk_rows)}", file=sys.stderr)
    print(f"  Skipped (dedup): {skipped_dedup}", file=sys.stderr)
    print(f"  Skipped (size): {skipped_size}", file=sys.stderr)
    return len(fb_chunk_rows)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Backfill file body sub-chunks from Read/Edit tool results')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--limit', type=int, default=0, help='Max tool results to process (0=all)')
    args = parser.parse_args()

    db_path = resolve_cell('claude_code')
    db = open_cell(str(db_path))
    print(f"Opened: {db_path}", file=sys.stderr)

    t0 = time.time()
    backfill(db, dry_run=args.dry_run, limit=args.limit)
    elapsed = time.time() - t0
    print(f"Total elapsed: {elapsed:.1f}s", file=sys.stderr)

    db.close()


if __name__ == '__main__':
    main()
