#!/usr/bin/env python3
"""
FlexSearch Worker — Direct capture to chunk-atom cell.

Writes to chunk-atom schema:
  _raw_chunks, _edges_source, _types_message, _edges_tool_ops,
  _edges_file_identity, _edges_repo_identity, _edges_url_identity,
  _edges_delegations, _edges_soft_ops

Queue: SQLite (~/.flex/queue.db) table claude_code_pending
Cell: resolved via flexsearch.registry

Tier 0 (every 2s): Queue → chunk-atom tables + embeddings
"""

import hashlib
import sqlite3
import json
import time
import sys
import struct
from pathlib import Path
from datetime import datetime

from flexsearch.registry import resolve_cell, FLEX_HOME
from flexsearch.onnx.embed import get_model, encode
from flexsearch.modules.claude_code.compile.enrich import enrich_event
from flexsearch.modules.claude_code.compile.soft_detect import detect_file_ops
from flexsearch.modules.claude_code.compile.skip import should_skip_event
from flexsearch.modules.docpac.compile.worker import process_queue as docpac_process_queue

QUEUE_DB = FLEX_HOME / "queue.db"
CLAUDE_PROJECTS = Path.home() / ".claude/projects"

# Global embedder — stays warm
_embedder = None


def get_embedder():
    """Lazy-load ONNX embedding model."""
    global _embedder
    if _embedder is None:
        print("[worker] Loading ONNX embedding model...", file=sys.stderr)
        _embedder = get_model()
        # Warm up
        encode("warmup")
        print("[worker] Model loaded.", file=sys.stderr)
    return _embedder


def serialize_f32(vector) -> bytes:
    return struct.pack(f'{len(vector)}f', *vector)


def find_jsonl(session_id: str) -> Path | None:
    for jsonl in CLAUDE_PROJECTS.rglob(f"{session_id}.jsonl"):
        return jsonl
    return None


def enrich_chunk(event: dict) -> dict:
    """Enrich event using compile/enrich.py — returns identity fields."""
    try:
        enriched = enrich_event(event, allow_slow=False)
        return {
            'file_uuid': enriched.get('file_uuid'),
            'repo_root': enriched.get('repo_root'),
            'blob_hash': enriched.get('blob_hash'),
            'content_hash': enriched.get('content_hash'),
            'is_tracked': enriched.get('is_tracked'),
            'url_uuid': enriched.get('url_uuid'),
        }
    except Exception as e:
        print(f"[worker] Enrichment error: {e}", file=sys.stderr)
        return {}


from flexsearch.utils.git import git_root_from_path as _git_root_from_path
from flexsearch.utils.git import project_from_git_root as _project_from_git_root


def _git_root(cwd: str) -> str | None:
    """Return git show-toplevel for cwd, or None if not a git repo."""
    if not cwd:
        return None
    return _git_root_from_path(cwd)


def ensure_source_exists(conn: sqlite3.Connection, session_id: str, cwd: str = None):
    """Ensure a source (session) exists in _raw_sources."""
    cur = conn.cursor()
    cur.execute("SELECT source_id FROM _raw_sources WHERE source_id = ?", (session_id,))
    if cur.fetchone():
        return

    git_root = _git_root(cwd)
    project = _project_from_git_root(git_root or cwd) if (git_root or cwd) else None

    cur.execute("""
        INSERT INTO _raw_sources
        (source_id, source, project, git_root, start_time, primary_cwd, message_count, episode_count)
        VALUES (?, ?, ?, ?, ?, ?, 0, 0)
    """, (session_id, f"claude_code:{session_id}", project, git_root, int(time.time()), cwd))


def update_source_stats(conn: sqlite3.Connection, session_id: str, chunk: dict):
    """Increment message_count and update title/end_time on source."""
    cur = conn.cursor()

    cur.execute("""
        UPDATE _raw_sources
        SET message_count = message_count + 1,
            end_time = ?
        WHERE source_id = ?
    """, (chunk['timestamp'], session_id))

    # Set title from first meaningful content (only if title is NULL)
    if chunk.get('content') and chunk.get('tool_name') != 'Read':
        cur.execute("""
            UPDATE _raw_sources
            SET title = ?
            WHERE source_id = ? AND title IS NULL
        """, (chunk['content'][:200], session_id))


def _ensure_content_tables(conn: sqlite3.Connection):
    """Create content store tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _raw_content (
            hash TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            tool_name TEXT,
            byte_length INTEGER,
            first_seen INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _edges_raw_content (
            chunk_id TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            PRIMARY KEY (chunk_id, content_hash)
        )
    """)


def _store_raw_content(conn: sqlite3.Connection, chunk_id: str, event: dict,
                       tool: str, ts: int):
    """Store raw content (old_string, new_string, write_content, web_content)."""
    for field in ['old_string', 'new_string', 'write_content', 'web_content']:
        raw = event.get(field)
        if raw and 10 < len(raw) < 500_000:
            h = hashlib.sha256(raw.encode()).hexdigest()
            conn.execute(
                "INSERT OR IGNORE INTO _raw_content VALUES (?,?,?,?,?)",
                (h, raw, tool, len(raw), ts)
            )
            conn.execute(
                "INSERT OR IGNORE INTO _edges_raw_content VALUES (?,?)",
                (chunk_id, h)
            )


def insert_chunk_atom(conn: sqlite3.Connection, chunk: dict):
    """Insert a chunk into all chunk-atom tables."""
    cur = conn.cursor()
    chunk_id = chunk['id']

    # _raw_chunks
    cur.execute("""
        INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
        VALUES (?, ?, ?, ?)
    """, (chunk_id, chunk['content'], chunk.get('embedding'), chunk['timestamp']))

    # _edges_source
    cur.execute("""
        INSERT OR IGNORE INTO _edges_source (chunk_id, source_id, source_type, position)
        VALUES (?, ?, 'claude-code', ?)
    """, (chunk_id, chunk['doc_id'], chunk['chunk_number']))

    # _types_message
    cur.execute("""
        INSERT OR IGNORE INTO _types_message (chunk_id, type, role, chunk_number)
        VALUES (?, ?, ?, ?)
    """, (chunk_id, chunk['type'], chunk['role'], chunk['chunk_number']))

    # _edges_tool_ops (only for tool calls)
    if chunk.get('tool_name'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_tool_ops (chunk_id, tool_name, target_file, success, cwd, git_branch)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chunk_id, chunk['tool_name'], chunk.get('target_file'),
              chunk.get('success'), chunk.get('cwd'), chunk.get('git_branch')))

    # _edges_file_identity (if file_uuid present)
    if chunk.get('file_uuid'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_file_identity (chunk_id, file_uuid)
            VALUES (?, ?)
        """, (chunk_id, chunk['file_uuid']))

    # _edges_repo_identity (if repo_root present)
    if chunk.get('repo_root'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked)
            VALUES (?, ?, ?)
        """, (chunk_id, chunk['repo_root'], chunk.get('is_tracked')))

    # _edges_content_identity (if content_hash present)
    if chunk.get('content_hash'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_content_identity (chunk_id, content_hash, blob_hash)
            VALUES (?, ?, ?)
        """, (chunk_id, chunk['content_hash'], chunk.get('blob_hash')))

    # _edges_url_identity (if url_uuid present)
    if chunk.get('url_uuid'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_url_identity (chunk_id, url_uuid)
            VALUES (?, ?)
        """, (chunk_id, chunk['url_uuid']))

    # _edges_delegations (Task spawns)
    if chunk.get('spawned_agent'):
        ensure_source_exists(conn, chunk['spawned_agent'])
        cur.execute("""
            INSERT OR IGNORE INTO _edges_delegations (chunk_id, child_doc_id, agent_type, created_at)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, chunk['spawned_agent'], chunk['timestamp']))

    # _enrich_types: stopped writing heuristic values (Plan 9).
    # AI queries role + tool_name directly via curated views.
    # Table kept as reserved slot for future semantic classification.

    # _edges_soft_ops (Bash commands)
    if chunk.get('tool_name') == 'Bash' and chunk.get('content'):
        cmd_text = chunk['content']
        if cmd_text.startswith('Bash '):
            cmd_text = cmd_text[5:]
        soft_ops = detect_file_ops(cmd_text, chunk.get('cwd'))
        for op in soft_ops:
            cur.execute("""
                INSERT OR IGNORE INTO _edges_soft_ops
                (chunk_id, file_path, file_uuid, inferred_op, confidence)
                VALUES (?, ?, NULL, ?, ?)
            """, (chunk_id, op.file_path, op.inferred_op, op.confidence))


def process_event(event: dict, conn: sqlite3.Connection) -> dict | None:
    """Process a queue event into a chunk dict."""
    session_id = event.get('session', '')
    tool = event.get('tool', '')
    msg = event.get('msg', 0)
    ts = event.get('ts', int(time.time()))
    cwd = event.get('cwd', '')
    file_path = event.get('file')

    if not session_id or not tool:
        return None

    # Skip noisy events
    if should_skip_event(event):
        return None

    ensure_source_exists(conn, session_id, cwd)

    chunk_id = f"{session_id}_{ts}_{msg}"
    chunk_type = 'tool_call' if tool in ['Read', 'Write', 'Edit', 'Grep', 'Glob', 'Bash',
                                          'WebFetch', 'WebSearch', 'Task', 'MultiEdit'] else 'episode'

    # Build content for embedding
    content_parts = [tool]
    if file_path:
        content_parts.append(file_path)
    if event.get('command'):
        content_parts.append(event['command'][:200])
    if event.get('pattern'):
        content_parts.append(event['pattern'])
    if event.get('query'):
        content_parts.append(event['query'])
    if event.get('url'):
        content_parts.append(event['url'])
    content = ' '.join(content_parts)

    enrichment = enrich_chunk({
        'tool': tool,
        'file': file_path,
        'cwd': cwd,
        'url': event.get('url'),
        'session': session_id,
        'msg': msg,
    })

    spawned_agent = event.get('spawned_agent') if tool == 'Task' else None

    chunk = {
        'id': chunk_id,
        'doc_id': session_id,
        'chunk_number': msg,
        'type': chunk_type,
        'content': content[:2000],
        'tool_name': tool,
        'target_file': file_path,
        'success': True,
        'timestamp': ts,
        'role': 'assistant',
        'cwd': cwd,
        'git_branch': event.get('git_branch'),
        'file_uuid': enrichment.get('file_uuid'),
        'repo_root': enrichment.get('repo_root'),
        'blob_hash': enrichment.get('blob_hash'),
        'content_hash': enrichment.get('content_hash'),
        'is_tracked': enrichment.get('is_tracked'),
        'url_uuid': enrichment.get('url_uuid'),
        'spawned_agent': spawned_agent,
    }

    # Store raw content (old_string, new_string, write_content, web_content)
    _store_raw_content(conn, chunk_id, event, tool, ts)

    return chunk


def sync_session_messages(session_id: str, conn: sqlite3.Connection) -> int:
    """Sync messages from JSONL to chunk-atom tables."""
    jsonl_path = find_jsonl(session_id)
    if not jsonl_path or not jsonl_path.exists():
        return 0

    # Ensure source row exists (backfill path may skip process_event)
    ensure_source_exists(conn, session_id)

    cur = conn.cursor()

    # Get max chunk_number for this session to avoid duplicates
    cur.execute("""
        SELECT COALESCE(MAX(tm.chunk_number), 0)
        FROM _types_message tm
        JOIN _edges_source es ON tm.chunk_id = es.chunk_id
        WHERE es.source_id = ? AND tm.type IN ('user_prompt', 'assistant')
    """, (session_id,))
    last_num = cur.fetchone()[0]

    try:
        with open(jsonl_path, 'r') as f:
            lines = f.readlines()
    except Exception:
        return 0

    new_chunks = []
    for line_num, line in enumerate(lines, 1):
        if line_num <= last_num:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        entry_type = entry.get('type')
        if entry_type not in ('user', 'assistant'):
            continue

        message = entry.get('message', {})
        uuid = entry.get('uuid')
        if not message or not uuid:
            continue

        content = message.get('content', [])
        text_parts = []

        if isinstance(content, str):
            text_parts.append(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get('type') == 'text':
                    text_parts.append(item.get('text', ''))

        text_content = '\n'.join(text_parts) if text_parts else None
        if not text_content:
            continue

        ts_int = int(time.time())
        timestamp = entry.get('timestamp')
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                ts_int = int(dt.timestamp())
            except Exception:
                pass

        chunk_type = 'user_prompt' if entry_type == 'user' else 'assistant'
        role = 'user' if entry_type == 'user' else 'assistant'

        new_chunks.append({
            'id': f"{session_id}_{line_num}",
            'doc_id': session_id,
            'chunk_number': line_num,
            'type': chunk_type,
            'content': text_content[:2000],
            'tool_name': None,
            'target_file': None,
            'success': None,
            'timestamp': ts_int,
            'role': role,
            'cwd': entry.get('cwd'),
            'git_branch': entry.get('gitBranch'),
        })

    if not new_chunks:
        return 0

    # Embed and insert
    embedder = get_embedder()
    texts = [c['content'] for c in new_chunks]
    embeddings = encode(texts)

    inserted = 0
    for chunk, emb in zip(new_chunks, embeddings):
        try:
            chunk['embedding'] = serialize_f32(emb)
            insert_chunk_atom(conn, chunk)
            update_source_stats(conn, chunk['doc_id'], chunk)
            inserted += 1
        except Exception as e:
            print(f"[worker] Chunk insert error: {e}", file=sys.stderr)

    return inserted


def process_queue(conn: sqlite3.Connection) -> dict:
    """Process SQLite queue events directly to chunk-atom cell."""
    qconn = sqlite3.connect(str(QUEUE_DB), timeout=5)
    qconn.execute("PRAGMA journal_mode=WAL")

    rows = qconn.execute(
        "SELECT rowid, payload FROM claude_code_pending ORDER BY ts LIMIT 100"
    ).fetchall()

    if not rows:
        qconn.close()
        return {'processed': 0, 'chunks': 0, 'embedded': 0}

    processed = chunks_inserted = embedded = 0
    sessions_synced = set()
    pending_chunks = []
    processed_rowids = []

    for rowid, payload in rows:
        try:
            event = json.loads(payload)
            chunk = process_event(event, conn)
            if chunk:
                pending_chunks.append(chunk)

            session_id = event.get('session', '')
            if session_id and session_id not in sessions_synced:
                embedded += sync_session_messages(session_id, conn)
                sessions_synced.add(session_id)
            processed += 1
            processed_rowids.append(rowid)
        except json.JSONDecodeError:
            processed_rowids.append(rowid)  # skip bad JSON
        except Exception as e:
            print(f"[worker] Error: {e}", file=sys.stderr)
            processed_rowids.append(rowid)

    # Batch embed and insert tool call chunks
    if pending_chunks:
        embedder = get_embedder()
        texts = [c['content'] for c in pending_chunks]
        embeddings = encode(texts)

        for chunk, emb in zip(pending_chunks, embeddings):
            try:
                chunk['embedding'] = serialize_f32(emb)
                insert_chunk_atom(conn, chunk)
                update_source_stats(conn, chunk['doc_id'], chunk)
                chunks_inserted += 1
            except Exception as e:
                print(f"[worker] Insert error: {e}", file=sys.stderr)

    conn.commit()

    # Delete processed rows from queue
    if processed_rowids:
        placeholders = ','.join('?' * len(processed_rowids))
        qconn.execute(
            f"DELETE FROM claude_code_pending WHERE rowid IN ({placeholders})",
            processed_rowids
        )
        qconn.commit()

    qconn.close()
    return {'processed': processed, 'chunks': chunks_inserted, 'embedded': embedded}


def startup_backfill(conn: sqlite3.Connection):
    """Backfill sessions missed during pipeline outage."""
    print("[worker] Running startup backfill...", file=sys.stderr)
    last_indexed = conn.execute(
        "SELECT COALESCE(MAX(end_time), 0) FROM _raw_sources"
    ).fetchone()[0]

    backfilled = 0
    for jsonl in CLAUDE_PROJECTS.rglob("*.jsonl"):
        try:
            if jsonl.stat().st_mtime > last_indexed:
                session_id = jsonl.stem
                count = sync_session_messages(session_id, conn)
                if count > 0:
                    backfilled += count
        except Exception:
            pass

    if backfilled > 0:
        conn.commit()
        print(f"[worker] Backfilled {backfilled} chunks", file=sys.stderr)
    else:
        print("[worker] No backfill needed", file=sys.stderr)


def daemon_loop(interval=2):
    """Main daemon loop."""
    # Resolve cell
    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[worker] FATAL: claude_code cell not found in registry", file=sys.stderr)
        sys.exit(1)

    print("[flexsearch-worker] Starting chunk-atom daemon", file=sys.stderr)
    print(f"  Target: {cell_path}", file=sys.stderr)
    print(f"  Queue: {QUEUE_DB}", file=sys.stderr)
    print(f"  Interval: {interval}s", file=sys.stderr)

    # Pre-warm embedder
    get_embedder()

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Ensure content store tables exist
    _ensure_content_tables(conn)

    # Startup backfill for sessions missed during outage
    startup_backfill(conn)

    print("  Docpac: incremental indexing enabled", file=sys.stderr)

    BACKFILL_INTERVAL = 24 * 3600  # 24 hours
    last_backfill = time.time()

    while True:
        try:
            stats = process_queue(conn)
            if stats['processed'] > 0:
                print(f"[worker] processed={stats['processed']} chunks={stats['chunks']} emb={stats['embedded']}",
                      file=sys.stderr)
        except Exception as e:
            print(f"[worker] Error: {e}", file=sys.stderr)

        # Drain docpac pending queue (same embedder, different cell connections)
        try:
            dp_stats = docpac_process_queue(encode)
            if dp_stats['indexed'] > 0:
                print(f"[docpac] indexed={dp_stats['indexed']} skipped={dp_stats['skipped']}",
                      file=sys.stderr)
        except Exception as e:
            print(f"[docpac] Error: {e}", file=sys.stderr)

        # Periodic backfill — catch anything hooks missed (24h cycle)
        if time.time() - last_backfill > BACKFILL_INTERVAL:
            try:
                print("[worker] Periodic backfill (24h cycle)...", file=sys.stderr)
                startup_backfill(conn)
                last_backfill = time.time()
            except Exception as e:
                print(f"[worker] Backfill error: {e}", file=sys.stderr)

        # Queue depth check
        try:
            qconn = sqlite3.connect(str(QUEUE_DB), timeout=5.0)
            cc_depth = qconn.execute(
                "SELECT COUNT(*) FROM claude_code_pending"
            ).fetchone()[0]
            dp_depth = qconn.execute(
                "SELECT COUNT(*) FROM pending"
            ).fetchone()[0]
            qconn.close()
            if cc_depth > 500:
                print(f"[worker] WARNING: claude_code queue depth {cc_depth}", file=sys.stderr)
            if dp_depth > 100:
                print(f"[docpac] WARNING: docpac queue depth {dp_depth}", file=sys.stderr)
        except Exception:
            pass  # queue may not exist yet on first boot

        time.sleep(interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    parser.add_argument("--interval", type=int, default=2, help="Queue poll interval")
    args = parser.parse_args()

    if args.daemon:
        daemon_loop(interval=args.interval)
    else:
        cell_path = resolve_cell('claude_code')
        if not cell_path:
            print("[worker] FATAL: claude_code cell not found", file=sys.stderr)
            sys.exit(1)
        conn = sqlite3.connect(str(cell_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        _ensure_content_tables(conn)
        stats = process_queue(conn)
        conn.close()
        print(f"[worker] Done: {stats}")
