#!/usr/bin/env python3
"""
Flex Worker — Direct capture to chunk-atom cell.

Writes to chunk-atom schema:
  _raw_chunks, _edges_source, _types_message, _edges_tool_ops,
  _edges_file_identity, _edges_repo_identity, _edges_url_identity,
  _edges_delegations, _edges_soft_ops

Queue: SQLite (~/.flex/queue.db) table claude_code_pending
Cell: resolved via flex.registry

Tier 0 (every 2s): Queue → chunk-atom tables + embeddings
"""

import hashlib
import re
import sqlite3
import json
import time
import sys
import struct
from pathlib import Path
from datetime import datetime

import uuid as _uuid
from flex.registry import resolve_cell, register_cell, FLEX_HOME
from flex.onnx.embed import get_model, encode
from flex.modules.claude_code.compile.soft_detect import detect_file_ops
# Docpac module — optional, graceful degradation when absent
try:
    from flex.modules.docpac.compile.worker import process_queue as docpac_process_queue
except ImportError:
    docpac_process_queue = None

# SOMA identity module — optional, graceful degradation when absent
try:
    from flex.modules.soma.compile import enrich as soma_enrich
    from flex.modules.soma.compile import insert_edges as soma_insert_edges
    from flex.modules.soma.compile import ensure_tables as soma_ensure_tables
    from flex.modules.soma.manage.heal import heal as soma_heal
except ImportError:
    soma_enrich = None
    soma_insert_edges = None
    soma_ensure_tables = None
    soma_heal = None

# Enrichment modules — optional, graceful degradation when absent
try:
    from flex.modules.claude_code.manage.enrich_summary import run as run_fingerprints
except ImportError:
    run_fingerprints = None

try:
    from flex.modules.claude_code.manage.enrich_repo_project import run as run_repo_project
except ImportError:
    run_repo_project = None

try:
    from flex.modules.claude_code.manage.rebuild_all import (
        rebuild_source_graph, rebuild_warmup_types, reembed_sources,
    )
except ImportError:
    rebuild_source_graph = None
    rebuild_warmup_types = None
    reembed_sources = None

QUEUE_DB = FLEX_HOME / "queue.db"
CLAUDE_PROJECTS = Path.home() / ".claude/projects"

# Session index cache: {project_dir_str: {session_id: {"summary": ..., "firstPrompt": ...}}}
_index_cache: dict[str, dict] = {}


def _load_session_index(project_dir: Path) -> dict:
    """Load sessions-index.json from a Claude project directory. Cached."""
    key = str(project_dir)
    if key in _index_cache:
        return _index_cache[key]

    index_path = project_dir / "sessions-index.json"
    result = {}
    try:
        with open(index_path, 'r') as f:
            data = json.load(f)
        for entry in data.get('entries', []):
            sid = entry.get('sessionId')
            if sid:
                result[sid] = {
                    'summary': entry.get('summary'),
                    'firstPrompt': entry.get('firstPrompt'),
                }
    except Exception:
        pass

    _index_cache[key] = result
    return result

# Tool input key → target_file extraction
_TARGET_FILE_KEYS = {
    'Read': 'file_path', 'Write': 'file_path', 'Edit': 'file_path',
    'MultiEdit': 'file_path', 'NotebookEdit': 'notebook_path',
    'Grep': 'path', 'Glob': 'path',
}

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


from flex.modules.soma.lib.git import git_root_from_path as _git_root_from_path
from flex.modules.soma.lib.git import project_from_git_root as _project_from_git_root


def _git_root(cwd: str) -> str | None:
    """Return git show-toplevel for cwd, or None if not a git repo."""
    if not cwd:
        return None
    return _git_root_from_path(cwd)


def ensure_source_exists(conn: sqlite3.Connection, session_id: str, cwd: str = None, title: str = None):
    """Ensure a source (session) exists in _raw_sources."""
    cur = conn.cursor()
    cur.execute("SELECT source_id FROM _raw_sources WHERE source_id = ?", (session_id,))
    if cur.fetchone():
        # If title provided and row exists, update if title is still NULL or bad
        if title:
            cur.execute("""
                UPDATE _raw_sources SET title = ?
                WHERE source_id = ? AND (title IS NULL OR title LIKE 'Read %' OR title LIKE 'Warmup%')
            """, (title, session_id))
        return

    git_root = _git_root(cwd)
    project = _project_from_git_root(git_root or cwd) if (git_root or cwd) else None

    cur.execute("""
        INSERT INTO _raw_sources
        (source_id, source, project, git_root, start_time, primary_cwd, message_count, episode_count, title)
        VALUES (?, ?, ?, ?, NULL, ?, 0, 0, ?)
    """, (session_id, f"claude_code:{session_id}", project, git_root, cwd, title))


def update_source_stats(conn: sqlite3.Connection, session_id: str, chunk: dict):
    """Increment message_count and update start_time/end_time on source."""
    cur = conn.cursor()
    ts = chunk['timestamp']

    cur.execute("""
        UPDATE _raw_sources
        SET message_count = message_count + 1,
            start_time = CASE
                WHEN start_time IS NULL THEN ?
                WHEN ? < start_time THEN ?
                ELSE start_time
            END,
            end_time = ?,
            duration_minutes = CASE
                WHEN start_time IS NOT NULL AND ? > start_time
                THEN (? - start_time) / 60
                ELSE duration_minutes
            END
        WHERE source_id = ?
    """, (ts, ts, ts, ts, ts, ts, session_id))

    # Set title from first user prompt (only if title is still NULL)
    if chunk.get('type') == 'user_prompt':
        content = chunk.get('content', '')
        if content:
            # Strip XML tags (system-reminder, local-command-caveat, command-name/message)
            clean = re.sub(r'<[^>]+>.*?</[^>]+>', '', content, flags=re.DOTALL).strip()
            if clean:
                cur.execute("""
                    UPDATE _raw_sources
                    SET title = ?
                    WHERE source_id = ? AND title IS NULL
                """, (clean[:250], session_id))


def _ensure_core_tables(conn: sqlite3.Connection):
    """Create all chunk-atom tables for a fresh cell. Idempotent."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS _raw_chunks (
            id TEXT PRIMARY KEY,
            content TEXT,
            embedding BLOB,
            timestamp INTEGER
        );

        CREATE TABLE IF NOT EXISTS _raw_sources (
            source_id TEXT PRIMARY KEY,
            project TEXT,
            title TEXT,
            summary TEXT,
            source TEXT,
            file_date TEXT,
            start_time INTEGER,
            end_time INTEGER,
            duration_minutes INTEGER,
            message_count INTEGER,
            episode_count INTEGER,
            primary_cwd TEXT,
            model TEXT,
            embedding BLOB,
            git_root TEXT
        );

        CREATE TABLE IF NOT EXISTS _edges_source (
            chunk_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_type TEXT DEFAULT 'claude-code',
            position INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

        CREATE TABLE IF NOT EXISTS _edges_tool_ops (
            chunk_id TEXT PRIMARY KEY,
            tool_name TEXT,
            target_file TEXT,
            success INTEGER,
            cwd TEXT,
            git_branch TEXT
        );

        CREATE TABLE IF NOT EXISTS _types_message (
            chunk_id TEXT PRIMARY KEY,
            type TEXT,
            role TEXT,
            chunk_number INTEGER,
            parent_uuid TEXT,
            is_sidechain INTEGER,
            entry_uuid TEXT
        );

        CREATE TABLE IF NOT EXISTS _edges_delegations (
            id INTEGER PRIMARY KEY,
            chunk_id TEXT,
            child_session_id TEXT,
            agent_type TEXT,
            created_at INTEGER,
            parent_source_id TEXT
        );

        CREATE TABLE IF NOT EXISTS _edges_soft_ops (
            id INTEGER PRIMARY KEY,
            chunk_id TEXT,
            file_path TEXT,
            file_uuid TEXT,
            inferred_op TEXT,
            confidence TEXT
        );

        CREATE TABLE IF NOT EXISTS _meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS _presets (
            name TEXT PRIMARY KEY,
            description TEXT,
            params TEXT DEFAULT '',
            sql TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
            content,
            content='_raw_chunks',
            content_rowid='rowid'
        );
    """)
    # FTS triggers — can't use IF NOT EXISTS, so check first
    has_trigger = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='raw_chunks_ai'"
    ).fetchone()
    if not has_trigger:
        conn.executescript("""
            CREATE TRIGGER raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
                INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
            CREATE TRIGGER raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
                INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
            END;
            CREATE TRIGGER raw_chunks_au AFTER UPDATE ON _raw_chunks BEGIN
                INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
                INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
        """)


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
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_edges_raw_content_hash
        ON _edges_raw_content(content_hash)
    """)
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS content_fts USING fts5(
            content,
            content='_raw_content',
            content_rowid='rowid'
        )
    """)
    has_trigger = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='raw_content_ai'"
    ).fetchone()
    if not has_trigger:
        conn.executescript("""
            CREATE TRIGGER raw_content_ai AFTER INSERT ON _raw_content BEGIN
                INSERT INTO content_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
            CREATE TRIGGER raw_content_ad AFTER DELETE ON _raw_content BEGIN
                INSERT INTO content_fts(content_fts, rowid, content) VALUES('delete', old.rowid, old.content);
            END;
            CREATE TRIGGER raw_content_au AFTER UPDATE ON _raw_content BEGIN
                INSERT INTO content_fts(content_fts, rowid, content) VALUES('delete', old.rowid, old.content);
                INSERT INTO content_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
        """)
        # Backfill existing rows into FTS index
        rc_count = conn.execute("SELECT COUNT(*) FROM _raw_content").fetchone()[0]
        if rc_count > 0:
            conn.execute("INSERT INTO content_fts(content_fts) VALUES('rebuild')")



def _store_content_raw(conn: sqlite3.Connection, chunk_id: str, raw: str,
                       tool_name: str, ts: int):
    """Store raw content — no size cap. SHA-256 dedup."""
    raw = raw.encode('utf-8', errors='surrogatepass').decode('utf-8', errors='replace')
    h = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    conn.execute(
        "INSERT OR IGNORE INTO _raw_content VALUES (?,?,?,?,?)",
        (h, raw, tool_name, len(raw), ts)
    )
    conn.execute(
        "INSERT OR IGNORE INTO _edges_raw_content VALUES (?,?)",
        (chunk_id, h)
    )


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
        INSERT OR IGNORE INTO _types_message
        (chunk_id, type, role, chunk_number, parent_uuid, is_sidechain, entry_uuid)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (chunk_id, chunk['type'], chunk['role'], chunk['chunk_number'],
          chunk.get('parent_uuid'), chunk.get('is_sidechain'), chunk.get('entry_uuid')))

    # _edges_tool_ops (only for tool calls)
    if chunk.get('tool_name'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_tool_ops (chunk_id, tool_name, target_file, success, cwd, git_branch)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chunk_id, chunk['tool_name'], chunk.get('target_file'),
              chunk.get('success'), chunk.get('cwd'), chunk.get('git_branch')))

    # SOMA identity edges (file_uuid, repo_root, content_hash, url_uuid)
    if soma_insert_edges:
        soma_insert_edges(conn, chunk)

    # _edges_delegations (Task spawns)
    if chunk.get('spawned_agent'):
        ensure_source_exists(conn, chunk['spawned_agent'])
        cur.execute("""
            INSERT OR IGNORE INTO _edges_delegations (chunk_id, child_session_id, agent_type, created_at)
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



def sync_session_messages(session_id: str, conn: sqlite3.Connection,
                          skip_embed: bool = False) -> int:
    """Sync ALL chunk types from JSONL to chunk-atom tables.

    Single write path. One JSONL pass produces: text chunks, tool_call chunks,
    tool_ops edges, SOMA identity edges, delegations, soft_ops, thinking blocks,
    and file-history-snapshots. Content is stored without truncation.

    Args:
        skip_embed: If True, insert chunks with embedding=NULL (for batch embed pass).
    """
    jsonl_path = find_jsonl(session_id)
    if not jsonl_path or not jsonl_path.exists():
        return 0

    # Load session title from sessions-index.json (matches VS Code sidebar)
    index = _load_session_index(jsonl_path.parent)
    index_entry = index.get(session_id, {})
    title = index_entry.get('summary') or None

    # Ensure source row exists
    ensure_source_exists(conn, session_id, title=title)

    cur = conn.cursor()

    # Get max chunk_number for this session to avoid duplicates
    cur.execute("""
        SELECT COALESCE(MAX(tm.chunk_number), 0)
        FROM _types_message tm
        JOIN _edges_source es ON tm.chunk_id = es.chunk_id
        WHERE es.source_id = ?
    """, (session_id,))
    last_num = cur.fetchone()[0]

    try:
        with open(jsonl_path, 'r') as f:
            lines = f.readlines()
    except Exception:
        return 0

    new_chunks = []
    tool_content_items = []   # (chunk_id, raw, tool_name, ts)
    tool_ops_items = []       # (chunk_id, tool_name, target_file, cwd, git_branch, success)
    soma_items = []           # (chunk_id, enrichment_dict)
    delegation_items = []     # (chunk_id, spawned_agent, ts)
    soft_ops_items = []       # (chunk_id, SoftFileOp)
    tool_use_id_map = {}      # tool_use.id -> tool_name
    snapshot_hashes = {}      # messageId -> {filepath: git_blob_hash}

    _ensure_content_tables(conn)

    for line_num, line in enumerate(lines, 1):
        if line_num <= last_num:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        entry_type = entry.get('type')
        chunk_id = f"{session_id}_{line_num}"

        # Parse timestamp (used by all entry types)
        ts_int = int(time.time())
        timestamp = entry.get('timestamp')
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                ts_int = int(dt.timestamp())
            except Exception:
                pass

        # --- File history snapshots → cache old_blob_hash + raw_content ---
        if entry_type == 'file-history-snapshot':
            snapshot = entry.get('snapshot', {})
            msg_id = entry.get('messageId') or (snapshot.get('messageId', '') if isinstance(snapshot, dict) else '')
            backups = snapshot.get('trackedFileBackups', {}) if isinstance(snapshot, dict) else {}
            if msg_id and backups:
                file_hashes = {}
                for filepath, info in backups.items():
                    if not isinstance(info, dict):
                        continue
                    backup_name = info.get('backupFileName', '')
                    if not backup_name:
                        continue
                    backup_path = Path.home() / '.claude' / 'file-history' / session_id / backup_name
                    if backup_path.exists():
                        try:
                            content = backup_path.read_bytes()
                            header = f"blob {len(content)}\0".encode()
                            file_hashes[filepath] = hashlib.sha1(header + content).hexdigest()
                        except Exception:
                            pass
                if file_hashes:
                    snapshot_hashes.setdefault(msg_id, {}).update(file_hashes)
            # Still store snapshot JSON in _raw_content for provenance
            if snapshot and isinstance(snapshot, dict):
                _store_content_raw(conn, chunk_id, json.dumps(snapshot), '_file_snapshot', ts_int)
            continue

        # --- custom-title from /rename → override title ---
        if entry_type == 'custom-title':
            custom_title = entry.get('customTitle')
            if custom_title:
                cur.execute("""
                    UPDATE _raw_sources SET title = ?
                    WHERE source_id = ?
                """, (custom_title[:250], session_id))
            continue

        # --- progress / system → skip ---
        if entry_type not in ('user', 'assistant'):
            continue

        message = entry.get('message', {})
        uuid = entry.get('uuid')
        if not message or not uuid:
            continue

        cwd = entry.get('cwd', '')
        git_branch = entry.get('gitBranch')
        content = message.get('content', [])
        text_parts = []
        thinking_parts = []
        tool_ops_for_line = []  # (tool_name, tool_input_dict)

        if isinstance(content, str):
            text_parts.append(content)
        elif isinstance(content, list):
            for item in content:
                if not isinstance(item, dict):
                    continue
                item_type = item.get('type')

                if item_type == 'text':
                    text_parts.append(item.get('text', ''))

                elif item_type == 'thinking':
                    thinking_text = item.get('thinking', '')
                    if thinking_text:
                        tool_content_items.append((chunk_id, thinking_text, '_thinking', ts_int))
                        thinking_parts.append(thinking_text)

                elif item_type == 'tool_use':
                    tool_name = item.get('name', 'unknown')
                    tool_input = item.get('input', {})
                    tool_use_id_map[item.get('id', '')] = tool_name

                    # Store full tool input in _raw_content
                    raw = json.dumps(tool_input)
                    if len(raw) > 10:
                        tool_content_items.append((chunk_id, raw, tool_name, ts_int))

                    # Extract target_file
                    target_key = _TARGET_FILE_KEYS.get(tool_name)
                    target_file = tool_input.get(target_key) if target_key else None

                    tool_ops_for_line.append((tool_name, tool_input, target_file))

                    # Build tool_ops edge
                    tool_ops_items.append((
                        chunk_id, tool_name, target_file, cwd, git_branch, True
                    ))

                    # SOMA enrichment
                    if soma_enrich:
                        try:
                            enrichment = soma_enrich({
                                'tool': tool_name,
                                'file': target_file,
                                'cwd': cwd,
                                'url': tool_input.get('url'),
                                'web_content': None,
                                'web_status': None,
                                'session': session_id,
                                'msg': line_num,
                            })
                            # File-history backup hash overrides git rev-parse
                            entry_uuid = uuid or ''
                            if entry_uuid in snapshot_hashes and target_file and \
                                    target_file in snapshot_hashes[entry_uuid]:
                                enrichment['old_blob_hash'] = snapshot_hashes[entry_uuid][target_file]
                            if any(enrichment.get(k) for k in
                                   ('file_uuid', 'repo_root', 'blob_hash',
                                    'old_blob_hash', 'content_hash', 'url_uuid')):
                                soma_items.append((chunk_id, enrichment))
                        except Exception as e:
                            print(f"[worker] SOMA enrichment error: {e}", file=sys.stderr)

                    # Soft detect for Bash commands
                    if tool_name == 'Bash':
                        cmd = tool_input.get('command', '')
                        if cmd:
                            for op in detect_file_ops(cmd, cwd):
                                soft_ops_items.append((chunk_id, op))

                elif item_type == 'tool_result':
                    tool_use_id = item.get('tool_use_id', '')
                    tool_name = tool_use_id_map.get(tool_use_id, 'unknown')
                    raw = _normalize_tool_result(item.get('content'))
                    if raw and len(raw) > 10:
                        tool_content_items.append((chunk_id, raw, tool_name, ts_int))

                    # Detect Task delegations from tool_result
                    if tool_name == 'Task' and raw:
                        agent_match = re.search(r'agentId: ([a-f0-9]+)', raw)
                        if agent_match:
                            spawned = f"agent-{agent_match.group(1)}"
                            delegation_items.append((chunk_id, spawned, ts_int, session_id))

        # --- Build chunk content ---
        text_content = '\n'.join(text_parts) if text_parts else None

        # Tool-only chunks: assistant lines with tool_use but no text
        if not text_content and not tool_ops_for_line:
            continue
        if not text_content and tool_ops_for_line:
            content_parts = []
            for tool_name, tool_input, target_file in tool_ops_for_line:
                content_parts.append(tool_name)
                if target_file:
                    content_parts.append(target_file)
                if tool_name == 'Bash':
                    cmd = tool_input.get('command', '')
                    if cmd:
                        content_parts.append(cmd)
                if tool_name in ('Grep', 'Glob'):
                    pattern = tool_input.get('pattern', '')
                    if pattern:
                        content_parts.append(pattern)
                if tool_name == 'WebFetch':
                    url = tool_input.get('url', '')
                    if url:
                        content_parts.append(url)
                if tool_name == 'WebSearch':
                    query = tool_input.get('query', '')
                    if query:
                        content_parts.append(query)
                if tool_name == 'Task':
                    prompt = tool_input.get('prompt', '')
                    if prompt:
                        content_parts.append(prompt)
            text_content = ' '.join(content_parts)
            chunk_type = 'tool_call'
            role = 'assistant'
        else:
            chunk_type = 'user_prompt' if entry_type == 'user' else 'assistant'
            role = 'user' if entry_type == 'user' else 'assistant'

        new_chunks.append({
            'id': chunk_id,
            'doc_id': session_id,
            'chunk_number': line_num,
            'type': chunk_type,
            'content': text_content,
            'tool_name': None,
            'target_file': None,
            'success': None,
            'timestamp': ts_int,
            'role': role,
            'cwd': cwd or None,
            'git_branch': git_branch,
            'parent_uuid': entry.get('parentUuid'),
            'is_sidechain': 1 if entry.get('isSidechain') else 0,
            'entry_uuid': uuid,
        })

    # --- Embed and insert chunks ---
    inserted = 0
    if new_chunks:
        if skip_embed:
            # Phase 1 of decoupled backfill: insert without embeddings
            for chunk in new_chunks:
                try:
                    insert_chunk_atom(conn, chunk)
                    update_source_stats(conn, chunk['doc_id'], chunk)
                    inserted += 1
                except Exception as e:
                    print(f"[worker] Chunk insert error: {e}", file=sys.stderr)
        else:
            embedder = get_embedder()
            texts = [c['content'] for c in new_chunks]
            embeddings = encode(texts)

            for chunk, emb in zip(new_chunks, embeddings):
                try:
                    chunk['embedding'] = serialize_f32(emb)
                    insert_chunk_atom(conn, chunk)
                    update_source_stats(conn, chunk['doc_id'], chunk)
                    inserted += 1
                except Exception as e:
                    print(f"[worker] Chunk insert error: {e}", file=sys.stderr)

    # --- Write tool_ops edges ---
    for chunk_id, tool_name, target_file, cwd, git_branch, success in tool_ops_items:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO _edges_tool_ops
                (chunk_id, tool_name, target_file, success, cwd, git_branch)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (chunk_id, tool_name, target_file, success, cwd, git_branch))
        except Exception as e:
            print(f"[worker] Tool ops insert error: {e}", file=sys.stderr)

    # --- Write SOMA identity edges ---
    if soma_insert_edges:
        for chunk_id, enrichment in soma_items:
            try:
                chunk_dict = {'id': chunk_id}
                for key in ('file_uuid', 'repo_root', 'blob_hash', 'old_blob_hash',
                            'content_hash', 'is_tracked', 'url_uuid'):
                    if enrichment.get(key) is not None:
                        chunk_dict[key] = enrichment[key]
                soma_insert_edges(conn, chunk_dict)
            except Exception as e:
                print(f"[worker] SOMA insert error: {e}", file=sys.stderr)

    # --- Write delegation edges ---
    for chunk_id, spawned_agent, ts, parent_sid in delegation_items:
        try:
            ensure_source_exists(conn, spawned_agent)
            cur.execute("""
                INSERT OR IGNORE INTO _edges_delegations
                (chunk_id, child_session_id, agent_type, created_at, parent_source_id)
                VALUES (?, ?, NULL, ?, ?)
            """, (chunk_id, spawned_agent, ts, parent_sid))
        except Exception as e:
            print(f"[worker] Delegation insert error: {e}", file=sys.stderr)

    # --- Write soft_ops edges ---
    for chunk_id, op in soft_ops_items:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO _edges_soft_ops
                (chunk_id, file_path, file_uuid, inferred_op, confidence)
                VALUES (?, ?, NULL, ?, ?)
            """, (chunk_id, op.file_path, op.inferred_op, op.confidence))
        except Exception as e:
            print(f"[worker] Soft ops insert error: {e}", file=sys.stderr)

    # --- Store tool content (tool_use inputs, tool_results, thinking) ---
    for chunk_id, raw, tool_name, ts in tool_content_items:
        try:
            _store_content_raw(conn, chunk_id, raw, tool_name, ts)
        except Exception as e:
            print(f"[worker] Tool content store error: {e}", file=sys.stderr)

    return inserted


def process_queue(conn: sqlite3.Connection) -> dict:
    """Process queue: read session_ids, sync each from JSONL."""
    qconn = sqlite3.connect(str(QUEUE_DB), timeout=5)
    qconn.execute("PRAGMA journal_mode=WAL")

    rows = qconn.execute(
        "SELECT session_id FROM claude_code_pending ORDER BY ts LIMIT 100"
    ).fetchall()

    if not rows:
        qconn.close()
        return {'processed': 0, 'embedded': 0}

    embedded = 0
    session_ids = [r[0] for r in rows]

    for session_id in session_ids:
        try:
            embedded += sync_session_messages(session_id, conn)
        except Exception as e:
            print(f"[worker] Error syncing {session_id[:8]}: {e}", file=sys.stderr)

    conn.commit()

    placeholders = ','.join('?' * len(session_ids))
    qconn.execute(
        f"DELETE FROM claude_code_pending WHERE session_id IN ({placeholders})",
        session_ids
    )
    qconn.commit()
    qconn.close()

    return {'processed': len(session_ids), 'embedded': embedded}


def startup_backfill(conn: sqlite3.Connection, commit_every: int = 50):
    """Backfill sessions missed during pipeline outage.

    Commits every `commit_every` sessions to keep WAL bounded.
    Resumable: on restart, last_indexed advances past committed sessions.
    """
    print("[worker] Running startup backfill...", file=sys.stderr)
    last_indexed = conn.execute(
        "SELECT COALESCE(MAX(end_time), 0) FROM _raw_sources"
    ).fetchone()[0]

    backfilled = 0
    sessions_since_commit = 0
    for jsonl in CLAUDE_PROJECTS.rglob("*.jsonl"):
        try:
            if jsonl.stat().st_mtime > last_indexed:
                session_id = jsonl.stem
                count = sync_session_messages(session_id, conn)
                if count > 0:
                    backfilled += count
                    sessions_since_commit += 1
                    if sessions_since_commit >= commit_every:
                        conn.commit()
                        print(f"[worker] Backfill progress: {backfilled} chunks",
                              file=sys.stderr)
                        sessions_since_commit = 0
        except Exception:
            pass

    if sessions_since_commit > 0:
        conn.commit()

    if backfilled > 0:
        print(f"[worker] Backfilled {backfilled} chunks", file=sys.stderr)
    else:
        print("[worker] No backfill needed", file=sys.stderr)


def bootstrap_claude_code_cell() -> Path:
    """Create claude_code cell if not exists. Returns db path."""
    existing = resolve_cell('claude_code')
    if existing and existing.exists():
        return existing

    cell_uuid = str(_uuid.uuid4())
    cells_dir = FLEX_HOME / "cells"
    cells_dir.mkdir(parents=True, exist_ok=True)
    db_path = cells_dir / f"{cell_uuid}.db"

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)
    if soma_ensure_tables:
        soma_ensure_tables(conn)

    # Populate _meta
    conn.execute(
        "INSERT OR IGNORE INTO _meta VALUES ('description', ?)",
        ('Claude Code session provenance. Each doc is a session, '
         'each chunk is a tool call/prompt/response.',)
    )
    conn.execute(
        "INSERT OR IGNORE INTO _meta VALUES ('cell_type', 'claude-code')"
    )
    conn.commit()
    conn.close()

    register_cell('claude_code', str(db_path), cell_type='claude-code',
                   description='Claude Code session provenance')
    return db_path


def _batch_embed_chunks(conn, batch_size: int = 500) -> int:
    """Phase 2 of decoupled backfill: batch embed all NULL-embedding chunks.

    SELECT content WHERE embedding IS NULL → encode(batch=500) → UPDATE.
    Same pattern as reembed_nomic.py but uses the worker's embedder/encode.
    Commits after each batch. Returns total embedded count.
    """
    embedder = get_embedder()
    done = 0
    t0 = time.time()

    total = conn.execute(
        "SELECT count(*) FROM _raw_chunks WHERE embedding IS NULL AND content IS NOT NULL"
    ).fetchone()[0]

    while True:
        rows = conn.execute("""
            SELECT id, content FROM _raw_chunks
            WHERE embedding IS NULL AND content IS NOT NULL
            ORDER BY id
            LIMIT ?
        """, (batch_size,)).fetchall()

        if not rows:
            break

        texts = [r[1] for r in rows]
        embeddings = encode(texts)

        conn.executemany(
            "UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
            [(serialize_f32(emb), r[0]) for emb, r in zip(embeddings, rows)]
        )
        conn.commit()
        done += len(rows)

        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        remaining = (total - done) / rate if rate > 0 else 0
        sys.stdout.write(
            f"\r  ~ {done:,}/{total:,} chunks embedded  ({rate:.0f}/s, ~{remaining:.0f}s left)    "
        )
        sys.stdout.flush()

    elapsed = time.time() - t0
    if done > 0:
        rate = done / elapsed if elapsed > 0 else 0
        sys.stdout.write("\r" + " " * 70 + "\r")  # clear progress line
        print(f"  [ok] {done:,} chunks embedded in {elapsed:.0f}s ({rate:.0f}/s)")
    return done


def initial_backfill(conn, progress_cb=None) -> dict:
    """Backfill all sessions with per-session commits and progress.

    Decoupled two-phase approach:
      Phase 1 — parse all sessions, insert chunks with embedding=NULL (I/O bound)
      Phase 2 — batch embed all NULL chunks in one pass (CPU bound, 258 chunks/s)

    Args:
        conn: Open SQLite connection to the cell.
        progress_cb: Optional callback(files_done, files_total, sessions, chunks, elapsed).

    Returns:
        dict with sessions, chunks, elapsed.
    """
    jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))
    total = len(jsonls)
    sessions = 0
    chunks = 0
    t0 = time.time()

    # Phase 1: parse all sessions without embedding
    for i, jsonl in enumerate(jsonls, 1):
        session_id = jsonl.stem
        try:
            count = sync_session_messages(session_id, conn, skip_embed=True)
            conn.commit()
            if count > 0:
                chunks += count
                sessions += 1
        except Exception as e:
            print(f"[init] Error syncing {session_id[:8]}: {e}", file=sys.stderr)

        if progress_cb:
            progress_cb(i, total, sessions, chunks, time.time() - t0)

    # Phase 2: batch embed all NULL-embedding chunks
    print(f"[init] Phase 1 done: {sessions} sessions, {chunks} chunks. "
          "Starting batch embed...", file=sys.stderr)
    _batch_embed_chunks(conn)

    return {'sessions': sessions, 'chunks': chunks, 'elapsed': time.time() - t0}


def _cc_graph_stale(conn, threshold=50):
    """True if enough new sessions synced since last graph build."""
    try:
        last_graph = conn.execute("""
            SELECT MAX(timestamp) FROM _ops
            WHERE operation = 'build_similarity_graph'
        """).fetchone()[0]
    except sqlite3.OperationalError:
        return False  # no _ops table yet

    if last_graph is None:
        return True  # never built

    try:
        new_sessions = conn.execute("""
            SELECT COUNT(DISTINCT source_id) FROM _raw_sources
            WHERE end_time > ?
        """, (last_graph,)).fetchone()[0]
    except sqlite3.OperationalError:
        return False

    return new_sessions >= threshold


def _run_enrichment_cycle(conn, graph_threshold=50):
    """Run the enrichment cycle: graph (if stale), fingerprints, repo_project."""
    t0 = time.time()

    # 1. Graph rebuild if stale
    if rebuild_source_graph and _cc_graph_stale(conn, graph_threshold):
        print("[enrich] Graph stale — rebuilding...", file=sys.stderr)
        try:
            if rebuild_warmup_types:
                rebuild_warmup_types(conn)
            if reembed_sources:
                reembed_sources(conn)
            rebuild_source_graph(conn)
            print(f"[enrich] Graph rebuilt in {time.time()-t0:.1f}s", file=sys.stderr)
        except Exception as e:
            print(f"[enrich] Graph error: {e}", file=sys.stderr)

    # 2. Incremental fingerprints
    if run_fingerprints:
        try:
            n = run_fingerprints(conn)
            if n > 0:
                print(f"[enrich] {n} sessions fingerprinted", file=sys.stderr)
        except Exception as e:
            print(f"[enrich] Fingerprint error: {e}", file=sys.stderr)

    # 3. Incremental repo_project
    if run_repo_project:
        try:
            n = run_repo_project(conn)
            if n > 0:
                print(f"[enrich] {n} sources attributed", file=sys.stderr)
        except Exception as e:
            print(f"[enrich] Repo project error: {e}", file=sys.stderr)

    elapsed = time.time() - t0
    if elapsed > 1.0:
        print(f"[enrich] Cycle done in {elapsed:.1f}s", file=sys.stderr)


def daemon_loop(interval=2):
    """Main daemon loop."""
    # Resolve cell
    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[worker] FATAL: claude_code cell not found in registry", file=sys.stderr)
        sys.exit(1)

    print("[flex-worker] Starting chunk-atom daemon", file=sys.stderr)
    print(f"  Target: {cell_path}", file=sys.stderr)
    print(f"  Queue: {QUEUE_DB}", file=sys.stderr)
    print(f"  Interval: {interval}s", file=sys.stderr)

    # Pre-warm embedder
    get_embedder()

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Ensure all tables exist (fresh cell bootstrap)
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)

    # Ensure SOMA identity tables exist
    if soma_ensure_tables:
        soma_ensure_tables(conn)

    # Startup backfill for sessions missed during outage
    startup_backfill(conn)

    print("  Docpac: incremental indexing enabled", file=sys.stderr)

    BACKFILL_INTERVAL = 24 * 3600   # 24 hours — expensive JSONL scan
    ENRICHMENT_INTERVAL = 30 * 60   # 30 minutes — graph, fingerprints, repo_project
    GRAPH_STALENESS_THRESHOLD = 50  # sessions since last graph build

    last_backfill = time.time()
    last_enrichment = 0  # run enrichment immediately after first startup

    while True:
        try:
            stats = process_queue(conn)
            if stats['processed'] > 0:
                print(f"[worker] sessions={stats['processed']} emb={stats['embedded']}",
                      file=sys.stderr)
        except Exception as e:
            print(f"[worker] Error: {e}", file=sys.stderr)

        # Drain docpac pending queue (same embedder, different cell connections)
        if docpac_process_queue:
            try:
                dp_stats = docpac_process_queue(encode)
                if dp_stats['indexed'] > 0:
                    print(f"[docpac] indexed={dp_stats['indexed']} skipped={dp_stats['skipped']}",
                          file=sys.stderr)
            except Exception as e:
                print(f"[docpac] Error: {e}", file=sys.stderr)

        # Periodic backfill + heal — catch anything hooks missed (24h cycle)
        if time.time() - last_backfill > BACKFILL_INTERVAL:
            try:
                print("[worker] Periodic backfill (24h cycle)...", file=sys.stderr)
                startup_backfill(conn)
                if soma_heal:
                    soma_heal(conn)
                last_backfill = time.time()
            except Exception as e:
                print(f"[worker] Backfill error: {e}", file=sys.stderr)

        # Enrichment cycle — graph, fingerprints, repo_project (30min)
        if time.time() - last_enrichment > ENRICHMENT_INTERVAL:
            try:
                _run_enrichment_cycle(conn, GRAPH_STALENESS_THRESHOLD)
                last_enrichment = time.time()
            except Exception as e:
                print(f"[worker] Enrichment error: {e}", file=sys.stderr)
                last_enrichment = time.time()  # don't retry immediately

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
