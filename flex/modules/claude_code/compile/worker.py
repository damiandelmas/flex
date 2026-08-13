#!/usr/bin/env python3
"""
Flex Worker — session capture and indexing for Claude Code.

Scans ~/.claude/projects/ for JSONL session files, syncs new content
into the claude_code cell, embeds chunks, and runs enrichment.
"""

import hashlib
import os
import re
import sqlite3
import json
import subprocess
import time
import sys
import struct
from pathlib import Path
from datetime import datetime

import uuid as _uuid
from flex.registry import resolve_cell, register_cell, FLEX_HOME
from flex.core import log_op
from flex.views import install_views
from flex.onnx.embed import get_model, encode
from flex.modules.claude_code.compile.soft_detect import detect_file_ops
from flex.modules.claude_code.compile.scope import excluded_tool as _excluded_tool
from flex.compile.edges_schema import ensure_edges_source, ensure_edges_delegations
_secondary_cell_drainer = None
_corpus_drainer = None
_corpus_path_drainer = None
_corpus_graph_refresher = None
try:
    from flex.modules.engines import drain_local_cells as _secondary_cell_drainer
    from flex.modules.engines import drain_corpus as _corpus_drainer
    from flex.modules.engines import drain_corpus_paths as _corpus_path_drainer
    from flex.modules.engines import refresh_corpus_graphs as _corpus_graph_refresher
except ImportError:
    pass

# The aggregate lifecycle integration is optional. The filesystem compiler owns
# a narrow fallback so watched cells still receive event invalidations and
# periodic reconciliation in minimal installations.
_filesystem_scanner = None
_filesystem_path_drainer = None
try:
    from flex.modules.fs.compile.worker import (
        drain_filesystem_invalidations as _filesystem_path_drainer,
        scan_filesystem_cells as _filesystem_scanner,
    )
except ImportError:
    pass

_markdown_scanner = None
try:
    from flex.modules.markdown.compile.worker import scan_markdown_cells as _markdown_scanner
except ImportError:
    pass

# Reserved compatibility seam for the retired aggregate coding-agent scanner.
# The public worker no longer owns that broad scan, but keeping the optional
# symbol makes lifecycle integrations able to disable every scanner uniformly.
_coding_agent_scanner = None

FILESYSTEM_RECONCILE_INTERVAL = float(
    os.environ.get("FLEX_CORPUS_RECONCILE_INTERVAL_S", "45")
)

# Capture parses; the bounded per-tick sweep embeds. Embedding is slow by nature
# (sequence length is the wall — ~30-46 chunks/s regardless of device or thread
# count) and it ran INSIDE capture, holding the single tick loop for as long as it
# took. Every later phase — the NULL sweep, the corpus reconcile, enrichment — sits
# behind capture in the loop, so one large session froze the whole engine: cells
# stopped refreshing for three days while the worker burned 4 cores and said nothing.
# The sweep already exists and is already budgeted for exactly this ("a large backlog
# cannot monopolize the 2s loop"); capture was the one path violating that contract.
# Set FLEX_CAPTURE_INLINE_EMBED=1 to restore the pre-0.52 inline behavior.
CAPTURE_INLINE_EMBED = os.environ.get("FLEX_CAPTURE_INLINE_EMBED", "").lower() in ("1", "true", "yes")

# First batch of a deadline-bounded sweep, before throughput has been measured.
# Small on purpose: it is the one batch whose duration the bound cannot predict,
# and a full-size probe overruns the entire budget before learning the rate.
_EMBED_PROBE_BATCH = int(os.environ.get("FLEX_EMBED_PROBE_BATCH", "8"))

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
        rebuild_community_labels, rebuild_file_graph, rebuild_delegation_graph,
        rebuild_chunk_rollup,
    )
except ImportError:
    rebuild_source_graph = None
    rebuild_warmup_types = None
    reembed_sources = None
    rebuild_community_labels = None
    rebuild_file_graph = None
    rebuild_delegation_graph = None
    rebuild_chunk_rollup = None

CLAUDE_PROJECTS = Path.home() / ".claude/projects"

# Reconciliation cadence when filesystem events are enabled (flex.watch owns
# the same default for the observer side — kept here too so worker.py has no
# hard import-time dependency on flex.watch, whose Watcher/observer machinery
# imports watchdog lazily).
DEFAULT_RECONCILE_INTERVAL = 60.0

# Warmup threshold — imported at module level for fast per-session checks
try:
    from flex.modules.claude_code.manage.noise import WARMUP_MESSAGE_THRESHOLD
except ImportError:
    WARMUP_MESSAGE_THRESHOLD = 5


def _update_warmup(conn: sqlite3.Connection, session_id: str):
    """Reactive warmup classification for a single session.

    Called after every sync_session_messages(). If the session has grown
    past the warmup threshold, remove it from the warmup table (flip OFF).
    If it's below threshold, mark it as warmup (flip ON).
    """
    row = conn.execute(
        "SELECT message_count FROM _raw_sources WHERE source_id = ?",
        (session_id,)
    ).fetchone()
    if row is None:
        return
    is_warmup = 1 if row[0] < WARMUP_MESSAGE_THRESHOLD else 0
    conn.execute("""
        INSERT INTO _types_source_warmup (source_id, is_warmup_only)
        VALUES (?, ?)
        ON CONFLICT(source_id) DO UPDATE SET is_warmup_only = excluded.is_warmup_only
    """, (session_id, is_warmup))


# View directory resolution for auto-sync (user library takes precedence over stock)
_USER_VIEW_DIR  = FLEX_HOME / 'views' / 'claude_code'
_STOCK_VIEW_DIR = Path(__file__).parent.parent / 'stock' / 'views'

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


def _encode_for_cell(conn: sqlite3.Connection, texts):
    """Embed documents in the vector space declared by this cell's tag.

    Coding-agent ingestion predates the shared tag-aware pipeline and used the
    bundled MiniLM singleton directly. Once a cell is converted to Nomic that
    silently creates a mixed-width cell on every subsequent watch tick. Keep
    the legacy fast path for untagged/MiniLM cells, but resolve every explicit
    Nomic tag through the same fail-closed ingest resolver as ``embed_new``.
    """
    from flex.compile.embed import ensure_initial_vector_contract, _resolve_ingest_target

    tag = ensure_initial_vector_contract(conn)
    if tag in (None, 'minilm'):
        return encode(texts)
    embed_doc = _resolve_ingest_target(conn)[0]
    return embed_doc(texts, batch_size=64)


# JSONL path cache — one rglob populates, subsequent lookups are O(1).
# Invalidated by find_jsonl(session_id, bust_cache=True) or after 60s.
_jsonl_cache: dict[str, Path] = {}
_jsonl_cache_ts: float = 0


def find_jsonl(session_id: str, bust_cache: bool = False) -> Path | None:
    """Resolve session_id to its JSONL path. O(1) after first call."""
    global _jsonl_cache, _jsonl_cache_ts
    if not _jsonl_cache or bust_cache or (time.time() - _jsonl_cache_ts > 60):
        _jsonl_cache = {j.stem: j for j in CLAUDE_PROJECTS.rglob("*.jsonl")}
        _jsonl_cache_ts = time.time()
    return _jsonl_cache.get(session_id)


try:
    from flex.modules.soma.lib.git import git_root_from_path as _git_root_from_path
    from flex.modules.soma.lib.git import project_from_git_root as _project_from_git_root
except ImportError:
    import subprocess as _subprocess
    from pathlib import Path as _Path
    _GENERIC_DIR_NAMES = {'main', 'master', 'dev', 'staging', 'prod', 'context', 'sandbox'}

    def _git_root_from_path(path: str) -> str | None:
        p = _Path(path)
        check = p if p.is_dir() else p.parent
        if not check.exists():
            return None
        try:
            r = _subprocess.run(
                ["git", "-C", str(check), "rev-parse", "--show-toplevel"],
                capture_output=True, text=True, timeout=3
            )
            return r.stdout.strip() or None if r.returncode == 0 else None
        except Exception:
            return None

    def _project_from_git_root(git_root: str) -> str:
        p = _Path(git_root)
        parts = p.parts
        if 'worktrees' in parts:
            idx = parts.index('worktrees')
            if idx > 0:
                return parts[idx - 1]
        if p.name in _GENERIC_DIR_NAMES and p.parent.name:
            return p.parent.name
        return p.name


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
        if cwd:
            cur.execute("""
                UPDATE _raw_sources
                SET primary_cwd = ?
                WHERE source_id = ?
                  AND (primary_cwd IS NULL OR primary_cwd = '')
            """, (cwd, session_id))
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
            git_root TEXT,
            fork_count INTEGER DEFAULT 0
        );

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
            entry_uuid TEXT,
            branch_id INTEGER DEFAULT 0
        );
        -- Index the mask path: type(...) resolves to _types_message.type. Without
        -- this it is a full SCAN (~33ms on 551K); with it, a covering range seek
        -- (~2ms). Pays off only when the mask queries this base table directly,
        -- not via the chunks view (which CASE-computes type and can't use it).
        CREATE INDEX IF NOT EXISTS idx_types_message_type ON _types_message(type);

        -- Every "recent activity" query (the digest preset, freshness checks) filters
        -- _raw_chunks.timestamp through the messages/chunks views, which start FROM
        -- _raw_chunks with nothing to prune on. Without this it is a full SCAN: measured
        -- 48.8s on codex (929K rows) and 93.0s on claude_code (1.05M) -> 0.14s / 0.22s.
        CREATE INDEX IF NOT EXISTS idx_raw_chunks_timestamp ON _raw_chunks(timestamp);

        -- 1:1 rollup of _edges_file_identity (1:many) + _edges_delegations,
        -- keyed by chunk_id. The chunks/messages views PK-probe this instead
        -- of inlining aggregate GROUP BY subqueries (which SQLite MATERIALIZEs
        -- in full before applying any predicate — see chunk_rollup.py).
        -- Kept fresh at ingest by _upsert_chunk_rollup(); rebuilt from scratch
        -- each enrichment cycle by rebuild_chunk_rollup(). 6-column shape
        -- created directly (not the legacy 4-column one) so a brand-new
        -- cell never needs the ALTER-TABLE migration in
        -- views.py::_ensure_chunk_rollup_fresh — that migration path exists
        -- only for cells that predate the type/ext columns.
        CREATE TABLE IF NOT EXISTS _enrich_chunk_rollup (
            chunk_id TEXT PRIMARY KEY,
            file_uuids TEXT,
            child_session_id TEXT,
            agent_type TEXT,
            type TEXT,
            ext TEXT
        );

        CREATE TABLE IF NOT EXISTS _edges_soft_ops (
            id INTEGER PRIMARY KEY,
            chunk_id TEXT,
            file_path TEXT,
            file_uuid TEXT,
            inferred_op TEXT,
            confidence TEXT
        );

        CREATE TABLE IF NOT EXISTS _file_body_index (
            target_file TEXT PRIMARY KEY,
            content_hash TEXT NOT NULL,
            parent_chunk_id TEXT NOT NULL,
            chunk_count INTEGER,
            updated_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS _types_file_body (
            chunk_id TEXT PRIMARY KEY,
            target_file TEXT NOT NULL,
            title TEXT,
            position INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_tfb_file ON _types_file_body(target_file);

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
    ensure_edges_source(conn, 'claude-code')
    ensure_edges_delegations(conn)
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
            CREATE TRIGGER raw_chunks_au AFTER UPDATE OF content ON _raw_chunks BEGIN
                INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
                INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
        """)

    # Scope the FTS update trigger to `content` (migration — the block above is
    # guarded on trigger existence, so a DDL edit alone is inert on every cell
    # already on disk). Unscoped, it fires on ANY column update, so the embed
    # sweep's `UPDATE _raw_chunks SET embedding=?` re-tokenized the full chunk
    # into FTS: 1 FTS write per chunk became 3 (insert, then delete+reinsert).
    # That was survivable while the sweep only caught orphans; now that it is the
    # steady-state embed path it would be the norm. FTS indexes content — an
    # embedding write is not a content change.
    au_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='raw_chunks_au'"
    ).fetchone()
    if au_sql and au_sql[0] and "UPDATE OF content" not in au_sql[0]:
        conn.executescript("""
            DROP TRIGGER IF EXISTS raw_chunks_au;
            CREATE TRIGGER raw_chunks_au AFTER UPDATE OF content ON _raw_chunks BEGIN
                INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
                INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
        """)

    # Bring _enrich_chunk_rollup to the current schema (6 columns, type
    # index, default-row trigger) unconditionally here — the one choke
    # point every ingest path runs through before touching _raw_chunks.
    from flex.modules.claude_code.manage.chunk_rollup import ensure_rollup_schema
    ensure_rollup_schema(conn)
    from flex.modules.claude_code.manage.observations import ensure_observation_schema
    ensure_observation_schema(conn)
    conn.commit()


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
            ordinal INTEGER,
            role TEXT,
            PRIMARY KEY (chunk_id, content_hash)
        )
    """)
    # Older cells predate ordinal/role — add them in place so one writer serves both.
    _erc_cols = {r[1] for r in conn.execute("PRAGMA table_info(_edges_raw_content)")}
    for _col, _decl in (("ordinal", "INTEGER"), ("role", "TEXT")):
        if _col not in _erc_cols:
            conn.execute(f"ALTER TABLE _edges_raw_content ADD COLUMN {_col} {_decl}")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_edges_raw_content_hash
        ON _edges_raw_content(content_hash)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_erc_chunk_role
        ON _edges_raw_content(chunk_id, role, ordinal)
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
        "INSERT OR IGNORE INTO _raw_content "
        "(hash, content, tool_name, byte_length, first_seen) VALUES (?,?,?,?,?)",
        (h, raw, tool_name, len(raw), ts)
    )
    # ordinal = capture order within the chunk; role labels what the body IS.
    # One chunk may carry several bodies: call args, the payload, then exit status.
    ordinal = conn.execute(
        "SELECT COUNT(*) FROM _edges_raw_content WHERE chunk_id = ?", (chunk_id,)
    ).fetchone()[0]
    if tool_name in ('_file_backup', '_file_snapshot'):
        role = 'backup'
    elif tool_name and tool_name.endswith('_end'):
        role = 'status'
    elif ordinal == 0 and raw.lstrip()[:1] == '{':
        role = 'input'
    else:
        role = 'output'
    conn.execute(
        "INSERT OR IGNORE INTO _edges_raw_content (chunk_id, content_hash, ordinal, role) "
        "VALUES (?,?,?,?)",
        (chunk_id, h, ordinal, role)
    )


def _ingest_file_body(conn: sqlite3.Connection, parent_chunk_id: str,
                      target_file: str, content: str, session_id: str,
                      ts: int) -> int:
    """Chunk file body content from a Write tool and insert as sub-chunks.

    Sub-chunks get IDs like {parent_chunk_id}:fb:{position} and are inserted
    into _raw_chunks with embedding=NULL (picked up by _batch_embed_chunks).
    Content-hash dedup: same file rewritten keeps only the latest version.

    Returns number of sub-chunks inserted.
    """
    from flex.compile.chunkers import chunk_file_body, MIN_BODY_SIZE, MAX_BODY_SIZE

    if not content or len(content) < MIN_BODY_SIZE or len(content) > MAX_BODY_SIZE:
        return 0

    # Content-hash dedup — truncated SHA-256
    content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    existing = conn.execute(
        "SELECT content_hash, parent_chunk_id FROM _file_body_index WHERE target_file = ?",
        (target_file,)).fetchone()

    if existing and existing[0] == content_hash:
        return 0  # identical content, skip

    # Delete old fb sub-chunks for this file.
    #
    # GLOB, not LIKE. SQLite's LIKE is case-insensitive by default, which
    # DISABLES the index optimization — `WHERE id LIKE 'parent:fb:%'` is a full
    # SCAN of _raw_chunks (944k rows here), and this runs three times per file
    # body, i.e. per Read/Edit/Write tool result. GLOB is case-sensitive, so the
    # same prefix becomes `SEARCH ... USING COVERING INDEX (id>? AND id<?)`.
    # Verified with EXPLAIN QUERY PLAN: LIKE -> SCAN, GLOB -> SEARCH. Semantics
    # are identical here because the prefix is a literal chunk id; the only
    # metacharacters LIKE and GLOB disagree on (_ vs ?, [ ]) cannot appear in a
    # `{uuid}:{n}:{sha}` id, and `*` is inert inside the literal prefix.
    if existing:
        old_parent = existing[1]
        conn.execute(
            "DELETE FROM _raw_chunks WHERE id GLOB ?",
            (f"{old_parent}:fb:*",))
        conn.execute(
            "DELETE FROM _edges_source WHERE chunk_id GLOB ?",
            (f"{old_parent}:fb:*",))
        conn.execute(
            "DELETE FROM _types_file_body WHERE chunk_id GLOB ?",
            (f"{old_parent}:fb:*",))

    # Chunk by language
    chunks = chunk_file_body(content, target_file)

    # Insert sub-chunks
    inserted = 0
    for chunk in chunks:
        fb_id = f"{parent_chunk_id}:fb:{chunk['position']}"
        conn.execute(
            "INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp) "
            "VALUES (?,?,NULL,?)",
            (fb_id, chunk['content'], ts))
        conn.execute(
            "INSERT OR IGNORE INTO _edges_source "
            "(chunk_id, source_id, source_type, position) VALUES (?,?,'file-body',?)",
            (fb_id, session_id, chunk['position']))
        conn.execute(
            "INSERT OR IGNORE INTO _types_file_body "
            "(chunk_id, target_file, title, position) VALUES (?,?,?,?)",
            (fb_id, target_file, chunk['title'], chunk['position']))
        # These :fb: sub-chunks bypass insert_chunk_atom (they're inserted
        # directly above), so nothing else upserts their rollup row —
        # without this, the view's LEFT JOIN would silently return NULL
        # for their type instead of 'file' until the next enrichment cycle.
        _upsert_chunk_rollup(conn, fb_id)
        inserted += 1

    # Update index
    conn.execute(
        "INSERT OR REPLACE INTO _file_body_index "
        "(target_file, content_hash, parent_chunk_id, chunk_count, updated_at) "
        "VALUES (?,?,?,?,?)",
        (target_file, content_hash, parent_chunk_id, inserted, ts))

    return inserted


_LINE_NUM_RE = re.compile(r'^\s*\d+\t', re.MULTILINE)


def _strip_line_numbers(text: str) -> str:
    """Strip cat -n line number prefixes from Read tool output.

    Read results arrive as '    1\\tcontent\\n    2\\tcontent\\n...'.
    Returns clean source code for the AST/tree-sitter chunker.
    """
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


def _is_missing_table_error(exc: sqlite3.OperationalError, table: str) -> bool:
    """True only for 'no such table: <table>' — never locks, IO errors, etc."""
    return str(exc).strip() == f"no such table: {table}"


def _upsert_chunk_rollup(conn: sqlite3.Connection, chunk_id: str):
    """Keep _enrich_chunk_rollup fresh for one chunk at ingest time.

    Re-derives the rollup row from _edges_file_identity / _edges_delegations
    for this chunk_id — called right after either edge table is written for
    the chunk, so a chunk ingested now has correct file_uuids/child_session_id
    immediately, not after the next 30-minute enrichment cycle. Also
    (re)computes type/ext — every chunk gets a row now, since type/ext must
    be a real materialized value (indexable) for every chunk, not just
    edge-bearing ones.

    Missing-table errors (transitional cell states — SOMA disabled, or a
    cell that predates this rollup table) are expected and silently
    skipped. Anything else (lock timeout, IO error, corruption) is logged,
    not swallowed — a chunk silently missing its identity with zero trace
    is exactly the failure mode this function exists to prevent elsewhere.
    """
    file_uuids = None
    try:
        fi_count = conn.execute(
            "SELECT COUNT(*) FROM _edges_file_identity WHERE chunk_id = ?", (chunk_id,)
        ).fetchone()[0]
        if fi_count:
            file_uuids = conn.execute(
                "SELECT json_group_array(file_uuid) FROM _edges_file_identity WHERE chunk_id = ?",
                (chunk_id,)
            ).fetchone()[0]
    except sqlite3.OperationalError as e:
        if not _is_missing_table_error(e, '_edges_file_identity'):
            print(f"[worker] Chunk rollup file_uuids read error for {chunk_id}: {e}", file=sys.stderr)
        file_uuids = None

    try:
        deleg = conn.execute(
            "SELECT child_session_id, agent_type FROM _edges_delegations WHERE chunk_id = ? LIMIT 1",
            (chunk_id,)
        ).fetchone()
        child_session_id, agent_type = (deleg[0], deleg[1]) if deleg else (None, None)
    except sqlite3.OperationalError as e:
        if not _is_missing_table_error(e, '_edges_delegations'):
            print(f"[worker] Chunk rollup delegation read error for {chunk_id}: {e}", file=sys.stderr)
        child_session_id, agent_type = None, None

    # type/ext must be computed for EVERY chunk (the index over `type` is
    # only useful if 'chunk' is a real materialized value, not something
    # left NULL until the next enrichment cycle) — so, unlike file_uuids/
    # child_session_id/agent_type above, there is no early return here.
    # Same CASE logic as chunk_rollup.rebuild_chunk_rollup / the old
    # inline view expressions, single-row form.
    chunk_type, ext = 'chunk', None
    try:
        row = conn.execute("""
            SELECT
                CASE
                    WHEN fb.chunk_id IS NOT NULL THEN 'file'
                    WHEN tp.type IS NOT NULL THEN tp.type
                    ELSE 'chunk'
                END AS type,
                CASE
                    WHEN COALESCE(fb.target_file, t.target_file) LIKE '%.%'
                    THEN LOWER(SUBSTR(COALESCE(fb.target_file, t.target_file),
                        LENGTH(RTRIM(COALESCE(fb.target_file, t.target_file),
                        REPLACE(REPLACE(COALESCE(fb.target_file, t.target_file), '/', ''), '.', ''))) + 1))
                    ELSE ''
                END AS ext
            FROM (SELECT ? AS id) r
            LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
            LEFT JOIN _types_message tp ON r.id = tp.chunk_id
            LEFT JOIN _types_file_body fb ON r.id = fb.chunk_id
        """, (chunk_id,)).fetchone()
        if row:
            chunk_type, ext = row[0], row[1]
    except sqlite3.OperationalError as e:
        if not _is_missing_table_error(e, '_types_message') and \
           not _is_missing_table_error(e, '_types_file_body') and \
           not _is_missing_table_error(e, '_edges_tool_ops'):
            print(f"[worker] Chunk rollup type/ext read error for {chunk_id}: {e}", file=sys.stderr)
        chunk_type, ext = 'chunk', None

    try:
        conn.execute("""
            INSERT INTO _enrich_chunk_rollup
                (chunk_id, file_uuids, child_session_id, agent_type, type, ext)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chunk_id) DO UPDATE SET
                file_uuids = excluded.file_uuids,
                child_session_id = excluded.child_session_id,
                agent_type = excluded.agent_type,
                type = excluded.type,
                ext = excluded.ext
        """, (chunk_id, file_uuids, child_session_id, agent_type, chunk_type, ext))
    except sqlite3.OperationalError as e:
        if not _is_missing_table_error(e, '_enrich_chunk_rollup'):
            print(f"[worker] Chunk rollup write error for {chunk_id}: {e}", file=sys.stderr)
        # else: cell predates this table and _ensure_core_tables hasn't run
        # yet — next enrichment cycle's rebuild_chunk_rollup() picks it up.


def insert_chunk_atom(
    conn: sqlite3.Connection, chunk: dict, *, enrich_identity: bool = True,
) -> bool:
    """Insert a chunk into all chunk-atom tables; return whether it was new."""
    cur = conn.cursor()
    chunk_id = chunk['id']

    # _raw_chunks
    cur.execute("""
        INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
        VALUES (?, ?, ?, ?)
    """, (chunk_id, chunk['content'], chunk.get('embedding'), chunk['timestamp']))
    inserted_new = cur.rowcount > 0

    # _edges_source
    cur.execute("""
        INSERT OR IGNORE INTO _edges_source (chunk_id, source_id, source_type, position)
        VALUES (?, ?, 'claude-code', ?)
    """, (chunk_id, chunk['doc_id'], chunk['chunk_number']))

    # _types_message
    cur.execute("""
        INSERT OR IGNORE INTO _types_message
        (chunk_id, type, role, chunk_number, parent_uuid, is_sidechain, entry_uuid, branch_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (chunk_id, chunk['type'], chunk['role'], chunk['chunk_number'],
          chunk.get('parent_uuid'), chunk.get('is_sidechain'), chunk.get('entry_uuid'),
          chunk.get('branch_id', 0)))

    # _edges_tool_ops (only for tool calls)
    if chunk.get('tool_name'):
        cur.execute("""
            INSERT OR IGNORE INTO _edges_tool_ops (chunk_id, tool_name, target_file, success, cwd, git_branch)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chunk_id, chunk['tool_name'], chunk.get('target_file'),
              chunk.get('success'), chunk.get('cwd'), chunk.get('git_branch')))

    # SOMA identity edges (file_uuid, repo_root, content_hash, url_uuid)
    if enrich_identity and soma_insert_edges:
        soma_insert_edges(conn, chunk)

    # _edges_delegations (Task spawns)
    if chunk.get('spawned_agent'):
        ensure_source_exists(conn, chunk['spawned_agent'])
        cur.execute("""
            INSERT OR IGNORE INTO _edges_delegations (chunk_id, child_session_id, agent_type, created_at)
            VALUES (?, ?, ?, ?)
        """, (chunk_id, chunk['spawned_agent'], chunk.get('agent_type'), chunk['timestamp']))

    # _enrich_types: reserved for future semantic classification.
    # AI queries role + tool_name directly via curated views.

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

    # Keep the chunks/messages view rollup fresh — must run after the SOMA
    # and delegation edge writes above.
    _upsert_chunk_rollup(conn, chunk_id)
    return inserted_new



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

    # H7 — bound memory on pathological JSONL files. Anything over 512MB
    # streams line-by-line; smaller files still use readlines() for speed.
    _MAX_JSONL_BYTES = 512 * 1024 * 1024
    try:
        _jsize = Path(jsonl_path).stat().st_size
        with open(jsonl_path, 'r') as f:
            if _jsize > _MAX_JSONL_BYTES:
                print(f"[worker] {jsonl_path.name}: {_jsize // (1024*1024)}MB — streaming",
                      file=sys.stderr)
                lines = list(f)  # iterate file object (one line at a time)
            else:
                lines = f.readlines()
    except Exception:
        return 0

    # Claude records launch context directly in the JSONL. Source rows are
    # created before parsing so older capture left primary_cwd empty; recover
    # it cheaply from the first explicit cwd and preserve it independently of
    # whichever repositories the session later touches.
    from flex.modules.claude_code.manage.session_scope import first_cwd_from_lines
    session_cwd = first_cwd_from_lines(lines)
    if session_cwd:
        ensure_source_exists(conn, session_id, cwd=session_cwd, title=title)

    new_chunks = []
    tool_content_items = []   # (chunk_id, raw, tool_name, ts)
    tool_ops_items = []       # (chunk_id, tool_name, target_file, cwd, git_branch, success)
    soma_items = []           # (chunk_id, enrichment_dict)
    delegation_items = []     # (chunk_id, spawned_agent, ts, parent_sid, agent_type)
    soft_ops_items = []       # (chunk_id, SoftFileOp)
    fb_items = []             # (chunk_id, target_file, content, ts) for Write file bodies
    snapshot_content_items = []  # (msg_id, raw, tool_name, ts) — resolved to a chunk after the loop
    tool_use_id_map = {}      # tool_use.id -> tool_name
    tool_use_to_chunk = {}    # tool_use.id -> chunk_id (for delegation resolution)
    tool_use_agent_type = {}  # tool_use.id -> subagent_type (Task spawns only)
    _seen_delegations = set() # dedup: agent ids already captured from progress entries
    snapshot_hashes = {}      # messageId -> {filepath: git_blob_hash}
    all_uuids = set()         # every entry_uuid seen (for fork tree completeness)
    ua_parent_links = []      # (entry_uuid, parent_uuid) from user/assistant only

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

        # Track UUIDs for fork tree
        _eu = entry.get('uuid')
        _pu = entry.get('parentUuid')
        if _eu:
            all_uuids.add(_eu)
        # Only user/assistant entries participate in fork branching
        if entry_type in ('user', 'assistant') and _eu:
            ua_parent_links.append((_eu, _pu))

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
                    # H1 — strip any directory components from the JSONL-supplied
                    # backupFileName before using it, and verify the resolved
                    # path stays inside the per-session backup dir.
                    _safe_name = Path(backup_name).name
                    if not _safe_name or _safe_name.startswith('.'):
                        continue
                    _backup_root = (Path.home() / '.claude' / 'file-history' / session_id).resolve()
                    backup_path = (_backup_root / _safe_name).resolve()
                    try:
                        _ok_rel = backup_path.is_relative_to(_backup_root)
                    except AttributeError:
                        _ok_rel = str(backup_path).startswith(str(_backup_root))
                    if not _ok_rel:
                        continue
                    if backup_path.exists():
                        try:
                            content = backup_path.read_bytes()
                            header = f"blob {len(content)}\0".encode()
                            file_hashes[filepath] = hashlib.sha1(header + content).hexdigest()
                            # Store actual file content (pre-edit snapshot).
                            # Anchored to msg_id (the entry_uuid of the
                            # user/assistant line this snapshot belongs to),
                            # resolved to a real chunk_id after the loop —
                            # NOT this file-history-snapshot line's own
                            # chunk_id, which is never inserted into
                            # _raw_chunks (this entry_type always `continue`s
                            # below, before chunk construction). Storing
                            # under chunk_id here queued content/edges for a
                            # chunk that would never materialize (proven
                            # orphan mechanism, ~5% of the observed defect).
                            text = content.decode('utf-8', errors='replace')
                            snapshot_content_items.append((msg_id, text, '_file_backup', ts_int))
                        except Exception:
                            pass
                if file_hashes:
                    snapshot_hashes.setdefault(msg_id, {}).update(file_hashes)
            # Still store snapshot JSON in _raw_content for provenance
            if snapshot and isinstance(snapshot, dict) and msg_id:
                snapshot_content_items.append((msg_id, json.dumps(snapshot), '_file_snapshot', ts_int))
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

        # --- progress → extract delegation signals (first per agent), then skip ---
        if entry_type == 'progress':
            data = entry.get('data', {})
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    data = {}
            agent_id = data.get('agentId', '') if isinstance(data, dict) else ''
            parent_tuid = entry.get('parentToolUseID', '')
            if agent_id and parent_tuid and parent_tuid in tool_use_to_chunk:
                spawned = f"agent-{agent_id}"
                if spawned not in _seen_delegations:
                    _seen_delegations.add(spawned)
                    parent_chunk = tool_use_to_chunk[parent_tuid]
                    at = tool_use_agent_type.get(parent_tuid)
                    delegation_items.append((parent_chunk, spawned, ts_int, session_id, at))
            continue

        # --- system / other → skip ---
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
                    tool_use_id = item.get('id', '')
                    tool_use_id_map[tool_use_id] = tool_name
                    tool_use_to_chunk[tool_use_id] = chunk_id
                    if tool_name == 'Task':
                        tool_use_agent_type[tool_use_id] = tool_input.get('subagent_type')

                    # Scope exclusion — skip excluded tools
                    if _excluded_tool(tool_name):
                        continue

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

                    # Collect file body for Write tool sub-chunking
                    if tool_name == 'Write':
                        fb_content = tool_input.get('content', '')
                        if fb_content and target_file:
                            fb_items.append((chunk_id, target_file, fb_content, ts_int))

                elif item_type == 'tool_result':
                    tool_use_id = item.get('tool_use_id', '')
                    tool_name = tool_use_id_map.get(tool_use_id, 'unknown')
                    if _excluded_tool(tool_name):
                        continue
                    raw = _normalize_tool_result(item.get('content'))
                    if raw and len(raw) > 10:
                        # Anchor to the originating tool_use's chunk, not this
                        # reply line's own chunk_id. A 'user' line whose
                        # content is ONLY tool_result items (no text) never
                        # gets a _raw_chunks row — see the "Tool-only chunks"
                        # filter below, which only synthesizes a chunk for
                        # assistant lines carrying tool_use. Storing under
                        # chunk_id here queued content/edges for a chunk that
                        # would never materialize (proven orphan mechanism).
                        # The call's chunk already carries the input body
                        # (ordinal 0, role='input'); the result becomes its
                        # 'output' — same chunk, per _store_content_raw's
                        # existing ordinal/role design.
                        content_chunk_id = tool_use_to_chunk.get(tool_use_id, chunk_id)
                        tool_content_items.append((content_chunk_id, raw, tool_name, ts_int))

                    # File body sub-chunking for Read/Edit tool results
                    # Read returns full file content (cat -n format with line nums);
                    # Edit returns context snippet. Match back to tool_use to get
                    # target_file, strip line numbers, then feed through the same
                    # tree-sitter/AST chunker pipeline as Write.
                    if tool_name in ('Read', 'Edit') and raw:
                        result_chunk = tool_use_to_chunk.get(tool_use_id)
                        if result_chunk:
                            target_file = None
                            for ops_cid, ops_tn, ops_tf, *_ in tool_ops_items:
                                if ops_cid == result_chunk and ops_tn == tool_name:
                                    target_file = ops_tf
                                    break
                            if target_file and len(raw) > 50:
                                # Strip cat -n line number prefixes from Read output
                                # Format: "    1\tcontent" or "1\tcontent"
                                clean = _strip_line_numbers(raw) if tool_name == 'Read' else raw
                                fb_items.append((result_chunk, target_file, clean, ts_int))

                    # Detect Task delegations from tool_result
                    if tool_name == 'Task' and raw:
                        agent_match = re.search(r'agentId: ([a-f0-9]+)', raw)
                        if agent_match:
                            spawned = f"agent-{agent_match.group(1)}"
                            # Use the Task tool_use chunk, not the tool_result line
                            task_chunk = tool_use_to_chunk.get(tool_use_id, chunk_id)
                            at = tool_use_agent_type.get(tool_use_id)
                            delegation_items.append((task_chunk, spawned, ts_int, session_id, at))

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

    # --- Assign branch IDs (fork detection) ---
    # Walk the parentUuid tree built from ALL entry types (not just chunks).
    # Branch 0 = main trunk (first child at each fork point).
    # Each additional child at a fork point gets a new branch ID.
    fork_count = 0
    if new_chunks:
        # Build parent→children map from user/assistant entries only
        ua_children_of = {}  # parent_uuid -> [entry_uuids]
        ua_uuids = set()
        for eu, pu in ua_parent_links:
            ua_uuids.add(eu)
            if pu:
                ua_children_of.setdefault(pu, []).append(eu)

        # Fork points: any parent with >1 user/assistant child
        fork_points = {pu for pu, kids in ua_children_of.items() if len(kids) > 1}
        fork_count = len(fork_points)

        if fork_count > 0:
            # Assign branch IDs via BFS through the user/assistant tree.
            # Roots = ua entries whose parent is not another ua entry
            # (parent may be system/progress/absent — all treated as root)
            from collections import deque
            branch_counter = 0
            uuid_branch = {}  # entry_uuid -> branch_id

            root_uuids = [eu for eu, pu in ua_parent_links
                          if not pu or pu not in ua_uuids]
            for ru in root_uuids:
                if ru not in uuid_branch:
                    uuid_branch[ru] = 0

            queue = deque(root_uuids)
            while queue:
                current = queue.popleft()
                if current not in ua_children_of:
                    continue
                kids = ua_children_of[current]
                for j, kid in enumerate(kids):
                    if kid in uuid_branch:
                        continue
                    if j == 0:
                        uuid_branch[kid] = uuid_branch.get(current, 0)
                    else:
                        branch_counter += 1
                        uuid_branch[kid] = branch_counter
                    queue.append(kid)

            # Entries whose parent is non-ua but shares that parent with
            # another ua entry → they're fork siblings, not independent roots.
            # Group roots by their parentUuid and assign branches.
            roots_by_parent = {}
            for eu, pu in ua_parent_links:
                if pu and pu not in ua_uuids and pu in fork_points:
                    roots_by_parent.setdefault(pu, []).append(eu)
            for pu, siblings in roots_by_parent.items():
                for j, sib in enumerate(siblings):
                    if j == 0:
                        uuid_branch[sib] = 0  # first stays trunk
                    else:
                        branch_counter += 1
                        uuid_branch[sib] = branch_counter
                    # Re-propagate from this node
                    queue.append(sib)
                    while queue:
                        current = queue.popleft()
                        if current not in ua_children_of:
                            continue
                        for k, kid in enumerate(ua_children_of[current]):
                            if kid in uuid_branch:
                                continue
                            uuid_branch[kid] = uuid_branch[current]
                            queue.append(kid)

            # Apply branch IDs to chunks
            for chunk in new_chunks:
                eu = chunk.get('entry_uuid')
                if eu and eu in uuid_branch:
                    chunk['branch_id'] = uuid_branch[eu]

            # Update source fork_count
            cur.execute("""
                UPDATE _raw_sources SET fork_count = ?
                WHERE source_id = ?
            """, (fork_count, session_id))

    # --- Embed and insert chunks ---
    inserted = 0
    if new_chunks:
        if skip_embed:
            # Phase 1 of decoupled backfill: insert without embeddings
            for chunk in new_chunks:
                try:
                    if insert_chunk_atom(conn, chunk):
                        update_source_stats(conn, chunk['doc_id'], chunk)
                        inserted += 1
                except Exception as e:
                    print(f"[worker] Chunk insert error: {e}", file=sys.stderr)
        else:
            texts = [c['content'] for c in new_chunks]
            embeddings = _encode_for_cell(conn, texts)

            for chunk, emb in zip(new_chunks, embeddings):
                try:
                    chunk['embedding'] = serialize_f32(emb)
                    if insert_chunk_atom(conn, chunk):
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
    for chunk_id, spawned_agent, ts, parent_sid, agent_type in delegation_items:
        try:
            ensure_source_exists(conn, spawned_agent)
            cur.execute("""
                INSERT OR IGNORE INTO _edges_delegations
                (chunk_id, child_session_id, agent_type, created_at, parent_source_id)
                VALUES (?, ?, ?, ?, ?)
            """, (chunk_id, spawned_agent, agent_type, ts, parent_sid))
        except Exception as e:
            print(f"[worker] Delegation insert error: {e}", file=sys.stderr)

    # --- Keep chunk rollup fresh for chunks whose SOMA/delegation edges were
    # just written above (these chunk_ids are resolved late, via tool_use_id
    # matching, so insert_chunk_atom ran before these edges existed). ---
    _rollup_chunk_ids = {c for c, _ in soma_items} | {c for c, *_ in delegation_items}
    for chunk_id in _rollup_chunk_ids:
        try:
            _upsert_chunk_rollup(conn, chunk_id)
        except Exception as e:
            print(f"[worker] Chunk rollup upsert error: {e}", file=sys.stderr)

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

    # --- Store file-history-snapshot content, anchored to the real chunk ---
    # snapshot_content_items carries (msg_id, ...) where msg_id is the
    # entry_uuid of the user/assistant line the snapshot belongs to — resolve
    # it to that line's actual chunk_id now that new_chunks is final. Falls
    # back to _types_message for snapshots whose owning line was synced in an
    # earlier tick (this session resumes mid-JSONL). If neither resolves —
    # the owning line was itself filtered out and never became a chunk —
    # skip storing rather than anchor to a chunk_id that will never exist.
    if snapshot_content_items:
        _uuid_to_chunk = {c['entry_uuid']: c['id'] for c in new_chunks if c.get('entry_uuid')}
        for msg_id, raw, tool_name, ts in snapshot_content_items:
            try:
                target_chunk_id = _uuid_to_chunk.get(msg_id)
                if not target_chunk_id:
                    row = cur.execute(
                        "SELECT chunk_id FROM _types_message WHERE entry_uuid = ? LIMIT 1",
                        (msg_id,)
                    ).fetchone()
                    target_chunk_id = row[0] if row else None
                if not target_chunk_id:
                    continue
                _store_content_raw(conn, target_chunk_id, raw, tool_name, ts)
            except Exception as e:
                print(f"[worker] Snapshot content store error: {e}", file=sys.stderr)

    # --- Ingest file body sub-chunks for Write tools ---
    for parent_id, tfile, fb_content, ts in fb_items:
        try:
            _ingest_file_body(conn, parent_id, tfile, fb_content, session_id, ts)
        except Exception as e:
            print(f"[worker] File body ingest error: {e}", file=sys.stderr)

    # Projection maintenance happens after tool/content/SOMA writes so the row
    # is a committed observation, not an early guess from the message chunk.
    from flex.modules.claude_code.manage.observations import upsert_observation
    for chunk_id in {c['id'] for c in new_chunks}:
        try:
            upsert_observation(conn, chunk_id)
        except Exception as e:
            print(f"[worker] Observation upsert error for {chunk_id}: {e}", file=sys.stderr)

    # Clean up empty source stubs — if we parsed the JSONL but produced zero
    # chunks (e.g. file-history-snapshot only), remove the source row so it
    # doesn't pollute session counts and graph coverage metrics.
    if inserted == 0 and last_num == 0:
        cur.execute("""
            DELETE FROM _raw_sources
            WHERE source_id = ? AND message_count = 0
        """, (session_id,))

    return inserted


def _sync_one_jsonl(
    conn: sqlite3.Connection,
    jsonl: Path,
    size_cache: dict,
    error_cache: dict,
    now: float,
) -> tuple[int, int, int, float | None]:
    """Shared per-path sync primitive.

    Both the full-tree reconciliation scan (scan_sessions) and the
    path-targeted event sync (sync_session_paths) call this — identical
    size-cache and error-backoff semantics on both routes, per the
    technical contract. Symlinks are rejected here so neither route can be
    tricked into following one.

    Returns (synced_delta, chunks, failed_delta, retry_after). The final value
    is non-None only when an earlier failure is still in backoff; it is debt,
    but not a new attempt failure.
    """
    if jsonl.is_symlink():
        return (0, 0, 0, None)
    try:
        current_size = jsonl.stat().st_size
    except (FileNotFoundError, OSError):
        return (0, 0, 0, None)

    session_id = jsonl.stem
    last_size = size_cache.get(session_id, -1)
    error_state = error_cache.get(session_id)

    if current_size == last_size:
        return (0, 0, 0, None)  # unchanged — skip
    if (
        error_state
        and error_state.get("size") == current_size
        and now < error_state.get("retry_after", 0)
    ):
        # Backoff is deliberately not success. Surface it separately so both
        # event and reconciliation callers retain debt without pretending a
        # new attempt failed on every daemon heartbeat.
        return (0, 0, 0, error_state["retry_after"])

    # New or grown file — sync it. Parse only: the chunks land with embedding=NULL
    # and the tick's bounded sweep embeds them. Capture must never block the loop.
    try:
        count = sync_session_messages(session_id, conn, skip_embed=not CAPTURE_INLINE_EMBED)
        _update_warmup(conn, session_id)
        size_cache[session_id] = current_size
        error_cache.pop(session_id, None)
        return (1 if count > 0 else 0, count, 0, None)
    except Exception as e:
        failures = int(error_state.get("failures", 0)) + 1 if error_state else 1
        delay = min(300, 5 * (2 ** min(failures - 1, 6)))
        error_cache[session_id] = {
            "size": current_size,
            "failures": failures,
            "retry_after": now + delay,
        }
        print(
            f"[worker] sync error {session_id[:12]}: {e} "
            f"(retry in {delay}s)",
            file=sys.stderr,
        )
        return (0, 0, 1, None)


def scan_sessions(conn: sqlite3.Connection, size_cache: dict, error_cache: dict | None = None) -> dict:
    """Scan all JSONLs by file size, sync only those that grew.

    Replaces the old queue-drain and startup-backfill paths. Pure stat()-based
    polling — the Filebeat pattern. Also serves as the periodic reconciliation
    pass when filesystem events are enabled: same full-tree scan, just run on
    a slower cadence instead of every tick.

    Args:
        conn: Open cell connection.
        size_cache: Mutable dict {session_id: last_synced_size}. Persisted in
                    memory across ticks. On first call, empty dict triggers
                    full initial scan.

    Returns:
        dict with 'synced' (sessions touched), 'chunks' (new chunks inserted),
        'failed' (new attempt errors), and delayed backoff debt fields.
    """
    synced = 0
    chunks = 0
    failed = 0
    backoff_pending = 0
    next_retry_after = float('inf')
    error_cache = error_cache if error_cache is not None else {}
    now = time.monotonic()

    for jsonl in CLAUDE_PROJECTS.rglob("*.jsonl"):
        s, c, f, retry_after = _sync_one_jsonl(
            conn, jsonl, size_cache, error_cache, now,
        )
        synced += s
        chunks += c
        failed += f
        if retry_after is not None:
            backoff_pending += 1
            next_retry_after = min(next_retry_after, retry_after)
        if s:
            # Chunked commit. On a cold start the size_cache is empty, so
            # the first tick syncs the entire backlog (e.g. 6 days down =
            # ~1400 grown files) in ONE transaction that only committed at
            # end-of-scan — all-or-nothing, invisible until done, unbounded
            # WAL, and any kill mid-scan loses everything and restarts from
            # scratch. Commit every 50 synced sessions so catch-up is
            # incremental, crash-safe, and observable. Steady-state ticks
            # sync 1-2 files and hit the end-of-loop commit as before.
            if synced % 50 == 0:
                conn.commit()

    if chunks > 0:
        conn.commit()

    result = {'synced': synced, 'chunks': chunks, 'failed': failed}
    if backoff_pending:
        result.update(
            backoff_pending=backoff_pending,
            next_retry_after=next_retry_after,
        )
    return result


def sync_session_paths(
    conn: sqlite3.Connection,
    paths,
    size_cache: dict,
    error_cache: dict | None = None,
) -> dict:
    """Path-targeted Claude Code sync — the event-driven counterpart to
    scan_sessions(). Calls the same _sync_one_jsonl() primitive, so event
    delivery and full reconciliation share identical size-cache and
    error-backoff semantics.

    Re-validates every path itself (regular file, non-symlink, .jsonl,
    resolved beneath CLAUDE_PROJECTS) rather than trusting the caller —
    this function rejects paths outside CLAUDE_PROJECTS even if an
    invalidation was forged or a registration/pattern check upstream had a
    bug.

    Args:
        paths: iterable of candidate JSONL paths (str or Path).

    Returns:
        Same result shape as scan_sessions(), including delayed backoff debt.
    """
    synced = 0
    chunks = 0
    failed = 0
    backoff_pending = 0
    next_retry_after = float('inf')
    error_cache = error_cache if error_cache is not None else {}
    now = time.monotonic()

    try:
        claude_root = CLAUDE_PROJECTS.resolve()
    except OSError:
        return {'synced': 0, 'chunks': 0, 'failed': 0}

    for raw_path in paths:
        jsonl = Path(raw_path)
        if jsonl.suffix != '.jsonl':
            continue
        if jsonl.is_symlink():
            continue
        try:
            resolved = jsonl.resolve()
        except OSError:
            continue
        try:
            resolved.relative_to(claude_root)
        except ValueError:
            continue  # escapes CLAUDE_PROJECTS — reject even if it was "invalidated"
        if not resolved.is_file():
            continue  # deleted/moved away before we got to it — reconciliation will notice

        s, c, f, retry_after = _sync_one_jsonl(
            conn, resolved, size_cache, error_cache, now,
        )
        synced += s
        chunks += c
        failed += f
        if retry_after is not None:
            backoff_pending += 1
            next_retry_after = min(next_retry_after, retry_after)
        if s:
            # Same chunked-commit safety net as scan_sessions: a large event
            # backlog must not hold one unbounded uncommitted transaction.
            # Commit every 50 synced so catch-up is incremental and crash-safe.
            if synced % 50 == 0:
                conn.commit()

    if chunks > 0:
        conn.commit()

    result = {'synced': synced, 'chunks': chunks, 'failed': failed}
    if backoff_pending:
        result.update(
            backoff_pending=backoff_pending,
            next_retry_after=next_retry_after,
        )
    return result


# Compatibility wrapper for background-indexer callers.
def scan_primary_cell(conn: sqlite3.Connection) -> dict:
    """Preserve the historical return shape over stat-based scanning."""
    stats = scan_sessions(conn, _global_size_cache, _global_error_cache)
    return {'processed': stats['synced'], 'embedded': stats['chunks']}


# Backward-compatible alias for older callers.
def process_queue(conn: sqlite3.Connection) -> dict:
    return scan_primary_cell(conn)


# Global size cache for background-indexer callers.
_global_size_cache: dict = {}
_global_error_cache: dict = {}


_DEFAULT_CC_DESCRIPTION = (
    'Claude Code session provenance. Each doc is a session, '
    'each chunk is a tool call/prompt/response.'
)


def bootstrap_claude_code_cell(
    name: str = 'claude_code',
    cell_type: str = 'claude-code',
    description: str | None = None,
) -> Path:
    """Create a coding-agent cell with the CC canonical schema. Idempotent.

    Defaults preserve the original behavior — existing CC callers pass
    nothing and get a cell named 'claude_code' / cell_type='claude-code'.
    Compatible coding-agent modules pass their own name/cell_type to reuse
    the same substrate.
    """
    desc = description or _DEFAULT_CC_DESCRIPTION
    existing = resolve_cell(name)
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

    conn.execute("INSERT OR IGNORE INTO _meta VALUES ('description', ?)", (desc,))
    conn.execute("INSERT OR IGNORE INTO _meta VALUES ('cell_type', ?)", (cell_type,))
    conn.commit()
    conn.close()

    register_cell(name, str(db_path), cell_type=cell_type, description=desc)
    return db_path


def _batch_embed_chunks(conn, batch_size: int = 500, quiet: bool = False,
                        progress_cb=None, embedder=None,
                        max_chunks: int | None = None,
                        deadline: float | None = None) -> int:
    """Phase 2 of decoupled backfill: batch embed NULL-embedding chunks.

    SELECT content WHERE embedding IS NULL → encode(batch) → UPDATE.
    Commits after each batch. Returns total embedded count.

    batch_size defaults to 500 for local ONNX (GPU-friendly).
    Overridden automatically if embedder exposes ._batch_size (e.g. NomicEmbedder=64).

    Args:
        progress_cb: Optional callback(done, total) called after each batch.
        embedder: Optional embedder instance (e.g. NomicEmbedder). If None,
                  uses the default ONNX singleton via encode().
        max_chunks: Optional cap on chunks embedded in this call. None (the
                    default) preserves full-drain behavior for one-shot callers
                    (initial_backfill, CLI recompile) where draining the whole
                    backlog IS the job.
        deadline: Optional time.time()-style wall-clock deadline. None means
                  no time limit.

    ANY CALLER ON THE DAEMON TICK MUST PASS `deadline`. The tick is a single
    serial loop: an unbounded call here does not slow one phase, it stops every
    phase behind it, and the process looks alive and silent while it happens.

    This docstring used to name the "enrichment sweep" among the callers that
    deliberately kept full-drain behavior for compatibility. That caller runs
    INSIDE the tick. On 2026-07-16 it took the whole ~20k NULL backlog in one
    pass at ~12 chunks/s: ~28 minutes of frozen tick at 8.3GB RSS, printing
    nothing, and it took a py-spy dump to find. The compatibility default did
    not preserve behavior; it preserved a bug, and named it a feature.

    `max_chunks` is NOT a substitute: a count is not a clock. Per-chunk cost
    varies with sequence length, and 128 chunks measured 54s in the codex scan.
    The deadline is also only tested BETWEEN batches, so batch_size is the
    granularity of the bound — pass 64, not the 500 default, on the tick.
    """
    _enc = embedder.encode if embedder is not None else None
    # Use embedder's preferred batch size if it exposes one (e.g. NomicEmbedder=64)
    if embedder is not None and hasattr(embedder, '_batch_size'):
        batch_size = embedder._batch_size
    done = 0
    t0 = time.time()
    _rate: float | None = None   # measured chunks/s (EMA); sizes batches to the deadline

    total = conn.execute(
        "SELECT count(*) FROM _raw_chunks WHERE embedding IS NULL AND content IS NOT NULL"
    ).fetchone()[0]
    # Bounded calls report against the selected work allowance, not the
    # global backlog — a capped sweep must never claim it will finish the
    # whole backlog in this call.
    if max_chunks is not None:
        total = min(total, max_chunks)

    if progress_cb:
        progress_cb(0, total)

    while True:
        if max_chunks is not None and done >= max_chunks:
            break
        if deadline is not None and time.time() >= deadline:
            break

        limit = batch_size
        if max_chunks is not None:
            limit = min(limit, max_chunks - done)
            if limit <= 0:
                break

        # Size the batch to the time LEFT, not to a constant.
        #
        # The deadline is only tested here, between batches — so batch_size is the
        # granularity of the bound, and a batch that overruns cannot be stopped.
        # Per-chunk cost varies with sequence length (the wall is sequence length,
        # not matmul), and it varies a lot: ~12 chunks/s on claude_code sessions,
        # ~2.4/s on codex rollouts. At 2.4/s a 64-chunk batch runs 26s, so a 10s
        # budget was checked once every 26s and overshot by 2x every time
        # (embed_sweep_s=22.1s against 10s; codex=20.7s against its share).
        #
        # Measure the rate and take only what fits. A bound whose granularity is
        # a guess about throughput is a guess about the bound.
        if deadline is not None:
            _left = deadline - time.time()
            if _left <= 0:
                break
            if _rate is None:
                # No estimate yet, and the first batch cannot be sized by a rate it
                # has not measured. A full batch here overruns the whole budget
                # before learning anything (64 codex chunks = 26s against a 10s
                # budget). Probe small, then adapt: the seeding batch is the one
                # cost this bound cannot avoid, so make it the cheapest one.
                limit = min(limit, _EMBED_PROBE_BATCH)
            else:
                _fits = int(_rate * _left)
                if _fits < 1:
                    break      # not even one chunk fits: yield, resume next tick
                limit = min(limit, _fits)

        # Newest first. A chunk id is "{source_id}:{position}:{sha256[:12]}", so
        # ORDER BY id is a content hash — effectively random with respect to time,
        # which left a ten-minute-old session queued behind days of stale backlog.
        # Recency is what an agent actually searches; drain the useful end first and
        # let the tail trickle. (Dimension is not a lever here: Nomic is Matryoshka,
        # truncating after the full 768d forward pass, so a narrower vector costs the
        # same to produce. Priority is the only axis that buys anything.)
        rows = conn.execute("""
            SELECT id, content FROM _raw_chunks
            WHERE embedding IS NULL AND content IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,)).fetchall()

        if not rows:
            break

        texts = [r[1] for r in rows]
        _batch_t0 = time.time()
        embeddings = _enc(texts) if _enc is not None else _encode_for_cell(conn, texts)
        _batch_s = time.time() - _batch_t0

        conn.executemany(
            "UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
            [(serialize_f32(emb), r[0]) for emb, r in zip(embeddings, rows)]
        )
        conn.commit()
        done += len(rows)

        # Track throughput so the NEXT batch can be sized to the time left. EMA,
        # not last-batch: one atypically long chunk should bias the estimate, not
        # define it. Seeded from the first real batch — there is no useful prior
        # (12/s on sessions vs 2.4/s on codex rollouts is a 5x spread).
        if _batch_s > 0:
            _observed = len(rows) / _batch_s
            _rate = _observed if _rate is None else (0.7 * _rate + 0.3 * _observed)

        if progress_cb:
            progress_cb(done, total)
        elif not quiet:
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (total - done) / rate if rate > 0 else 0
            sys.stdout.write(
                f"\r  ~ {done:,}/{total:,} chunks embedded  ({rate:.0f}/s, ~{remaining:.0f}s left)    "
            )
            sys.stdout.flush()

    if not quiet and not progress_cb:
        elapsed = time.time() - t0
        if done > 0:
            rate = done / elapsed if elapsed > 0 else 0
            sys.stdout.write("\r" + " " * 70 + "\r")  # clear progress line
            print(f"  [ok] {done:,} chunks embedded in {elapsed:.0f}s ({rate:.0f}/s)")
    return done


def initial_backfill(conn, progress_cb=None, phase2_cb=None,
                     commit_every: int = 50, quiet_embed: bool = False,
                     embed_progress_cb=None, embedder_ref=None,
                     skip_embed: bool = False) -> dict:
    """Backfill all sessions with batched commits and progress.

    Decoupled two-phase approach:
      Phase 1 — parse all sessions, insert chunks with embedding=NULL (I/O bound)
      Phase 2 — batch embed all NULL chunks in one pass (CPU bound, 258 chunks/s)

    Args:
        conn: Open SQLite connection to the cell.
        progress_cb: Optional callback(files_done, files_total, sessions, chunks, elapsed).
        phase2_cb: Optional callback(sessions, chunks, elapsed) fired just before Phase 2
                   begins — lets the caller stop the Phase 1 spinner and print a header.
        commit_every: Commit after this many sessions (default 50). Higher = fewer fsyncs,
                      faster on overlay2/Docker. All inserts use INSERT OR IGNORE so
                      re-parsing uncommitted sessions on crash is safe.
        skip_embed: If True, skip Phase 2 entirely (model unavailable).

    Returns:
        dict with sessions, chunks, elapsed, embed_ok.
    """
    jsonls = list(CLAUDE_PROJECTS.rglob("*.jsonl"))

    # Skip already-indexed sessions — avoids re-parsing on resume after crash/cancel.
    already = {row[0] for row in conn.execute("SELECT source_id FROM _raw_sources").fetchall()}
    pending = [j for j in jsonls if j.stem not in already]
    total = len(pending)
    # Seed counters from existing cell so progress display is cumulative.
    sessions = len(already)
    chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    t0 = time.time()

    # Phase 1: parse pending sessions without embedding
    for i, jsonl in enumerate(pending, 1):
        session_id = jsonl.stem
        try:
            count = sync_session_messages(session_id, conn, skip_embed=True)
            if count > 0:
                chunks += count
                sessions += 1
        except Exception as e:
            print(f"[init] Error syncing {session_id[:8]}: {e}", file=sys.stderr)

        # Batch commits: amortise fsync cost across N sessions.
        # Safe because all inserts are INSERT OR IGNORE — re-parse on crash is idempotent.
        if i % commit_every == 0 or i == total:
            conn.commit()

        if progress_cb:
            progress_cb(i, total, sessions, chunks, time.time() - t0)

    # Phase 2: batch embed all NULL-embedding chunks (non-fatal)
    embed_ok = True
    if phase2_cb:
        phase2_cb(sessions, chunks, time.time() - t0)

    if skip_embed:
        embed_ok = False
    else:
        try:
            _embedder = embedder_ref[0] if embedder_ref is not None else None
            _batch_embed_chunks(conn, quiet=quiet_embed, progress_cb=embed_progress_cb,
                                embedder=_embedder)
        except Exception as e:
            print(f"[init] Embedding failed: {e}", file=sys.stderr)
            print("[init] Chunks saved. vec_ops disabled until re-embedded.", file=sys.stderr)
            conn.commit()  # persist whatever embeds landed
            embed_ok = False

    return {'sessions': sessions, 'chunks': chunks, 'elapsed': time.time() - t0,
            'embed_ok': embed_ok}


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


def _check_and_sync_views(conn: sqlite3.Connection) -> None:
    """Auto-install curated views if any .sql file is newer than last install."""
    view_dir = _USER_VIEW_DIR if _USER_VIEW_DIR.exists() else (
        _STOCK_VIEW_DIR if _STOCK_VIEW_DIR.exists() else None
    )
    if not view_dir:
        return
    row = conn.execute(
        "SELECT MAX(timestamp) FROM _ops WHERE operation = 'install_views'"
    ).fetchone()
    last_install = row[0] if row and row[0] else 0
    stale = any(f.stat().st_mtime > last_install for f in view_dir.glob('*.sql'))
    if stale:
        install_views(conn, view_dir)
        conn.commit()
        print(f"[worker] Auto-synced views from {view_dir}", file=sys.stderr)


def _heal_delegations(conn: sqlite3.Connection) -> int:
    """Backfill delegation edges for sessions synced before progress-entry detection.

    Finds sessions with Task tool_ops but no delegation edges, parses their
    JSONLs to extract delegation relationships, and inserts missing edges.
    Handles two JSONL formats:
      - New: agentId in progress entries, matched via parentToolUseID
      - Old: agentId in user entries (Task result rendered as user message),
             matched to preceding assistant Task tool_use by proximity
    Idempotent — unique index on (chunk_id, child_session_id) prevents dupes.
    """
    gap_sessions = conn.execute("""
        SELECT DISTINCT es.source_id
        FROM _edges_tool_ops t
        JOIN _edges_source es ON t.chunk_id = es.chunk_id
        WHERE t.tool_name = 'Task'
        AND es.source_id NOT IN (
            SELECT DISTINCT parent_source_id
            FROM _edges_delegations
            WHERE parent_source_id IS NOT NULL
        )
    """).fetchall()

    if not gap_sessions:
        return 0

    inserted = 0
    for (session_id,) in gap_sessions:
        jsonl_path = find_jsonl(session_id)
        if not jsonl_path or not jsonl_path.exists():
            continue

        try:
            with open(jsonl_path, 'r') as f:
                content = f.read()
        except Exception:
            continue

        if 'agentId' not in content:
            continue

        lines = content.split('\n')

        # Collect Task tool_use blocks: tool_use_id -> (line_num, ts)
        # and line_num -> tool_use_id for proximity matching
        tool_use_info = {}   # tool_use_id -> (line_num, ts_int, agent_type)
        task_lines = []      # [(line_num, tool_use_id, ts_int)] ordered

        for line_num_0, line in enumerate(lines):
            line_num = line_num_0 + 1
            if 'Task' not in line:
                continue
            try:
                e = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if e.get('type') != 'assistant':
                continue
            ts_int = 0
            timestamp = e.get('timestamp')
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    ts_int = int(dt.timestamp())
                except Exception:
                    pass
            msg = e.get('message', {})
            for block in msg.get('content', []):
                if isinstance(block, dict) and block.get('type') == 'tool_use' \
                        and block.get('name') == 'Task':
                    tuid = block.get('id', '')
                    at = block.get('input', {}).get('subagent_type')
                    tool_use_info[tuid] = (line_num, ts_int, at)
                    task_lines.append((line_num, tuid, ts_int))

        if not task_lines:
            continue

        # Strategy 1: progress entries (new format)
        agent_to_parent = {}
        for line in lines:
            if 'agentId' not in line or 'progress' not in line:
                continue
            try:
                e = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if e.get('type') != 'progress':
                continue
            data = e.get('data', {})
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, ValueError):
                    continue
            aid = data.get('agentId', '') if isinstance(data, dict) else ''
            ptuid = e.get('parentToolUseID', '')
            if aid and ptuid and aid not in agent_to_parent:
                agent_to_parent[aid] = ptuid

        # Strategy 2: user entries with "agentId: xxx" (old format)
        # Task results rendered as user messages with tool_result content.
        # Match each to the nearest preceding Task tool_use line.
        user_delegations = []  # (chunk_id, spawned, ts_int, session_id)
        for line_num_0, line in enumerate(lines):
            line_num = line_num_0 + 1
            if 'agentId' not in line:
                continue
            try:
                e = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if e.get('type') != 'user':
                continue
            # Search raw JSON — agentId can be nested in tool_result.content
            agent_match = re.search(r'agentId: ([a-f0-9]+)', line)
            if not agent_match:
                continue
            aid = agent_match.group(1)
            # Match via tool_use_id in tool_result, fall back to proximity
            msg = e.get('message', {})
            msg_content = msg.get('content', [])
            result_tuid = None
            if isinstance(msg_content, list):
                for item in msg_content:
                    if isinstance(item, dict) and item.get('type') == 'tool_result':
                        result_tuid = item.get('tool_use_id', '')
                        break
            best = None
            if result_tuid and result_tuid in tool_use_info:
                best = tool_use_info[result_tuid]  # (line_num, ts, at)
            else:
                for tl, tuid, ts in task_lines:
                    if tl < line_num:
                        best_at = tool_use_info.get(tuid, (None, None, None))[2]
                        best = (tl, ts, best_at)
            if best:
                chunk_id = f"{session_id}_{best[0]}"
                spawned = f"agent-{aid}"
                best_at = best[2] if len(best) > 2 else None
                user_delegations.append((chunk_id, spawned, best[1], session_id, best_at))

        # Merge: progress-based takes precedence, user-based fills remainder
        seen = set()
        delegation_pairs = []

        for aid, ptuid in agent_to_parent.items():
            if ptuid not in tool_use_info:
                continue
            line_num, ts_int, at = tool_use_info[ptuid]
            chunk_id = f"{session_id}_{line_num}"
            spawned = f"agent-{aid}"
            key = (chunk_id, spawned)
            if key not in seen:
                seen.add(key)
                delegation_pairs.append((chunk_id, spawned, ts_int, session_id, at))

        for chunk_id, spawned, ts_int, sid, at in user_delegations:
            key = (chunk_id, spawned)
            if key not in seen:
                seen.add(key)
                delegation_pairs.append((chunk_id, spawned, ts_int, sid, at))

        # Insert
        for chunk_id, spawned, ts_int, sid, at in delegation_pairs:
            exists = conn.execute(
                "SELECT 1 FROM _raw_chunks WHERE id = ?", (chunk_id,)
            ).fetchone()
            if not exists:
                continue
            ensure_source_exists(conn, spawned)
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO _edges_delegations
                    (chunk_id, child_session_id, agent_type, created_at, parent_source_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (chunk_id, spawned, at, ts_int, sid))
                inserted += 1
                _upsert_chunk_rollup(conn, chunk_id)
            except Exception:
                pass

    if inserted > 0:
        conn.commit()
        log_op(conn, 'heal_delegations', '_edges_delegations',
               params={'inserted': inserted, 'gap_sessions': len(gap_sessions)})
        conn.commit()

    return inserted


def _run_enrichment_cycle(conn, graph_threshold=50):
    """Run the full enrichment cycle — everything heals within one pass.

    Steps: reembed sources (unconditional) → graph (if stale) → community labels
    (after graph) → file graph → delegation graph → fingerprints → repo_project
    → delegation edge heal → NULL embed sweep → view sync.
    """
    t0 = time.time()

    print("[enrich] Starting enrichment cycle", file=sys.stderr)

    # 1. Unconditional: reembed sources (~1-3s, mean-pool aggregation, no ONNX)
    if reembed_sources:
        try:
            reembed_sources(conn)
        except Exception as e:
            print(f"[enrich] Reembed error: {e}", file=sys.stderr)

    # 2. Warmup rebuild — unconditional, decoupled from graph staleness.
    # Per-session reactive updates happen during scan_sessions() via _update_warmup(),
    # but the batch rebuild catches anything missed (e.g. backfill sessions).
    if rebuild_warmup_types:
        try:
            rebuild_warmup_types(conn)
        except Exception as e:
            print(f"[enrich] Warmup rebuild error: {e}", file=sys.stderr)

    # 3. Graph rebuild — unconditional (~4s at 4K sources, free at 30min cadence)
    if rebuild_source_graph:
        print("[enrich] Rebuilding graph...", file=sys.stderr)
        try:
            rebuild_source_graph(conn)
            # Community labels depend on graph — rebuild immediately after
            if rebuild_community_labels:
                rebuild_community_labels(conn)
            print(f"[enrich] Graph rebuilt in {time.time()-t0:.1f}s", file=sys.stderr)
        except Exception as e:
            print(f"[enrich] Graph error: {e}", file=sys.stderr)

    # 4. File co-edit graph (early-exit if no SOMA file identity data, ~2-5s)
    if rebuild_file_graph:
        try:
            rebuild_file_graph(conn)
        except Exception as e:
            print(f"[enrich] File graph error: {e}", file=sys.stderr)

    # 5. Delegation graph (early-exit if no delegation data, ~1-2s)
    if rebuild_delegation_graph:
        try:
            rebuild_delegation_graph(conn)
        except Exception as e:
            print(f"[enrich] Delegation graph error: {e}", file=sys.stderr)

    # 6. Incremental fingerprints
    if run_fingerprints:
        try:
            n = run_fingerprints(conn)
            if n > 0:
                print(f"[enrich] {n} sessions fingerprinted", file=sys.stderr)
        except Exception as e:
            print(f"[enrich] Fingerprint error: {e}", file=sys.stderr)

    # 7. Recover launch context before deriving any repository attribution.
    # This is additive and becomes a cheap no-op once the backlog is healed.
    try:
        from flex.modules.claude_code.manage.session_scope import backfill_primary_cwds
        n = backfill_primary_cwds(conn)
        if n > 0:
            conn.commit()
            print(f"[enrich] {n} session launch cwd values recovered", file=sys.stderr)
    except Exception as e:
        print(f"[enrich] Session cwd recovery error: {e}", file=sys.stderr)

    # 8. Incremental repo_project
    if run_repo_project:
        try:
            n = run_repo_project(conn)
            if n > 0:
                print(f"[enrich] {n} sources attributed", file=sys.stderr)
        except Exception as e:
            print(f"[enrich] Repo project error: {e}", file=sys.stderr)

    # 9. Heal delegation edges for sessions synced before progress-entry detection
    try:
        n = _heal_delegations(conn)
        if n > 0:
            print(f"[enrich] {n} delegation edges healed", file=sys.stderr)
    except Exception as e:
        print(f"[enrich] Delegation heal error: {e}", file=sys.stderr)

    # 10. NULL embedding sweep — a SAFETY NET for chunks that missed the embed
    # phase, not the drain. The tick runs a bounded sweep every ~2s; that is what
    # clears the backlog.
    #
    # Unbounded, this call was the drain: it took the whole NULL backlog in one
    # pass at ~12 chunks/s (sequence length is the wall), so a 20k backlog pinned
    # enrichment — and the entire serial tick behind it — for ~28 minutes at 8GB
    # RSS, while printing nothing. Measured by py-spy, not inferred.
    #
    # Bounded, it does the same job: whatever it leaves, the next tick's sweep and
    # the next cycle take. A safety net must never outrun the thing it backs up.
    try:
        _sweep_t0 = time.time()
        # batch_size=64, NOT the 500 default: the deadline is only tested BETWEEN
        # batches, so the batch size IS the granularity of the bound. At 500 the
        # first batch ran 188s against a 15s budget before anything checked the
        # clock — a bound coarser than the work it bounds is not a bound.
        n = _batch_embed_chunks(conn, batch_size=64, quiet=True,
                                deadline=_sweep_t0 + _ENRICH_EMBED_BUDGET_S)
        if n > 0:
            print(f"[enrich] Embedded {n} orphaned chunks "
                  f"in {time.time() - _sweep_t0:.0f}s", file=sys.stderr)
    except Exception as e:
        print(f"[enrich] Embed sweep error: {e}", file=sys.stderr)

    # 9.5. Chunk rollup — MUST run before view auto-sync below. The curated
    # chunks/messages views PK-probe _enrich_chunk_rollup instead of an
    # inline aggregate; if a new view installs while the rollup is stale or
    # empty, file_uuids/child_session_id/agent_type silently go NULL for
    # every row until the next cycle repopulates it.
    if rebuild_chunk_rollup:
        try:
            rebuild_chunk_rollup(conn)
        except Exception as e:
            print(f"[enrich] Chunk rollup error: {e}", file=sys.stderr)

    # 10. View auto-sync — install views if .sql files changed since last install
    try:
        _check_and_sync_views(conn)
    except Exception as e:
        print(f"[enrich] View sync error: {e}", file=sys.stderr)

    elapsed = time.time() - t0
    print(f"[enrich] Cycle done in {elapsed:.1f}s", file=sys.stderr)


_WATCH_HEALTH_META_KEY = 'watch_health'
_EMBED_TICK_BUDGET_S = 1.5  # real per-tick embed budget — bounded, not full-drain
# Catch-up mode: while the NULL backlog is deep the sweep is the primary embed path,
# not a backstop, and 1.5s/tick would take hours to drain what a burst produces in
# minutes. Both env-tunable; both still bounded (the loop is never monopolized).
_EMBED_CATCHUP_THRESHOLD = int(os.environ.get("FLEX_EMBED_CATCHUP_THRESHOLD", "5000"))
_EMBED_CATCHUP_BUDGET_S = float(os.environ.get("FLEX_EMBED_CATCHUP_BUDGET_S", "10"))
# Enrichment's NULL sweep is a safety net behind the per-tick sweep above, so it
# gets a safety net's budget. Unbounded it became the drain and pinned the tick
# for ~28 minutes on a 20k backlog (py-spy, 2026-07-16).
_ENRICH_EMBED_BUDGET_S = float(os.environ.get("FLEX_ENRICH_EMBED_BUDGET_S", "15"))
# Vault scan: same reason, different corpus. A watched vault can be tens of
# thousands of notes on a slow mount; its per-cell file cap is not a time cap.
_MARKDOWN_TICK_BUDGET_S = float(os.environ.get("FLEX_MARKDOWN_TICK_BUDGET_S", "10"))

# Event drain: runs FIRST in the serial tick, so it is the phase best positioned
# to starve everything behind it. A save-storm across a watched vault arrives as
# hundreds of paths; some embed inline. Over-budget events are re-queued, so the
# only thing this bound costs is the tail's latency — one tick.
_EVENT_DRAIN_BUDGET_S = float(os.environ.get("FLEX_EVENT_DRAIN_BUDGET_S", "10"))

# A tick is meant to be ~2s (a stat-scan + a bounded sweep + a throttled reconcile).
# This is a WARNING line, not a kill: some ticks legitimately run long (a 30-min
# enrichment cycle, a cold graph rebuild). The point is that a tick eating the loop
# must NAME ITSELF — silence is what let a blocked tick starve every corpus cell for
# three days while the process sat at 400% CPU looking alive.
TICK_BUDGET_WARN_S = float(os.environ.get("FLEX_TICK_BUDGET_WARN_S", "30"))

# Enrichment is intentionally not a daemon thread: it needs its own SQLite
# connection and the same OS semantic lease as refreshes.  The parent only
# supervises one ordinary child in its service control group.
_enrichment_child: subprocess.Popen | None = None
_enrichment_retry_after = 0.0
_ENRICHMENT_BUSY_RETRY_S = float(os.environ.get("FLEX_ENRICHMENT_BUSY_RETRY_S", "60"))


def _poll_enrichment_child() -> int | None:
    """Return a completed enrichment child's status and free its local slot."""
    global _enrichment_child
    if _enrichment_child is None:
        return None
    status = _enrichment_child.poll()
    if status is None:
        return None
    _enrichment_child = None
    return status


def _launch_enrichment_child(cell_path: str | Path, graph_threshold: int) -> bool:
    """Launch exactly one supervised enrichment child, inheriting service logs."""
    global _enrichment_child
    if _enrichment_child is not None:
        return False
    _enrichment_child = subprocess.Popen([
        sys.executable, "-m", "flex.modules.claude_code.compile.enrichment_runner",
        "--cell-path", str(cell_path), "--graph-threshold", str(graph_threshold),
    ])
    return True


def _snapshot_watch_health(conn, invalidation_queue, watcher, last_reconcile_ts,
                           phase_durations: dict) -> None:
    """Persist watcher/queue/phase telemetry to _meta for cross-process health
    reads (flex health / /health run in a separate process from the worker).
    Best-effort only — never load-bearing for ingestion correctness, so any
    failure here is swallowed rather than raised.
    """
    try:
        payload = {
            'enabled': invalidation_queue is not None,
            'healthy': bool(watcher.healthy) if watcher is not None else None,
            'backend': watcher.backend if watcher is not None else None,
            'last_error': watcher.last_error if watcher is not None else None,
            'last_reconcile_ts': last_reconcile_ts,
            'reconciliation_required': (
                invalidation_queue.reconciliation_required()
                if invalidation_queue is not None else None
            ),
            'queue': invalidation_queue.stats() if invalidation_queue is not None else None,
            'phase_durations_s': dict(phase_durations),
            'updated_at': time.time(),
        }
        from flex.core import set_meta as _set_meta
        _set_meta(conn, _WATCH_HEALTH_META_KEY, json.dumps(payload))
    except Exception as e:
        print(f"[worker] watch health snapshot error: {e}", file=sys.stderr)


class _EventBackoff(RuntimeError):
    """A prior path error is waiting for its retry deadline, not a new error."""

    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(f"targeted Claude sync waiting for retry at {retry_after:.3f}")


def _drain_ready_invalidations(conn, invalidation_queue, size_cache, error_cache,
                               phase_durations: dict) -> list:
    """Process one ready event batch without losing dequeued invalidations.

    ``drain_ready`` removes a batch before its handlers run.  Treat that as a
    dequeue, not a successful acknowledgement: if either targeted handler
    raises, restore the whole batch (at-least-once is safe for these idempotent
    sync paths) and leave reconciliation visibly required.
    """
    ready = []
    try:
        now_mono = time.monotonic()
        ready = invalidation_queue.drain_ready(now_mono)
        claude_paths = [inv.source_path for inv in ready if inv.cell_name == 'claude_code']
        if claude_paths:
            _t0 = time.time()
            stats = sync_session_paths(conn, claude_paths, size_cache, error_cache)
            phase_durations['targeted_sync_s'] = time.time() - _t0
            if stats.get('failed', 0):
                raise RuntimeError(
                    f"targeted Claude sync reported {stats['failed']} failed path(s)"
                )
            if stats.get('backoff_pending', 0):
                raise _EventBackoff(stats['next_retry_after'])
            if stats['synced'] > 0:
                print(
                    f"[worker] event-synced={stats['synced']} chunks={stats['chunks']}",
                    file=sys.stderr,
                )
        corpus_events = [inv for inv in ready if inv.cell_name != 'claude_code']
        corpus_event_drainer = _corpus_path_drainer or _filesystem_path_drainer
        if corpus_events and corpus_event_drainer:
            _t0 = time.time()
            if _corpus_path_drainer:
                from flex.admission import try_heavy_lease
                with try_heavy_lease(
                    detail="claude worker semantic event drain",
                ) as _event_lease:
                    event_stats = corpus_event_drainer(
                        corpus_events, encode_fn=encode,
                        deadline=_t0 + _EVENT_DRAIN_BUDGET_S,
                        semantic_allowed=_event_lease.acquired,
                    )
            else:
                event_stats = corpus_event_drainer(corpus_events)
            phase_durations['corpus_targeted_sync_s'] = time.time() - _t0
            if event_stats.get('failed', 0):
                raise RuntimeError(
                    f"corpus event drain reported {event_stats['failed']} failed path(s)"
                )
            # Park, don't drop: an event that ran out of budget goes back
            # on the queue with its original observed_at, so it is already
            # past its quiet window and drains at the head of the next tick.
            for _inv in event_stats.get('deferred') or ():
                # A new callback may have arrived while this bounded handler
                # ran. Merge instead of ordinary put(), which would let an
                # older deferred kind/deadline overwrite newer queue state.
                invalidation_queue.requeue((_inv,), reason="deferred")
            if event_stats['indexed']:
                print(f"[worker] corpus event-indexed={event_stats['indexed']}",
                      file=sys.stderr)
        return ready
    except _EventBackoff as backoff:
        # Do not record a delayed retry as another failure. Keep the batch
        # visible and let the reconciliation scheduler wait for its real
        # monotonic retry deadline instead of rescanning every heartbeat.
        if ready:
            invalidation_queue.requeue(ready, reason="backoff")
            invalidation_queue.mark_reconciliation_required(
                inv.cell_name for inv in ready
            )
        else:
            invalidation_queue.mark_reconciliation_required('claude_code')
        phase_durations['claude_next_retry_after'] = backoff.retry_after
        return ready
    except Exception:
        # A handler may have partially completed before it raised. Retrying the
        # complete batch is deliberate: both targeted sync paths are idempotent,
        # while losing an event would conceal freshness debt until a later scan.
        if ready:
            invalidation_queue.requeue(ready)
            invalidation_queue.mark_reconciliation_required(
                inv.cell_name for inv in ready
            )
        else:
            # A queue failure before yielding a batch has no safe scope.
            invalidation_queue.mark_reconciliation_required()
        raise


_NO_DEBT_GENERATION = object()


def _reconciliation_succeeded(invalidation_queue, stats: dict,
                              *, cell_name: str = 'claude_code',
                              debt_generation=_NO_DEBT_GENERATION) -> bool:
    """A scan that reports failed paths is not a fresh reconciliation.

    The Claude session scan is authoritative only for Claude debt. Corpus
    invalidation debt remains visible until its own full reconciler reports a
    cell-scoped completion receipt.
    """
    if stats.get('failed', 0) or stats.get('backoff_pending', 0):
        invalidation_queue.mark_reconciliation_required(cell_name)
        return False
    if debt_generation is _NO_DEBT_GENERATION:
        # Compatibility for direct callers that have no scan-start snapshot.
        invalidation_queue.clear_reconciliation_required(cell_name)
    elif debt_generation is not None:
        invalidation_queue.clear_reconciliation_required(
            cell_name, through_generation=debt_generation,
        )
    return True


def _drain_corpus_reconciliation_debt(invalidation_queue, corpus_drainer,
                                      *, encode_fn, semantic_allowed: bool) -> None:
    """Run corpus reconciliation and acknowledge only proven scoped receipts.

    The aggregate corpus drainer does not return a cell receipt. Its registry
    generation is the narrow durable proof available here: a debt cell must
    advance during this invocation, finish with no pending work, and report no
    registry reconciliation debt. The queue generation is compare-and-cleared
    so a new callback during the scan survives a stale acknowledgement.
    """
    from flex.registry import list_cells

    debt_cells = [
        name for name in invalidation_queue.reconciliation_debt_cells()
        if name not in {'claude_code', '__unknown__'}
    ]
    before = {cell['name']: cell for cell in list_cells()}
    debt_generations = {
        name: invalidation_queue.reconciliation_debt_generation(name)
        for name in debt_cells
    }

    corpus_drainer(encode_fn=encode_fn, semantic_allowed=semantic_allowed)

    after = {cell['name']: cell for cell in list_cells()}
    for name, debt_generation in debt_generations.items():
        previous = before.get(name)
        current = after.get(name)
        if debt_generation is None or previous is None or current is None:
            continue
        if int(current.get('refresh_generation') or 0) <= int(
            previous.get('refresh_generation') or 0
        ):
            continue
        if int(current.get('refresh_pending') or 0) != 0:
            continue
        if bool(current.get('reconciliation_required')):
            continue
        invalidation_queue.clear_reconciliation_required(
            name, through_generation=debt_generation,
        )


def daemon_loop(interval=2, invalidation_queue=None, watcher=None,
                reconcile_interval: float | None = None):
    """Main daemon loop.

    Args:
        interval: Scheduling heartbeat in seconds (default 2). Legacy
                  scanners (docpac, markdown, coding-agent watch) and the
                  polling fallback keep ticking at this cadence unchanged.
        invalidation_queue: Optional flex.watch.InvalidationQueue. When
                  None (the default), Claude Code ingestion uses the
                  original every-tick full-tree scan_sessions() — exact
                  legacy polling behavior, unchanged. When provided, each
                  tick drains ready invalidations for path-targeted sync
                  and a slower periodic scan_sessions() call becomes the
                  reconciliation authority instead.
        watcher: Optional flex.watch.Watcher — used only to read/refresh
                  backend health each tick and to react to backend death.
        reconcile_interval: Reconciliation cadence in seconds when
                  invalidation_queue is provided. Defaults to 60s.
    """
    global _enrichment_retry_after
    # Resolve cell
    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[worker] FATAL: claude_code cell not found in registry", file=sys.stderr)
        sys.exit(1)

    print("[flex-worker] Starting chunk-atom daemon", file=sys.stderr)
    print(f"  Target: {cell_path}", file=sys.stderr)
    print(f"  Interval: {interval}s", file=sys.stderr)

    # Do not initialize the semantic runtime on the capture thread. Model load
    # can take tens of seconds and is unrelated to publishing source text,
    # metadata, relationships, and FTS. The admitted semantic sweep below loads
    # it lazily after the startup grace; structural event handling starts now.

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.row_factory = sqlite3.Row  # enrichment functions use column-name access
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")

    # Ensure all tables exist (fresh cell bootstrap)
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)

    # Ensure SOMA identity tables exist
    if soma_ensure_tables:
        soma_ensure_tables(conn)

    # Stock presets are executable file contracts, not install-time snapshots.
    # Validate them without opening a write transaction or copying SQL.
    try:
        from flex.manage.install_presets import ensure_cell_presets
        ensure_cell_presets(conn, "claude-code")
    except Exception as exc:
        # Capture remains authoritative even if query-contract validation fails;
        # keep the worker alive and let the next deployment/reconciliation retry.
        print(f"[worker] preset contract validation error: {exc}", file=sys.stderr)

    if _corpus_drainer or _filesystem_scanner:
        print("  Corpus indexing: enabled", file=sys.stderr)

    ENRICHMENT_INTERVAL = 30 * 60   # 30 minutes — graph, fingerprints, repo_project
    GRAPH_STALENESS_THRESHOLD = 50  # sessions since last graph build

    last_soma_heal = time.time()
    last_filesystem_reconcile = 0.0
    worker_start = time.time()
    SEMANTIC_STARTUP_GRACE = 120  # capture/events must run + prove stable first

    # Enrichment cadence PERSISTS across restarts (in _meta) so a (re)start does
    # NOT reset the 30-min clock. Seeding to 0 fired the heavy cycle on the FIRST
    # tick of every restart → fat (~2.9G) python → external SIGKILL under memory
    # pressure → restart → re-fire → crash-loop, and capture never reached steady
    # state. With a persisted timestamp the cycle runs only when genuinely due;
    # absent a stored value (existing cell's first run / fresh cell) we defer one
    # interval rather than firing immediately. The startup grace below keeps the
    # heavy pass off the vulnerable first ticks, so a kill mid-cycle (before the
    # timestamp can be re-persisted) cannot instantly re-trigger it on restart.
    from flex.core import get_meta, set_meta
    try:
        _le = get_meta(conn, 'last_enrichment_ts')
        last_enrichment = float(_le) if _le else time.time()
    except (TypeError, ValueError):
        last_enrichment = time.time()

    # Size/error caches — empty dicts trigger full initial scan on first tick.
    # Shared identically between the polling path and (when enabled) the
    # event + reconciliation paths, per the technical contract.
    size_cache: dict = {}
    error_cache: dict = {}

    use_events = invalidation_queue is not None
    RECONCILE_INTERVAL = reconcile_interval or DEFAULT_RECONCILE_INTERVAL
    # When the full registry watch topology is still initializing, let ready
    # source events publish before the cold full-tree reconciliation. That
    # reconciliation remains due on the ordinary interval and is still the
    # missed-event authority; it simply may not monopolize the worker's very
    # first tick while live events are already waiting. Polling/test paths keep
    # the historical immediate reconciliation behavior.
    last_reconcile = (
        time.time()
        if watcher is not None and getattr(watcher, "_starting", False)
        else 0.0
    )
    # A failed JSONL gets an exponential monotonic retry deadline. Keep its
    # debt visible, but do not turn a 2s heartbeat into a full-tree scan loop.
    next_claude_reconcile_after = 0.0
    _phase_durations: dict = {}

    if use_events:
        _snapshot_watch_health(conn, invalidation_queue, watcher, None, _phase_durations)

    while True:
        _tick_start = time.time()
        # Clear per tick. This dict is built once, outside the loop; leaving last
        # tick's values in it made SLOW TICK attribute phases that did not run —
        # a 49.8s tick reported enrichment_s=101.1s (from a tick minutes earlier)
        # and unaccounted went NEGATIVE, which is the only reason it was caught.
        # Instrumentation that carries stale values is not instrumentation; it is
        # a second bug wearing the first one's uniform.
        _phase_durations.clear()
        _enrichment_status = _poll_enrichment_child()
        if _enrichment_status is not None:
            if _enrichment_status == 0:
                last_enrichment = time.time()
                _enrichment_retry_after = 0.0
                print("[worker] enrichment child completed", file=sys.stderr)
            elif _enrichment_status == 75:
                _enrichment_retry_after = time.time() + _ENRICHMENT_BUSY_RETRY_S
                print("[worker] enrichment deferred by semantic admission; retry scheduled",
                      file=sys.stderr)
            else:
                _enrichment_retry_after = time.time() + _ENRICHMENT_BUSY_RETRY_S
                print(f"[worker] enrichment child exited {_enrichment_status}; retry scheduled",
                      file=sys.stderr)
        if use_events:
            # Phase 0a: drain ready invalidations — cheap, wake-driven.
            # Observer callbacks only enqueued; all I/O happens here.
            try:
                _drain_ready_invalidations(
                    conn, invalidation_queue, size_cache, error_cache, _phase_durations,
                )
            except Exception as e:
                print(f"[worker] Event drain error: {e}", file=sys.stderr)
            _event_retry_after = _phase_durations.get('claude_next_retry_after')
            if _event_retry_after is not None:
                next_claude_reconcile_after = max(
                    next_claude_reconcile_after, _event_retry_after,
                )

            # Watcher backend health — a dead observer thread forces
            # reconciliation to become the sole correctness path, without
            # restarting the daemon.
            if watcher is not None:
                try:
                    watcher.check_health()
                except Exception as e:
                    watcher.mark_unhealthy(str(e))

            # Phase 0b: periodic full reconciliation — the correctness
            # authority. Runs on RECONCILE_INTERVAL, immediately on startup,
            # and whenever the queue overflowed or the watcher is unhealthy.
            _claude_retry_blocked = (
                invalidation_queue.reconciliation_required('claude_code')
                and time.monotonic() < next_claude_reconcile_after
            )
            due_reconcile = (
                (
                    not _claude_retry_blocked
                    and (
                        time.time() - last_reconcile > RECONCILE_INTERVAL
                        or invalidation_queue.reconciliation_required('claude_code')
                    )
                )
                or (watcher is not None and not watcher.healthy)
            )
            if due_reconcile:
                try:
                    _claude_debt_generation = (
                        invalidation_queue.reconciliation_debt_generation('claude_code')
                    )
                    _t0 = time.time()
                    stats = scan_sessions(conn, size_cache, error_cache)
                    _phase_durations['reconcile_s'] = time.time() - _t0
                    if stats['synced'] > 0:
                        print(
                            f"[worker] reconcile synced={stats['synced']} chunks={stats['chunks']}",
                            file=sys.stderr,
                        )
                    if _reconciliation_succeeded(
                        invalidation_queue, stats,
                        debt_generation=_claude_debt_generation,
                    ):
                        last_reconcile = time.time()
                        next_claude_reconcile_after = 0.0
                    else:
                        next_claude_reconcile_after = max(
                            next_claude_reconcile_after,
                            stats.get('next_retry_after') or 0.0,
                        )
                        print(
                            "[worker] Reconcile retained debt: "
                            f"failed={stats.get('failed', 0)} "
                            f"backoff={stats.get('backoff_pending', 0)}",
                            file=sys.stderr,
                        )
                except Exception as e:
                    print(f"[worker] Reconcile error: {e}", file=sys.stderr)
                    # Do NOT advance last_reconcile on failure — a failed
                    # reconcile is exactly when the backstop is needed, so leave
                    # the clock so due_reconcile fires again on a near tick
                    # instead of waiting the full RECONCILE_INTERVAL.
                _snapshot_watch_health(conn, invalidation_queue, watcher, last_reconcile, _phase_durations)
        else:
            # Phase 0: no event source wired — exact legacy polling behavior.
            try:
                stats = scan_sessions(conn, size_cache, error_cache)
                if stats['synced'] > 0:
                    print(f"[worker] synced={stats['synced']} chunks={stats['chunks']}",
                          file=sys.stderr)
            except Exception as e:
                print(f"[worker] Scan error: {e}", file=sys.stderr)

        # Sweep NULL embeddings every tick — catches interrupted flex init,
        # failed embeds, or any other orphaned chunks. Bounded to a real
        # per-tick wall-clock budget (not just a small ONNX batch size) so a
        # large backlog cannot monopolize the 2s loop; the remainder is
        # picked up on the next tick and the 30min enrichment sweep.
        # Agent refreshes already own the semantic lane. Keep capture and event
        # drains live while they run, but leave NULL embeddings durable for the
        # next idle tick. Even the adaptive probe cannot interrupt one unusually
        # expensive batch after it starts, so overlapping it with a provider
        # refresh can still freeze convergence for minutes.
        from flex.admission import try_heavy_lease
        _semantic_lease = try_heavy_lease(detail="claude worker semantic sweep/corpus")
        _semantic_allowed = _semantic_lease.acquired
        if (time.time() - worker_start > SEMANTIC_STARTUP_GRACE
                and _semantic_allowed):
            try:
                _t0 = time.time()
                # Catch-up: a steady-state constant must not govern bulk work. 1.5s/tick
                # is right when the sweep is a backstop trickling in stragglers; it is
                # wrong when it is the primary path draining a real backlog (a restart,
                # an interrupted init, or — since capture stopped embedding inline —
                # every burst of new sessions). Widen the budget while behind, decay to
                # steady state once caught up. The tick still cannot be monopolized: the
                # ceiling is bounded and the phases below still run every tick.
                _null_backlog = conn.execute(
                    "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NULL AND content IS NOT NULL"
                ).fetchone()[0]
                _budget = (_EMBED_CATCHUP_BUDGET_S if _null_backlog > _EMBED_CATCHUP_THRESHOLD
                           else _EMBED_TICK_BUDGET_S)
                swept = _batch_embed_chunks(
                    conn, batch_size=64, quiet=True, deadline=_t0 + _budget,
                )
                _phase_durations['embed_sweep_s'] = time.time() - _t0
                if swept > 0:
                    conn.commit()
                    _mode = " (catch-up)" if _budget != _EMBED_TICK_BUDGET_S else ""
                    print(f"[worker] Embedded {swept} chunks{_mode}, {_null_backlog - swept} remaining",
                          file=sys.stderr)
            except Exception as e:
                print(f"[worker] Embed sweep error: {e}", file=sys.stderr)
        elif time.time() - worker_start > SEMANTIC_STARTUP_GRACE:
            print("[worker] semantic sweep deferred: semantic work already active",
                  file=sys.stderr)

        # Corpus scan (document indexing)
        if _corpus_drainer:
            try:
                _cd0 = time.time()
                if use_events:
                    _drain_corpus_reconciliation_debt(
                        invalidation_queue, _corpus_drainer,
                        encode_fn=encode, semantic_allowed=_semantic_allowed,
                    )
                else:
                    _corpus_drainer(encode_fn=encode, semantic_allowed=_semantic_allowed)
                _phase_durations['corpus_drain_s'] = time.time() - _cd0
            except Exception as e:
                print(f"[worker] Corpus drain error: {e}", file=sys.stderr)
        elif (_filesystem_scanner
              and time.monotonic() - last_filesystem_reconcile
              >= FILESYSTEM_RECONCILE_INTERVAL):
            try:
                _fs0 = time.time()
                _filesystem_scanner()
                last_filesystem_reconcile = time.monotonic()
                _phase_durations['filesystem_scan_s'] = time.time() - _fs0
            except Exception as e:
                print(f"[worker] Filesystem drain error: {e}", file=sys.stderr)
        _semantic_lease.close()

        # Secondary local cells (e.g. chat exports)
        if _secondary_cell_drainer:
            try:
                _sd0 = time.time()
                _secondary_cell_drainer()
                _phase_durations['secondary_drain_s'] = time.time() - _sd0
            except Exception as e:
                print(f"[worker] Secondary cell drain error: {e}", file=sys.stderr)

        # Markdown vault scan (lifecycle='watch' cells) — bounded. A vault can hold
        # tens of thousands of notes on a slow mount (/mnt/c under WSL); unbounded,
        # its walk+index stalled this serial tick for minutes and starved every
        # phase behind it, including capture. Durable cursor: the rest resumes next
        # tick. Shares the sweep's budget shape, not a new knob.
        if _markdown_scanner:
            try:
                _md0 = time.time()
                _markdown_scanner(deadline=_md0 + _MARKDOWN_TICK_BUDGET_S)
                _phase_durations['markdown_scan_s'] = time.time() - _md0
            except Exception as e:
                print(f"[worker] Markdown scan error: {e}", file=sys.stderr)

        # SOMA identity heal + optional eternity backup (24h cycle)
        if soma_heal and time.time() - last_soma_heal > 24 * 3600:
            try:
                soma_heal(conn)
                last_soma_heal = time.time()
            except Exception as e:
                print(f"[worker] SOMA heal error: {e}", file=sys.stderr)
                last_soma_heal = time.time()
            # Eternity backup — opt-in via ~/.flex/config.json {"eternity": {"auto_backup": true}}
            try:
                import json as _json
                _cfg_path = FLEX_HOME / "config.json"
                if _cfg_path.exists():
                    _cfg = _json.loads(_cfg_path.read_text())
                    if _cfg.get("eternity", {}).get("auto_backup", False):
                        from flex.modules.soma.lib.eternity.eternity import Eternity
                        Eternity().run()
            except Exception as e:
                print(f"[worker] eternity backup error: {e}", file=sys.stderr)

        # Enrichment cycle — full heal pass (30-min cadence, persisted across
        # restarts). Startup grace keeps the heavy pass off the first ticks:
        # capture runs first and the worker proves it survives, so a kill
        # mid-cycle cannot instantly re-trigger it on the next start.
        if (time.time() - worker_start > SEMANTIC_STARTUP_GRACE
                and time.time() >= _enrichment_retry_after
                and time.time() - last_enrichment > ENRICHMENT_INTERVAL):
            try:
                _overdue = (time.time() - last_enrichment) / 60.0
                if _launch_enrichment_child(cell_path, GRAPH_STALENESS_THRESHOLD):
                    print(f"[worker] enrichment: child launched (due {_overdue:.0f} min ago)",
                          file=sys.stderr)
            except Exception as e:
                _enrichment_retry_after = time.time() + _ENRICHMENT_BUSY_RETRY_S
                print(f"[worker] enrichment launch error: {e}", file=sys.stderr)

        # Heartbeat: stamp the END of every tick, not just after a reconcile.
        # There was no way to tell a BUSY worker from a WEDGED one — both are a
        # live pid at high CPU printing nothing. A tick that completes says so;
        # a stale heartbeat is then a fact, not an inference. (`updated_at` in
        # this payload is the timestamp health reads.)
        _phase_durations['tick_s'] = time.time() - _tick_start
        if use_events:
            _snapshot_watch_health(conn, invalidation_queue, watcher, last_reconcile, _phase_durations)

        # Name a phase that ate the loop. Every phase below capture — the embed
        # sweep, the corpus reconcile, enrichment — is starved for exactly as long
        # as any phase above it runs, and nothing used to say which one, or that
        # anything was wrong at all.
        if _phase_durations['tick_s'] > TICK_BUDGET_WARN_S:
            slow = sorted(
                ((k, v) for k, v in _phase_durations.items() if k != 'tick_s' and v is not None),
                key=lambda kv: kv[1], reverse=True,
            )[:3]
            detail = " ".join(f"{k}={v:.1f}s" for k, v in slow)
            # Report the time NO phase claims. A top-3 list looks authoritative
            # while an unnamed phase eats the loop — 111s of tick against 63s of
            # named phases is how the next starvation hides. If this number is
            # large, the instrumentation is the bug, not the phases it names.
            _named = sum(v for k, v in _phase_durations.items()
                         if k != 'tick_s' and v is not None)
            _unaccounted = _phase_durations['tick_s'] - _named
            # Negative means the phases claim more time than the tick took, which
            # is arithmetically impossible and therefore a bug in THIS reporting —
            # stale entries, double counting, a timer spanning ticks. Say so
            # instead of printing a quietly absurd number.
            if _unaccounted < -0.5:
                print(f"[worker] BUG: phase timings exceed tick by "
                      f"{-_unaccounted:.1f}s — {dict(_phase_durations)}",
                      file=sys.stderr)
            print(
                f"[worker] SLOW TICK {_phase_durations['tick_s']:.1f}s "
                f"(budget {TICK_BUDGET_WARN_S:.0f}s) — {detail or 'no phase timings'}"
                f" unaccounted={_unaccounted:.1f}s",
                file=sys.stderr,
            )

        time.sleep(interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    parser.add_argument("--interval", type=int, default=2, help="Scan interval")
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
        stats = scan_primary_cell(conn)
        conn.close()
        print(f"[worker] Done: {stats}")
