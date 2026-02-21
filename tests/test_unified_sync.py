"""
Tests for the unified write path (sync_session_messages).

Verifies: tool_ops extraction, thinking blocks, file-history-snapshots,
is_sidechain, entry_uuid, single ID format, no truncation, tool-only chunks,
idempotency, simplified process_queue.
"""

import json
import sqlite3
import struct
import tempfile
import os
import pytest
from pathlib import Path


def _can_import():
    try:
        import flex.modules.claude_code.compile.worker
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(not _can_import(), reason="flex not importable")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_embedding(dim=384):
    return struct.pack(f'{dim}f', *([0.1] * dim))


def _make_cell(tmp_path):
    """Create a minimal chunk-atom cell for testing sync."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE _raw_chunks (
            id TEXT PRIMARY KEY, content TEXT, embedding BLOB, timestamp INTEGER
        );
        CREATE TABLE _raw_sources (
            source_id TEXT PRIMARY KEY, source TEXT, project TEXT,
            git_root TEXT, start_time INTEGER, primary_cwd TEXT,
            message_count INTEGER DEFAULT 0, episode_count INTEGER DEFAULT 0,
            end_time INTEGER, duration_minutes INTEGER,
            title TEXT, embedding BLOB
        );
        CREATE TABLE _edges_source (
            chunk_id TEXT NOT NULL, source_id TEXT NOT NULL,
            source_type TEXT DEFAULT 'claude-code', position INTEGER
        );
        CREATE TABLE _types_message (
            chunk_id TEXT PRIMARY KEY, type TEXT, role TEXT,
            chunk_number INTEGER, parent_uuid TEXT,
            is_sidechain INTEGER, entry_uuid TEXT
        );
        CREATE TABLE _edges_tool_ops (
            chunk_id TEXT NOT NULL, tool_name TEXT, target_file TEXT,
            success INTEGER, cwd TEXT, git_branch TEXT
        );
        CREATE TABLE _edges_delegations (
            id INTEGER PRIMARY KEY, chunk_id TEXT, child_doc_id TEXT,
            agent_type TEXT, created_at INTEGER
        );
        CREATE TABLE _edges_soft_ops (
            id INTEGER PRIMARY KEY, chunk_id TEXT, file_path TEXT,
            file_uuid TEXT, inferred_op TEXT, confidence TEXT
        );
        CREATE TABLE _raw_content (
            hash TEXT PRIMARY KEY, content TEXT NOT NULL,
            tool_name TEXT, byte_length INTEGER, first_seen INTEGER
        );
        CREATE TABLE _edges_raw_content (
            chunk_id TEXT NOT NULL, content_hash TEXT NOT NULL,
            PRIMARY KEY (chunk_id, content_hash)
        );
    """)
    conn.commit()
    return conn


def _write_jsonl(tmp_path, session_id, entries):
    """Write JSONL entries to a mock session file."""
    # Create nested structure matching CLAUDE_PROJECTS glob
    session_dir = tmp_path / "claude" / "projects" / "test"
    session_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = session_dir / f"{session_id}.jsonl"
    with open(jsonl_path, 'w') as f:
        for entry in entries:
            f.write(json.dumps(entry) + '\n')
    return jsonl_path


# ─────────────────────────────────────────────────────────────────────────────
# Sample JSONL entries
# ─────────────────────────────────────────────────────────────────────────────

def _user_entry(text, uuid="uuid-001", sidechain=False):
    entry = {
        "type": "user",
        "uuid": uuid,
        "timestamp": "2026-02-19T15:00:00Z",
        "message": {"role": "user", "content": text},
        "cwd": "/home/test/projects/myapp",
        "parentUuid": "parent-001",
    }
    if sidechain:
        entry["isSidechain"] = True
    return entry


def _assistant_text_entry(text, uuid="uuid-002"):
    return {
        "type": "assistant",
        "uuid": uuid,
        "timestamp": "2026-02-19T15:01:00Z",
        "message": {"role": "assistant", "content": [
            {"type": "text", "text": text}
        ]},
        "cwd": "/home/test/projects/myapp",
    }


def _assistant_tool_entry(tools, text=None, uuid="uuid-003", thinking=None):
    """Build an assistant entry with tool_use items.

    tools: list of (tool_name, tool_input_dict) tuples
    """
    content_items = []
    if text:
        content_items.append({"type": "text", "text": text})
    if thinking:
        content_items.append({"type": "thinking", "thinking": thinking})
    for i, (name, inp) in enumerate(tools):
        content_items.append({
            "type": "tool_use",
            "id": f"toolu_{i:04d}",
            "name": name,
            "input": inp,
        })
        content_items.append({
            "type": "tool_result",
            "tool_use_id": f"toolu_{i:04d}",
            "content": f"Result of {name}",
        })
    entry = {
        "type": "assistant",
        "uuid": uuid,
        "timestamp": "2026-02-19T15:02:00Z",
        "message": {"role": "assistant", "content": content_items},
        "cwd": "/home/test/projects/myapp",
    }
    return entry


def _snapshot_entry():
    return {
        "type": "file-history-snapshot",
        "timestamp": "2026-02-19T15:03:00Z",
        "snapshot": {"/home/test/file.py": "old content here"},
        "messageId": "msg-001",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSingleIDFormat:
    """All chunks from sync have {session}_{line_num} format."""

    def test_chunk_ids_are_session_linenum(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-aaa"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("Hello"),
            _assistant_text_entry("Hi there"),
        ])

        # Patch find_jsonl to return our test file
        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        # Disable SOMA
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        inserted = w.sync_session_messages(session_id, conn)
        assert inserted == 2

        chunk_ids = [r[0] for r in conn.execute("SELECT id FROM _raw_chunks").fetchall()]
        for cid in chunk_ids:
            assert cid.startswith(f"{session_id}_")
            parts = cid.split('_')
            # Last part should be a line number (integer)
            assert parts[-1].isdigit()


class TestNoTruncation:
    """Content is stored without truncation."""

    def test_long_content_not_truncated(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-bbb"
        long_text = "A" * 5000  # >2000 chars
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry(long_text),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        w.sync_session_messages(session_id, conn)
        content = conn.execute("SELECT content FROM _raw_chunks").fetchone()[0]
        assert len(content) == 5000


class TestToolOpsExtraction:
    """Tool_use items produce _edges_tool_ops rows."""

    def test_tool_ops_from_jsonl(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-ccc"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("read the file"),
            _assistant_tool_entry([
                ("Read", {"file_path": "/home/test/app.py"}),
                ("Grep", {"pattern": "def main", "path": "/home/test"}),
            ], text="Let me check"),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        w.sync_session_messages(session_id, conn)

        tool_ops = conn.execute(
            "SELECT chunk_id, tool_name, target_file FROM _edges_tool_ops ORDER BY tool_name"
        ).fetchall()
        assert len(tool_ops) == 2
        tools = {r[1] for r in tool_ops}
        assert tools == {'Read', 'Grep'}
        # Read should have target_file
        read_op = [r for r in tool_ops if r[1] == 'Read'][0]
        assert read_op[2] == '/home/test/app.py'


class TestToolOnlyChunks:
    """Assistant lines with tool_use but no text still produce chunks."""

    def test_tool_only_creates_chunk(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-ddd"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("do it"),
            _assistant_tool_entry([
                ("Write", {"file_path": "/tmp/test.py", "content": "print('hello')"}),
            ]),  # No text= argument → tool-only
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        inserted = w.sync_session_messages(session_id, conn)
        assert inserted == 2  # user + tool-only assistant

        # Tool-only chunk should have type='tool_call'
        tool_chunk = conn.execute(
            "SELECT type FROM _types_message WHERE type = 'tool_call'"
        ).fetchone()
        assert tool_chunk is not None

        # Content should contain tool info
        chunk_content = conn.execute(
            "SELECT rc.content FROM _raw_chunks rc "
            "JOIN _types_message tm ON rc.id = tm.chunk_id "
            "WHERE tm.type = 'tool_call'"
        ).fetchone()
        assert chunk_content is not None
        assert "Write" in chunk_content[0]


class TestThinkingBlocks:
    """Thinking content stored in _raw_content with _thinking discriminator."""

    def test_thinking_stored(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-eee"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("think about this"),
            _assistant_tool_entry(
                [("Read", {"file_path": "/tmp/x.py"})],
                text="Here's my analysis",
                thinking="I need to carefully consider the architecture...",
            ),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        w.sync_session_messages(session_id, conn)

        thinking = conn.execute(
            "SELECT content FROM _raw_content WHERE tool_name = '_thinking'"
        ).fetchone()
        assert thinking is not None
        assert "architecture" in thinking[0]


class TestFileHistorySnapshot:
    """file-history-snapshot entries go to _raw_content, no chunk created."""

    def test_snapshot_stored_no_chunk(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-fff"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("edit the file"),
            _snapshot_entry(),
            _assistant_text_entry("Done editing"),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        inserted = w.sync_session_messages(session_id, conn)
        # Only user + assistant text, not the snapshot
        assert inserted == 2

        # But snapshot content is in _raw_content
        snapshot = conn.execute(
            "SELECT content FROM _raw_content WHERE tool_name = '_file_snapshot'"
        ).fetchone()
        assert snapshot is not None
        assert "old content" in snapshot[0]


class TestMetadataFields:
    """is_sidechain and entry_uuid populated on _types_message."""

    def test_sidechain_and_uuid(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-ggg"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("sidechain msg", uuid="uuid-sc", sidechain=True),
            _assistant_text_entry("response", uuid="uuid-resp"),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        w.sync_session_messages(session_id, conn)

        rows = conn.execute(
            "SELECT chunk_id, is_sidechain, entry_uuid FROM _types_message ORDER BY chunk_number"
        ).fetchall()
        assert len(rows) == 2

        # User entry is sidechain
        assert rows[0][1] == 1
        assert rows[0][2] == "uuid-sc"

        # Assistant entry is not sidechain
        assert rows[1][1] == 0
        assert rows[1][2] == "uuid-resp"


class TestIdempotency:
    """Calling sync twice on the same session produces same result."""

    def test_idempotent_sync(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-iii"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("first"),
            _assistant_text_entry("second"),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        first = w.sync_session_messages(session_id, conn)
        conn.commit()
        second = w.sync_session_messages(session_id, conn)

        assert first == 2
        assert second == 0  # Already synced

        count = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        assert count == 2


class TestToolContentBridge:
    """Tool content in _raw_content bridges to chunks via _edges_raw_content."""

    def test_tool_content_joinable(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-jjj"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("read it"),
            _assistant_tool_entry([
                ("Read", {"file_path": "/home/test/main.py"}),
            ], text="Reading the file"),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        w.sync_session_messages(session_id, conn)

        # The key test: tool_ops chunk_ids match raw_content chunk_ids
        ops_ids = {r[0] for r in conn.execute(
            "SELECT chunk_id FROM _edges_tool_ops"
        ).fetchall()}
        content_ids = {r[0] for r in conn.execute(
            "SELECT chunk_id FROM _edges_raw_content"
        ).fetchall()}
        # Tool ops chunks should have corresponding content entries
        assert ops_ids.issubset(content_ids)


class TestProcessEventDeleted:
    """process_event no longer exists."""

    def test_no_process_event(self):
        import flex.modules.claude_code.compile.worker as w
        assert not hasattr(w, 'process_event')


class TestOldBlobHashFromSnapshot:
    """file-history-snapshot backup files produce old_blob_hash on SOMA edges."""

    def test_snapshot_populates_old_blob_hash(self, tmp_path, monkeypatch):
        import hashlib
        conn = _make_cell(tmp_path)
        session_id = "test-session-snap"
        target_file = "/home/test/projects/myapp/main.py"
        assistant_uuid = "assist-uuid-snap"
        backup_name = "abc123@v1"

        # Create file-history backup on disk
        fh_dir = tmp_path / ".claude" / "file-history" / session_id
        fh_dir.mkdir(parents=True)
        backup_content = b"def hello():\n    return 'world'\n"
        (fh_dir / backup_name).write_bytes(backup_content)

        # Expected git blob hash
        header = f"blob {len(backup_content)}\0".encode()
        expected_hash = hashlib.sha1(header + backup_content).hexdigest()

        # Build JSONL with snapshot → assistant Edit
        entries = [
            _user_entry("edit the file"),
            {
                "type": "file-history-snapshot",
                "messageId": assistant_uuid,
                "timestamp": "2026-02-19T15:02:00Z",
                "snapshot": {
                    "messageId": assistant_uuid,
                    "trackedFileBackups": {
                        target_file: {
                            "backupFileName": backup_name,
                            "version": 1,
                            "backupTime": "2026-02-19T15:02:00Z",
                        }
                    },
                    "timestamp": "2026-02-19T15:02:00Z",
                },
            },
            _assistant_tool_entry(
                [("Edit", {"file_path": target_file, "old_string": "x", "new_string": "y"})],
                uuid=assistant_uuid,
            ),
        ]
        jsonl_path = _write_jsonl(tmp_path, session_id, entries)

        import flex.modules.claude_code.compile.worker as w
        from pathlib import Path
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))

        # Mock SOMA: capture enrichment dicts to verify old_blob_hash
        captured = []

        def mock_enrich(chunk):
            return chunk  # pass through

        def mock_insert(conn, chunk):
            captured.append(dict(chunk))

        monkeypatch.setattr(w, 'soma_enrich', mock_enrich)
        monkeypatch.setattr(w, 'soma_insert_edges', mock_insert)

        w.sync_session_messages(session_id, conn)

        # Verify old_blob_hash was set from backup file
        edits = [c for c in captured if c.get('old_blob_hash')]
        assert len(edits) >= 1
        assert edits[0]['old_blob_hash'] == expected_hash

    def test_snapshot_update_merges(self, tmp_path, monkeypatch):
        """isSnapshotUpdate=True snapshots merge file hashes."""
        conn = _make_cell(tmp_path)
        session_id = "test-session-merge"
        assistant_uuid = "assist-uuid-merge"
        file_a = "/home/test/a.py"
        file_b = "/home/test/b.py"

        fh_dir = tmp_path / ".claude" / "file-history" / session_id
        fh_dir.mkdir(parents=True)
        (fh_dir / "aaa@v1").write_bytes(b"content a")
        (fh_dir / "bbb@v1").write_bytes(b"content b")

        entries = [
            _user_entry("edit both"),
            {
                "type": "file-history-snapshot",
                "messageId": assistant_uuid,
                "timestamp": "2026-02-19T15:02:00Z",
                "snapshot": {
                    "messageId": assistant_uuid,
                    "trackedFileBackups": {
                        file_a: {"backupFileName": "aaa@v1", "version": 1, "backupTime": "..."},
                    },
                    "timestamp": "2026-02-19T15:02:00Z",
                },
            },
            {
                "type": "file-history-snapshot",
                "messageId": assistant_uuid,
                "isSnapshotUpdate": True,
                "timestamp": "2026-02-19T15:02:01Z",
                "snapshot": {
                    "messageId": assistant_uuid,
                    "trackedFileBackups": {
                        file_b: {"backupFileName": "bbb@v1", "version": 1, "backupTime": "..."},
                    },
                    "timestamp": "2026-02-19T15:02:01Z",
                },
            },
            _assistant_tool_entry(
                [
                    ("Edit", {"file_path": file_a, "old_string": "x", "new_string": "y"}),
                    ("Edit", {"file_path": file_b, "old_string": "x", "new_string": "y"}),
                ],
                uuid=assistant_uuid,
            ),
        ]
        jsonl_path = _write_jsonl(tmp_path, session_id, entries)

        import flex.modules.claude_code.compile.worker as w
        from pathlib import Path
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))

        captured = []
        monkeypatch.setattr(w, 'soma_enrich', lambda chunk: chunk)
        monkeypatch.setattr(w, 'soma_insert_edges', lambda conn, chunk: captured.append(dict(chunk)))

        w.sync_session_messages(session_id, conn)

        # Both files should have old_blob_hash
        hashes = [c for c in captured if c.get('old_blob_hash')]
        assert len(hashes) >= 2


class TestProgressAndSystemSkipped:
    """progress and system entries produce no chunks."""

    def test_progress_skipped(self, tmp_path, monkeypatch):
        conn = _make_cell(tmp_path)
        session_id = "test-session-kkk"
        jsonl_path = _write_jsonl(tmp_path, session_id, [
            _user_entry("hello"),
            {"type": "progress", "content": "streaming..."},
            {"type": "system", "subtype": "init", "duration": 5},
            _assistant_text_entry("done"),
        ])

        import flex.modules.claude_code.compile.worker as w
        monkeypatch.setattr(w, 'find_jsonl', lambda sid: jsonl_path if sid == session_id else None)
        monkeypatch.setattr(w, 'get_embedder', lambda: None)
        monkeypatch.setattr(w, 'encode', lambda texts: [[0.1] * 384 for _ in texts])
        monkeypatch.setattr(w, 'serialize_f32', lambda v: _make_embedding())
        monkeypatch.setattr(w, 'soma_enrich', None)
        monkeypatch.setattr(w, 'soma_insert_edges', None)

        inserted = w.sync_session_messages(session_id, conn)
        # Only user + assistant, not progress or system
        assert inserted == 2
