"""Codex installs seed append receipts so the public worker can follow rollouts."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from flex.modules.claude_code import ENRICHMENT_STUBS
from flex.modules.claude_code.compile.worker import (
    _ensure_content_tables,
    _ensure_core_tables,
)
from flex.modules.codex.compile.worker import (
    drain_codex_paths,
    ensure_codex_tables,
    scan_codex_cells,
    transpile,
)


def _message(text: str, role: str, timestamp: str) -> dict:
    content_type = "input_text" if role == "user" else "output_text"
    return {
        "type": "response_item",
        "timestamp": timestamp,
        "payload": {
            "type": "message",
            "role": role,
            "content": [{"type": content_type, "text": text}],
        },
    }


def test_initial_transpile_receipt_drives_live_append_and_worker_restart(
    tmp_path, monkeypatch,
):
    from flex import registry

    sessions = tmp_path / "codex" / "sessions"
    rollout = sessions / "2026" / "09" / "20" / "rollout-test.jsonl"
    rollout.parent.mkdir(parents=True)
    session_id = "11111111-1111-4111-8111-111111111111"
    initial = "WINDOWS_CONTAINER_CODEX_INITIAL_TEST"
    live = "WINDOWS_CONTAINER_CODEX_LIVE_TEST"
    entries = [
        {
            "type": "session_meta",
            "timestamp": "2026-09-20T12:00:00Z",
            "payload": {"id": session_id, "cwd": "/workspace/codex"},
        },
        _message(initial, "user", "2026-09-20T12:00:01Z"),
        _message("Codex fixture ready", "assistant", "2026-09-20T12:00:02Z"),
    ]
    rollout.write_text("".join(json.dumps(entry) + "\n" for entry in entries))

    cell_path = tmp_path / "codex.db"
    conn = sqlite3.connect(cell_path)
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)
    for ddl in ENRICHMENT_STUBS:
        conn.execute(ddl)
    ensure_codex_tables(conn)
    conn.execute(
        "INSERT INTO _meta(key,value) VALUES('codex_source_path',?)",
        (str(sessions),),
    )
    conn.commit()
    transpile(sessions, conn, state_db=tmp_path / "state_5.sqlite")
    receipt = conn.execute(
        "SELECT committed_offset,session_id,parser_state "
        "FROM _codex_source_state WHERE source_path=?",
        (str(rollout.resolve()),),
    ).fetchone()
    conn.close()

    assert receipt is not None
    assert receipt[0] == rollout.stat().st_size
    assert receipt[1] == session_id
    assert json.loads(receipt[2])["session_id"] == session_id

    with rollout.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_message(live, "user", "2026-09-20T12:00:03Z")) + "\n")

    monkeypatch.setattr(
        registry,
        "list_cells",
        lambda: [{
            "name": "codex",
            "path": str(cell_path),
            "cell_type": "codex",
            "lifecycle": "watch",
            "active": 1,
        }],
    )
    stats = scan_codex_cells(
        deadline=time.time() + 15,
        embed=False,
        discover=False,
        cell_names={"codex"},
    )
    assert stats["indexed"] == 1

    conn = sqlite3.connect(cell_path)
    try:
        assert conn.execute(
            "SELECT count(*) FROM _raw_chunks WHERE content LIKE ?",
            (f"%{live}%",),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH ?",
            (live,),
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_public_event_fallback_reconciles_codex_path(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from flex import registry

    sessions = tmp_path / "codex" / "sessions"
    rollout = sessions / "2026" / "09" / "20" / "rollout-new.jsonl"
    rollout.parent.mkdir(parents=True)
    session_id = "33333333-3333-4333-8333-333333333333"
    marker = "WINDOWS_CONTAINER_CODEX_NEW_ROLLOUT"
    rollout.write_text("".join(json.dumps(entry) + "\n" for entry in [
        {
            "type": "session_meta",
            "timestamp": "2026-09-20T12:00:00Z",
            "payload": {"id": session_id, "cwd": "/workspace/codex"},
        },
        _message(marker, "user", "2026-09-20T12:00:01Z"),
    ]))

    cell_path = tmp_path / "codex-event.db"
    conn = sqlite3.connect(cell_path)
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)
    for ddl in ENRICHMENT_STUBS:
        conn.execute(ddl)
    ensure_codex_tables(conn)
    conn.execute(
        "INSERT INTO _meta(key,value) VALUES('codex_source_path',?)",
        (str(sessions),),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        registry,
        "list_cells",
        lambda: [{
            "name": "codex",
            "path": str(cell_path),
            "cell_type": "codex",
            "lifecycle": "watch",
            "active": 1,
        }],
    )

    stats = drain_codex_paths([
        SimpleNamespace(cell_name="codex", source_path=str(rollout))
    ])
    assert stats["indexed"] > 0
    conn = sqlite3.connect(cell_path)
    try:
        assert conn.execute(
            "SELECT count(*) FROM _raw_chunks WHERE content LIKE ?",
            (f"%{marker}%",),
        ).fetchone()[0] == 1
    finally:
        conn.close()
