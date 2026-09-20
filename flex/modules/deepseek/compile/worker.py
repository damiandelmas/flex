"""DeepSeek Harness transpiler.

The Harness source of truth is an append-only ``session.jsonl.zstd`` file per
session.  This module decodes those artifacts, preserves every native event in
``_types_deepseek_event``, keeps session/request metadata in
``_types_deepseek_session``, and projects semantic events into the shared
Claude-shaped coding-agent substrate.  SQLite is only the Flex query cell; it
is never used as the source of truth for DSH history.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from flex.modules.claude_code.compile.worker import (
    _ensure_content_tables,
    _ensure_core_tables,
    _ingest_file_body,
    _store_content_raw,
    ensure_source_exists,
    insert_chunk_atom,
    update_source_stats,
)

try:
    from flex.modules.soma.coding_agent import enrich_operation as soma_enrich_operation
except ImportError:  # pragma: no cover - SOMA is optional in minimal installs
    soma_enrich_operation = None


DEFAULT_DSH_HOME = Path.home() / ".dsh"

DEEPSEEK_TABLES_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS _types_deepseek_session (
        source_id TEXT PRIMARY KEY,
        session_path TEXT NOT NULL,
        workspace_path TEXT,
        created_at_ms INTEGER,
        cwd TEXT,
        parent_session_id TEXT,
        delegation_depth INTEGER,
        origin TEXT,
        agent_preset TEXT,
        session_title TEXT,
        title_source TEXT,
        provider TEXT,
        model TEXT,
        reasoning_effort TEXT,
        max_tokens INTEGER,
        context_window INTEGER,
        system_prompt TEXT,
        event_count INTEGER DEFAULT 0,
        last_event_seq INTEGER,
        completed INTEGER DEFAULT 0,
        terminal_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS _types_deepseek_event (
        event_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        native_seq INTEGER,
        event_type TEXT NOT NULL,
        event_time_ms INTEGER,
        role TEXT,
        surface_op TEXT,
        block_type TEXT,
        payload_json TEXT NOT NULL,
        raw_payload_ref TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_deepseek_event_source_seq ON _types_deepseek_event(source_id, native_seq)",
    "CREATE INDEX IF NOT EXISTS idx_deepseek_event_type ON _types_deepseek_event(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_deepseek_session_model ON _types_deepseek_session(model)",
)


def ensure_deepseek_tables(conn: sqlite3.Connection) -> None:
    """Create DeepSeek-native sidecars idempotently."""
    for ddl in DEEPSEEK_TABLES_DDL:
        conn.execute(ddl)


def ensure_deepseek_cell_schema(conn: sqlite3.Connection) -> None:
    """Heal the shared coding-agent envelope before a provider write."""
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)
    ensure_deepseek_tables(conn)
    conn.commit()


def _source_files(root: Path) -> list[Path]:
    root = root.expanduser()
    if root.is_file():
        return [root] if root.name in {"session.jsonl", "session.jsonl.zstd"} else []
    if not root.is_dir():
        return []
    paths: dict[str, Path] = {}
    for pattern in ("**/session.jsonl.zstd", "**/session.jsonl"):
        for path in root.glob(pattern):
            if path.is_file():
                paths[str(path.resolve())] = path
    return sorted(paths.values(), key=lambda path: str(path))


def compute_source_signature(source_path: Path) -> tuple[str, int]:
    """Return a deterministic signature and byte total for DSH artifacts."""
    source = Path(source_path).expanduser()
    payload: list[dict[str, Any]] = []
    total_size = 0
    for path in _source_files(source):
        try:
            stat = path.stat()
        except OSError:
            continue
        try:
            relative = str(path.relative_to(source)) if source.is_dir() else path.name
        except ValueError:
            relative = str(path)
        total_size += stat.st_size
        payload.append({"path": relative, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), total_size


def _decode_zstd(path: Path) -> bytes:
    """Decode one DSH zstd artifact without making zstandard a hard dependency."""
    executable = shutil.which("zstd")
    if executable is None:
        raise RuntimeError(
            "DeepSeek session is compressed with zstd, but the zstd executable is not installed"
        )
    result = subprocess.run(
        [executable, "--quiet", "--decompress", "--stdout", str(path)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"could not decode {path}: {detail or 'zstd failed'}")
    return result.stdout


def _read_events(path: Path) -> list[dict[str, Any]]:
    raw = _decode_zstd(path) if path.name.endswith(".zstd") else path.read_bytes()
    events: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            # DSH deliberately ignores a torn final JSONL line until the next
            # reconciliation, matching its own committed-prefix semantics.
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def _json_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(part for part in (_json_text(item) for item in value) if part)
    if isinstance(value, dict):
        for key in ("text", "content", "value", "output"):
            if key in value:
                rendered = _json_text(value[key])
                if rendered:
                    return rendered
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _event_time_ms(event: Mapping[str, Any], default: int) -> int:
    value = event.get("time", event.get("createdAt", default))
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _epoch_seconds(time_ms: int) -> int:
    return int(time_ms / 1000) if time_ms > 10_000_000_000 else int(time_ms)


def _session_id(path: Path, events: list[dict[str, Any]]) -> str:
    header = events[0] if events else {}
    value = header.get("id")
    if isinstance(value, str) and value:
        return value
    # A malformed/missing header remains addressable and deterministic.
    return path.parent.name if path.parent.name.startswith("session-") else path.stem


def _header(events: list[dict[str, Any]]) -> dict[str, Any]:
    if events and events[0].get("type") == "session":
        return events[0]
    return {}


def _message_blocks(message: Mapping[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if isinstance(content, list):
        return [item for item in content if isinstance(item, dict)]
    if isinstance(content, dict):
        return [content]
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return []


def _block_type(block: Mapping[str, Any]) -> str:
    return str(block.get("type") or block.get("blockType") or "text")


def _tool_name(block: Mapping[str, Any]) -> str | None:
    for key in ("name", "toolName", "tool_name", "tool"):
        value = block.get(key)
        if isinstance(value, str) and value:
            return value
    tool_call = block.get("toolCall")
    if isinstance(tool_call, dict):
        return _tool_name(tool_call)
    return None


def _block_arguments(block: Mapping[str, Any]) -> dict[str, Any]:
    for key in ("arguments", "args", "input", "parameters"):
        value = block.get(key)
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return {}
        if isinstance(value, dict):
            return value
    tool_call = block.get("toolCall")
    if isinstance(tool_call, dict):
        return _block_arguments(tool_call)
    return {}


def _target_file(arguments: Mapping[str, Any]) -> str | None:
    for key in ("path", "file_path", "file", "filename", "notebook_path"):
        value = arguments.get(key)
        if value:
            return str(value)
    return None


def _block_success(block: Mapping[str, Any]) -> int | None:
    if "isError" in block:
        return 0 if block["isError"] else 1
    if "success" in block:
        return 1 if block["success"] else 0
    reason = block.get("reason")
    if isinstance(reason, dict) and reason.get("kind") == "error":
        return 0
    return None


def _chunk(
    *,
    session_id: str,
    chunk_number: int,
    event_id: str,
    kind: str,
    content: str,
    timestamp: int,
    role: str,
    cwd: str | None,
    tool_name: str | None = None,
    target_file: str | None = None,
    success: int | None = None,
    spawned_agent: str | None = None,
) -> dict[str, Any]:
    return {
        "id": f"{session_id}:{event_id}:{chunk_number}",
        "doc_id": session_id,
        "chunk_number": chunk_number,
        "type": kind,
        "content": content,
        "tool_name": tool_name,
        "target_file": target_file,
        "success": success,
        "timestamp": timestamp,
        "role": role,
        "cwd": cwd,
        "git_branch": None,
        "parent_uuid": None,
        "is_sidechain": 0,
        "entry_uuid": event_id,
        "branch_id": 0,
        "spawned_agent": spawned_agent,
        "agent_type": "deepseek",
    }


def _insert_deepseek_chunk(conn: sqlite3.Connection, chunk: dict[str, Any], raw: Any = None) -> bool:
    inserted = insert_chunk_atom(conn, chunk)
    conn.execute(
        "UPDATE _edges_source SET source_type = 'deepseek' WHERE chunk_id = ?",
        (chunk["id"],),
    )
    if raw is not None:
        _store_content_raw(
            conn,
            chunk["id"],
            json.dumps(raw, ensure_ascii=False, separators=(",", ":")),
            chunk.get("tool_name") or "deepseek_event",
            chunk["timestamp"],
        )
    if chunk.get("target_file") and chunk.get("content"):
        tool = chunk.get("tool_name") or ""
        if tool.lower() in {"write", "edit", "str_replace", "read", "view"}:
            try:
                _ingest_file_body(
                    conn,
                    chunk["id"],
                    chunk["target_file"],
                    chunk["content"],
                    chunk["doc_id"],
                    chunk["timestamp"],
                )
            except Exception as exc:  # file-body enrichment must not lose the event
                print(f"[deepseek] file-body enrichment failed: {exc}", file=sys.stderr)
    if soma_enrich_operation and chunk.get("tool_name") and chunk.get("target_file"):
        try:
            soma_enrich_operation(
                conn,
                {
                    "chunk_id": chunk["id"],
                    "tool_name": chunk["tool_name"],
                    "target_file": chunk["target_file"],
                    "cwd": chunk.get("cwd"),
                    "source_id": chunk["doc_id"],
                },
            )
        except Exception as exc:  # identity is optional enrichment
            print(f"[deepseek] SOMA enrichment failed: {exc}", file=sys.stderr)
    return inserted


def _record_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    event: Mapping[str, Any],
    path: Path,
    created_at_ms: int,
) -> None:
    native_seq = event.get("seq")
    try:
        native_seq = int(native_seq) if native_seq is not None else None
    except (TypeError, ValueError):
        native_seq = None
    event_id = f"{session_id}:{native_seq if native_seq is not None else event.get('type', 'event')}:{hashlib.sha1(json.dumps(event, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()[:12]}"
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    chunk = data.get("chunk") if isinstance(data.get("chunk"), dict) else {}
    message = data.get("message") if isinstance(data.get("message"), dict) else {}
    conn.execute(
        """
        INSERT OR REPLACE INTO _types_deepseek_event
        (event_id, source_id, native_seq, event_type, event_time_ms, role,
         surface_op, block_type, payload_json, raw_payload_ref)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            event_id,
            session_id,
            native_seq,
            str(event.get("type") or "unknown"),
            _event_time_ms(event, created_at_ms),
            str(message.get("role") or data.get("role") or "") or None,
            str(event.get("surfaceOp") or "") or None,
            str(chunk.get("blockType") or chunk.get("type") or "") or None,
            json.dumps(event, ensure_ascii=False, separators=(",", ":")),
        ),
    )


def _session_sidecar(
    conn: sqlite3.Connection,
    session_id: str,
    path: Path,
    header: Mapping[str, Any],
) -> None:
    cwd = header.get("cwd") if isinstance(header.get("cwd"), str) else None
    workspace_path = path.parent.parent.name if path.parent.parent else None
    conn.execute(
        """
        INSERT OR REPLACE INTO _types_deepseek_session
        (source_id, session_path, workspace_path, created_at_ms, cwd,
         parent_session_id, delegation_depth, origin, agent_preset,
         session_title, title_source, provider, model, reasoning_effort,
         max_tokens, context_window, system_prompt, event_count,
         last_event_seq, completed, terminal_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, 0, NULL)
        """,
        (
            session_id,
            str(path),
            workspace_path,
            header.get("createdAt"),
            cwd,
            header.get("parentSession"),
            header.get("delegationDepth", 0),
            header.get("origin"),
            header.get("agentPreset"),
        ),
    )


def _update_session_metadata(
    conn: sqlite3.Connection,
    session_id: str,
    event: Mapping[str, Any],
) -> None:
    event_type = str(event.get("type") or "")
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    updates: dict[str, Any] = {}
    if event_type == "session/title" and isinstance(data.get("title"), str):
        updates.update(session_title=data["title"], title_source=str(data.get("source") or "native"))
    if event_type == "request/header":
        header = data.get("header") if isinstance(data.get("header"), dict) else {}
        config = header.get("config") if isinstance(header.get("config"), dict) else {}
        updates.update(
            provider=config.get("provider"),
            model=config.get("model"),
            reasoning_effort=config.get("reasoningEffort"),
            max_tokens=config.get("maxTokens"),
            system_prompt=header.get("system") if isinstance(header.get("system"), str) else None,
        )
    if event_type == "request/context":
        updates.update(
            provider=data.get("provider"),
            model=data.get("model"),
            context_window=data.get("contextWindow"),
        )
    if event_type == "turn/end":
        reason = data.get("reason")
        updates["completed"] = 1
        updates["terminal_reason"] = json.dumps(reason, ensure_ascii=False, separators=(",", ":")) if reason else None
    if updates:
        assignments = ", ".join(f"{key} = ?" for key in updates)
        conn.execute(
            f"UPDATE _types_deepseek_session SET {assignments} WHERE source_id = ?",
            (*updates.values(), session_id),
        )


def _event_chunks(
    session_id: str,
    event: Mapping[str, Any],
    cwd: str | None,
    created_at_ms: int,
    chunk_number: int,
) -> list[dict[str, Any]]:
    event_type = str(event.get("type") or "")
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    timestamp = _epoch_seconds(_event_time_ms(event, created_at_ms))
    event_id = str(event.get("seq") if event.get("seq") is not None else event_type)
    output: list[dict[str, Any]] = []

    if event_type == "user/message":
        role = str(data.get("role") or "user")
        source = data.get("source") if isinstance(data.get("source"), dict) else {}
        kind = "user_prompt" if source.get("kind") == "user" else "agent_context"
        content = _json_text(data.get("content"))
        if content:
            output.append(_chunk(
                session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                kind=kind, content=content, timestamp=timestamp, role=role, cwd=cwd,
            ))
        return output

    if event_type == "assistant/message":
        message = data.get("message") if isinstance(data.get("message"), dict) else data
        role = str(message.get("role") or "assistant")
        for index, block in enumerate(_message_blocks(message)):
            block_type = _block_type(block)
            tool_name = _tool_name(block)
            arguments = _block_arguments(block)
            if block_type in {"tool-call", "tool_call", "toolCall", "tool-result", "tool_result", "toolResult"} or tool_name:
                kind = "tool_call"
                if block_type in {"tool-result", "tool_result", "toolResult"}:
                    kind = "tool_result"
                content = _json_text(block)
                if kind == "tool_result":
                    result_content = block.get("content")
                    content = _json_text(result_content if result_content is not None else block)
                output.append(_chunk(
                    session_id=session_id, chunk_number=chunk_number + index,
                    event_id=f"{event_id}:{index}", kind=kind, content=content,
                    timestamp=timestamp, role=role, cwd=cwd, tool_name=tool_name,
                    target_file=_target_file(arguments), success=_block_success(block),
                    spawned_agent=(arguments.get("sessionId") or arguments.get("session_id")),
                ))
                continue
            kind = "agent_thought" if block_type == "reasoning" else "assistant"
            content = _json_text(block.get("text") if "text" in block else block)
            if content:
                output.append(_chunk(
                    session_id=session_id, chunk_number=chunk_number + index,
                    event_id=f"{event_id}:{index}", kind=kind, content=content,
                    timestamp=timestamp, role=role, cwd=cwd,
                ))
        if not output:
            content = _json_text(message.get("content"))
            if content:
                output.append(_chunk(
                    session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                    kind="assistant", content=content, timestamp=timestamp,
                    role=role, cwd=cwd,
                ))
        return output

    if event_type == "tool/call":
        name = str(data.get("name") or "unknown")
        arguments = data.get("arguments")
        arguments_dict = _block_arguments({"arguments": arguments})
        output.append(_chunk(
            session_id=session_id, chunk_number=chunk_number, event_id=event_id,
            kind="tool_call", content=f"{name}: {_json_text(arguments)}",
            timestamp=timestamp, role="assistant", cwd=cwd, tool_name=name,
            target_file=_target_file(arguments_dict),
        ))
        return output

    if event_type == "tool/result":
        message = data.get("message") if isinstance(data.get("message"), dict) else data
        content = _json_text(message.get("content") if isinstance(message, dict) else message)
        call_id = data.get("callId") or data.get("call_id")
        is_error = data.get("isError")
        if is_error is None and isinstance(data.get("error"), dict):
            is_error = True
        output.append(_chunk(
            session_id=session_id, chunk_number=chunk_number, event_id=event_id,
            kind="tool_result", content=content or _json_text(data),
            timestamp=timestamp, role="user", cwd=cwd,
            tool_name=f"tool:{call_id}" if call_id else "tool_result",
            success=0 if is_error else 1 if is_error is not None else None,
        ))
        return output

    if event_type in {"todo/write", "plan/mode", "goal/change"}:
        output.append(_chunk(
            session_id=session_id, chunk_number=chunk_number, event_id=event_id,
            kind="agent_plan", content=f"{event_type}: {_json_text(data)}",
            timestamp=timestamp, role="system", cwd=cwd,
        ))
        return output

    if event_type == "subagent/descriptor":
        child = data.get("sessionId") or data.get("session_id") or data.get("id")
        output.append(_chunk(
            session_id=session_id, chunk_number=chunk_number, event_id=event_id,
            kind="agent_delegation", content=_json_text(data), timestamp=timestamp,
            role="assistant", cwd=cwd, tool_name="Task", spawned_agent=child,
        ))
        return output

    if event_type in {"permission/preset", "sandbox/mode", "approval/policy"}:
        output.append(_chunk(
            session_id=session_id, chunk_number=chunk_number, event_id=event_id,
            kind="agent_config", content=f"{event_type}: {_json_text(data)}",
            timestamp=timestamp, role="system", cwd=cwd,
        ))
        return output

    if event_type == "turn/end":
        reason = data.get("reason")
        if isinstance(reason, dict) and reason.get("kind") == "error":
            error = reason.get("error") if isinstance(reason.get("error"), dict) else reason
            output.append(_chunk(
                session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                kind="agent_stop", content=f"turn failed: {_json_text(error)}",
                timestamp=timestamp, role="system", cwd=cwd, success=0,
            ))
        return output

    if event_type == "agent/delegation":
        child = data.get("childSessionId") or data.get("child_session_id") or data.get("sessionId")
        content = _json_text(data)
        output.append(_chunk(
            session_id=session_id, chunk_number=chunk_number, event_id=event_id,
            kind="agent_delegation", content=content, timestamp=timestamp,
            role="assistant", cwd=cwd, tool_name="Task", spawned_agent=child,
        ))
        return output

    # A failed/incomplete stream may have no assistant/message finalization.
    # Keep individual delta chunks as a lossless fallback rather than dropping
    # model-visible text.
    if event_type == "assistant/chunk":
        chunk = data.get("chunk") if isinstance(data.get("chunk"), dict) else {}
        chunk_type = str(chunk.get("type") or "")
        if chunk_type in {"text-delta", "reasoning-delta"} and chunk.get("text"):
            kind = "agent_thought" if chunk_type == "reasoning-delta" else "assistant"
            output.append(_chunk(
                session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                kind=kind, content=str(chunk["text"]), timestamp=timestamp,
                role="assistant", cwd=cwd,
            ))
        elif chunk_type == "tool-call-delta" and (
            chunk.get("argumentsDelta") or chunk.get("name")
        ):
            output.append(_chunk(
                session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                kind="tool_call", content=_json_text(chunk), timestamp=timestamp,
                role="assistant", cwd=cwd, tool_name=chunk.get("name") or "tool_call",
            ))
        elif chunk_type == "block-end" and isinstance(chunk.get("block"), dict):
            block = chunk["block"]
            block_kind = _block_type(block)
            if block_kind in {"tool-call", "tool_call", "toolCall"}:
                output.append(_chunk(
                    session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                    kind="tool_call", content=_json_text(block), timestamp=timestamp,
                    role="assistant", cwd=cwd, tool_name=_tool_name(block),
                    target_file=_target_file(_block_arguments(block)),
                ))
        elif chunk_type == "finish":
            reason = chunk.get("reason")
            if isinstance(reason, dict) and reason.get("kind") in {"error", "aborted"}:
                output.append(_chunk(
                    session_id=session_id, chunk_number=chunk_number, event_id=event_id,
                    kind="agent_stop", content=f"stream ended: {_json_text(reason)}",
                    timestamp=timestamp, role="system", cwd=cwd, success=0,
                ))
    return output


def _clear_provider_rows(conn: sqlite3.Connection) -> None:
    source_ids = [row[0] for row in conn.execute(
        "SELECT DISTINCT source_id FROM _edges_source WHERE source_type = 'deepseek'"
    )]
    if not source_ids:
        conn.execute("DELETE FROM _types_deepseek_event")
        conn.execute("DELETE FROM _types_deepseek_session")
        return
    marks = ",".join("?" for _ in source_ids)
    chunk_ids = [row[0] for row in conn.execute(
        f"SELECT chunk_id FROM _edges_source WHERE source_id IN ({marks})", source_ids
    )]
    if chunk_ids:
        chunk_marks = ",".join("?" for _ in chunk_ids)
        for table, column in (
            ("_edges_delegations", "chunk_id"),
            ("_edges_soft_ops", "chunk_id"),
            ("_edges_raw_content", "chunk_id"),
            ("_edges_tool_ops", "chunk_id"),
            ("_types_message", "chunk_id"),
            ("_types_file_body", "chunk_id"),
            ("_enrich_chunk_rollup", "chunk_id"),
            ("_raw_chunks", "id"),
        ):
            try:
                conn.execute(f"DELETE FROM {table} WHERE {column} IN ({chunk_marks})", chunk_ids)
            except sqlite3.OperationalError:
                pass
    conn.execute(f"DELETE FROM _edges_source WHERE source_id IN ({marks})", source_ids)
    conn.execute(f"DELETE FROM _raw_sources WHERE source_id IN ({marks})", source_ids)
    conn.execute(f"DELETE FROM _types_deepseek_event WHERE source_id IN ({marks})", source_ids)
    conn.execute(f"DELETE FROM _types_deepseek_session WHERE source_id IN ({marks})", source_ids)


def _transpile_session(path: Path, conn: sqlite3.Connection) -> tuple[int, int]:
    events = _read_events(path)
    if not events:
        return "", 0
    header = _header(events)
    session_id = _session_id(path, events)
    created_at_ms = int(header.get("createdAt") or int(time.time() * 1000))
    cwd = header.get("cwd") if isinstance(header.get("cwd"), str) else None
    ensure_source_exists(conn, session_id, cwd=cwd)
    conn.execute(
        "UPDATE _raw_sources SET source = 'deepseek:' || source_id WHERE source_id = ?",
        (session_id,),
    )
    _session_sidecar(conn, session_id, path, header)

    native_chunks = 0
    emitted = 0
    for event in events[1:] if header else events:
        _record_event(conn, session_id=session_id, event=event, path=path, created_at_ms=created_at_ms)
        _update_session_metadata(conn, session_id, event)
        chunks = _event_chunks(session_id, event, cwd, created_at_ms, emitted)
        for chunk in chunks:
            if _insert_deepseek_chunk(conn, chunk, raw=event):
                native_chunks += 1
            update_source_stats(conn, session_id, chunk)
            emitted += 1

    sidecar = conn.execute(
        "SELECT session_title, provider, model FROM _types_deepseek_session WHERE source_id = ?",
        (session_id,),
    ).fetchone()
    if sidecar:
        conn.execute(
            "UPDATE _raw_sources SET title = COALESCE(?, title), model = ? WHERE source_id = ?",
            (sidecar[0], sidecar[2] or sidecar[1], session_id),
        )
    conn.execute(
        "UPDATE _types_deepseek_session SET event_count = ?, last_event_seq = ? WHERE source_id = ?",
        (len(events[1:] if header else events), events[-1].get("seq") if events else None, session_id),
    )
    return session_id, native_chunks


def transpile(
    source: Path,
    conn: sqlite3.Connection,
    progress_cb: Callable[[int, int, int, int, float], None] | None = None,
) -> dict[str, int]:
    """Rebuild the DeepSeek projection from the selected native source root."""
    ensure_deepseek_cell_schema(conn)
    _clear_provider_rows(conn)
    paths = _source_files(Path(source))
    started = time.monotonic()
    sessions = 0
    chunks = 0
    events = 0
    for index, path in enumerate(paths, start=1):
        try:
            session_id, inserted = _transpile_session(path, conn)
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            print(f"[deepseek] skipped {path}: {exc}", file=sys.stderr)
            continue
        if session_id:
            sessions += 1
            chunks += inserted
            try:
                events += int(conn.execute(
                    "SELECT event_count FROM _types_deepseek_session WHERE source_id = ?",
                    (session_id,),
                ).fetchone()[0] or 0)
            except (TypeError, ValueError, sqlite3.Error):
                pass
        if progress_cb:
            progress_cb(index, len(paths), sessions, chunks, time.monotonic() - started)
    conn.commit()
    return {"sessions": sessions, "chunks": chunks, "events": events}
