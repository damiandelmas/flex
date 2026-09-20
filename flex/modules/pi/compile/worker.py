"""Pi coding-agent session transpiler.

Pi persists one append-only, versioned JSONL tree per session. This compiler
validates the native header, preserves every entry and parent edge in Pi
sidecars, derives the active leaf path without discarding abandoned branches,
and projects model-visible messages into Flex's shared coding-agent substrate.
SQLite is a rebuildable query projection; the JSONL files remain authoritative.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime
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


DEFAULT_PI_SESSIONS = Path.home() / ".pi" / "agent" / "sessions"

PI_TABLES_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS _types_pi_session (
        source_id TEXT PRIMARY KEY,
        session_path TEXT NOT NULL,
        workspace_path TEXT,
        session_version INTEGER,
        created_at_ms INTEGER,
        cwd TEXT,
        parent_session_path TEXT,
        latest_leaf_id TEXT,
        entry_count INTEGER DEFAULT 0,
        active_entry_count INTEGER DEFAULT 0,
        message_count INTEGER DEFAULT 0,
        branch_point_count INTEGER DEFAULT 0,
        orphan_count INTEGER DEFAULT 0,
        session_title TEXT,
        provider TEXT,
        model TEXT,
        thinking_level TEXT,
        last_stop_reason TEXT,
        errored INTEGER DEFAULT 0,
        usage_input INTEGER DEFAULT 0,
        usage_output INTEGER DEFAULT 0,
        cache_read INTEGER DEFAULT 0,
        cache_write INTEGER DEFAULT 0,
        reasoning_tokens INTEGER DEFAULT 0,
        total_tokens INTEGER DEFAULT 0,
        total_cost REAL DEFAULT 0,
        first_user_prompt TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS _types_pi_entry (
        entry_key TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        native_entry_id TEXT NOT NULL,
        parent_native_id TEXT,
        file_position INTEGER NOT NULL,
        entry_type TEXT NOT NULL,
        timestamp_ms INTEGER,
        role TEXT,
        block_types TEXT,
        custom_type TEXT,
        provider TEXT,
        model TEXT,
        thinking_level TEXT,
        tool_call_id TEXT,
        tool_name TEXT,
        target_id TEXT,
        label TEXT,
        is_active_branch INTEGER DEFAULT 0,
        depth INTEGER,
        child_count INTEGER DEFAULT 0,
        payload_json TEXT NOT NULL,
        raw_payload_ref TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pi_entry_source_position ON _types_pi_entry(source_id, file_position)",
    "CREATE INDEX IF NOT EXISTS idx_pi_entry_parent ON _types_pi_entry(source_id, parent_native_id)",
    "CREATE INDEX IF NOT EXISTS idx_pi_entry_type ON _types_pi_entry(entry_type)",
    "CREATE INDEX IF NOT EXISTS idx_pi_entry_active ON _types_pi_entry(source_id, is_active_branch)",
    "CREATE INDEX IF NOT EXISTS idx_pi_session_model ON _types_pi_session(model)",
)


def ensure_pi_tables(conn: sqlite3.Connection) -> None:
    """Create Pi-native sidecars idempotently."""
    for ddl in PI_TABLES_DDL:
        conn.execute(ddl)


def ensure_pi_cell_schema(conn: sqlite3.Connection) -> None:
    """Heal the shared coding-agent envelope before a provider write."""
    _ensure_core_tables(conn)
    _ensure_content_tables(conn)
    ensure_pi_tables(conn)
    conn.commit()


def _read_entries(path: Path) -> list[dict[str, Any]]:
    """Parse Pi's committed JSONL prefix and require its native header."""
    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                # Pi's loader skips malformed physical lines. An active append
                # is reconciled on the next watch pass once it becomes valid.
                continue
            if isinstance(value, dict):
                entries.append(value)
    if not entries:
        return []
    header = entries[0]
    if header.get("type") != "session" or not isinstance(header.get("id"), str):
        return []
    return entries


def _is_session_file(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                return (
                    isinstance(value, dict)
                    and value.get("type") == "session"
                    and isinstance(value.get("id"), str)
                )
    except (OSError, UnicodeError):
        return False
    return False


def _source_files(root: Path) -> list[Path]:
    root = root.expanduser()
    if root.is_file():
        return [root] if root.suffix == ".jsonl" and _is_session_file(root) else []
    if not root.is_dir():
        return []
    return sorted(
        (path for path in root.rglob("*.jsonl") if path.is_file() and _is_session_file(path)),
        key=lambda path: str(path),
    )


def compute_source_signature(source_path: Path) -> tuple[str, int]:
    """Return a deterministic signature and byte total for canonical Pi sessions."""
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


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _timestamp_ms(value: Any, default: int = 0) -> int:
    if isinstance(value, (int, float)):
        return int(value * 1000) if value < 10_000_000_000 else int(value)
    if isinstance(value, str) and value:
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return int(datetime.fromisoformat(text).timestamp() * 1000)
        except ValueError:
            try:
                numeric = float(value)
            except ValueError:
                return default
            return int(numeric * 1000) if numeric < 10_000_000_000 else int(numeric)
    return default


def _epoch_seconds(value: Any, default_ms: int) -> int:
    return int(_timestamp_ms(value, default_ms) / 1000)


def _content_blocks(content: Any) -> list[dict[str, Any]]:
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if isinstance(content, dict):
        return [content]
    if isinstance(content, list):
        return [block for block in content if isinstance(block, dict)]
    return []


def _block_text(block: Mapping[str, Any]) -> str:
    block_type = str(block.get("type") or "")
    if block_type == "text":
        return str(block.get("text") or "")
    if block_type == "thinking":
        return str(block.get("thinking") or "")
    if block_type == "image":
        mime = str(block.get("mimeType") or "image")
        return f"[image: {mime}]"
    return _json(block)


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    rendered = [_block_text(block) for block in _content_blocks(content)]
    return "\n".join(text for text in rendered if text)


def _target_file(arguments: Mapping[str, Any]) -> str | None:
    for key in ("path", "file_path", "file", "filename", "notebook_path"):
        value = arguments.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _nested_identifier(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in (
            "sessionId",
            "session_id",
            "missionId",
            "mission_id",
            "asyncId",
            "agentId",
            "agent_id",
            "id",
        ):
            item = value.get(key)
            if isinstance(item, str) and item:
                return item
        for item in value.values():
            found = _nested_identifier(item)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _nested_identifier(item)
            if found:
                return found
    return None


def _chunk(
    *,
    session_id: str,
    chunk_number: int,
    entry_id: str,
    block_index: int,
    kind: str,
    content: str,
    timestamp: int,
    role: str,
    cwd: str | None,
    parent_id: str | None,
    active: bool,
    tool_name: str | None = None,
    target_file: str | None = None,
    success: int | None = None,
    spawned_agent: str | None = None,
) -> dict[str, Any]:
    return {
        "id": f"{session_id}:{entry_id}:{block_index}",
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
        "parent_uuid": parent_id,
        "is_sidechain": 0 if active else 1,
        "entry_uuid": entry_id,
        "branch_id": 0 if active else 1,
        "spawned_agent": spawned_agent,
        "agent_type": "pi",
    }


def _insert_pi_chunk(conn: sqlite3.Connection, chunk: dict[str, Any], raw: Any) -> bool:
    inserted = insert_chunk_atom(conn, chunk)
    conn.execute("UPDATE _edges_source SET source_type = 'pi' WHERE chunk_id = ?", (chunk["id"],))
    _store_content_raw(
        conn,
        chunk["id"],
        _json(raw),
        chunk.get("tool_name") or "pi_entry",
        chunk["timestamp"],
    )
    if chunk.get("target_file") and chunk.get("content"):
        tool = str(chunk.get("tool_name") or "").lower()
        if tool in {"write", "edit", "str_replace", "read", "view"}:
            try:
                _ingest_file_body(
                    conn,
                    chunk["id"],
                    chunk["target_file"],
                    chunk["content"],
                    chunk["doc_id"],
                    chunk["timestamp"],
                )
            except Exception as exc:
                print(f"[pi] file-body enrichment failed: {exc}", file=sys.stderr)
    tool = str(chunk.get("tool_name") or "").lower()
    if (
        soma_enrich_operation
        and chunk.get("type") == "tool_call"
        and tool in {"read", "write", "edit", "multiedit", "str_replace", "view", "grep", "glob"}
        and chunk.get("target_file")
    ):
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
        except Exception as exc:
            print(f"[pi] SOMA enrichment failed: {exc}", file=sys.stderr)
    return inserted


def _tree_metadata(entries: list[dict[str, Any]]) -> tuple[str | None, set[str], Counter, dict[str, int], int]:
    by_id = {
        str(entry["id"]): entry
        for entry in entries
        if isinstance(entry.get("id"), str) and entry.get("id")
    }
    latest_leaf: str | None = None
    # Pi persists the current cursor as replayable state: ordinary entries
    # advance to their own id, while a leaf control entry redirects to targetId.
    for entry in entries:
        native_id = entry.get("id")
        if not isinstance(native_id, str) or not native_id:
            continue
        if entry.get("type") == "leaf":
            target_id = entry.get("targetId")
            latest_leaf = target_id if isinstance(target_id, str) and target_id else None
        else:
            latest_leaf = native_id
    child_counts: Counter = Counter(
        str(entry["parentId"])
        for entry in entries
        if isinstance(entry.get("parentId"), str) and entry.get("parentId")
    )
    active: set[str] = set()
    current = latest_leaf
    while current and current not in active:
        entry = by_id.get(current)
        if entry is None:
            break
        active.add(current)
        parent = entry.get("parentId")
        current = str(parent) if isinstance(parent, str) and parent else None

    depths: dict[str, int] = {}
    orphans = sum(
        1
        for entry in by_id.values()
        if isinstance(entry.get("parentId"), str) and entry.get("parentId") not in by_id
    )
    for native_id, entry in by_id.items():
        chain: list[str] = []
        seen: set[str] = set()
        cursor: str | None = native_id
        base = -1
        while cursor and cursor not in seen:
            if cursor in depths:
                base = depths[cursor]
                break
            seen.add(cursor)
            chain.append(cursor)
            parent = by_id.get(cursor, {}).get("parentId")
            if parent is None:
                base = -1
                break
            if not isinstance(parent, str) or parent not in by_id:
                base = -1
                break
            cursor = parent
        for item in reversed(chain):
            base += 1
            depths[item] = base
    return latest_leaf, active, child_counts, depths, orphans


def _tool_calls(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    calls: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if entry.get("type") != "message":
            continue
        message = entry.get("message") if isinstance(entry.get("message"), dict) else {}
        if message.get("role") != "assistant":
            continue
        for block in _content_blocks(message.get("content")):
            if block.get("type") != "toolCall" or not isinstance(block.get("id"), str):
                continue
            arguments = block.get("arguments") if isinstance(block.get("arguments"), dict) else {}
            calls[str(block["id"])] = {
                "name": str(block.get("name") or "tool"),
                "arguments": arguments,
                "target_file": _target_file(arguments),
                "spawned_agent": _nested_identifier(arguments),
            }
    return calls


def _usage_values(usage: Any) -> dict[str, float]:
    if not isinstance(usage, dict):
        return {}
    cost = usage.get("cost") if isinstance(usage.get("cost"), dict) else {}
    return {
        "input": float(usage.get("input") or 0),
        "output": float(usage.get("output") or 0),
        "cacheRead": float(usage.get("cacheRead") or 0),
        "cacheWrite": float(usage.get("cacheWrite") or 0),
        "reasoning": float(usage.get("reasoning") or 0),
        "totalTokens": float(usage.get("totalTokens") or 0),
        "cost": float(cost.get("total") or 0),
    }


def _session_metadata(entries: list[dict[str, Any]], active: set[str]) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "title": None,
        "provider": None,
        "model": None,
        "thinking": None,
        "stop_reason": None,
        "errored": 0,
        "first_user_prompt": None,
        "message_count": 0,
        "usage": Counter(),
    }
    for entry in entries:
        entry_type = entry.get("type")
        entry_id = entry.get("id")
        is_active = isinstance(entry_id, str) and entry_id in active
        if entry_type == "session_info":
            name = entry.get("name")
            metadata["title"] = name.strip() if isinstance(name, str) and name.strip() else None
        elif entry_type == "model_change" and is_active:
            metadata["provider"] = entry.get("provider")
            metadata["model"] = entry.get("modelId")
        elif entry_type == "thinking_level_change" and is_active:
            metadata["thinking"] = entry.get("thinkingLevel")

        usage = None
        if entry_type in {"compaction", "branch_summary"}:
            usage = entry.get("usage")
        if entry_type != "message":
            for key, value in _usage_values(usage).items():
                metadata["usage"][key] += value
            continue

        metadata["message_count"] += 1
        message = entry.get("message") if isinstance(entry.get("message"), dict) else {}
        role = message.get("role")
        if role == "user" and metadata["first_user_prompt"] is None:
            prompt = _content_text(message.get("content")).strip()
            metadata["first_user_prompt"] = prompt or None
        if role == "assistant" and is_active:
            metadata["provider"] = message.get("provider") or metadata["provider"]
            metadata["model"] = message.get("model") or metadata["model"]
            metadata["stop_reason"] = message.get("stopReason") or metadata["stop_reason"]
            if message.get("stopReason") in {"error", "aborted"} or message.get("errorMessage"):
                metadata["errored"] = 1
        if role == "assistant":
            usage = message.get("usage")
        elif role == "toolResult":
            usage = message.get("usage")
        for key, value in _usage_values(usage).items():
            metadata["usage"][key] += value
    if metadata["title"] is None and metadata["first_user_prompt"]:
        metadata["title"] = str(metadata["first_user_prompt"])[:160]
    return metadata


def _record_entry(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    entry: Mapping[str, Any],
    position: int,
    active: set[str],
    child_counts: Counter,
    depths: Mapping[str, int],
) -> None:
    native_id = str(entry.get("id") or f"position-{position}")
    message = entry.get("message") if isinstance(entry.get("message"), dict) else {}
    blocks = _content_blocks(message.get("content"))
    tool_blocks = [block for block in blocks if block.get("type") == "toolCall"]
    conn.execute(
        """
        INSERT OR REPLACE INTO _types_pi_entry
        (entry_key, source_id, native_entry_id, parent_native_id, file_position,
         entry_type, timestamp_ms, role, block_types, custom_type, provider,
         model, thinking_level, tool_call_id, tool_name, target_id, label,
         is_active_branch, depth, child_count, payload_json, raw_payload_ref)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            f"{session_id}:{native_id}",
            session_id,
            native_id,
            entry.get("parentId"),
            position,
            str(entry.get("type") or "unknown"),
            _timestamp_ms(entry.get("timestamp")),
            message.get("role"),
            ",".join(str(block.get("type") or "unknown") for block in blocks) or None,
            entry.get("customType") or message.get("customType"),
            entry.get("provider") or message.get("provider"),
            entry.get("modelId") or message.get("model"),
            entry.get("thinkingLevel"),
            tool_blocks[0].get("id") if tool_blocks else message.get("toolCallId"),
            tool_blocks[0].get("name") if tool_blocks else message.get("toolName"),
            entry.get("targetId"),
            entry.get("label"),
            1 if native_id in active else 0,
            depths.get(native_id),
            child_counts.get(native_id, 0),
            _json(entry),
        ),
    )


def _entry_chunks(
    *,
    session_id: str,
    entry: Mapping[str, Any],
    chunk_number: int,
    cwd: str | None,
    active: bool,
    calls: Mapping[str, Mapping[str, Any]],
    default_ms: int,
) -> list[dict[str, Any]]:
    entry_type = str(entry.get("type") or "")
    entry_id = str(entry.get("id") or f"position-{chunk_number}")
    parent_id = str(entry["parentId"]) if isinstance(entry.get("parentId"), str) else None
    timestamp = _epoch_seconds(entry.get("timestamp"), default_ms)
    output: list[dict[str, Any]] = []

    def add(
        kind: str,
        content: str,
        role: str,
        block_index: int = 0,
        tool_name: str | None = None,
        target_file: str | None = None,
        success: int | None = None,
        spawned_agent: str | None = None,
    ) -> None:
        if not content:
            return
        output.append(
            _chunk(
                session_id=session_id,
                chunk_number=chunk_number + len(output),
                entry_id=entry_id,
                block_index=block_index,
                kind=kind,
                content=content,
                timestamp=timestamp,
                role=role,
                cwd=cwd,
                parent_id=parent_id,
                active=active,
                tool_name=tool_name,
                target_file=target_file,
                success=success,
                spawned_agent=spawned_agent,
            )
        )

    if entry_type == "message":
        message = entry.get("message") if isinstance(entry.get("message"), dict) else {}
        role = str(message.get("role") or "unknown")
        if role == "user":
            add("user_prompt", _content_text(message.get("content")), role)
        elif role == "assistant":
            for index, block in enumerate(_content_blocks(message.get("content"))):
                block_type = str(block.get("type") or "")
                if block_type == "text":
                    add("assistant", str(block.get("text") or ""), role, index)
                elif block_type == "thinking":
                    add("agent_thought", str(block.get("thinking") or ""), role, index)
                elif block_type == "toolCall":
                    name = str(block.get("name") or "tool")
                    arguments = block.get("arguments") if isinstance(block.get("arguments"), dict) else {}
                    kind = "agent_delegation" if name in {"subagent", "Task"} else "tool_call"
                    add(
                        kind,
                        f"{name}: {_json(arguments)}",
                        role,
                        index,
                        tool_name=name,
                        target_file=_target_file(arguments),
                        spawned_agent=_nested_identifier(arguments),
                    )
                elif block_type == "image":
                    add("assistant", _block_text(block), role, index)
            if message.get("errorMessage"):
                add(
                    "agent_stop",
                    str(message["errorMessage"]),
                    "system",
                    len(_content_blocks(message.get("content"))),
                    success=0,
                )
        elif role == "toolResult":
            call_id = str(message.get("toolCallId") or "")
            call = calls.get(call_id, {})
            name = str(message.get("toolName") or call.get("name") or "tool")
            details = message.get("details")
            spawned = _nested_identifier(details) if name in {"subagent", "Task"} else None
            add(
                "tool_result",
                _content_text(message.get("content")) or _json(details or {}),
                role,
                tool_name=name,
                target_file=call.get("target_file"),
                success=0 if message.get("isError") else 1,
                spawned_agent=spawned or call.get("spawned_agent"),
            )
        elif role == "bashExecution":
            command = str(message.get("command") or "")
            content = f"$ {command}\n{message.get('output') or ''}".rstrip()
            success = 0 if message.get("cancelled") or message.get("exitCode") not in {0, None} else 1
            add("tool_result", content, role, tool_name="bash", success=success)
        elif role == "custom":
            add("agent_context", _content_text(message.get("content")), role)
        elif role == "branchSummary":
            add("agent_context", str(message.get("summary") or ""), role)
        elif role == "compactionSummary":
            add("agent_context", str(message.get("summary") or ""), role)
        return output

    if entry_type == "model_change":
        add(
            "agent_config",
            f"model: {entry.get('provider')}/{entry.get('modelId')}",
            "system",
        )
    elif entry_type == "thinking_level_change":
        add("agent_config", f"thinking level: {entry.get('thinkingLevel')}", "system")
    elif entry_type == "compaction":
        add("agent_context", str(entry.get("summary") or ""), "system")
    elif entry_type == "branch_summary":
        add("agent_context", str(entry.get("summary") or ""), "system")
    elif entry_type == "custom_message":
        add("agent_context", _content_text(entry.get("content")), "custom")
    return output


def _clear_provider_rows(conn: sqlite3.Connection) -> None:
    source_ids = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT source_id FROM _edges_source WHERE source_type = 'pi'"
        )
    ]
    if not source_ids:
        conn.execute("DELETE FROM _types_pi_entry")
        conn.execute("DELETE FROM _types_pi_session")
        return
    marks = ",".join("?" for _ in source_ids)
    chunk_ids = [
        row[0]
        for row in conn.execute(
            f"SELECT chunk_id FROM _edges_source WHERE source_id IN ({marks})", source_ids
        )
    ]
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
    conn.execute(f"DELETE FROM _types_pi_entry WHERE source_id IN ({marks})", source_ids)
    conn.execute(f"DELETE FROM _types_pi_session WHERE source_id IN ({marks})", source_ids)


def _transpile_session(path: Path, conn: sqlite3.Connection) -> tuple[str, int, int]:
    file_entries = _read_entries(path)
    if not file_entries:
        return "", 0, 0
    header = file_entries[0]
    entries = file_entries[1:]
    session_id = str(header["id"])
    created_at_ms = _timestamp_ms(header.get("timestamp"), int(time.time() * 1000))
    cwd = header.get("cwd") if isinstance(header.get("cwd"), str) else None
    latest_leaf, active, child_counts, depths, orphans = _tree_metadata(entries)
    calls = _tool_calls(entries)
    metadata = _session_metadata(entries, active)

    ensure_source_exists(conn, session_id, cwd=cwd)
    conn.execute(
        "UPDATE _raw_sources SET source = 'pi:' || source_id WHERE source_id = ?",
        (session_id,),
    )
    workspace_path = next(
        (
            parent.name
            for parent in path.parents
            if parent.name.startswith("--") and parent.name.endswith("--")
        ),
        path.parent.name,
    )
    usage = metadata["usage"]
    conn.execute(
        """
        INSERT OR REPLACE INTO _types_pi_session
        (source_id, session_path, workspace_path, session_version, created_at_ms,
         cwd, parent_session_path, latest_leaf_id, entry_count,
         active_entry_count, message_count, branch_point_count, orphan_count,
         session_title, provider, model, thinking_level, last_stop_reason,
         errored, usage_input, usage_output, cache_read, cache_write,
         reasoning_tokens, total_tokens, total_cost, first_user_prompt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            str(path),
            workspace_path,
            header.get("version"),
            created_at_ms,
            cwd,
            header.get("parentSession"),
            latest_leaf,
            len(entries),
            len(active),
            metadata["message_count"],
            sum(1 for count in child_counts.values() if count > 1),
            orphans,
            metadata["title"],
            metadata["provider"],
            metadata["model"],
            metadata["thinking"],
            metadata["stop_reason"],
            metadata["errored"],
            int(usage["input"]),
            int(usage["output"]),
            int(usage["cacheRead"]),
            int(usage["cacheWrite"]),
            int(usage["reasoning"]),
            int(usage["totalTokens"]),
            float(usage["cost"]),
            metadata["first_user_prompt"],
        ),
    )

    inserted = 0
    emitted = 0
    for position, entry in enumerate(entries, start=1):
        native_id = str(entry.get("id") or f"position-{position}")
        _record_entry(
            conn,
            session_id=session_id,
            entry=entry,
            position=position,
            active=active,
            child_counts=child_counts,
            depths=depths,
        )
        chunks = _entry_chunks(
            session_id=session_id,
            entry=entry,
            chunk_number=emitted,
            cwd=cwd,
            active=native_id in active,
            calls=calls,
            default_ms=created_at_ms,
        )
        for chunk in chunks:
            if _insert_pi_chunk(conn, chunk, entry):
                inserted += 1
            update_source_stats(conn, session_id, chunk)
            emitted += 1

    conn.execute(
        """
        UPDATE _raw_sources
        SET title = COALESCE(?, title), model = ?
        WHERE source_id = ?
        """,
        (metadata["title"], metadata["model"] or metadata["provider"], session_id),
    )
    return session_id, inserted, len(entries)


def transpile(
    source: Path,
    conn: sqlite3.Connection,
    progress_cb: Callable[[int, int, int, int, float], None] | None = None,
) -> dict[str, int]:
    """Rebuild the Pi projection from the selected native session root."""
    ensure_pi_cell_schema(conn)
    _clear_provider_rows(conn)
    paths = _source_files(Path(source))
    started = time.monotonic()
    sessions = 0
    chunks = 0
    entries = 0
    for index, path in enumerate(paths, start=1):
        try:
            session_id, inserted, native_entries = _transpile_session(path, conn)
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            print(f"[pi] skipped {path}: {exc}", file=sys.stderr)
            continue
        if session_id:
            sessions += 1
            chunks += inserted
            entries += native_entries
        if progress_cb:
            progress_cb(index, len(paths), sessions, chunks, time.monotonic() - started)
    conn.commit()
    return {"sessions": sessions, "chunks": chunks, "entries": entries}
