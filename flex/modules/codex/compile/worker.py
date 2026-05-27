"""Codex transpiler — the only codex-specific code in the module.

Reads `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl` and emits CC-canonical
rows: `_raw_chunks`, `_raw_sources`, `_edges_source`, `_edges_tool_ops`,
`_types_message`, `_types_file_body`, `_file_body_index`, `_raw_content`,
`_edges_raw_content`.

Everything downstream (cell bootstrap, stubs, embedding, enrichment, stock
views, presets, lifecycle) is handled by the Claude Code substrate via
`bootstrap_claude_code_cell`, `ENRICHMENT_STUBS`, `_batch_embed_chunks`,
and `run_enrichment`.

Wire format observations (codex CLI 0.125+):

1. Source = directory of per-session JSONL files (`rollout-*.jsonl`),
   architecturally similar to claude_code (NOT a single SQLite like goose).
   First line of every file is a `session_meta` event with `id`, `cwd`,
   `git`, `model_provider`, `base_instructions`.

2. Four event types per session:
     session_meta  — first-line metadata
     turn_context  — per-turn cwd/model/sandbox/approval state (skipped)
     event_msg     — runtime events (token_count, exec_command_end,
                     patch_apply_end, task_started/complete, ...). Most
                     skipped; `patch_apply_end` and `exec_command_end`
                     are kept as call-id-keyed lookups for emission.
     response_item — the actual chat content (message, function_call,
                     function_call_output, reasoning, custom_tool_*).

3. `apply_patch` function_call carries empty `arguments`. The real diff
   lives in a sibling `event_msg/patch_apply_end` keyed by `call_id`,
   under `payload.changes[path] = {type, content}`. Two-pass build:
   first scan collects patch_apply_end + exec_command_end keyed by
   call_id; second pass emits chunks.

4. Tool name normalization (raw → CC canonical):
     exec_command   → Bash
     apply_patch    → Edit       (file body from patch_apply_end.changes)
     write_stdin    → Bash
     spawn_agent    → Task
     wait_agent     → Task
     close_agent    → Task
     update_plan    → TodoWrite
     view_image     → Read
   Other names (mcp__*, _list_*, _search, ...) pass through verbatim.

5. Source titles are pulled from `~/.codex/state_5.sqlite.threads.title`
   when available — much more useful than raw UUIDs in `@story`/`@orient`.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional

# Vendored CC helpers — used as a dependency, not modified.
from flex.modules.claude_code.compile.worker import (
    _ingest_file_body,
    _store_content_raw,
    ensure_source_exists,
    insert_chunk_atom,
    update_source_stats,
)

try:
    from flex.modules.soma.coding_agent import enrich_operation as soma_enrich_operation
except ImportError:  # pragma: no cover - older Flex core without shared bridge
    soma_enrich_operation = None


DEFAULT_CODEX_SESSIONS_DIR = Path.home() / ".codex" / "sessions"
DEFAULT_CODEX_STATE_DB = Path.home() / ".codex" / "state_5.sqlite"


# ── Tool-name mapping ───────────────────────────────────────────────────────

# Codex raw → CC canonical tool name. We map only when the tool's *function*
# matches a CC canonical (shell, file edit, file read, delegation, plan). This
# lets CC's stock presets (`@file`, `@story`, file co-edit graph, fingerprints)
# work across agents. Tools without a canonical equivalent stay raw —
# `write_stdin` is not Bash (it pipes to a running process), `wait_agent` /
# `close_agent` are agent lifecycle control, MCP tools (`mcp__*`) carry their
    # own identity. Raw codex name is preserved as `_raw_content.tool_name` so
    # `SELECT * FROM _raw_content WHERE tool_name='exec_command'` still works.
_CODEX_TOOL_MAP: dict[str, str] = {
    "exec_command":  "Bash",       # codex runs `cmd` via system shell (bash on Linux/Mac)
    "apply_patch":   "Edit",       # file edit (v4 patch format)
    "spawn_agent":   "Task",       # delegate to subagent
    "update_plan":   "TodoWrite",  # plan/todo state
    "view_image":    "Read",       # file path read (image bytes)
    "local_shell":   "Bash",       # Responses local shell item
    "web_search":    "WebSearch",  # Responses web_search item
}

_PATH_KEYS = ("path", "file_path", "file", "filename", "notebook_path")


def _map_tool_name(raw_name: Optional[str]) -> str:
    if not raw_name:
        return "unknown"
    return _CODEX_TOOL_MAP.get(raw_name, raw_name)


def _parse_v4_patch_paths(patch_text: str) -> list[str]:
    """Extract file paths from codex v4 patch format (`*** Update File: PATH`)."""
    if not isinstance(patch_text, str):
        return []
    paths: list[str] = []
    for line in patch_text.splitlines():
        line = line.rstrip()
        for marker in ("*** Update File: ", "*** Add File: ", "*** Delete File: "):
            if line.startswith(marker):
                paths.append(line[len(marker):].strip())
                break
    return paths


def _patch_files(call_id: str, patch_ends: dict, raw_input: Optional[str]) -> list[tuple[str, str]]:
    """Return list of (path, body) for an apply_patch call.

    Prefers patch_apply_end.changes (post-apply, structured); falls back to
    parsing the v4 patch text from `input` when no event was captured.
    """
    out: list[tuple[str, str]] = []
    pe = patch_ends.get(call_id) or {}
    changes = pe.get("changes") or {}
    if isinstance(changes, dict) and changes:
        for path, change in changes.items():
            if not isinstance(change, dict):
                continue
            ctype = change.get("type", "")
            body = (
                change.get("content")
                or change.get("new_content")
                or change.get("unified_diff")
            )
            if isinstance(body, str) and body:
                out.append((str(path), body))
        if out:
            return out

    # Fallback: parse v4 patch text — gives paths but no structured body
    if isinstance(raw_input, str):
        for path in _parse_v4_patch_paths(raw_input):
            out.append((path, raw_input))  # store the patch text itself
    return out


def _target_file(
    canonical: str,
    arguments: dict,
    raw_name: str,
    call_id: str,
    patch_ends: dict,
    raw_input: Optional[str] = None,
) -> Optional[str]:
    # apply_patch: pull first changed file from patch_apply_end (or v4 input)
    if raw_name == "apply_patch":
        files = _patch_files(call_id, patch_ends, raw_input)
        return files[0][0] if files else None
    if not isinstance(arguments, dict):
        return None
    for k in _PATH_KEYS:
        v = arguments.get(k)
        if v:
            return str(v)
    return None


def _extract_bodies(
    raw_name: str,
    call_id: str,
    patch_ends: dict,
    raw_input: Optional[str] = None,
) -> list[tuple[str, str]]:
    """All (path, body) pairs for an apply_patch op. Empty list for other tools."""
    if raw_name == "apply_patch":
        return _patch_files(call_id, patch_ends, raw_input)
    return []


def _flatten_output(val) -> str:
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, dict):
        for k in ("content", "text", "output", "value"):
            v = val.get(k)
            if isinstance(v, str) and v:
                return v
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    if isinstance(val, list):
        bits = []
        for item in val:
            if isinstance(item, dict):
                t = item.get("text") or item.get("content")
                if t:
                    bits.append(str(t))
                    continue
                bits.append(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            else:
                bits.append(str(item))
        return " ".join(bits)
    return str(val)


def _iso_to_epoch(value) -> Optional[int]:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        s = str(value).replace("Z", "+00:00")
        if "T" not in s and "+" not in s:
            s = s.replace(" ", "T") + "+00:00"
        return int(datetime.fromisoformat(s).timestamp())
    except (ValueError, TypeError):
        return None


def _chunk_dict(
    chunk_id: str,
    session_id: str,
    chunk_number: int,
    ctype: str,
    content: str,
    ts: int,
    role: str,
    cwd: Optional[str],
    git_branch: Optional[str],
) -> dict:
    return {
        "id": chunk_id,
        "doc_id": session_id,
        "chunk_number": chunk_number,
        "type": ctype,
        "content": content,
        "tool_name": None,
        "target_file": None,
        "success": None,
        "timestamp": ts,
        "role": role,
        "cwd": cwd,
        "git_branch": git_branch,
        "parent_uuid": None,
        "is_sidechain": 0,
        "entry_uuid": None,
        "branch_id": 0,
    }


def _compact_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _tool_search_text(arguments: dict) -> str:
    query = arguments.get("query") if isinstance(arguments, dict) else None
    limit = arguments.get("limit") if isinstance(arguments, dict) else None
    text = "tool_search"
    if query:
        text += f": {query}"
    if limit:
        text += f" limit={limit}"
    return text


def _web_search_text(payload: dict) -> str:
    action = payload.get("action") if isinstance(payload, dict) else None
    query = None
    if isinstance(action, dict):
        query = action.get("query")
        if not query and isinstance(action.get("queries"), list):
            query = "; ".join(str(q) for q in action["queries"][:3])
    return f"WebSearch: {query}" if query else "WebSearch"


def _local_shell_text(payload: dict) -> tuple[str, dict]:
    action = payload.get("action") if isinstance(payload, dict) else None
    if isinstance(action, dict):
        cmd = action.get("command") or action.get("cmd")
        if cmd:
            return f"Bash: {cmd}", {"cmd": cmd}
    cmd = None
    if isinstance(payload, dict):
        cmd = payload.get("command") or payload.get("cmd")
    if cmd:
        return f"Bash: {cmd}", {"cmd": cmd}
    return "Bash", {}


def _image_generation_text(payload: dict) -> str:
    prompt = None
    if isinstance(payload, dict):
        prompt = payload.get("prompt") or payload.get("revised_prompt")
    return f"image_generation: {str(prompt)[:300]}" if prompt else "image_generation"


# ── Per-session transpile ───────────────────────────────────────────────────


def _sync_session_jsonl(
    jsonl_path: Path,
    conn: sqlite3.Connection,
    thread_meta: dict[str, dict],
    spawn_edges: Optional[dict[str, list[tuple[str, str]]]] = None,
    session_memories: Optional[dict[str, dict]] = None,
    job_items: Optional[dict[str, dict]] = None,
    source_meta: Optional[Mapping[str, object]] = None,
) -> int:
    """Read one codex rollout JSONL and emit CC-canonical chunks. Idempotent.

    `thread_meta` provides per-session git/model/title context from state_5.
    `spawn_edges` provides parent→[children] from state_5.thread_spawn_edges.
    `session_memories` provides codex-generated rollout summaries (state_5.stage1_outputs).
    `job_items` provides batch-runner job lineage (state_5.agent_job_items).
    All args are precomputed once per transpile() call.
    """
    spawn_edges = spawn_edges or {}
    session_memories = session_memories or {}
    job_items = job_items or {}

    # Load all lines + extract session_meta, call-id-keyed event lookups,
    # and per-line turn_id mapping (sticky: lines belong to the most recent
    # turn_context they follow until the next one).
    lines: list[dict] = []
    session_id: Optional[str] = None
    cwd: Optional[str] = None
    git_branch: Optional[str] = None
    forked_from_id: Optional[str] = None
    start_ts: Optional[int] = None
    patch_ends: dict[str, dict] = {}
    exec_ends: dict[str, dict] = {}
    turn_contexts: dict[str, dict] = {}     # turn_id → full payload
    line_to_turn: dict[int, str] = {}       # 1-indexed line idx → turn_id
    _current_turn: Optional[str] = None

    try:
        fh = jsonl_path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return 0

    try:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                continue
            lines.append(d)

            t = d.get("type", "")
            p = d.get("payload") or {}
            if not isinstance(p, dict):
                continue

            if t == "session_meta":
                # Forked sessions inline a SECOND session_meta carrying the
                # parent's identity (codex preserves lineage by embedding the
                # ancestor's setup). We must only honor the first — overwriting
                # corrupts session_id and clobbers forked_from_id.
                if session_id is not None:
                    continue
                session_id = p.get("id")
                cwd = p.get("cwd")
                # JSONL session_meta.git is often null — prefer state_5.threads
                git = p.get("git")
                if isinstance(git, dict):
                    git_branch = git.get("branch") or git.get("branch_name")
                forked_from_id = p.get("forked_from_id") or None
                ts_iso = p.get("timestamp") or d.get("timestamp")
                start_ts = _iso_to_epoch(ts_iso)
            elif t == "turn_context":
                tid = p.get("turn_id")
                if tid:
                    turn_contexts[tid] = p
                    _current_turn = tid
            elif t in ("response_item", "compacted"):
                # Lines belong to the most recent turn_context (sticky)
                if _current_turn:
                    line_to_turn[len(lines)] = _current_turn
            elif t == "event_msg":
                ev = p.get("type")
                cid = p.get("call_id")
                if not cid:
                    continue
                if ev == "patch_apply_end":
                    patch_ends[cid] = p
                elif ev == "exec_command_end":
                    exec_ends[cid] = p
    finally:
        fh.close()

    if not session_id:
        return 0

    # state_5 is the authoritative source for git context, title, model
    meta = thread_meta.get(session_id) or {}
    title = meta.get("title")
    if not git_branch:
        git_branch = meta.get("git_branch")
    git_sha = meta.get("git_sha")
    git_origin = meta.get("git_origin_url")

    ensure_source_exists(conn, session_id, cwd=cwd, title=title)
    conn.execute(
        """
        UPDATE _raw_sources
        SET source = ?,
            model = COALESCE(?, model),
            primary_cwd = COALESCE(primary_cwd, ?)
        WHERE source_id = ?
        """,
        (f"codex:{session_id}", meta.get("model"), cwd, session_id),
    )

    # Resume: skip line indices already ingested
    last_num = conn.execute(
        """
        SELECT COALESCE(MAX(tm.chunk_number), 0)
        FROM _types_message tm
        JOIN _edges_source es ON tm.chunk_id = es.chunk_id
        WHERE es.source_id = ?
        """,
        (session_id,),
    ).fetchone()[0]

    inserted = 0
    new_chunks: list[dict] = []
    tool_ops_items: list[tuple] = []
    tool_content_items: list[tuple] = []
    fb_items: list[tuple] = []
    delegation_items: list[tuple] = []
    spawn_agent_chunks: list[str] = []        # chunk_ids of spawn_agent calls in line order
    spawn_agent_args:   list[dict] = []       # parallel: arguments dict for each spawn
    first_chunk_id: Optional[str] = None      # for fork edge attachment
    codex_turn_items: list[tuple] = []        # per tool_call: (chunk_id, turn_payload)
    codex_spawn_items: list[tuple] = []       # (chunk_id, agent_type, fork_context, message_preview)

    # Track call_id → emitted chunk metadata across response_items in this session
    call_to_chunk: dict[str, str] = {}
    call_to_tool: dict[str, str] = {}     # canonical (Bash, Edit, ...)
    call_to_raw: dict[str, str] = {}      # codex raw name (exec_command, apply_patch, ...)
    call_to_target: dict[str, str] = {}
    call_to_tool_op_idx: dict[str, int] = {}  # call_id → index into tool_ops_items (for success backfill)

    for chunk_number, d in enumerate(lines, start=1):
        if chunk_number <= last_num:
            continue

        t = d.get("type", "")
        if t not in ("response_item", "compacted"):
            continue  # only response_items and compaction markers become chunks

        p = d.get("payload") or {}
        if not isinstance(p, dict):
            continue

        ts_int = _iso_to_epoch(d.get("timestamp")) or start_ts or int(time.time())
        chunk_id = f"{session_id}_{chunk_number}"
        line_turn_id = line_to_turn.get(chunk_number)
        line_tc = turn_contexts.get(line_turn_id) if line_turn_id else None
        effective_cwd = (line_tc.get("cwd") if isinstance(line_tc, dict) else None) or cwd

        if t == "compacted":
            raw = _compact_json(p)
            content = p.get("message") or raw[:1500]
            tool_content_items.append((chunk_id, raw, "_compacted", ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "assistant",
                str(content)[:1500], ts_int, "assistant", effective_cwd, git_branch,
            ))
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))
            continue

        if t != "response_item":
            continue  # only response_items and compaction markers become chunks

        ptype = p.get("type", "")

        # Per-turn cwd override: turn_context can drift during a session
        # (codex changes cwd per turn). Use turn-level cwd when available.

        if ptype == "message":
            role = (p.get("role") or "").lower()
            content = p.get("content") or []
            text_parts: list[str] = []
            for c in content:
                if not isinstance(c, dict):
                    continue
                ct = c.get("type", "")
                if ct in ("input_text", "output_text", "text"):
                    txt = c.get("text") or ""
                    if txt:
                        text_parts.append(str(txt))
                elif ct == "input_image":
                    text_parts.append("[image]")
            text_content = "\n".join(text_parts).strip()
            if not text_content:
                continue

            if role == "user":
                ctype, crole = "user_prompt", "user"
            elif role == "developer":
                # developer = system instructions; skip — repetitive boilerplate
                continue
            else:
                ctype, crole = "assistant", "assistant"

            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, ctype,
                text_content, ts_int, crole, effective_cwd, git_branch,
            ))
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))

        elif ptype == "reasoning":
            summary = p.get("summary") or []
            text_parts = [
                s.get("text", "")
                for s in summary
                if isinstance(s, dict) and s.get("type") == "summary_text"
            ]
            text_content = "\n".join(t for t in text_parts if t).strip()
            if not text_content:
                continue
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "assistant",
                text_content, ts_int, "assistant", effective_cwd, git_branch,
            ))
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))
            tool_content_items.append((chunk_id, text_content, "_thinking", ts_int))

        elif ptype in ("function_call", "custom_tool_call"):
            raw_name = p.get("name") or "unknown"
            call_id = p.get("call_id") or ""
            args_raw = p.get("arguments")
            if args_raw is None:
                args_raw = p.get("input")
            arguments: dict = {}
            if isinstance(args_raw, str):
                try:
                    parsed = json.loads(args_raw) if args_raw else {}
                    arguments = parsed if isinstance(parsed, dict) else {"_value": parsed}
                except json.JSONDecodeError:
                    arguments = {"_raw": args_raw}
            elif isinstance(args_raw, dict):
                arguments = args_raw

            canonical = _map_tool_name(raw_name)
            raw_input_str = args_raw if isinstance(args_raw, str) else None
            tfile = _target_file(
                canonical, arguments, raw_name, call_id, patch_ends, raw_input_str,
            )

            if call_id:
                call_to_chunk[call_id] = chunk_id
                call_to_tool[call_id] = canonical
                call_to_raw[call_id] = raw_name
                if tfile:
                    call_to_target[call_id] = tfile
                # Index the upcoming tool_ops_items entry so we can backfill
                # `success` from event_msg/exec_command_end after the loop.
                call_to_tool_op_idx[call_id] = len(tool_ops_items)

            if raw_name == "spawn_agent":
                spawn_agent_chunks.append(chunk_id)
                spawn_agent_args.append(arguments)

            # _types_codex_turn: capture per-turn state for tool_call chunks
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))

            # success defaults to True; bug #1 backfill happens after the loop
            tool_ops_items.append((chunk_id, canonical, tfile, effective_cwd, git_branch, True))

            # Raw codex tool name lives in _raw_content.content_type so
            # codex-specific queries (`WHERE content_type='exec_command'`)
            # remain possible even after canonical mapping.
            raw_args = json.dumps(arguments, ensure_ascii=False)
            if len(raw_args) > 10:
                tool_content_items.append((chunk_id, raw_args, raw_name, ts_int))
            elif raw_input_str and len(raw_input_str) > 10:
                # apply_patch v4 text (and other custom_tool_call inputs) live
                # in `input` rather than `arguments` — preserve as content.
                tool_content_items.append((chunk_id, raw_input_str, raw_name, ts_int))

            for path, body in _extract_bodies(raw_name, call_id, patch_ends, raw_input_str):
                fb_items.append((chunk_id, path, body, ts_int))

            # Build readable text content for retrieval
            text_content = canonical
            if tfile:
                text_content += f" {tfile}"
            if raw_name == "exec_command":
                cmd = arguments.get("cmd")
                if cmd:
                    text_content = f"Bash: {cmd}"
            elif raw_name == "write_stdin":
                ch = arguments.get("chars")
                if ch:
                    text_content = f"write_stdin: {ch[:200]}"
            elif raw_name == "spawn_agent":
                at = arguments.get("agent_type", "")
                msg = arguments.get("message", "")
                text_content = f"spawn_agent[{at}]: {msg[:300]}".strip()
            elif raw_name == "update_plan":
                steps = arguments.get("plan") or []
                if isinstance(steps, list):
                    pieces = []
                    for s in steps[:6]:
                        if isinstance(s, dict):
                            pieces.append(f"[{s.get('status','?')}] {s.get('step','')}")
                    text_content = "TodoWrite: " + "; ".join(pieces)
            elif raw_name == "apply_patch":
                files = _patch_files(call_id, patch_ends, raw_input_str)
                if files:
                    paths = [p for p, _ in files]
                    text_content = f"Edit: {paths[0]}"
                    if len(paths) > 1:
                        text_content += f" (+{len(paths)-1} more)"

            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                text_content, ts_int, "assistant", effective_cwd, git_branch,
            ))

        elif ptype == "tool_search_call":
            call_id = p.get("call_id") or ""
            arguments = p.get("arguments") if isinstance(p.get("arguments"), dict) else {}
            raw_name = "tool_search"
            canonical = "tool_search"
            if call_id:
                call_to_chunk[call_id] = chunk_id
                call_to_tool[call_id] = canonical
                call_to_raw[call_id] = raw_name
                call_to_tool_op_idx[call_id] = len(tool_ops_items)
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))
            tool_ops_items.append((chunk_id, canonical, None, effective_cwd, git_branch, True))
            tool_content_items.append((chunk_id, _compact_json(arguments), raw_name, ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                _tool_search_text(arguments), ts_int, "assistant", effective_cwd, git_branch,
            ))

        elif ptype == "tool_search_output":
            call_id = p.get("call_id") or ""
            parent_chunk = call_to_chunk.get(call_id, chunk_id)
            raw = _compact_json(p.get("tools") or p)
            if len(raw) > 10:
                tool_content_items.append((parent_chunk, raw, "tool_search", ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                raw[:1500], ts_int, "tool", effective_cwd, git_branch,
            ))
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))

        elif ptype == "web_search_call":
            raw_name = "web_search"
            canonical = _map_tool_name(raw_name)
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))
            tool_ops_items.append((chunk_id, canonical, None, effective_cwd, git_branch, True))
            tool_content_items.append((chunk_id, _compact_json(p), raw_name, ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                _web_search_text(p), ts_int, "assistant", effective_cwd, git_branch,
            ))

        elif ptype == "local_shell_call":
            raw_name = "local_shell"
            canonical = _map_tool_name(raw_name)
            text_content, args = _local_shell_text(p)
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))
            tool_ops_items.append((chunk_id, canonical, None, effective_cwd, git_branch, True))
            tool_content_items.append((chunk_id, _compact_json(args or p), raw_name, ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                text_content, ts_int, "assistant", effective_cwd, git_branch,
            ))

        elif ptype == "image_generation_call":
            raw_name = "image_generation"
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))
            tool_ops_items.append((chunk_id, raw_name, None, effective_cwd, git_branch, True))
            tool_content_items.append((chunk_id, _compact_json(p), raw_name, ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                _image_generation_text(p), ts_int, "assistant", effective_cwd, git_branch,
            ))

        elif ptype in ("function_call_output", "custom_tool_call_output"):
            call_id = p.get("call_id") or ""
            output = p.get("output")
            output_text = _flatten_output(output)
            canonical = call_to_tool.get(call_id, "unknown")
            raw_for_output = call_to_raw.get(call_id, canonical)
            parent_chunk = call_to_chunk.get(call_id, chunk_id)

            if output_text and len(output_text) > 10:
                tool_content_items.append((parent_chunk, output_text, raw_for_output, ts_int))

            if not output_text:
                continue

            # Display content: truncated; full body lives in _raw_content
            display = output_text[:1500]
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "tool_call",
                display, ts_int, "tool", effective_cwd, git_branch,
            ))

            # Read-like result: route through file body sub-chunking
            if canonical == "Read":
                tfile = call_to_target.get(call_id)
                if tfile and len(output_text) > 50:
                    fb_items.append((parent_chunk, tfile, output_text, ts_int))

        elif ptype in ("compaction", "ghost_snapshot"):
            raw = _compact_json(p)
            tool_content_items.append((chunk_id, raw, f"_{ptype}", ts_int))
            content = p.get("message") or p.get("summary") or raw[:1500]
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "assistant",
                str(content)[:1500], ts_int, "assistant", effective_cwd, git_branch,
            ))
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))

        elif ptype and ptype != "other":
            # Forward-compatible fallback: if Codex starts persisting a new
            # ResponseItem variant, keep it searchable instead of dropping it.
            raw = _compact_json(p)
            tool_content_items.append((chunk_id, raw, f"_{ptype}", ts_int))
            new_chunks.append(_chunk_dict(
                chunk_id, session_id, chunk_number, "assistant",
                raw[:1500], ts_int, "assistant", effective_cwd, git_branch,
            ))
            if line_tc:
                codex_turn_items.append((chunk_id, line_tc))

    # Insert chunks (without embeddings; caller batch-embeds)
    for chunk in new_chunks:
        chunk["embedding"] = None
        try:
            insert_chunk_atom(conn, chunk)
            update_source_stats(conn, session_id, chunk)
            inserted += 1
            if first_chunk_id is None:
                first_chunk_id = chunk["id"]
        except Exception as e:
            print(f"[codex] chunk insert error: {e}", file=sys.stderr)

    # insert_chunk_atom is deliberately Claude Code-canonical and hardcodes the
    # edge source type; normalize the Codex provenance after reuse.
    if new_chunks:
        conn.execute(
            "UPDATE _edges_source SET source_type = 'codex' WHERE source_id = ?",
            (session_id,),
        )

    # ── Bug #1: backfill success from event_msg/exec_command_end.exit_code ─
    # codex sets success=None on the event but exit_code is reliable.
    for cid, idx in call_to_tool_op_idx.items():
        ev = exec_ends.get(cid)
        if not ev:
            continue
        exit_code = ev.get("exit_code")
        if exit_code is None:
            continue
        ok = exit_code == 0
        chunk_id_v, tn, tf, cwd_v, gb, _old_ok = tool_ops_items[idx]
        tool_ops_items[idx] = (chunk_id_v, tn, tf, cwd_v, gb, ok)

        # Store only event metadata (exit_code, duration, command, status).
        # Actual stdout is already captured via function_call_output.output —
        # storing it again here duplicates content and balloons the cell.
        # CC follows the same convention: shell stdout lives in tool_result
        # text, not in a separate telemetry record.
        try:
            payload = {
                "exit_code": exit_code,
                "duration":  ev.get("duration"),
                "command":   ev.get("command"),
                "status":    ev.get("status"),
            }
            tool_content_items.append((
                chunk_id_v,
                json.dumps(payload, ensure_ascii=False),
                "exec_command_end",
                ts_int if ts_int else (start_ts or int(time.time())),
            ))
        except Exception:
            pass

    for chunk_id, tn, tf, cwd_v, gb, ok in tool_ops_items:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_tool_ops "
                "(chunk_id, tool_name, target_file, success, cwd, git_branch) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chunk_id, tn, tf, ok, cwd_v, gb),
            )
            if soma_enrich_operation:
                soma_enrich_operation(
                    conn,
                    {
                        "chunk_id": chunk_id,
                        "tool_name": tn,
                        "target_file": tf,
                        "cwd": cwd_v,
                        "source_id": session_id,
                    },
                )
        except Exception as e:
            print(f"[codex] tool_ops insert error: {e}", file=sys.stderr)

    for cid, raw, tname, ts in tool_content_items:
        try:
            _store_content_raw(conn, cid, raw, tname, ts)
        except Exception as e:
            print(f"[codex] content store error: {e}", file=sys.stderr)

    for parent_id, tfile, body, ts in fb_items:
        try:
            _ingest_file_body(conn, parent_id, tfile, body, session_id, ts)
        except Exception as e:
            print(f"[codex] file body ingest error: {e}", file=sys.stderr)

    # ── Bug #3: delegation edges from state_5.thread_spawn_edges ──────────
    # Use codex args.agent_type ('explorer', 'worker', 'default') as agent_type
    # rather than generic 'spawn_agent'; matches CC convention where agent_type
    # is the named subagent role.
    children = spawn_edges.get(session_id, [])
    if children:
        for i, (child_id, status) in enumerate(children):
            # Pair positionally with spawn_agent chunks if counts roughly match;
            # otherwise attach to the last spawn_agent chunk (or first chunk).
            if i < len(spawn_agent_chunks):
                parent_chunk = spawn_agent_chunks[i]
                spawn_args = spawn_agent_args[i]
            elif spawn_agent_chunks:
                parent_chunk = spawn_agent_chunks[-1]
                spawn_args = spawn_agent_args[-1] if spawn_agent_args else {}
            else:
                parent_chunk = first_chunk_id
                spawn_args = {}

            agent_type_val = (spawn_args.get("agent_type") or "spawn_agent") if isinstance(spawn_args, dict) else "spawn_agent"

            try:
                ensure_source_exists(conn, child_id)
                conn.execute(
                    "INSERT OR IGNORE INTO _edges_delegations "
                    "(chunk_id, child_session_id, agent_type, created_at, parent_source_id) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (parent_chunk, child_id, agent_type_val, start_ts, session_id),
                )
            except Exception as e:
                print(f"[codex] delegation insert error: {e}", file=sys.stderr)

            # Codex-specific spawn metadata sidecar
            if parent_chunk and isinstance(spawn_args, dict):
                fork_ctx = spawn_args.get("fork_context")
                if isinstance(fork_ctx, bool):
                    fork_ctx_int = 1 if fork_ctx else 0
                else:
                    fork_ctx_int = None
                msg_preview = (spawn_args.get("message") or "")[:500]
                codex_spawn_items.append((parent_chunk, agent_type_val, fork_ctx_int, msg_preview))

    # ── Bug #4: fork lineage from session_meta.forked_from_id ─────────────
    if forked_from_id and forked_from_id != session_id:
        try:
            ensure_source_exists(conn, forked_from_id)
            conn.execute(
                "INSERT OR IGNORE INTO _edges_delegations "
                "(chunk_id, child_session_id, agent_type, created_at, parent_source_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (first_chunk_id, session_id, "fork", start_ts, forked_from_id),
            )
        except Exception as e:
            print(f"[codex] fork edge insert error: {e}", file=sys.stderr)

    # ── Codex-specific tables (per-turn state, spawn metadata, memory, job) ─
    for chk_id, tc in codex_turn_items:
        try:
            sandbox_pol = tc.get("sandbox_policy")
            sandbox_str = (
                json.dumps(sandbox_pol, ensure_ascii=False)
                if isinstance(sandbox_pol, (dict, list))
                else (str(sandbox_pol) if sandbox_pol else None)
            )
            conn.execute(
                "INSERT OR IGNORE INTO _types_codex_turn "
                "(chunk_id, turn_id, model, effort, cwd, personality, "
                " sandbox_policy, approval_policy) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    chk_id,
                    tc.get("turn_id"),
                    tc.get("model"),
                    tc.get("effort"),
                    tc.get("cwd"),
                    tc.get("personality"),
                    sandbox_str,
                    tc.get("approval_policy"),
                ),
            )
        except Exception as e:
            print(f"[codex] turn insert error: {e}", file=sys.stderr)

    for chk_id, agent_t, fork_ctx, msg_prev in codex_spawn_items:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO _types_codex_spawn "
                "(chunk_id, agent_type_arg, fork_context, message_preview) "
                "VALUES (?, ?, ?, ?)",
                (chk_id, agent_t, fork_ctx, msg_prev),
            )
        except Exception as e:
            print(f"[codex] spawn insert error: {e}", file=sys.stderr)

    # Codex auto-generated session memory (state_5.stage1_outputs)
    mem = session_memories.get(session_id)
    if mem:
        try:
            conn.execute(
                "INSERT OR REPLACE INTO _raw_codex_memory "
                "(source_id, raw_memory, rollout_summary, generated_at, "
                " rollout_slug, usage_count, last_usage) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    mem.get("raw_memory"),
                    mem.get("rollout_summary"),
                    mem.get("generated_at"),
                    mem.get("rollout_slug"),
                    mem.get("usage_count"),
                    mem.get("last_usage"),
                ),
            )
        except Exception as e:
            print(f"[codex] memory insert error: {e}", file=sys.stderr)

    # Codex batch-runner lineage (state_5.agent_job_items)
    job = job_items.get(session_id)
    if job:
        try:
            conn.execute(
                "INSERT OR REPLACE INTO _types_codex_job "
                "(source_id, job_id, job_name, job_instruction, item_id, "
                " row_index, row_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    job.get("job_id"),
                    job.get("job_name"),
                    job.get("job_instruction"),
                    job.get("item_id"),
                    job.get("row_index"),
                    job.get("row_json"),
                ),
            )
        except Exception as e:
            print(f"[codex] job insert error: {e}", file=sys.stderr)

    if source_meta:
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO _types_codex_source (
                    session_id, source_kind, codex_home, sessions_dir,
                    state_db, rollout_path
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    source_meta.get("source_kind"),
                    source_meta.get("codex_home"),
                    source_meta.get("sessions_dir"),
                    source_meta.get("state_db"),
                    str(jsonl_path),
                ),
            )
        except Exception as e:
            print(f"[codex] source provenance insert error: {e}", file=sys.stderr)

    if inserted == 0 and last_num == 0 and not forked_from_id:
        conn.execute(
            "DELETE FROM _raw_sources WHERE source_id = ? AND message_count = 0",
            (session_id,),
        )

    return inserted


# ── State DB lookups ────────────────────────────────────────────────────────


def _load_thread_meta(state_db: Path) -> dict[str, dict]:
    """Read thread metadata from ~/.codex/state_5.sqlite.

    Returns {session_id: {title, git_branch, git_sha, git_origin_url, model,
    agent_role, agent_nickname, source}}. session_meta.git in JSONL is often
    null — state_5 is the authoritative source for git context.
    """
    if not state_db.exists():
        return {}
    out: dict[str, dict] = {}
    cols = (
        "id, title, git_branch, git_sha, git_origin_url, model, "
        "agent_role, agent_nickname, source, cli_version"
    )
    try:
        uri = f"file:{state_db}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        try:
            for row in conn.execute(f"SELECT {cols} FROM threads"):
                tid = row[0]
                if not tid:
                    continue
                out[str(tid)] = {
                    "title":          row[1] or None,
                    "git_branch":     row[2] or None,
                    "git_sha":        row[3] or None,
                    "git_origin_url": row[4] or None,
                    "model":          row[5] or None,
                    "agent_role":     row[6] or None,
                    "agent_nickname": row[7] or None,
                    "source":         row[8] or None,
                    "cli_version":    row[9] or None,
                }
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


def _load_spawn_edges(state_db: Path) -> dict[str, list[tuple[str, str]]]:
    """Read state_5.thread_spawn_edges. Returns {parent_id: [(child_id, status)]}."""
    if not state_db.exists():
        return {}
    out: dict[str, list[tuple[str, str]]] = {}
    try:
        uri = f"file:{state_db}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        try:
            for row in conn.execute(
                "SELECT parent_thread_id, child_thread_id, status "
                "FROM thread_spawn_edges"
            ):
                parent, child, status = row
                if not parent or not child:
                    continue
                out.setdefault(str(parent), []).append((str(child), str(status or "")))
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


def _load_session_memories(state_db: Path) -> dict[str, dict]:
    """Read state_5.stage1_outputs — codex's auto-generated session summaries.

    Returns {thread_id: {raw_memory, rollout_summary, generated_at,
    rollout_slug, usage_count, last_usage}}. Empty if codex hasn't generated
    any (the table is wired but populates over time).
    """
    if not state_db.exists():
        return {}
    out: dict[str, dict] = {}
    try:
        uri = f"file:{state_db}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        try:
            for row in conn.execute(
                "SELECT thread_id, raw_memory, rollout_summary, generated_at, "
                "rollout_slug, usage_count, last_usage FROM stage1_outputs"
            ):
                tid = row[0]
                if not tid:
                    continue
                out[str(tid)] = {
                    "raw_memory":      row[1] or "",
                    "rollout_summary": row[2] or "",
                    "generated_at":    row[3],
                    "rollout_slug":    row[4],
                    "usage_count":     row[5],
                    "last_usage":      row[6],
                }
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


def _load_job_items(state_db: Path) -> dict[str, dict]:
    """Read state_5.agent_job_items joined to agent_jobs.

    Returns {assigned_thread_id: {job_id, job_name, job_instruction, item_id,
    row_index, row_json}} for sessions that originated as batch-runner items.
    """
    if not state_db.exists():
        return {}
    out: dict[str, dict] = {}
    try:
        uri = f"file:{state_db}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        try:
            for row in conn.execute(
                "SELECT i.assigned_thread_id, i.job_id, j.name, j.instruction, "
                "i.item_id, i.row_index, i.row_json "
                "FROM agent_job_items i "
                "LEFT JOIN agent_jobs j ON i.job_id = j.id "
                "WHERE i.assigned_thread_id IS NOT NULL"
            ):
                tid = row[0]
                if not tid:
                    continue
                out[str(tid)] = {
                    "job_id":          row[1],
                    "job_name":        row[2],
                    "job_instruction": row[3],
                    "item_id":         row[4],
                    "row_index":       row[5],
                    "row_json":        row[6],
                }
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


# Optional tables — not in CC contract; codex-specific richness.
CODEX_OPTIONAL_TABLES_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS _types_codex_turn (
        chunk_id TEXT PRIMARY KEY,
        turn_id TEXT,
        model TEXT,
        effort TEXT,
        cwd TEXT,
        personality TEXT,
        sandbox_policy TEXT,
        approval_policy TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_codex_turn_id ON _types_codex_turn(turn_id)",
    "CREATE INDEX IF NOT EXISTS idx_codex_turn_model ON _types_codex_turn(model)",
    """
    CREATE TABLE IF NOT EXISTS _types_codex_spawn (
        chunk_id TEXT PRIMARY KEY,
        agent_type_arg TEXT,
        fork_context INTEGER,
        message_preview TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS _raw_codex_memory (
        source_id TEXT PRIMARY KEY,
        raw_memory TEXT,
        rollout_summary TEXT,
        generated_at INTEGER,
        rollout_slug TEXT,
        usage_count INTEGER,
        last_usage INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS _types_codex_job (
        source_id TEXT PRIMARY KEY,
        job_id TEXT,
        job_name TEXT,
        job_instruction TEXT,
        item_id TEXT,
        row_index INTEGER,
        row_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS _types_codex_source (
        session_id TEXT PRIMARY KEY,
        source_kind TEXT,
        codex_home TEXT,
        sessions_dir TEXT,
        state_db TEXT,
        rollout_path TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_codex_source_home ON _types_codex_source(codex_home)",
)


def ensure_codex_tables(conn: sqlite3.Connection) -> None:
    """Create codex-specific optional tables. Idempotent. Called from install."""
    for ddl in CODEX_OPTIONAL_TABLES_DDL:
        conn.execute(ddl)
    conn.commit()


# Back-compat alias — earlier callers (and the smoke tests) used _load_titles
def _load_titles(state_db: Path) -> dict[str, str]:
    return {sid: meta["title"] for sid, meta in _load_thread_meta(state_db).items() if meta.get("title")}


# ── Source signature for refresh short-circuit ─────────────────────────────


def compute_dir_signature(sessions_dir: Path) -> tuple[int, int]:
    """Return (total_size_bytes, file_count) for cheap drift detection."""
    total = 0
    count = 0
    for f in sessions_dir.rglob("rollout-*.jsonl"):
        try:
            total += f.stat().st_size
            count += 1
        except OSError:
            continue
    return total, count


# ── Public transpile entry point ────────────────────────────────────────────


def transpile(
    source_path: Path,
    conn: sqlite3.Connection,
    progress_cb=None,
    limit: Optional[int] = None,
    commit_every: int = 50,
    *,
    state_db: Path | None = None,
    source_meta: Mapping[str, object] | None = None,
) -> dict:
    """Read codex sessions directory and write CC-canonical rows. Idempotent.

    Signature matches the install/refresh call sites used by goose:
        (source_path, conn, progress_cb) → stats dict
    """
    if not source_path.exists():
        raise FileNotFoundError(
            f"codex sessions directory not found at {source_path}. "
            "Install codex CLI and run at least one session first."
        )
    if not source_path.is_dir():
        raise NotADirectoryError(
            f"codex source must be a directory of rollout-*.jsonl files, "
            f"got {source_path}"
        )

    files = sorted(source_path.rglob("rollout-*.jsonl"))
    if limit:
        files = files[: int(limit)]
    total = len(files)

    # Precompute state_5 lookups once; all read-only and cheap.
    state_db_path = Path(state_db) if state_db is not None else DEFAULT_CODEX_STATE_DB
    thread_meta     = _load_thread_meta(state_db_path)
    spawn_edges     = _load_spawn_edges(state_db_path)
    session_memories = _load_session_memories(state_db_path)
    job_items       = _load_job_items(state_db_path)

    # Ensure codex-specific optional tables exist (idempotent)
    ensure_codex_tables(conn)

    t0 = time.time()
    n_sessions = 0
    n_chunks = 0

    for i, jsonl in enumerate(files, 1):
        try:
            added = _sync_session_jsonl(
                jsonl, conn, thread_meta, spawn_edges,
                session_memories, job_items, source_meta=source_meta,
            )
            n_chunks += added
            if added > 0:
                n_sessions += 1
        except Exception as e:
            print(f"[codex] {jsonl.name} failed: {e}", file=sys.stderr)

        if i % commit_every == 0 or i == total:
            try:
                conn.commit()
            except sqlite3.Error as e:
                print(f"[codex] commit error: {e}", file=sys.stderr)

        if progress_cb:
            progress_cb(i, total, n_sessions, n_chunks, time.time() - t0)

    return {
        "sessions": n_sessions,
        "chunks": n_chunks,
        "elapsed": time.time() - t0,
    }
