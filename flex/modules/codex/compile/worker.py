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
import os
import sqlite3
import threading
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional

# Vendored CC helpers — used as a dependency, not modified.
from flex.modules.claude_code.compile.worker import (
    _batch_embed_chunks,
    _ingest_file_body,
    _store_content_raw,
    ensure_source_exists,
    insert_chunk_atom,
    update_source_stats,
)
from flex.modules.claude_code.compile.soft_detect import detect_file_ops

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


def _shell_cmd_str(cmd) -> str | None:
    """Normalize a Codex shell cmd to a script string for soft-op detection.

    Codex `exec_command.cmd` is usually a plain string. `local_shell_call`
    may give an argv list; if it's a shell wrapper (`bash -lc <script>`,
    `sh -c <script>`), the script is the last element — otherwise join argv.
    """
    if not cmd:
        return None
    if isinstance(cmd, str):
        return cmd
    if isinstance(cmd, (list, tuple)):
        parts = [str(x) for x in cmd]
        if len(parts) >= 3 and parts[0] in ("bash", "sh", "zsh") and parts[1] in ("-lc", "-c", "-lic"):
            return parts[-1]
        return " ".join(parts)
    return None


def _read_js_string_literal(source: str, offset: int) -> tuple[int, str | None] | None:
    """Decode one static JS string literal.

    This is deliberately a tiny literal reader, not a JavaScript evaluator.
    Single/double quoted strings and interpolation-free template literals are
    accepted. Template interpolation and malformed escapes abstain.
    """
    if offset >= len(source) or source[offset] not in "'\"`":
        return None
    quote = source[offset]
    out: list[str] = []
    static = True
    i = offset + 1
    escapes = {
        "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t",
        "v": "\v", "0": "\0", "\\": "\\", "'": "'", '"': '"',
        "`": "`", "$": "$",
    }
    while i < len(source):
        char = source[i]
        if char == quote:
            return i + 1, "".join(out) if static else None
        if quote == "`" and char == "$" and i + 1 < len(source) and source[i + 1] == "{":
            static = False
            i += 2
            continue
        if char in "\r\n" and quote != "`":
            return None
        if char != "\\":
            out.append(char)
            i += 1
            continue

        i += 1
        if i >= len(source):
            return None
        escaped = source[i]
        if escaped == "\r" or escaped == "\n":
            if escaped == "\r" and i + 1 < len(source) and source[i + 1] == "\n":
                i += 1
            i += 1
            continue
        if escaped == "x":
            digits = source[i + 1:i + 3]
            if len(digits) != 2 or any(c not in "0123456789abcdefABCDEF" for c in digits):
                return None
            out.append(chr(int(digits, 16)))
            i += 3
            continue
        if escaped == "u":
            if i + 1 < len(source) and source[i + 1] == "{":
                end = source.find("}", i + 2)
                digits = source[i + 2:end] if end >= 0 else ""
                if not digits or len(digits) > 6 or any(c not in "0123456789abcdefABCDEF" for c in digits):
                    return None
                codepoint = int(digits, 16)
                if codepoint > 0x10FFFF:
                    return None
                out.append(chr(codepoint))
                i = end + 1
                continue
            digits = source[i + 1:i + 5]
            if len(digits) != 4 or any(c not in "0123456789abcdefABCDEF" for c in digits):
                return None
            out.append(chr(int(digits, 16)))
            i += 5
            continue
        out.append(escapes.get(escaped, escaped))
        i += 1
    return None


_CODEX_JS_PARSER = None
_CODEX_JS_PARSER_UNAVAILABLE = False


def _codex_js_parser():
    """Lazily build the required tree-sitter JavaScript parser."""
    global _CODEX_JS_PARSER, _CODEX_JS_PARSER_UNAVAILABLE
    if _CODEX_JS_PARSER is not None:
        return _CODEX_JS_PARSER
    if _CODEX_JS_PARSER_UNAVAILABLE:
        return None
    try:
        from tree_sitter import Language, Parser
        import tree_sitter_javascript as tsjs

        _CODEX_JS_PARSER = Parser(Language(tsjs.language()))
    except Exception:
        # Static recovery is optional enrichment. If the grammar is somehow
        # absent, abstain; never fall back to evaluating or regex-parsing JS.
        _CODEX_JS_PARSER_UNAVAILABLE = True
        return None
    return _CODEX_JS_PARSER


def _decode_js_literal_node(node) -> str | None:
    if node.type not in ("string", "template_string"):
        return None
    if node.type == "template_string" and any(
        child.type == "template_substitution" for child in node.named_children
    ):
        return None
    try:
        text = node.text.decode("utf-8")
    except (AttributeError, UnicodeDecodeError):
        return None
    literal = _read_js_string_literal(text, 0)
    if literal is None or literal[0] != len(text):
        return None
    return literal[1]


def _literal_cmd_from_call(node) -> tuple[str, str | None] | None:
    function = node.child_by_field_name("function")
    if function is None or function.type != "member_expression":
        return None
    receiver = function.child_by_field_name("object")
    member = function.child_by_field_name("property")
    if (
        receiver is None or receiver.type != "identifier"
        or receiver.text != b"tools"
        or member is None or member.type != "property_identifier"
        or member.text != b"exec_command"
    ):
        return None

    arguments = node.child_by_field_name("arguments")
    if arguments is None or len(arguments.named_children) != 1:
        return None
    options = arguments.named_children[0]
    if options.type != "object":
        return None
    # Spreads, computed methods, and shorthand properties may override cmd.
    if any(child.type != "pair" for child in options.named_children):
        return None

    command: str | None = None
    workdir: str | None = None
    saw_workdir = False
    for pair in options.named_children:
        key = pair.child_by_field_name("key")
        value = pair.child_by_field_name("value")
        if key is None or value is None:
            return None
        if key.type in ("property_identifier", "identifier"):
            key_text = key.text.decode("utf-8", errors="replace")
        else:
            key_text = _decode_js_literal_node(key)
        if key_text is None:
            # A computed key could override cmd/workdir.
            return None
        if key_text not in ("cmd", "workdir"):
            continue
        literal_value = _decode_js_literal_node(value)
        if not literal_value:
            return None
        if key_text == "cmd":
            if command is not None:
                return None
            command = literal_value
        else:
            if saw_workdir:
                return None
            saw_workdir = True
            workdir = literal_value
    return (command, workdir) if command is not None else None


def _extract_exec_command_literals(source: str) -> list[tuple[str, str | None]]:
    """Statically recover literal shell commands from Codex ``exec`` JavaScript.

    Tree-sitter identifies exact call/object/string nodes, so comments, quoted
    source, regex literals, interpolation, concatenation, and malformed programs
    all abstain without evaluating the persisted JavaScript.
    """
    if not isinstance(source, str) or not source:
        return []
    parser = _codex_js_parser()
    if parser is None:
        return []
    try:
        root = parser.parse(source.encode("utf-8")).root_node
    except Exception:
        return []
    if root.has_error:
        return []

    recovered: list[tuple[int, str, str | None]] = []
    pending = [root]
    while pending:
        node = pending.pop()
        if node.type == "call_expression":
            command = _literal_cmd_from_call(node)
            if command is not None:
                recovered.append((node.start_byte, command[0], command[1]))
        pending.extend(reversed(node.named_children))
    recovered.sort(key=lambda item: item[0])
    return [(command, workdir) for _, command, workdir in recovered]


def _detected_shell_ops(
    commands: list[tuple[str, str | None]], fallback_cwd: str | None,
):
    """Run the shared detector across commands, preserving order and deduping."""
    confidence_rank = {"high": 3, "medium": 2, "low": 1}
    seen = {}
    for command, workdir in commands:
        command_cwd = workdir or fallback_cwd
        for op in detect_file_ops(command, command_cwd):
            key = (op.file_path, op.inferred_op)
            old = seen.get(key)
            if old is None or confidence_rank.get(op.confidence, 0) > confidence_rank.get(old.confidence, 0):
                seen[key] = op
    return list(seen.values())


def _shell_commands_text(commands: list[tuple[str, str | None]]) -> str:
    return "\n".join(f"inferred nested shell: {command}" for command, _ in commands)


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
    *,
    read_offset: int = 0,
    read_limit: int | None = None,
    line_number_base: int = 0,
    parser_state: dict | None = None,
    admit_enrichment: bool = True,
    deadline: float | None = None,
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
    # ``parser_state`` is a durable, provider-native cursor receipt used only
    # by the active-append path.  A normal reconciliation intentionally starts
    # from no state and remains the authoritative repair implementation.
    state = parser_state if parser_state is not None else {}

    # Load all lines + extract session_meta, call-id-keyed event lookups,
    # and per-line turn_id mapping (sticky: lines belong to the most recent
    # turn_context they follow until the next one).
    lines: list[dict] = []
    session_id: Optional[str] = state.get("session_id") or None
    cwd: Optional[str] = state.get("cwd") or None
    git_branch: Optional[str] = state.get("git_branch") or None
    forked_from_id: Optional[str] = None
    start_ts: Optional[int] = state.get("start_ts") or None
    patch_ends: dict[str, dict] = {}
    exec_ends: dict[str, dict] = {}
    turn_contexts: dict[str, dict] = {}     # turn_id → full payload
    line_to_turn: dict[int, str] = {}       # 1-indexed line idx → turn_id
    _current_turn: Optional[str] = state.get("current_turn_id") or None
    remembered_turn = state.get("current_turn")
    if _current_turn and isinstance(remembered_turn, dict):
        turn_contexts[_current_turn] = remembered_turn

    try:
        if read_offset:
            # The receipt is a byte offset at a known newline boundary. Decode
            # only the stable appended prefix; an unterminated writer tail is
            # deliberately left for the next tick rather than guessed at.
            with jsonl_path.open("rb") as binary:
                binary.seek(read_offset)
                payload = binary.read(-1 if read_limit is None else max(0, read_limit - read_offset))
            raw_lines = payload.decode("utf-8", errors="replace").splitlines()
            fh = None
        else:
            fh = jsonl_path.open("r", encoding="utf-8", errors="replace")
            raw_lines = fh
    except OSError:
        return 0

    try:
        for raw in raw_lines:
            if deadline is not None and time.time() >= deadline:
                raise TimeoutError("codex append admission deadline reached")
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
                    line_to_turn[line_number_base + len(lines)] = _current_turn
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
        if fh is not None:
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
    soft_ops_items: list[tuple] = []          # (chunk_id, SoftFileOp) inferred shell file ops
    delegation_items: list[tuple] = []
    spawn_agent_chunks: list[str] = []        # chunk_ids of spawn_agent calls in line order
    spawn_agent_args:   list[dict] = []       # parallel: arguments dict for each spawn
    first_chunk_id: Optional[str] = None      # for fork edge attachment
    codex_turn_items: list[tuple] = []        # per tool_call: (chunk_id, turn_payload)
    codex_spawn_items: list[tuple] = []       # (chunk_id, agent_type, fork_context, message_preview)

    # Track call_id → emitted chunk metadata across response_items in this session
    persisted_calls = state.get("open_calls") if isinstance(state.get("open_calls"), dict) else {}
    call_to_chunk: dict[str, str] = {
        str(call_id): str(details.get("chunk_id"))
        for call_id, details in persisted_calls.items()
        if isinstance(details, dict) and details.get("chunk_id")
    }
    call_to_tool: dict[str, str] = {
        str(call_id): str(details.get("tool") or "unknown")
        for call_id, details in persisted_calls.items() if isinstance(details, dict)
    }
    call_to_raw: dict[str, str] = {
        str(call_id): str(details.get("raw_name") or details.get("tool") or "unknown")
        for call_id, details in persisted_calls.items() if isinstance(details, dict)
    }
    call_to_target: dict[str, str] = {
        str(call_id): str(details.get("target"))
        for call_id, details in persisted_calls.items()
        if isinstance(details, dict) and details.get("target")
    }
    call_to_tool_op_idx: dict[str, int] = {}  # call_id → index into tool_ops_items (for success backfill)

    for chunk_number, d in enumerate(lines, start=1 + line_number_base):
        if deadline is not None and time.time() >= deadline:
            raise TimeoutError("codex append admission deadline reached")
        if read_offset == 0 and chunk_number <= last_num:
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

            raw_input_str = args_raw if isinstance(args_raw, str) else None
            nested_shell_commands = (
                _extract_exec_command_literals(raw_input_str)
                if ptype == "custom_tool_call" and raw_name == "exec" and raw_input_str
                else []
            )
            # ``exec`` is the provider-direct orchestration event. Static
            # recovery can reveal probable nested shell/file operations, but it
            # cannot prove that a syntactic call executed (dead branches,
            # shadowed receivers, and uncalled functions all remain possible).
            # Preserve the direct outer identity and put recovered paths only
            # in the explicitly inferred soft-op surface.
            canonical = _map_tool_name(raw_name)
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
                if parser_state is not None:
                    persisted_calls[call_id] = {
                        "chunk_id": chunk_id,
                        "tool": canonical,
                        "raw_name": raw_name,
                        "target": tfile,
                    }

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
                    # Soft file-op provenance: infer file mutations from the shell
                    # command (cat >, heredoc, tee, cp/mv/rm/mkdir). Codex chunks
                    # carry tool_name=None, so insert_chunk_atom's CC soft-op path
                    # never fires — accumulate here and batch-insert below.
                    cmd_str = _shell_cmd_str(cmd)
                    if cmd_str:
                        for op in _detected_shell_ops([(cmd_str, None)], effective_cwd):
                            soft_ops_items.append((chunk_id, op))
            elif raw_name == "exec" and nested_shell_commands:
                text_content = _shell_commands_text(nested_shell_commands)
                for op in _detected_shell_ops(nested_shell_commands, effective_cwd):
                    soft_ops_items.append((chunk_id, op))
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
            cmd_str = _shell_cmd_str(args.get("cmd")) if isinstance(args, dict) else None
            if cmd_str:
                for op in _detected_shell_ops([(cmd_str, None)], effective_cwd):
                    soft_ops_items.append((chunk_id, op))
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
            if parser_state is not None and call_id:
                persisted_calls.pop(call_id, None)

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
            if insert_chunk_atom(conn, chunk, enrich_identity=admit_enrichment):
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

    # An exec completion may arrive in a later active-append interval. Its
    # originating call is retained in the receipt, not re-read from the
    # rollout; update the already-published operation directly.
    for cid, ev in exec_ends.items():
        if cid in call_to_tool_op_idx or cid not in call_to_chunk:
            continue
        exit_code = ev.get("exit_code")
        if exit_code is None:
            continue
        parent_chunk = call_to_chunk[cid]
        try:
            conn.execute(
                "UPDATE _edges_tool_ops SET success=? WHERE chunk_id=?",
                (exit_code == 0, parent_chunk),
            )
            payload = {
                "exit_code": exit_code, "duration": ev.get("duration"),
                "command": ev.get("command"), "status": ev.get("status"),
            }
            _store_content_raw(
                conn, parent_chunk, json.dumps(payload, ensure_ascii=False),
                "exec_command_end", start_ts or int(time.time()),
            )
        except Exception as e:
            print(f"[codex] deferred exec completion error: {e}", file=sys.stderr)

    for chunk_id, tn, tf, cwd_v, gb, ok in tool_ops_items:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_tool_ops "
                "(chunk_id, tool_name, target_file, success, cwd, git_branch) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chunk_id, tn, tf, ok, cwd_v, gb),
            )
            if admit_enrichment and soma_enrich_operation:
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

    for chunk_id, op in soft_ops_items:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_soft_ops "
                "(chunk_id, file_path, file_uuid, inferred_op, confidence) "
                "VALUES (?, ?, NULL, ?, ?)",
                (chunk_id, op.file_path, op.inferred_op, op.confidence),
            )
        except Exception as e:
            print(f"[codex] soft_ops insert error: {e}", file=sys.stderr)

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

    from flex.modules.claude_code.manage.observations import upsert_observation
    for chunk_id in {c['id'] for c in new_chunks}:
        try:
            upsert_observation(conn, chunk_id)
        except Exception as e:
            print(f"[codex] observation upsert error for {chunk_id}: {e}", file=sys.stderr)

    if inserted == 0 and last_num == 0 and not forked_from_id:
        conn.execute(
            "DELETE FROM _raw_sources WHERE source_id = ? AND message_count = 0",
            (session_id,),
        )

    if parser_state is not None:
        state.update({
            "session_id": session_id,
            "cwd": cwd,
            "git_branch": git_branch,
            "start_ts": start_ts,
            "current_turn_id": _current_turn,
            "current_turn": turn_contexts.get(_current_turn) if _current_turn else None,
            "open_calls": persisted_calls,
        })
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
    """
    CREATE TABLE IF NOT EXISTS _codex_source_state (
        source_path TEXT PRIMARY KEY,
        size_bytes INTEGER NOT NULL,
        mtime_ns INTEGER NOT NULL,
        -- The committed prefix is the only prefix the active-append detector
        -- may skip.  NULL fields identify legacy/full-reconcile state.
        committed_offset INTEGER,
        committed_lines INTEGER,
        source_generation TEXT,
        session_id TEXT,
        parser_state TEXT
    )
    """,
)

_EXEC_WRAPPER_BACKFILL_KEY = "codex_exec_wrapper_backfill_version"
_EXEC_WRAPPER_BACKFILL_VERSION = "2"
_EXEC_WRAPPER_BACKFILL_CURSOR_KEY = "codex_exec_wrapper_backfill_v2_cursor"
_EXEC_WRAPPER_LEGACY_CURSOR_KEY = "codex_exec_wrapper_backfill_v1_cursor"
_EXEC_WRAPPER_BACKFILL_BATCH = 500
_EXEC_WRAPPER_CANDIDATE_SQL = """
rc.tool_name='exec'
AND t.tool_name IN ('exec','Bash')
AND rc.content LIKE '{"_raw":%'
AND instr(rc.content, 'tools.exec_command') > 0
"""


def ensure_codex_tables(conn: sqlite3.Connection) -> None:
    """Create codex-specific optional tables. Idempotent. Called from install."""
    for ddl in CODEX_OPTIONAL_TABLES_DDL:
        conn.execute(ddl)
    # Existing cells predate append receipts.  Keep their rows deliberately
    # incomplete so the first post-migration sync takes the authoritative full
    # path and only subsequent stable appends may use the fast path.
    columns = {row[1] for row in conn.execute("PRAGMA table_info(_codex_source_state)")}
    for name, declaration in (
        ("committed_offset", "INTEGER"),
        ("committed_lines", "INTEGER"),
        ("source_generation", "TEXT"),
        ("session_id", "TEXT"),
        ("parser_state", "TEXT"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE _codex_source_state ADD COLUMN {name} {declaration}")
    conn.commit()


def _stored_exec_source(raw_content: str) -> str | None:
    """Unwrap the legacy JSON envelope used for custom-tool ``input`` text."""
    try:
        parsed = json.loads(raw_content)
    except (TypeError, json.JSONDecodeError):
        return None
    if (
        isinstance(parsed, dict)
        and set(parsed) == {"_raw"}
        and isinstance(parsed.get("_raw"), str)
    ):
        return parsed["_raw"]
    return None


def backfill_exec_command_wrappers(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
    deadline: float | None = None,
) -> int:
    """Repair historical literal shell calls hidden inside raw Codex ``exec``.

    Version receipt lives in ``_meta`` so watched cells pay for the raw-content
    scan once. Inserts remain independently idempotent in case an interrupted
    run is retried before the receipt is committed.
    """
    required = {
        "_meta", "_raw_chunks", "_raw_content", "_edges_raw_content",
        "_edges_tool_ops", "_edges_soft_ops", "_enrich_observations",
    }
    available = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if not required <= available:
        return 0
    prior = conn.execute(
        "SELECT value FROM _meta WHERE key=?", (_EXEC_WRAPPER_BACKFILL_KEY,)
    ).fetchone()
    if prior and str(prior[0]) == _EXEC_WRAPPER_BACKFILL_VERSION:
        return 0
    if _codex_js_parser() is None:
        # Do not receipt an unattempted migration when the static parser is
        # unavailable; a later healthy watch tick must be allowed to retry.
        return 0

    if deadline is not None and time.time() >= deadline:
        return 0
    batch_limit = max(
        1,
        int(limit or os.environ.get(
            "FLEX_CODEX_EXEC_REPAIR_PER_TICK", _EXEC_WRAPPER_BACKFILL_BATCH,
        )),
    )
    cursor_row = conn.execute(
        "SELECT value FROM _meta WHERE key=?", (_EXEC_WRAPPER_BACKFILL_CURSOR_KEY,)
    ).fetchone()
    cursor_rowid, cursor_chunk = 0, ""
    if cursor_row and cursor_row[0]:
        try:
            decoded = json.loads(cursor_row[0])
            cursor_rowid, cursor_chunk = int(decoded[0]), str(decoded[1])
        except (TypeError, ValueError, json.JSONDecodeError, IndexError):
            cursor_rowid, cursor_chunk = 0, ""

    rows = conn.execute(
        f"""
        SELECT rc.rowid, erc.chunk_id, rc.content, t.cwd
        FROM _raw_content rc
        JOIN _edges_raw_content erc ON erc.content_hash=rc.hash
        JOIN _edges_tool_ops t ON t.chunk_id=erc.chunk_id
        WHERE {_EXEC_WRAPPER_CANDIDATE_SQL}
          AND (rc.rowid > ? OR (rc.rowid = ? AND erc.chunk_id > ?))
        ORDER BY rc.rowid, erc.chunk_id
        LIMIT ?
        """,
        (cursor_rowid, cursor_rowid, cursor_chunk, batch_limit),
    ).fetchall()

    repaired = 0
    last_cursor = (cursor_rowid, cursor_chunk)
    conn.execute("SAVEPOINT codex_exec_wrapper_backfill")
    try:
        from flex.modules.claude_code.manage.observations import upsert_observation

        for raw_rowid, chunk_id, raw_content, cwd in rows:
            source = _stored_exec_source(raw_content)
            commands = _extract_exec_command_literals(source) if source else []
            last_cursor = (int(raw_rowid), str(chunk_id))
            if not commands:
                if deadline is not None and time.time() >= deadline:
                    break
                continue

            # Identical calls within one wrapper do not need duplicate edges.
            commands = list(dict.fromkeys(commands))
            # A short-lived pre-release implementation rewrote these outer
            # provider events to Bash. Repair that fidelity laundering while
            # preserving the raw `exec` identity as the direct event.
            conn.execute(
                "UPDATE _edges_tool_ops SET tool_name='exec' "
                "WHERE chunk_id=? AND tool_name='Bash'",
                (chunk_id,),
            )
            for op in _detected_shell_ops(commands, cwd):
                conn.execute(
                    """
                    INSERT INTO _edges_soft_ops
                      (chunk_id,file_path,file_uuid,inferred_op,confidence)
                    SELECT ?,?,NULL,?,?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM _edges_soft_ops
                        WHERE chunk_id=? AND file_path=? AND inferred_op=?
                    )
                    """,
                    (
                        chunk_id, op.file_path, op.inferred_op, op.confidence,
                        chunk_id, op.file_path, op.inferred_op,
                    ),
                )
            upsert_observation(conn, chunk_id)
            repaired += 1
            if deadline is not None and time.time() >= deadline:
                break

        conn.execute(
            "INSERT OR REPLACE INTO _meta (key,value) VALUES (?,?)",
            (_EXEC_WRAPPER_BACKFILL_CURSOR_KEY, json.dumps(last_cursor)),
        )
        more = conn.execute(
            f"""
            SELECT 1
            FROM _raw_content rc
            JOIN _edges_raw_content erc ON erc.content_hash=rc.hash
            JOIN _edges_tool_ops t ON t.chunk_id=erc.chunk_id
            WHERE {_EXEC_WRAPPER_CANDIDATE_SQL}
              AND (rc.rowid > ? OR (rc.rowid = ? AND erc.chunk_id > ?))
            LIMIT 1
            """,
            (last_cursor[0], last_cursor[0], last_cursor[1]),
        ).fetchone()
        if more is None:
            conn.execute(
                "INSERT OR REPLACE INTO _meta (key,value) VALUES (?,?)",
                (_EXEC_WRAPPER_BACKFILL_KEY, _EXEC_WRAPPER_BACKFILL_VERSION),
            )
            conn.execute(
                "DELETE FROM _meta WHERE key IN (?,?)",
                (_EXEC_WRAPPER_BACKFILL_CURSOR_KEY, _EXEC_WRAPPER_LEGACY_CURSOR_KEY),
            )
        conn.execute("RELEASE SAVEPOINT codex_exec_wrapper_backfill")
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT codex_exec_wrapper_backfill")
        conn.execute("RELEASE SAVEPOINT codex_exec_wrapper_backfill")
        raise
    conn.commit()
    return repaired


def _rollout_generation(stat: os.stat_result) -> str:
    """Stable local identity used to reject a replaced rollout at one path."""
    return f"{stat.st_dev}:{stat.st_ino}"


def _stable_jsonl_end(path: Path, offset: int, *, max_bytes: int | None = None) -> int:
    """Return the byte after the last complete newline in a bounded suffix."""
    try:
        with path.open("rb") as handle:
            handle.seek(offset)
            suffix = handle.read(-1 if max_bytes is None else max(0, max_bytes))
    except OSError:
        return offset
    newline = suffix.rfind(b"\n")
    return offset if newline < 0 else offset + newline + 1


def _append_byte_budget() -> int:
    """Maximum stable JSONL bytes one active-append admission may materialize."""
    try:
        # This lane has a two-second latency contract; large backlogs drain
        # over successive receipts rather than consuming a core for seconds.
        return max(1024, int(os.environ.get("FLEX_CODEX_APPEND_MAX_BYTES", str(64 * 1024))))
    except ValueError:
        return 64 * 1024


def _count_jsonl_lines(path: Path, start: int, end: int) -> int:
    """Count physical records in an already bounded byte interval."""
    if end <= start:
        return 0
    try:
        with path.open("rb") as handle:
            handle.seek(start)
            return handle.read(end - start).count(b"\n")
    except OSError:
        return 0


def _load_targeted_thread_meta(state_db: Path, session_id: str) -> dict[str, dict]:
    """Fetch only the state_5 row needed by a stable active append."""
    if not state_db.exists() or not session_id:
        return {}
    try:
        uri = f"file:{state_db}?mode=ro"
        state = sqlite3.connect(uri, uri=True, timeout=2.0)
        try:
            row = state.execute(
                """SELECT id,title,git_branch,git_sha,git_origin_url,model,
                          agent_role,agent_nickname,source,cli_version
                   FROM threads WHERE id=?""",
                (session_id,),
            ).fetchone()
        finally:
            state.close()
    except sqlite3.Error:
        return {}
    if not row:
        return {}
    return {str(row[0]): {
        "title": row[1] or None, "git_branch": row[2] or None,
        "git_sha": row[3] or None, "git_origin_url": row[4] or None,
        "model": row[5] or None, "agent_role": row[6] or None,
        "agent_nickname": row[7] or None, "source": row[8] or None,
        "cli_version": row[9] or None,
    }}


def _load_targeted_rollout_state(
    state_db: Path, session_id: str,
) -> tuple[dict[str, dict], dict[str, list[tuple[str, str]]], dict[str, dict], dict[str, dict]]:
    """Read only state_5 facts owned by one append receipt's session.

    Full reconciliation still reads the provider's complete lineage tables.
    The active path must not reload them per two-second append: title/context,
    child edges, memory, and batch-job facts are keyed by this one session.
    """
    meta = _load_targeted_thread_meta(state_db, session_id)
    if not state_db.exists() or not session_id:
        return meta, {}, {}, {}
    try:
        state = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True, timeout=2.0)
        try:
            children = {
                session_id: [
                    (str(row[0]), str(row[1] or ""))
                    for row in state.execute(
                        "SELECT child_thread_id,status FROM thread_spawn_edges WHERE parent_thread_id=?",
                        (session_id,),
                    )
                    if row[0]
                ]
            }
            memory_row = state.execute(
                """SELECT raw_memory,rollout_summary,generated_at,rollout_slug,usage_count,last_usage
                   FROM stage1_outputs WHERE thread_id=?""",
                (session_id,),
            ).fetchone()
            job_row = state.execute(
                """SELECT i.job_id,j.name,j.instruction,i.item_id,i.row_index,i.row_json
                   FROM agent_job_items i LEFT JOIN agent_jobs j ON j.id=i.job_id
                   WHERE i.assigned_thread_id=? LIMIT 1""",
                (session_id,),
            ).fetchone()
        finally:
            state.close()
    except sqlite3.Error:
        return meta, {}, {}, {}
    memories = ({session_id: {
        "raw_memory": memory_row[0] or "", "rollout_summary": memory_row[1] or "",
        "generated_at": memory_row[2], "rollout_slug": memory_row[3],
        "usage_count": memory_row[4], "last_usage": memory_row[5],
    }} if memory_row else {})
    jobs = ({session_id: {
        "job_id": job_row[0], "job_name": job_row[1], "job_instruction": job_row[2],
        "item_id": job_row[3], "row_index": job_row[4], "row_json": job_row[5],
    }} if job_row else {})
    return meta, children, memories, jobs


def _clear_codex_session(conn: sqlite3.Connection, session_id: str) -> None:
    """Remove one replaced rollout's derived rows before authoritative replay.

    A rollout path is a provider locator, not enduring identity.  When its
    inode/generation changes we must not retain a higher historical line
    number and silently skip the replacement's early records.
    """
    chunk_ids = [
        row[0] for row in conn.execute(
            "SELECT chunk_id FROM _edges_source WHERE source_id=?", (session_id,),
        )
    ]
    for start in range(0, len(chunk_ids), 400):
        batch = chunk_ids[start:start + 400]
        marks = ",".join("?" for _ in batch)
        for table in (
            "_types_message", "_edges_tool_ops", "_edges_soft_ops",
            "_types_file_body", "_types_codex_turn", "_types_codex_spawn",
            "_edges_raw_content", "_enrich_observations", "_enrich_chunk_rollup",
        ):
            try:
                conn.execute(f"DELETE FROM {table} WHERE chunk_id IN ({marks})", batch)
            except sqlite3.OperationalError:
                # Optional profiles differ across old cells; canonical chunks
                # and source edges below remain the authoritative boundary.
                pass
        conn.execute(f"DELETE FROM _raw_chunks WHERE id IN ({marks})", batch)
        conn.execute(f"DELETE FROM _edges_source WHERE chunk_id IN ({marks})", batch)
    try:
        conn.execute("DELETE FROM _file_body_index WHERE parent_chunk_id LIKE ?", (f"{session_id}_%",))
        conn.execute("DELETE FROM _raw_codex_memory WHERE source_id=?", (session_id,))
        conn.execute("DELETE FROM _types_codex_job WHERE source_id=?", (session_id,))
        conn.execute("DELETE FROM _types_codex_source WHERE session_id=?", (session_id,))
    except sqlite3.OperationalError:
        pass
    conn.execute("DELETE FROM _raw_sources WHERE source_id=?", (session_id,))


def sync_rollout_path(
    conn: sqlite3.Connection,
    rollout_path: Path,
    *,
    state_db: Path | None = None,
    source_meta: Mapping[str, object] | None = None,
    deadline: float | None = None,
    allow_reconcile: bool = True,
    ensure_schema: bool = True,
) -> int:
    """Synchronize one rollout through a generation-qualified append receipt.

    The first visit, legacy receipt, rotated/truncated file, or incomplete
    writer tail takes the full parser path.  Only a stable growth suffix whose
    prior cursor ended on a newline may use the bounded append parser.
    """
    rollout_path = Path(rollout_path).resolve()
    if deadline is not None and time.time() >= deadline:
        return 0
    try:
        stat = rollout_path.stat()
    except OSError:
        return 0
    if ensure_schema:
        ensure_codex_tables(conn)
    prior = conn.execute(
        """SELECT size_bytes,mtime_ns,committed_offset,committed_lines,
                  source_generation,session_id,parser_state
           FROM _codex_source_state WHERE source_path=?""",
        (str(rollout_path),),
    ).fetchone()
    if prior and prior[0:2] == (stat.st_size, stat.st_mtime_ns):
        # Pre-receipt installations are common. Do not turn their whole
        # historical archive into work merely because this code was upgraded;
        # promote one rollout to the authoritative baseline only on its next
        # source change.
        if prior[2] is None or prior[2] == stat.st_size:
            return 0

    state_path = Path(state_db) if state_db is not None else DEFAULT_CODEX_STATE_DB
    generation = _rollout_generation(stat)
    append_state: dict | None = None
    append_offset = 0
    append_limit: int | None = None
    append_lines = 0
    if prior:
        try:
            append_offset = int(prior[2]) if prior[2] is not None else 0
            append_lines = int(prior[3]) if prior[3] is not None else 0
            decoded = json.loads(prior[6]) if prior[6] else None
            if isinstance(decoded, dict):
                append_state = decoded
        except (TypeError, ValueError, json.JSONDecodeError):
            append_state = None
        if (
            append_state is not None
            and prior[4] == generation
            and prior[5] == append_state.get("session_id")
            and 0 <= append_offset <= stat.st_size
        ):
            append_limit = _stable_jsonl_end(
                rollout_path, append_offset, max_bytes=_append_byte_budget(),
            )
            # No newline-complete records is not an error and must not advance
            # the receipt beyond the known good prefix.
            if append_limit == append_offset:
                return 0
        else:
            append_state = None

    if append_state is not None and append_limit is not None:
        thread_meta, spawn_edges, session_memories, job_items = _load_targeted_rollout_state(
            state_path, append_state["session_id"],
        )
        conn.execute("SAVEPOINT codex_append_publication")
        try:
            added = _sync_session_jsonl(
                rollout_path, conn, thread_meta, spawn_edges, session_memories, job_items,
                source_meta=source_meta,
                read_offset=append_offset, read_limit=append_limit,
                line_number_base=append_lines, parser_state=append_state,
                admit_enrichment=False,
                deadline=deadline,
            )
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT codex_append_publication")
            conn.execute("RELEASE SAVEPOINT codex_append_publication")
            raise
        committed_offset = append_limit
        committed_lines = append_lines + _count_jsonl_lines(rollout_path, append_offset, append_limit)
    else:
        if not allow_reconcile:
            # The two-second active lane must never turn a cursor failure into
            # a full archive parse. Its caller records reconciliation debt;
            # the ordinary bounded reconciliation owner repairs it later.
            return 0
        # Authoritative reconciliation. Full parsing is intentionally retained
        # for first capture and any source-generation/cursor failure.
        if (
            prior and prior[5]
            and (prior[4] != generation or (prior[2] is not None and stat.st_size < prior[2]))
        ):
            _clear_codex_session(conn, str(prior[5]))
        append_state = {}
        added = _sync_session_jsonl(
            rollout_path, conn, _load_thread_meta(state_path),
            _load_spawn_edges(state_path), _load_session_memories(state_path),
            _load_job_items(state_path), source_meta=source_meta,
            parser_state=append_state,
        )
        committed_offset = _stable_jsonl_end(rollout_path, 0)
        committed_lines = _count_jsonl_lines(rollout_path, 0, committed_offset)

    try:
        conn.execute(
            """INSERT INTO _codex_source_state(source_path,size_bytes,mtime_ns,
                   committed_offset,committed_lines,source_generation,session_id,parser_state)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(source_path) DO UPDATE SET
                 size_bytes=excluded.size_bytes,
                 mtime_ns=excluded.mtime_ns,
                 committed_offset=excluded.committed_offset,
                 committed_lines=excluded.committed_lines,
                 source_generation=excluded.source_generation,
                 session_id=excluded.session_id,
                 parser_state=excluded.parser_state""",
            (str(rollout_path), stat.st_size, stat.st_mtime_ns,
             committed_offset, committed_lines, generation,
             append_state.get("session_id"), json.dumps(append_state, separators=(",", ":"))),
        )
        if append_limit is not None and append_state is not None:
            conn.execute("RELEASE SAVEPOINT codex_append_publication")
    except Exception:
        if append_limit is not None and append_state is not None:
            conn.execute("ROLLBACK TO SAVEPOINT codex_append_publication")
            conn.execute("RELEASE SAVEPOINT codex_append_publication")
        raise
    conn.commit()
    return added


_CODEX_DRAIN_CURSOR_KEY = "drain_cursor:codex"


def _select_codex_rollout_batch(
    conn: sqlite3.Connection,
    candidates: list[tuple[str, str]],
    limit: int,
) -> list[tuple[str, str]]:
    """Return a fair rollout slice without advancing its durable cursor.

    A rollout can take materially longer than the shared drain window.  Its
    cursor is therefore a completion receipt, unlike generic selection cursors:
    it advances only after the rollout's own source-state transaction commits.
    """
    ordered = sorted(candidates, key=lambda item: item[0])
    if not ordered or limit <= 0:
        return []
    row = conn.execute(
        "SELECT value FROM _meta WHERE key=?", (_CODEX_DRAIN_CURSOR_KEY,),
    ).fetchone()
    cursor = str(row[0]) if row and row[0] else ""
    start = next(
        (index for index, (key, _) in enumerate(ordered) if key > cursor),
        len(ordered),
    )
    return (ordered[start:] + ordered[:start])[:limit]


def _record_codex_rollout_completion(
    conn: sqlite3.Connection,
    rollout_key: str,
) -> None:
    """Durably advance the Codex fair cursor after a committed rollout sync."""
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key,value) VALUES (?,?)",
        (_CODEX_DRAIN_CURSOR_KEY, rollout_key),
    )
    conn.commit()


_CODEX_SCAN_LOCK = threading.Lock()


def scan_codex_cells(
    deadline: float | None = None,
    *,
    embed: bool = True,
    discover: bool = True,
    cell_names: set[str] | None = None,
) -> dict:
    """Serialize provider scans while allowing separate structural scheduling."""
    with _CODEX_SCAN_LOCK:
        return _scan_codex_cells(
            deadline=deadline,
            embed=embed,
            discover=discover,
            cell_names=cell_names,
        )


def _scan_codex_cells(
    deadline: float | None = None,
    *,
    embed: bool = True,
    discover: bool = True,
    cell_names: set[str] | None = None,
) -> dict:
    """Bounded fair reconciliation for local Codex cells.

    Structural rows always commit independently. ``embed=False`` leaves their
    embeddings as explicit NULL debt so a busy semantic lane cannot delay
    text, metadata, relationships, or FTS visibility. When embedding is admitted,
    ``deadline`` bounds both phases by one absolute clock.
    """
    from flex.registry import list_cells, mark_refresh_started, update_refresh_status
    from flex.modules.codex.sources import resolve_sources

    stats = {'indexed': 0, 'skipped': 0, 'embedded': 0, 'repaired': 0,
             'reconciliation_deferred': 0}
    cells = [c for c in list_cells() if c.get('cell_type') == 'codex'
             and c.get('lifecycle') == 'watch' and c.get('active', 1)
             and (cell_names is None or c.get('name') in cell_names)]
    limit = max(1, int(os.environ.get('FLEX_DRAIN_FILES_PER_CELL', '200')))
    embed_limit = max(1, int(os.environ.get('FLEX_CODEX_EMBED_PER_TICK', '128')))
    for cell in cells:
        if deadline is not None and time.time() >= deadline:
            stats['deadline_hit'] = stats.get('deadline_hit', 0) + 1
            break
        conn = sqlite3.connect(cell['path'], timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        # Schema repair is reconciliation work. Re-running CREATE/PRAGMA/COMMIT
        # in every active append tick was a fixed multi-second cost on a large
        # cell even when no rollout had changed.
        if discover:
            ensure_codex_tables(conn)
        # Historical wrapper repair is reconciliation/enrichment work, never
        # part of the two-second active append admission.
        repaired = (
            backfill_exec_command_wrappers(conn, deadline=deadline)
            if discover else 0
        )
        candidates = []
        source_by_path = {}
        sources = [source for source in resolve_sources(conn) if source.usable]

        # Stat recently synchronized rollouts first. An active long-running
        # session already has a durable source-state row, so this detects its
        # growth without first walking the entire Codex archive. The bounded
        # discovery pass below remains responsible for never-before-seen files.
        known_rows = conn.execute(
            """SELECT source_path,size_bytes,mtime_ns,committed_offset,
                      source_generation,session_id,parser_state
               FROM _codex_source_state """
            "ORDER BY mtime_ns DESC LIMIT ?",
            (max(limit * 4, 256),),
        ).fetchall()
        known_paths = {str(row[0]) for row in known_rows}
        deferred_reconciles = 0
        for source_path, prior_size, prior_mtime, committed_offset, prior_generation, prior_session, parser_state in known_rows:
            if deadline is not None and time.time() >= deadline:
                break
            resolved_path = Path(source_path)
            source = next(
                (
                    candidate for candidate in sources
                    if resolved_path.is_relative_to(candidate.sessions_dir)
                ),
                None,
            )
            if source is None:
                continue
            try:
                stat = resolved_path.stat()
            except OSError:
                continue
            resolved = str(resolved_path)
            source_by_path[resolved] = source
            if (prior_size, prior_mtime) != (stat.st_size, stat.st_mtime_ns):
                # A two-second active tick is allowed only to advance a
                # generation-qualified append receipt. Anything else is
                # explicit reconciliation debt for the ordinary owner.
                if not discover and (
                    committed_offset is None or not prior_session or not parser_state
                    or prior_generation != _rollout_generation(stat)
                ):
                    deferred_reconciles += 1
                    continue
                candidates.append((resolved, resolved))

        if deferred_reconciles:
            mark_refresh_started(
                cell['name'], pending=deferred_reconciles,
                reconciliation_required=True,
            )
            stats['reconciliation_deferred'] += deferred_reconciles

        # Discover new rollouts within the same absolute budget. Check the
        # clock on every yielded path so a large historical tree cannot turn a
        # two-second structural pass into a minute-long query blackout.
        if discover and not candidates:
            for source in sources:
                if deadline is not None and time.time() >= deadline:
                    break
                for path in source.sessions_dir.rglob('rollout-*.jsonl'):
                    if deadline is not None and time.time() >= deadline:
                        break
                    resolved = str(path.resolve())
                    if resolved in known_paths:
                        continue
                    source_by_path[resolved] = source
                    try:
                        path.stat()
                    except OSError:
                        continue
                    candidates.append((resolved, resolved))
        batch = _select_codex_rollout_batch(conn, candidates, limit)
        changed = 0
        completed = 0
        deadline_hit = False
        for rollout_key, resolved in batch:
            # Do not begin a potentially long structural transaction after its
            # caller's absolute drain window has closed.
            if deadline is not None and time.time() >= deadline:
                deadline_hit = True
                break
            source = source_by_path[resolved]
            changed += sync_rollout_path(
                conn, Path(resolved), state_db=source.state_db,
                deadline=deadline,
                allow_reconcile=discover,
                ensure_schema=discover,
                source_meta={
                    'source_kind': source.source_kind,
                    'codex_home': str(source.codex_home),
                    'sessions_dir': str(source.sessions_dir),
                    'state_db': str(source.state_db),
                    'rollout_path': resolved,
                },
            )
            completed += 1
            # sync_rollout_path commits the corpus plus its source-state
            # receipt as one rollout transaction. Only then may fair resume
            # advance past it; an expired tick leaves untouched rollouts next.
            _record_codex_rollout_completion(conn, rollout_key)

        if not embed:
            embedded = 0
        elif deadline is not None and time.time() >= deadline:
            deadline_hit = True
            embedded = 0
        else:
            embedded = _batch_embed_chunks(
                conn, batch_size=64, quiet=True, max_chunks=embed_limit,
                deadline=deadline,
            )
        stats['indexed'] += changed
        stats['embedded'] += embedded
        stats['repaired'] += repaired
        stats['skipped'] += max(0, completed - changed)
        if changed or embedded or repaired:
            update_refresh_status(cell['name'], 'ok')
        conn.close()
        if deadline_hit:
            stats['deadline_hit'] = stats.get('deadline_hit', 0) + 1
            break
    return stats


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
