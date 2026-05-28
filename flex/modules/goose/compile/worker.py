"""Goose transpiler — the only goose-specific code in the module.

Reads goose's sessions.db (read-only) and emits claude_code-canonical rows:
    _raw_chunks, _raw_sources, _edges_source, _edges_tool_ops,
    _types_message, _types_file_body, _file_body_index,
    _raw_content, _edges_raw_content, _edges_delegations.

Everything downstream (cell bootstrap, stubs, embedding, enrichment, stock
views, presets, lifecycle) is handled by the Claude Code substrate directly
via `bootstrap_claude_code_cell`, `ENRICHMENT_STUBS`, `_batch_embed_chunks`,
and `run_enrichment`.

Wire format observations (from external vendor/goose source + live sessions.db):

1. Tool names are BARE (`write`, `edit`, `shell`, `view`) with the extension
   carried in a SIBLING `_meta.goose_extension` field on the toolRequest.
   Canonical name lookup is `(goose_extension, bare_name)` → CC tool name.

2. Delegation: a `summon.delegate` tool call returns a `toolResult.value._meta`
   with `subagent_session_id` pointing at a session (`session_type='SubAgent'`)
   that lives in the same sessions.db. This is the parent→child linkage.

3. Session-level recipes: `sessions.recipe_json` carries the recipe text that
   defined the session. We synthesize it as a position-0 tool_call chunk with
   tool_name='GooseRecipe' so CC presets pick it up through tool_ops.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

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
except ImportError:  # pragma: no cover - older Flex installs without shared bridge
    soma_enrich_operation = None


DEFAULT_GOOSE_DB = Path.home() / ".local" / "share" / "goose" / "sessions" / "sessions.db"


# ── Tool-name mapping (wire format: (ext, bare_name) → canonical CC) ────────

_TOOL_NAME_MAP: dict[tuple[str, str], str] = {
    ("developer", "write"):       "Write",
    ("developer", "edit"):        "Edit",
    ("developer", "str_replace"): "Edit",
    ("developer", "insert"):      "Edit",
    ("developer", "view"):        "Read",
    ("developer", "read"):        "Read",
    ("developer", "shell"):       "Shell",   # generic; goose may use bash/zsh/fish
}

_PATH_KEYS = ("path", "file_path", "notebook_path")
_BODY_KEYS = ("file_text", "content", "text", "new_str")


GOOSE_OPTIONAL_TABLES_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS _types_goose_session (
        source_id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        session_type TEXT,
        working_dir TEXT,
        created_at TEXT,
        updated_at TEXT,
        provider_name TEXT,
        model_config_json TEXT,
        goose_mode TEXT,
        thread_id TEXT,
        total_tokens INTEGER,
        input_tokens INTEGER,
        output_tokens INTEGER,
        accumulated_total_tokens INTEGER,
        accumulated_input_tokens INTEGER,
        accumulated_output_tokens INTEGER,
        recipe_json TEXT,
        user_recipe_values_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_goose_session_type ON _types_goose_session(session_type)",
    "CREATE INDEX IF NOT EXISTS idx_goose_session_provider ON _types_goose_session(provider_name)",
    "CREATE INDEX IF NOT EXISTS idx_goose_session_mode ON _types_goose_session(goose_mode)",
)


def ensure_goose_tables(conn: sqlite3.Connection) -> None:
    """Create Goose-specific optional tables. Idempotent."""
    for ddl in GOOSE_OPTIONAL_TABLES_DDL:
        conn.execute(ddl)


def _map_tool_name(raw_name: str, goose_extension: Optional[str]) -> str:
    if not raw_name:
        return "unknown"
    if goose_extension:
        hit = _TOOL_NAME_MAP.get((goose_extension, raw_name))
        if hit:
            return hit
        # Some goose extensions self-prefix their bare name already
        # (e.g. `computercontroller__web_scrape`). Don't re-prepend.
        prefix = f"{goose_extension}__"
        if raw_name.startswith(prefix):
            return raw_name
        return f"{prefix}{raw_name}"
    return raw_name


def _target_file(tool_name: str, arguments: dict) -> Optional[str]:
    if not isinstance(arguments, dict):
        return None
    for k in _PATH_KEYS:
        v = arguments.get(k)
        if v:
            return str(v)
    return None


def _extract_body(tool_name: str, arguments: dict) -> Optional[str]:
    """File body from request arguments for Write-like ops."""
    if tool_name != "Write" or not isinstance(arguments, dict):
        return None
    for k in _BODY_KEYS:
        v = arguments.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def _flatten_tool_result(val) -> str:
    if isinstance(val, str):
        return val
    if isinstance(val, list):
        bits: list[str] = []
        for item in val:
            if isinstance(item, dict):
                txt = item.get("text")
                if txt:
                    bits.append(str(txt))
                    continue
                bits.append(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            else:
                bits.append(str(item))
        return " ".join(bits)
    if isinstance(val, dict):
        txt = val.get("text")
        if txt:
            return str(txt)
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    return str(val) if val is not None else ""


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


# ── Per-session transpile ───────────────────────────────────────────────────


def _synth_recipe_chunk(
    conn: sqlite3.Connection,
    session_id: str,
    recipe_json: str,
    user_recipe_values_json: Optional[str],
    ts: int,
) -> int:
    """Emit a position-0 GooseRecipe chunk so enrichment/presets see it."""
    try:
        recipe = json.loads(recipe_json)
    except json.JSONDecodeError:
        recipe = {"raw": recipe_json}
    try:
        values = json.loads(user_recipe_values_json) if user_recipe_values_json else None
    except json.JSONDecodeError:
        values = None

    desc = recipe.get("title") or recipe.get("name") or "recipe"
    body = recipe.get("instructions") or recipe.get("prompt") or ""
    text = f"GooseRecipe: {desc}\n\n{body}".strip()
    if values:
        text += "\n\nvalues: " + json.dumps(values, ensure_ascii=False)
    if not text:
        return 0

    chunk_id = f"{session_id}_0"
    chunk = {
        "id": chunk_id,
        "doc_id": session_id,
        "chunk_number": 0,
        "type": "tool_call",
        "content": text,
        "tool_name": "GooseRecipe",
        "target_file": None,
        "success": True,
        "timestamp": ts,
        "role": "assistant",
        "cwd": None,
        "git_branch": None,
        "parent_uuid": None,
        "is_sidechain": 0,
        "entry_uuid": None,
        "branch_id": 0,
    }
    insert_chunk_atom(conn, chunk)
    update_source_stats(conn, session_id, chunk)
    # Full recipe JSON preserved in _raw_content
    _store_content_raw(conn, chunk_id, recipe_json, "GooseRecipe", ts)
    return 1


def _sync_session(
    session_row: dict,
    message_rows: list[dict],
    conn: sqlite3.Connection,
    session_id_set: set[str],
) -> int:
    """Translate one goose session + its messages into CC-canonical chunks."""
    ensure_goose_tables(conn)
    session_id = session_row["id"]
    cwd = session_row.get("working_dir") or None
    title = session_row.get("name") or None

    ensure_source_exists(conn, session_id, cwd=cwd, title=title)
    conn.execute(
        """
        INSERT OR REPLACE INTO _types_goose_session (
            source_id, name, description, session_type, working_dir,
            created_at, updated_at, provider_name, model_config_json,
            goose_mode, thread_id, total_tokens, input_tokens, output_tokens,
            accumulated_total_tokens, accumulated_input_tokens,
            accumulated_output_tokens, recipe_json, user_recipe_values_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            session_id,
            session_row.get("name"),
            session_row.get("description"),
            session_row.get("session_type"),
            cwd,
            session_row.get("created_at"),
            session_row.get("updated_at"),
            session_row.get("provider_name"),
            session_row.get("model_config_json"),
            session_row.get("goose_mode"),
            session_row.get("thread_id"),
            session_row.get("total_tokens"),
            session_row.get("input_tokens"),
            session_row.get("output_tokens"),
            session_row.get("accumulated_total_tokens"),
            session_row.get("accumulated_input_tokens"),
            session_row.get("accumulated_output_tokens"),
            session_row.get("recipe_json"),
            session_row.get("user_recipe_values_json"),
        ),
    )

    # Resume: skip chunk_numbers already ingested
    last_num = conn.execute(
        """
        SELECT COALESCE(MAX(tm.chunk_number), 0)
        FROM _types_message tm
        JOIN _edges_source es ON tm.chunk_id = es.chunk_id
        WHERE es.source_id = ?
        """,
        (session_id,),
    ).fetchone()[0]

    # Session-level recipe → position-0 chunk (once)
    recipe_json = session_row.get("recipe_json")
    if recipe_json and last_num == 0:
        ts_s = _iso_to_epoch(session_row.get("created_at")) or int(time.time())
        try:
            _synth_recipe_chunk(
                conn,
                session_id,
                recipe_json,
                session_row.get("user_recipe_values_json"),
                ts_s,
            )
        except Exception as e:
            print(f"[goose] recipe synth error: {e}", file=sys.stderr)

    inserted = 0
    new_chunks: list[dict] = []
    tool_ops_items: list[tuple] = []
    tool_content_items: list[tuple] = []
    fb_items: list[tuple] = []
    delegation_items: list[tuple] = []

    tool_use_tool_name: dict[str, str] = {}
    tool_use_target_file: dict[str, str] = {}
    tool_use_to_chunk: dict[str, str] = {}

    for mrow in message_rows:
        row_num = int(mrow["id"])
        if row_num <= last_num:
            continue

        role = (mrow.get("role") or "").lower()
        content_json = mrow.get("content_json") or ""
        ts_int = int(mrow.get("created_timestamp") or 0) or int(time.time())

        try:
            parts = json.loads(content_json) if content_json else []
        except json.JSONDecodeError:
            parts = [{"type": "text", "text": content_json}]
        if not isinstance(parts, list):
            parts = []

        chunk_id = f"{session_id}_{row_num}"

        text_parts: list[str] = []
        thinking_parts: list[str] = []
        tool_ops_for_line: list[tuple[str, dict, Optional[str]]] = []

        for part in parts:
            if not isinstance(part, dict):
                continue
            ptype = part.get("type", "")

            if ptype == "text":
                txt = part.get("text") or ""
                if isinstance(txt, dict):
                    txt = txt.get("text") or ""
                if txt:
                    text_parts.append(str(txt))

            elif ptype == "thinking":
                tblock = part.get("thinking") or {}
                if isinstance(tblock, dict):
                    txt = tblock.get("thinking") or ""
                else:
                    txt = str(tblock)
                if txt:
                    thinking_parts.append(txt)
                    tool_content_items.append((chunk_id, txt, "_thinking", ts_int))

            elif ptype == "redactedThinking":
                thinking_parts.append("[thinking: redacted]")

            elif ptype == "toolRequest":
                meta = part.get("_meta") or {}
                ext = meta.get("goose_extension") if isinstance(meta, dict) else None
                call = part.get("toolCall") or part.get("tool_call") or {}
                val = call.get("value") if isinstance(call, dict) else None
                if not isinstance(val, dict):
                    val = call if isinstance(call, dict) else {}
                raw_name = val.get("name") or "unknown"
                arguments = val.get("arguments") or {}
                if not isinstance(arguments, dict):
                    arguments = {}
                canonical = _map_tool_name(raw_name, ext)
                tfile = _target_file(canonical, arguments)
                tu_id = part.get("id") or ""

                if tu_id:
                    tool_use_tool_name[tu_id] = canonical
                    tool_use_to_chunk[tu_id] = chunk_id
                    if tfile:
                        tool_use_target_file[tu_id] = tfile

                tool_ops_for_line.append((canonical, arguments, tfile))
                tool_ops_items.append((chunk_id, canonical, tfile, cwd, None, True))

                raw = json.dumps(arguments, ensure_ascii=False)
                if len(raw) > 10:
                    tool_content_items.append((chunk_id, raw, canonical, ts_int))

                body = _extract_body(canonical, arguments)
                if body and tfile:
                    fb_items.append((chunk_id, tfile, body, ts_int))

            elif ptype == "toolResponse":
                tu_id = part.get("id") or ""
                canonical = tool_use_tool_name.get(tu_id, "unknown")
                result = part.get("toolResult") or part.get("tool_result") or {}
                result_value = None
                if isinstance(result, dict):
                    result_value = result.get("value", result)
                else:
                    result_value = result
                raw_text = _flatten_tool_result(result_value)
                if raw_text and len(raw_text) > 10:
                    tool_content_items.append((chunk_id, raw_text, canonical, ts_int))

                # Read: body comes back in the response; pair via tool_use_id
                if canonical == "Read" and raw_text:
                    tfile = tool_use_target_file.get(tu_id)
                    parent_chunk = tool_use_to_chunk.get(tu_id, chunk_id)
                    if tfile and len(raw_text) > 50:
                        fb_items.append((parent_chunk, tfile, raw_text, ts_int))

                # Delegation: value._meta.subagent_session_id → _edges_delegations
                if isinstance(result_value, dict):
                    rmeta = result_value.get("_meta") or result_value.get("meta") or {}
                    if isinstance(rmeta, dict):
                        child = rmeta.get("subagent_session_id")
                        if child and child != session_id:
                            parent_chunk = tool_use_to_chunk.get(tu_id, chunk_id)
                            # agent_type: recipe name if the delegate call carried one, else 'delegate'
                            agent_type = "delegate"
                            # No easy back-ref to the request's arguments here; leave default.
                            delegation_items.append(
                                (parent_chunk, child, agent_type, ts_int, session_id)
                            )

            elif ptype == "image":
                mime = part.get("mimeType") or "image"
                text_parts.append(f"[image: {mime}]")

            elif ptype == "systemNotification":
                msg = part.get("msg") or ""
                if msg:
                    text_parts.append(f"[system: {msg}]")

            # toolConfirmationRequest / actionRequired / frontendToolRequest skip

        text_content = "\n".join(p for p in text_parts if p).strip()
        if thinking_parts and not text_content:
            text_content = "\n".join(thinking_parts)

        if not text_content and not tool_ops_for_line:
            continue
        if not text_content and tool_ops_for_line:
            pieces: list[str] = []
            for canonical, arguments, tfile in tool_ops_for_line:
                pieces.append(canonical)
                if tfile:
                    pieces.append(tfile)
                if canonical == "Bash":
                    cmd = arguments.get("command", "")
                    if cmd:
                        pieces.append(cmd)
            text_content = " ".join(pieces).strip() or tool_ops_for_line[0][0]
            chunk_type = "tool_call"
            chunk_role = "assistant"
        elif role == "tool":
            chunk_type = "tool_call"
            chunk_role = "tool"
        elif role == "user":
            chunk_type = "user_prompt"
            chunk_role = "user"
        else:
            chunk_type = "assistant"
            chunk_role = "assistant"

        new_chunks.append({
            "id": chunk_id,
            "doc_id": session_id,
            "chunk_number": row_num,
            "type": chunk_type,
            "content": text_content,
            "tool_name": None,
            "target_file": None,
            "success": None,
            "timestamp": ts_int,
            "role": chunk_role,
            "cwd": cwd,
            "git_branch": None,
            "parent_uuid": None,
            "is_sidechain": 0,
            "entry_uuid": mrow.get("message_id") or None,
            "branch_id": 0,
        })

    # Insert chunks without embeddings (caller batch-embeds)
    for chunk in new_chunks:
        chunk["embedding"] = None
        try:
            insert_chunk_atom(conn, chunk)
            update_source_stats(conn, session_id, chunk)
            inserted += 1
        except Exception as e:
            print(f"[goose] chunk insert error: {e}", file=sys.stderr)

    for chunk_id, tn, tf, cwd_val, gb, ok in tool_ops_items:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_tool_ops "
                "(chunk_id, tool_name, target_file, success, cwd, git_branch) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chunk_id, tn, tf, ok, cwd_val, gb),
            )
            if soma_enrich_operation:
                soma_enrich_operation(
                    conn,
                    {
                        "chunk_id": chunk_id,
                        "tool_name": tn,
                        "target_file": tf,
                        "cwd": cwd_val,
                        "source_id": session_id,
                    },
                )
        except Exception as e:
            print(f"[goose] tool_ops insert error: {e}", file=sys.stderr)

    for cid, raw, tname, ts in tool_content_items:
        try:
            _store_content_raw(conn, cid, raw, tname, ts)
        except Exception as e:
            print(f"[goose] content store error: {e}", file=sys.stderr)

    for parent_id, tfile, fb_content, ts in fb_items:
        try:
            _ingest_file_body(conn, parent_id, tfile, fb_content, session_id, ts)
        except Exception as e:
            print(f"[goose] file body ingest error: {e}", file=sys.stderr)

    # Delegation edges — ensure child row + emit
    for parent_chunk, child_sid, agent_type, ts, parent_sid in delegation_items:
        if child_sid not in session_id_set:
            # Child isn't a known goose session row; still emit the edge so
            # CC's delegation_graph sees orchestrator activity.
            ensure_source_exists(conn, child_sid)
            session_id_set.add(child_sid)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_delegations "
                "(chunk_id, child_session_id, agent_type, created_at, parent_source_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (parent_chunk, child_sid, agent_type, ts, parent_sid),
            )
        except Exception as e:
            print(f"[goose] delegation insert error: {e}", file=sys.stderr)

    conn.execute(
        "UPDATE _edges_source SET source_type = 'goose' WHERE source_id = ?",
        (session_id,),
    )

    if inserted == 0 and last_num == 0 and not recipe_json:
        conn.execute(
            "DELETE FROM _raw_sources WHERE source_id = ? AND message_count = 0",
            (session_id,),
        )

    return inserted


# ── Public transpile entry point ────────────────────────────────────────────


def transpile(
    source_path: Path,
    conn: sqlite3.Connection,
    progress_cb=None,
    limit: Optional[int] = None,
    commit_every: int = 50,
) -> dict:
    """Read goose sessions.db and write CC-canonical rows. Idempotent.

    Signature matches Goose install/refresh call sites:
        (source_path, conn, progress_cb) → stats dict
    """
    if not source_path.exists():
        raise FileNotFoundError(
            f"goose sessions.db not found at {source_path}. "
            "Install goose and run at least one session first."
        )

    uri = f"file:{source_path}?mode=ro"
    gdb = sqlite3.connect(uri, uri=True, timeout=10.0)
    gdb.row_factory = sqlite3.Row

    t0 = time.time()
    n_sessions = 0
    n_chunks = 0

    try:
        ensure_goose_tables(conn)
        q = "SELECT * FROM sessions ORDER BY created_at"
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = list(gdb.execute(q))
        total = len(rows)

        # Preload the full session id set so delegation edges can ensure
        # their child rows exist (they always do — sessions table is the
        # authoritative source).
        session_id_set = {str(r["id"]) for r in rows}

        for i, row in enumerate(rows, 1):
            s = {k: row[k] for k in row.keys()}
            source_id = s["id"]

            msg_rows_raw = gdb.execute(
                "SELECT * FROM messages WHERE session_id = ? "
                "ORDER BY created_timestamp, id",
                (source_id,),
            ).fetchall()
            msg_rows = [{k: mr[k] for k in mr.keys()} for mr in msg_rows_raw]

            try:
                added = _sync_session(s, msg_rows, conn, session_id_set)
                n_chunks += added
                if added > 0:
                    n_sessions += 1
            except Exception as e:
                print(f"[goose] session {source_id} failed: {e}", file=sys.stderr)

            if i % commit_every == 0 or i == total:
                conn.commit()

            if progress_cb:
                progress_cb(i, total, n_sessions, n_chunks, time.time() - t0)

    finally:
        gdb.close()

    return {
        "sessions": n_sessions,
        "chunks": n_chunks,
        "elapsed": time.time() - t0,
    }
