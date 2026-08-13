"""Runtime identity bindings shared by query-time Flex surfaces."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping

from flex import registry


@dataclass(frozen=True)
class RuntimeSeed:
    """One coding-agent session resolved from the current runtime."""

    session_id: str
    cell: str
    source: str
    cell_id: str | None = None


def environment_from_request_metadata(
    metadata: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Translate provider-native MCP request identity into runtime bindings.

    The MCP transport metadata is caller-scoped, so it can safely cross the
    long-running HTTP server boundary without consulting process-global state.
    Unknown metadata is ignored rather than guessed.
    """
    if not metadata:
        return {}

    codex = metadata.get("x-codex-turn-metadata")
    if isinstance(codex, Mapping):
        session_id = codex.get("session_id") or codex.get("thread_id")
        if session_id:
            return {"CODEX_THREAD_ID": str(session_id)}
    claude = metadata.get("x-claude-session-metadata")
    if isinstance(claude, Mapping):
        session_id = claude.get("session_id")
        if session_id:
            return {"CLAUDE_SESSION_ID": str(session_id)}
    claude_session_id = metadata.get("x-claude-session-id")
    if claude_session_id:
        return {"CLAUDE_SESSION_ID": str(claude_session_id)}

    runtime = metadata.get("x-flex-runtime-metadata")
    if isinstance(runtime, Mapping):
        session_id = runtime.get("session_id")
        provider = runtime.get("provider")
        if session_id and provider:
            return {
                "FLEX_RUNTIME_SESSION_ID": str(session_id),
                "FLEX_RUNTIME_PROVIDER": str(provider),
            }
    return {}


def resolve_runtime_seed(
    *,
    cell: str | None = None,
    session_id: str | None = None,
    environ: Mapping[str, str] | None = None,
    require_cell_id: bool = True,
) -> RuntimeSeed:
    """Resolve the current coding-agent session without consulting transcripts.

    Explicit values win. Otherwise provider-native process identity wins over
    the generic runtime binding. ``cell`` remains an explicit provider override
    for callers such as Ledger.
    """
    env = environ if environ is not None else os.environ
    if session_id:
        if not cell:
            raise ValueError("--session-id requires --cell")
        provider = cell
        source = "explicit"
        sid = session_id
    elif env.get("CODEX_THREAD_ID"):
        provider = cell or "codex"
        source = "CODEX_THREAD_ID"
        sid = env["CODEX_THREAD_ID"]
    elif env.get("CLAUDE_SESSION_ID"):
        provider = cell or "claude_code"
        source = "CLAUDE_SESSION_ID"
        sid = env["CLAUDE_SESSION_ID"]
    else:
        runtime_sid = env.get("FLEX_RUNTIME_SESSION_ID")
        runtime_provider = (env.get("FLEX_RUNTIME_PROVIDER") or "").lower()
        if not runtime_sid or runtime_provider not in {"codex", "claude", "claude_code"}:
            raise ValueError(
                "no current coding-agent session in the runtime environment "
                "(expected CODEX_THREAD_ID, CLAUDE_SESSION_ID, or a Flex runtime binding)"
            )
        provider = cell or (
            "claude_code" if runtime_provider.startswith("claude") else "codex"
        )
        source = "FLEX_RUNTIME_SESSION_ID"
        sid = runtime_sid

    metadata = registry.get_cell_metadata(provider)
    durable_id = str(metadata["id"]) if metadata and metadata.get("id") else None
    if require_cell_id and durable_id is None:
        raise ValueError(f"runtime provider cell has no durable registry identity: {provider}")
    return RuntimeSeed(str(sid), provider, source, durable_id)
