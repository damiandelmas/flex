---
name: flex:sessions:codex
description: Search Codex CLI session provenance in the codex cell through the Flex MCP server. Use when the user asks about Codex sessions, recent Codex turns, tool calls, files touched, forks/delegation, or semantic search over Codex coding-agent work.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Codex session/topic/file, e.g. 'latest turns in 019...', 'who touched flex/cli.py', 'semantic search release boundary'"
---

# flex:sessions:codex

Flex indexes Codex CLI session history in the `codex` cell. The cell is a
self-describing SQLite database with chunks, messages, sessions, files, ACP
coverage views, embeddings, and graph intelligence. Use this skill for Codex
prompts, assistant turns, tool calls, files touched, delegation/forks,
decisions, source recovery, and session history.

Use `mcp__flex__flex_search` with `cell="codex"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets, views, source limits, and
drillback notes. Every query must be SQL or an `@preset`; wrap plain text in
the cell's documented `keyword()` or `vec_ops()` pattern.
