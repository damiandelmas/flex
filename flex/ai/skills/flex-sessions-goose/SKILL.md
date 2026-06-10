---
name: flex:sessions:goose
description: Search Goose session provenance in the goose cell through the Flex MCP server. Use when the user asks about Goose sessions, recent Goose turns, tool calls, files touched, provider/model/mode metadata, or semantic search over Goose coding-agent work.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Goose session/topic/file, e.g. 'latest turns in 019...', 'who touched src/foo.py', 'sessions by provider and mode'"
---

# flex:sessions:goose

Flex indexes Goose `sessions.db` history in the `goose` cell. The cell is a
self-describing SQLite database on the claude_code substrate with chunks,
messages, sessions, files, a `_types_goose_session` sidecar surfaced through
the extended `sources` view (provider, model, mode, tokens), embeddings, and
graph intelligence. Use this skill for Goose prompts, assistant turns, tool
calls, files touched, provider/model metadata, and session history.

Use `mcp__flex__flex_search` with `cell="goose"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets, views, source limits, and
drillback notes. Every query must be SQL or an `@preset`; wrap plain text in
the cell's documented `keyword()` or `vec_ops()` pattern.
