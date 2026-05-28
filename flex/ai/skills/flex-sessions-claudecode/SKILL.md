---
name: flex:sessions:claudecode
description: Search Claude Code conversation history in the claude_code cell through the Flex MCP server. Use when the user asks about Claude Code sessions, recent turns, tool calls, files touched, delegation, or semantic search over Claude Code coding-agent work.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Claude Code session/topic/file, e.g. 'story agent-...', 'latest turns', 'who edited auth.py'"
---

# flex:sessions:claudecode

Flex indexes Claude Code session history in the `claude_code` cell. The cell
is a self-describing SQLite database with chunks, messages, sessions, files,
ACP coverage views, embeddings, and graph intelligence. Use this skill for
Claude Code prompts, assistant turns, tool calls, files touched,
delegation/forks, decisions, and session history.

Use `mcp__flex__flex_search` with `cell="claude_code"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets, views, source limits, and
drillback notes. Every query must be SQL or an `@preset`; wrap plain text in
the cell's documented `keyword()` or `vec_ops()` pattern.
