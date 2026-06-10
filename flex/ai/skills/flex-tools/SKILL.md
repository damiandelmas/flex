---
name: flex:tools
description: Search the public AI tooling catalog in the tools cell through the Flex MCP server. Use when the user asks about MCP servers, Claude Code skills, agents, hooks, commands, ecosystem rankings by stars or language, install instructions, or semantic search over tool capabilities.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "tool/capability/skill, e.g. 'top MCP servers for browser automation', 'user-invocable skills for testing', 'install command for X'"
---

# flex:tools

Flex indexes the Claude Code ecosystem in the `tools` cell: MCP servers,
skills, agents, hooks, commands, and plugin manifests gathered from public
GitHub repositories. The cell is a self-describing SQLite database with a
`tools` catalog surface (stars, language, `tool_class`, `is_mcp`), `docs`
README sections, a `skills` surface with parsed frontmatter
(`allowed_tools`, `user_invocable`, `permission_mode`), embeddings, and
graph intelligence.

Use `mcp__flex__flex_search` with `cell="tools"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets, views, and source limits.
Every query must be SQL or an `@preset`; wrap plain text in the cell's
documented `keyword()` or `vec_ops()` pattern.
