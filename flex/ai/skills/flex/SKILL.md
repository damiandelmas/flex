---
name: flex
description: Search knowledge cells through the Flex MCP server. Use when the user asks to flex, search conversations, memories, changes, documentation, session history, or any named Flex cell.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "cell and request, e.g. 'markdown @orient', 'instructions @orient', 'search notes about launch'"
---

# flex

Flex indexes conversations and knowledge bases as self-describing SQLite cells
with chunks, views, presets, embeddings, and graph intelligence. Use
`mcp__flex__flex_search` on the cell that matches the user's request.

First call `query="@orient"` for that cell unless it was already oriented in
the current turn. Then follow the bundled cell instructions, presets, views,
source notes, and query examples. Every query must be SQL or an `@preset`; use
the cell's documented `keyword()` or `vec_ops()` pattern for plain text.
