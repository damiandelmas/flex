---
name: flex:hn
description: Search public Hacker News stories and comments in the hn cell through the Flex MCP server. Use when the user asks what HN says about a technology, top stories by score or comment volume, thread sentiment, recurring complaints or praise, or semantic search over HN opinion and technical debate.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "HN topic/story/author, e.g. 'top stories about sqlite', 'what do comments say about MCP servers', 'drill into thread hn_44497045'"
---

# flex:hn

Flex indexes public Hacker News stories and comments (via the Algolia HN
Search API) in the `hn` cell. It is a remote-pull social cell: a
self-describing SQLite database where each source is a story thread and the
`chunks` view carries a universal `type` column (`story` or `comment`),
embeddings, and graph intelligence. Coverage is bounded by the queries and
date range set at init. Use this skill for top stories, comment-thread
sentiment, dissent and praise patterns, and thread drilldown.

Use `mcp__flex__flex_search` with `cell="hn"`. First call `query="@orient"`
unless this cell was already oriented in the current turn, then follow the
bundled cell instructions, presets, views, and drillback notes. Every query
must be SQL or an `@preset`; wrap plain text in the cell's documented
`keyword()` or `vec_ops()` pattern.
