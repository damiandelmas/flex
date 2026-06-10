---
name: flex:reddit
description: Search subreddit-scoped Reddit archives in the reddit cell through the Flex MCP server. Use when the user asks what Reddit users say about a product or topic, complaint patterns, top threads by score, comment drilldowns, per-subreddit coverage, or semantic search over community opinion.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Topic/subreddit/thread, e.g. 'complaints about rate limits', 'top ClaudeCode threads', 'semantic search agent memory'"
---

# flex:reddit

Flex indexes public Reddit threads in the `reddit` cell, a remote-pull social
cell scoped to the subreddits named at install or refresh time. The cell is a
self-describing SQLite database with a `chunks` view (one row per post or
comment, universal `type` column), a `threads` view, embeddings, and graph
intelligence (hubs, bridges, communities). Use this skill for community
opinion, complaint and praise patterns, top threads, comment drilldowns, and
semantic search over what users say.

Use `mcp__flex__flex_search` with `cell="reddit"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets, views, and drilldown
notes. Every query must be SQL or an `@preset`; wrap plain text in the cell's
documented `keyword()` or `vec_ops()` pattern.
