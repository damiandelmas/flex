---
name: flex:github
description: Search indexed GitHub issues and comments in the github cell through the Flex MCP server. Use when the user asks about open or closed issues, bug reports, feature requests, discussion threads, reply targets, or semantic search over issue trackers across configured repos.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Repo/issue/topic, e.g. 'open issues in owner/repo', 'thread for issue 42', 'semantic search memory leak reports'"
---

# flex:github

Flex indexes GitHub issues and comments in the `github` cell, a remote-pull
cell refreshed on a bounded schedule. Each source is one issue; each chunk is
the issue body or one comment. The cell is a self-describing SQLite database
with `issues` (source-level) and `posts` (chunk-level) surfaces, embeddings,
and graph intelligence. Use this skill for issue triage, thread drilldown,
engagement patterns, reply targets, and semantic search over problem reports.

Use `mcp__flex__flex_search` with `cell="github"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets (`@open-issues`,
`@reply-targets`), views, and rate-limit notes. Every query must be SQL or an
`@preset`; wrap plain text in the cell's documented `keyword()` or
`vec_ops()` pattern.
