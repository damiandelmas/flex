---
name: flex
description: Search Claude Code session history via the flex MCP server. Covers decision archaeology, file lineage, weekly digests, and semantic search. Use when the user asks about past sessions, file history, why something was done, or what happened recently.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "what to search for (e.g., 'history of auth.py', 'what did we build this week', 'why did we switch to Postgres')"
---

# Flex Search

Search the user's Claude Code session history. The MCP tool documentation covers syntax — this skill covers methodology.

## Setup

Requires the flex MCP server. If `mcp__flex__flex_search` is not available, run the setup script:

```bash
bash scripts/setup.sh
```

Or install manually: `curl -sSL https://getflex.dev/install.sh | bash`

After install, restart Claude Code and type `/mcp` to verify.

## Start Here

Run `@orient` first. Always. It returns the schema, views, and presets. Never assume column names.

## Question Types

### Decision Archaeology

*"Why did we do it this way?"*

1. Broad semantic search for the topic
2. Narrow by project or time range from what you find
3. Pull the session where the decision happened
4. Surface the exact user prompts and assistant responses — the user wants to see what was said, not a summary

### File Lineage

*"What's the history of this file?"*

Use `@file path=src/auth.py` — it resolves identity across renames automatically.

### Weekly Digest

*"What did we build this week?"*

Use `@digest days=7`. Follow up with specific queries on anything interesting.

### Session Details

*"Tell me about session X"*

Use `@story session={session_id}`.

### Concept Lineage

*"How did our auth approach evolve?"*

Use `@genealogy concept=authentication`. Traces a concept across sessions and surfaces hubs.

### Semantic Search

*"Find sessions about X but not Y"*

Use `suppress:` to push a dominant topic out. Use `diverse` for breadth. Use `decay:N` for recency. These compose — the MCP tool docs have the full token reference.

## Methodology

1. **Orient first.** `@orient` before anything. The cell describes itself.
2. **Presets first.** If a preset fits, use it. They encode multi-query workflows in one call.
3. **Discover then narrow.** First query broad, let results redirect. Pre-filter the next query with what you find.
4. **Push constraints early.** Known session, project, date? SQL pre-filter before scoring — not after.
5. **Exact terms use keyword.** Function names, error strings, UUIDs — semantic search won't find these. Use `keyword('term')`.
6. **Include excerpts.** Always surface actual user prompts and assistant responses. Never summarize what you can quote.

# Final

**USE YOUR JUDGEMENT TO BEST ANSWER THE USERS QUERY. ENSURE TO QUALIFY STATEMENTS WHEN THERE IS NO EXPLICIT EVIDENCE FOR YOUR CLAIMS**