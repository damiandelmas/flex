---
name: flex:markdown
description: Search a markdown or Obsidian vault cell through the Flex MCP server. Use when the user asks about their notes, vault sections, tags, wikilinks, backlinks, hub notes, Dataview fields, or semantic search over indexed markdown folders.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Note/topic/tag, e.g. 'sections of Architecture', 'notes tagged area/work', 'semantic search project planning'"
---

# flex:markdown

Flex indexes markdown folders and Obsidian vaults as note cells. The cell name
defaults to the vault's folder name (`--name` can override it at init). The
cell is a self-describing SQLite database with `sections` (heading-delimited
chunks), `notes` (per-file metadata), wikilink edge tables, Dataview inline
fields, embeddings, and graph intelligence. Use this skill for note retrieval,
tag and folder queries, backlinks, hubs, orphans, and ghost notes.

Use `mcp__flex__flex_search` with `cell="<vault_name>"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets (`@hubs`, `@orphans`,
`@ghost-notes`, `@communities`), views, and graph notes. Every query must be
SQL or an `@preset`; wrap plain text in the cell's documented `keyword()` or
`vec_ops()` pattern.
