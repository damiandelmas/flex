---
name: flex:arxiv
description: Search indexed arXiv papers in the arxiv cell through the Flex MCP server. Use when the user asks for literature surveys, paper sections, hub papers, concept lineage, category- or date-scoped research, or semantic search over the installed arXiv corpus.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "Paper/concept/category, e.g. 'sections of 2004.12832', 'cs.IR papers last 90 days', 'semantic search retrieval augmentation'"
---

# flex:arxiv

Flex indexes arXiv papers in the `arxiv` cell, pulled from the public arXiv
API at install/refresh time (not at retrieval time). The cell is a
self-describing SQLite database with papers/chunks views, sources, embeddings,
and graph intelligence (hubs, bridges, communities). Use this skill for paper
sections, abstracts, literature surveys, concept lineage, and hub discovery.

Use `mcp__flex__flex_search` with `cell="arxiv"`. First call
`query="@orient"` unless this cell was already oriented in the current turn,
then follow the bundled cell instructions, presets (`@landscape` for corpus
shape, `@bridges`, `@genealogy`), views, and drillback notes. Every query must
be SQL or an `@preset`; wrap plain text in the cell's documented `keyword()`
or `vec_ops()` pattern, pushing scope into the vec_ops pre-filter.
