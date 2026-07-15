---
name: flex:markdown
description: Query the Markdown capability of a Flex filesystem cell: headings, frontmatter, tags, Dataview fields, and optional Obsidian wikilinks and aliases.
allowed-tools:
  - Bash
user-invocable: true
---

# flex:markdown

Markdown is a file capability inside a filesystem cell, not a separate compiler.
Start with `flex core search --cell <name> "@orient"`, then scope ordinary
filesystem retrieval with `WHERE file_kind='markdown'`.

- Heading chunks expose `section_title`, `depth`, `container_id`, and `position`.
- Note metadata lives in `_types_markdown_source`.
- Dataview-style fields live in `_fields_inline`.
- When `obsidian=true`, tags, aliases, resolved links, and unresolved targets live
  in `_fields_inline`, `_edges_wikilink`, and `_edges_wikilink_unresolved`.
- Semantic retrieval is available only when the live cell reports `embed=true`.

Read a note in order:

```sql
SELECT section_title, depth, content FROM chunks
WHERE source_id=:path AND file_kind='markdown' ORDER BY position;
```

Find backlinks:

```sql
SELECT from_path FROM _edges_wikilink WHERE to_path=:path ORDER BY from_path;
```

The presence of a `.obsidian/` directory does not prove vault semantics were
enabled. Use the live metadata reported by `@orient`.
