---
name: flex:filesystem
description: Query a mixed Flex filesystem cell containing Markdown, code, and text through SQL, keyword search, semantic retrieval, file structure, and code graph edges.
allowed-tools:
  - Bash
user-invocable: true
---

# flex:filesystem

A filesystem cell is one folder compiled into one live cell. Markdown headings,
Python and JS/TS symbols, calls and imports, generic text, file identity, FTS5,
and—unless explicitly disabled—Nomic semantic vectors coexist on the same chunks.
Obsidian metadata is an additive capability when the cell was built with it.

Always begin with the cell's live contract:

```bash
flex core search --cell <name> "@orient"
```

Trust `@orient` over this skill for the actual root, columns, embedding mode,
file-kind counts, presets, and optional capabilities.

## Retrieval method

1. Establish shape with SQL counts, file kinds, paths, and columns.
2. Use `keyword('literal terms')` for identifiers, error strings, and exact prose.
3. Use `vec_ops('semantic idea')` when `@orient` reports embeddings enabled.
4. Navigate code with `@callers`, `@callees`, `@impact`, and imports.
5. Navigate heading or symbol containment with `@subtree`.
6. Read narrowed chunks in `position` order and retain `source_id` provenance.

Examples:

```sql
SELECT file_kind, count(DISTINCT source_id) AS files, count(*) AS chunks
FROM chunks GROUP BY file_kind ORDER BY files DESC;
```

```sql
SELECT k.snippet, c.source_id, c.section_title
FROM keyword('refresh failure') k JOIN chunks c ON c.id=k.id
ORDER BY k.rank DESC LIMIT 12;
```

```sql
SELECT v.score, c.source_id, c.section_title, c.content
FROM vec_ops('durable reconciliation after restart') v
JOIN chunks c ON c.id=v.id
ORDER BY v.score DESC LIMIT 12;
```

Use `_edges_fs_identity.file_uuid` to bridge a file into coding-session cells.
Use `DISTINCT` when optional edge or metadata joins can fan out a chunk. Never
claim semantic retrieval when `embed=false`, and qualify call-graph results:
same-named definitions are conservative candidates, not certain edges.
