---
name: flex:filesystem
description: Query a Flex filesystem cell over a folder tree using structural SQL, FTS5, paths, and SOMA file identity. Use it to find or read files, search exact text, inspect a subtree, find duplicate content, or bridge a file to coding-agent sessions that touched it. Filesystem cells in 0.52 do not provide semantic vector search; use flex:codegraph for repository call/import graphs.
allowed-tools:
  - Bash
user-invocable: true
---

# flex:filesystem

Use this skill to query an existing structural filesystem cell. The cell indexes a
folder tree as SQLite tables and views with FTS5, paths, containment, and stable SOMA
file identity. It has no embedding or `vec_ops()` surface in 0.52.

For a repository's symbol, call, and import graph, use `flex:codegraph`. For a
Markdown or Obsidian vault's note semantics, use the corresponding explicit skill.

## Start here

Filesystem cells are reached by exact cell name. Always inspect the cell before
assuming columns or presets:

```bash
flex core search --cell <name> "@orient"
```

Use the schema reported by `@orient` as truth. Common surfaces are:

- `chunks`: content units with `source_id`, `content`, `position`, and often
  `section_title`, `file_uuid`, and `content_hash`.
- `sources`: one row per indexed file, commonly with `source_id`, `title`, and
  `file_uuid`.
- `_edges_fs_identity(source_id, file_uuid)`: the stable bridge from a path to
  the same file in other Flex cells.
- `keyword('terms')`: FTS5 retrieval over indexed content.

`source_id` is the file path. Alias it to `path` within a query when that reads
better; do not assume a separate `path` column exists.

## Query recipes

Full-text search:

```sql
SELECT k.snippet, c.source_id, c.section_title
FROM keyword('retrieval scoring') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

List matching files:

```sql
SELECT DISTINCT source_id AS path, title
FROM sources
WHERE source_id LIKE '%/changes/code/%'
ORDER BY path
LIMIT 30;
```

Read one file in source order:

```sql
SELECT section_title, content
FROM chunks
WHERE source_id = '/absolute/path/to/file.md'
ORDER BY position;
```

Search within a subtree:

```sql
SELECT k.snippet, c.source_id AS path, c.section_title
FROM keyword('refresh failure') k
JOIN chunks c ON c.id = k.id
WHERE c.source_id LIKE '%/src/engine/%'
ORDER BY k.rank DESC
LIMIT 10;
```

Find duplicate content:

```sql
SELECT content_hash, count(*) AS copies,
       group_concat(DISTINCT source_id) AS paths
FROM chunks
WHERE content_hash IS NOT NULL
GROUP BY content_hash
HAVING count(*) > 1
ORDER BY copies DESC
LIMIT 15;
```

When `@orient` exposes a subtree preset or `_edges_tree`, prefer it for recursive
containment. A path-prefix filter is only a string-location filter.

## Bridge a file to session history

Filesystem cells describe what a file contains. Claude Code and Codex cells
describe who changed or referenced it and when. Join those worlds through the
machine-stable `file_uuid`, not by guessing from paths.

First retrieve the identity:

```sql
SELECT DISTINCT source_id, file_uuid
FROM sources
WHERE source_id LIKE '%/worker.py';
```

Then query the relevant session cell using the returned UUID and the schema shown
by that cell's `@orient`. Session schemas differ, so do not hardcode a join until
you inspect them. Use `flex:sessions` for the session-side workflow.

## Method

1. Run `@orient` and confirm the real schema and freshness state.
2. Use `COUNT`, `DISTINCT`, grouping, and path filters to establish shape.
3. Use `keyword()` for exact language; this surface is not semantic retrieval.
4. Read only the narrowed rows in source order.
5. Cross to session cells on `file_uuid` for authorship and history.

## Constraints

- A 0.52 filesystem cell is structural and has no embeddings. Never call
  `vec_ops()` or describe keyword matches as semantic results.
- Section-level fields depend on how the cell was compiled and may be absent or
  empty. Trust `@orient`.
- The filesystem cell does not establish authorship or edit time. Use session
  evidence joined by `file_uuid`.
- Use `DISTINCT` when a view can fan out one source across chunks or edges.
- Treat freshness reported by the cell and registry as data; do not infer it from
  a successful query.
