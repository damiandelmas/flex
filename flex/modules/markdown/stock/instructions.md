# Markdown / Obsidian Vault Cell Instructions

This cell indexes a markdown or Obsidian vault. Each source is a `.md` file;
each chunk is a heading-delimited section (or the full note when no headings
exist). The cell is self-describing.

First call:

```text
cell="<vault_name>" query="@orient"
```

`@orient` returns the schema, presets, examples, coverage notes, and graph
entry points. Every Flex query must be valid SQL or a preset. Plain text is
not accepted; wrap it in `keyword()` or `vec_ops()`.

Vault cells carry the `markdown` or `obsidian` cell type depending on whether
`.obsidian/` was present at compile time. `compile_vault()` walks the
directory, parses frontmatter, chunks by heading, resolves wikilinks, embeds,
and registers the cell.

## Core Surfaces

`sections` is the chunk-level retrieval surface. Each row is one heading
section (or preamble/full note). Columns: `id`, `content`, `source_id`,
`note_title`, `section_title`, `heading_depth`, `heading_chain`, `word_count`,
`char_start`, `char_end`, `folder`, `tags`, `centrality`, `community_id`,
`is_hub`.

`notes` is the source-level metadata surface. One row per file. Columns:
`source_id`, `title`, `folder`, `tags`, `aliases`, `file_modified`,
`note_created`, `outgoing_links`, `backlinks`, `unresolved_links`,
`centrality`, `community_id`, `is_hub`, `hub_type`, `is_bridge`.

`_edges_wikilink` and `_edges_wikilink_unresolved` are the resolved and
unresolved wikilink edge tables. Columns: `from_path`, `to_path` (resolved) or
`raw_target` (unresolved).

`_fields_inline` holds Dataview inline fields. Columns: `chunk_id`,
`source_id`, `field_key`, `field_value`.

## Choosing Search Mode

Structural first. Use `GROUP BY`, `COUNT(*)`, and `DISTINCT` to understand
vault shape before paying for embeddings.

```sql
SELECT folder, COUNT(*) AS note_count
FROM notes
GROUP BY folder
ORDER BY note_count DESC;
```

Use `keyword()` for exact terms, tag values, field keys, wikilink targets, and
quoted phrases.

```sql
SELECT k.id, k.rank, k.snippet, s.note_title, s.section_title
FROM keyword('"weekly review"', 'SELECT id FROM sections') k
JOIN sections s ON s.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

Use `vec_ops()` for conceptual or fuzzy search. Push folder, tag, or type
constraints into the pre-filter to avoid pool starvation.

```sql
SELECT v.score, s.note_title, s.section_title, substr(s.content, 1, 600) AS body
FROM vec_ops(
  'similar:notes about project planning and prioritization diverse',
  'SELECT id FROM sections WHERE folder LIKE ''projects/%'''
) v
JOIN sections s ON s.id = v.id
ORDER BY v.score DESC
LIMIT 12;
```

Pre-filter a tag:

```sql
SELECT v.score, s.note_title, substr(s.content, 1, 500) AS body
FROM vec_ops(
  'similar:meeting notes and action items decay:14',
  'SELECT id FROM sections WHERE tags LIKE ''%meeting%'''
) v
JOIN sections s ON s.id = v.id
ORDER BY v.score DESC
LIMIT 10;
```

## Section and Note Queries

Find all sections under a heading:

```sql
SELECT note_title, section_title, heading_depth, heading_chain, word_count
FROM sections
WHERE note_title = 'Architecture'
ORDER BY char_start;
```

Read a note's full text in order:

```sql
SELECT section_title, heading_depth, substr(content, 1, 800) AS body
FROM sections
WHERE note_title = 'Architecture'
ORDER BY char_start;
```

Find notes tagged with a value:

```sql
SELECT title, folder, tags, note_created
FROM notes
WHERE tags LIKE '%area/work%'
ORDER BY file_modified DESC
LIMIT 20;
```

## Wikilink Graph

Notes that link to a target:

```sql
SELECT from_path, COUNT(*) AS links
FROM _edges_wikilink
WHERE to_path = 'Architecture.md'
GROUP BY from_path
ORDER BY links DESC;
```

Notes with the most outgoing links:

```sql
SELECT title, outgoing_links, backlinks, centrality
FROM notes
ORDER BY outgoing_links DESC
LIMIT 20;
```

## Dataview Fields

Find notes with a specific field:

```sql
SELECT DISTINCT n.title, f.field_value
FROM _fields_inline f
JOIN notes n ON n.source_id = f.source_id
WHERE f.field_key = 'status' AND f.field_value = 'active'
ORDER BY n.title;
```

All distinct values for a field:

```sql
SELECT field_value, COUNT(*) AS n
FROM _fields_inline
WHERE field_key = 'project'
GROUP BY field_value
ORDER BY n DESC;
```

## Graph and Hubs

Hub notes with most backlinks:

```sql
SELECT title, folder, backlinks, centrality, hub_type
FROM notes
WHERE is_hub = 1
ORDER BY centrality DESC
LIMIT 10;
```

Semantic search restricted to hub notes:

```sql
SELECT v.score, n.title, n.folder, n.centrality
FROM vec_ops('similar:core concepts and key decisions diverse') v
JOIN sections s ON s.id = v.id
JOIN notes n ON n.source_id = s.source_id
WHERE n.is_hub = 1
ORDER BY v.score DESC
LIMIT 8;
```

## Presets

Use presets before writing long SQL. `@orient` discovers installed presets.

- `@orient` — schema, views, graph entry points, samples
- `@hubs` — notes with high centrality ranked by hub type
- `@orphans` — notes with no resolved incoming or outgoing wikilinks
- `@ghost-notes` — unresolved wikilink targets ranked by demand (notes to write)
- `@communities` — graph communities with member counts and hub notes

If a vault is small or unembedded, graph presets may be sparse. The `notes`,
`sections`, `_edges_wikilink`, and `_fields_inline` surfaces still answer all
structural questions.

## Methodology

Start with `@orient`. Structural before semantic — shape queries are free.
Discover then narrow: broad `vec_ops` finds themes, then pre-filter the next
query with those themes. Push constraints into the pre-filter argument, not a
sparse `WHERE` after the result. `heading_chain` records the full ancestry
path (`Architecture > Backend > Database`); use it for subsection scoping.

## Reporting

Include per result: note title, section title, folder, score or rank, and a
content excerpt. When evidence is partial, say so and name the next query.
