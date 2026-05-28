# Markdown Cell Instructions

Start with `@orient` for the target cell:

```text
cell="my_vault" query="@orient"
```

Use `sections` for chunk-level reading and semantic search. Use `notes` for
file-level metadata, folder scoping, tags, aliases, link counts, hub state, and
bridge state.

## Normal Moves

Find relevant sections:

```sql
SELECT v.score, s.note_title, s.section_title, s.content
FROM vec_ops('similar:architecture decisions') v
JOIN sections s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 10;
```

Scope semantic search to a folder or tag:

```sql
SELECT v.score, s.note_title, s.content
FROM vec_ops('similar:project planning',
             'SELECT id FROM sections WHERE folder LIKE ''projects/%'' OR tags LIKE ''%project%''') v
JOIN sections s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 10;
```

Read backlinks:

```sql
SELECT from_path, COUNT(*) AS links
FROM _edges_wikilink
WHERE to_path = 'Architecture.md'
GROUP BY from_path
ORDER BY links DESC;
```

Read Dataview fields:

```sql
SELECT DISTINCT n.title
FROM _fields_inline f
JOIN notes n ON n.source_id = f.source_id
WHERE f.field_key = 'status' AND f.field_value = 'active';
```

## Presets

Use module presets before writing long SQL:

- `@hubs`: notes with high graph centrality.
- `@orphans`: notes with no resolved incoming or outgoing wikilinks.
- `@ghost-notes`: unresolved wikilink targets ranked by references.
- `@communities`: graph groups when graph enrichment is available.

If a vault is tiny or unembedded, graph-heavy presets may be sparse. The
`notes`, `sections`, `_edges_wikilink`, `_edges_wikilink_unresolved`, and
`_fields_inline` surfaces still answer structural questions.
