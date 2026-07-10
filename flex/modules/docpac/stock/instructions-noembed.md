# Docpac Cell Instructions (embed-off / structural)

**This is an embed-off cell — it has no vectors.** `vec_ops()` / `similar:` /
semantic scoring are **INERT** here (the `@orient` `embed_mode` block says so), and
the similarity graph (`centrality`, `is_hub`, `community_id`) is not built. Retrieval
is **structural**: `keyword()` full-text search, structural SQL over typed columns,
and the section node tree. Everything below uses only that surface.

This cell indexes Markdown context documents as document and section rows. Each
source is one `.md` file; each row in `sections` is one parsed heading block.

Start here, not in a skill:

```text
cell="your-context" query="@orient"
```

`@orient` returns the schema, views, presets, doc types, section titles, the
node-tree surface, and runnable examples. Treat it as the live manual for this
cell. Its `embed_mode` line confirms this cell is structural-only.

## What This Cell Is For

Use it when the question is about written project context:

- changelogs, plans, specs, slots, design docs, philosophy, architecture, testing
- which decisions were made, what changed, what is planned, what the rationale was
- **keyword and structural** search over a project's written workstream at any granularity
- recovering full document bodies from clipped or partial search results

Each doc has a `doc_type` (e.g. `changelog`, `plan`, `spec`, `design`,
`architecture`, `philosophy`, `onboard`, `testing`) and a `temporal` axis
(`past`, `future`, `present`, `exogenous`). Use both to scope searches.

## Core Surfaces

`sections` is the primary reading surface. One row per heading block. Read
`content` for the section text. Order by `file_date` for recency, use
`keyword()` for terms, or filter by `section_title` or `doc_type` for
structural queries. (The `centrality`/`is_hub`/`community_id` columns exist but
are NULL on this cell — the graph is embed-derived and not built.)

`documents` is the source-level navigation surface. One row per source file.
Use it for corpus shape and document-level recency.

The **node tree** (`_edges_tree`: `id`, `parent_id`, `depth`) is the structural
navigation surface — section containment you can walk with a recursive CTE or
filter by `depth`.

## First Move

Call `@orient` once per task. Use the `doc_types` and `section_titles` blocks to
choose scope before querying. Every Flex query must be SQL or a preset — plain
English is not a query.

## Choosing Search Mode (structural only)

Use structural SQL first when you know doc types, section titles, dates, or ids.

```sql
SELECT source_id, title, source_path, file_date, doc_type, section_title,
       substr(content, 1, 1200) AS content
FROM sections
WHERE doc_type = 'changelog'
ORDER BY file_date DESC
LIMIT 10;
```

Use `keyword()` for concepts, exact terms, paths, function names, error strings,
and quoted phrases — this is the primary search here (there is no `vec_ops`).
Put scope constraints in the second argument so the candidate pool is narrowed
before BM25 ranking. Multi-word phrases must be quoted inside the term string.

```sql
SELECT k.rank, k.snippet, s.source_id, s.title, s.source_path, s.section_title,
       substr(s.content, 1, 1000) AS content
FROM keyword('"content hash"', 'SELECT id FROM sections WHERE doc_type = ''changelog''') k
JOIN sections s ON s.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

## Recent Mode

Query `sections` by `section_title` or `doc_type`, ordered by `file_date`.

```sql
SELECT source_id, title, source_path, file_date, doc_type,
       section_title, substr(content, 1, 1200) AS content
FROM sections
WHERE doc_type = 'plan' AND temporal = 'future'
ORDER BY file_date DESC
LIMIT 10;
```

## Node-Tree Navigation

Walk a document's section containment with the tree instead of a similarity graph.

```sql
SELECT t.depth, s.section_title, s.position, substr(s.content, 1, 800) AS content
FROM _edges_tree t
JOIN sections s ON s.id = t.id
WHERE t.id LIKE '<source_id>%'
ORDER BY t.depth, s.position
LIMIT 40;
```

## Drilldown

Once you have a `source_id`, retrieve the full document in section order.

```sql
SELECT section_title, position, substr(content, 1, 2200) AS content
FROM sections
WHERE source_id = '<source_id>'
ORDER BY position
LIMIT 30;
```

To filter to one file path when you do not yet have a `source_id`:

```sql
SELECT source_id, title, source_path, file_date, doc_type, chunk_count
FROM documents
WHERE source_path LIKE '%content-hash%'
ORDER BY file_date DESC
LIMIT 5;
```

## Preset Bias

Prefer presets when they fit:

- `@orient` for the live schema, doc types, section titles, node tree, and examples
- `@health` for pipeline health: chunk/source counts, recent ops

(`@genealogy`/`@bridges` and other graph presets are inert on this cell — no
similarity graph.)

## Reporting Results

For each result include:

- cell name
- `source_id` (enables stable drilldown)
- `file_date` if present
- `title` and `source_path`
- `doc_type` and `section_title`
- `keyword()` rank when used
- a compact content excerpt unless a full document was requested

When a result looks clipped, drilldown by `source_id` before concluding the
content is short.
