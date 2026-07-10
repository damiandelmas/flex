# Docpac Cell Instructions

This cell indexes Markdown context documents as document and section rows. Each
source is one `.md` file; each row in `sections` is one parsed heading block.

Start here, not in a skill:

```text
cell="your-context" query="@orient"
```

`@orient` returns the schema, views, presets, doc types, section titles, hubs,
graph communities, and runnable examples. Treat it as the live manual for this
cell.

## What This Cell Is For

Use a docpac cell when the question is about written project context:

- changelogs, plans, specs, slots, design docs, philosophy, architecture, testing
- which decisions were made, what changed, what is planned, what the rationale was
- semantic search over a project's written workstream at any granularity
- recovering full document bodies from clipped or partial search results

Each doc has a `doc_type` (e.g. `changelog`, `plan`, `spec`, `slot`, `design`,
`architecture`, `philosophy`, `onboard`, `testing`) and a `temporal` axis
(`past`, `future`, `present`, `exogenous`). Use both to scope searches before
scoring.

## Core Surfaces

`sections` is the primary reading surface. One row per heading block. Read
`content` for the section text. Order by `file_date` for recency, join
`vec_ops()` for semantic scoring, or filter by `section_title` or `doc_type`
for structural queries.

`documents` is the source-level navigation surface. One row per source file.
Use it for hub discovery, corpus shape, community navigation, and document-level
recency without fetching section bodies.

`chunks` is a richer compatibility surface with extra columns
(`semantic_role`, `yaml_type`, `status`, `keywords`, `confidence`). Use it when
you need those extra fields; for most context reads `sections` is sufficient.

## First Move

Call `@orient` once per task for the target cell. Use the `doc_types` and
`section_titles` blocks to choose scope before querying.

Every Flex query must be SQL or a preset. Plain English is not a query.

## Choosing Search Mode

Use structural SQL first when you know doc types, section titles, dates, or
source ids.

```sql
SELECT source_id, title, source_path, file_date, doc_type, section_title,
       substr(content, 1, 1200) AS content
FROM sections
WHERE doc_type = 'changelog'
ORDER BY file_date DESC
LIMIT 10;
```

Use `keyword()` for exact terms, paths, function names, error strings, and
quoted phrases. Multi-word phrases must be quoted inside the term string.

```sql
SELECT k.rank, k.snippet, s.source_id, s.title, s.source_path, s.section_title,
       substr(s.content, 1, 1000) AS content
FROM keyword('"content_hash"', 'SELECT id FROM sections') k
JOIN sections s ON s.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

Use `vec_ops()` for conceptual search. Put scope constraints in the second
argument so the candidate pool is narrowed before scoring; do not apply a
sparse post-filter on a global vector result.

```sql
SELECT v.score, s.source_id, s.title, s.source_path, s.file_date,
       s.doc_type, s.section_title, substr(s.content, 1, 1200) AS content
FROM vec_ops(
  'similar:two-tier change detection file watcher diverse pool:100',
  'SELECT id FROM sections WHERE doc_type = ''changelog'''
) v
JOIN sections s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 10;
```

## Recent Mode

Query `sections` by `section_title` or `doc_type`, ordered by `file_date`.

```sql
SELECT source_id, title, source_path, file_date, doc_type,
       section_title, substr(content, 1, 1200) AS content
FROM sections
WHERE section_title = 'Overview'
ORDER BY file_date DESC
LIMIT 10;
```

To get the most recent docs of a particular type:

```sql
SELECT source_id, title, source_path, file_date, doc_type,
       section_title, substr(content, 1, 1200) AS content
FROM sections
WHERE doc_type = 'plan'
  AND temporal = 'future'
ORDER BY file_date DESC
LIMIT 10;
```

## Relevant Mode

Use `vec_ops()` with a pre-filter to restrict the candidate pool before scoring.
Combine `section_title` and `doc_type` constraints for precision.

```sql
SELECT v.score, s.source_id, s.title, s.source_path, s.file_date,
       s.doc_type, s.section_title, substr(s.content, 1, 1200) AS content
FROM vec_ops(
  'similar:embedding skip content hash storm diverse pool:100',
  'SELECT id FROM sections WHERE section_title = ''Decisions'''
) v
JOIN sections s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 8;
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

## Hub and Graph Navigation

Hubs are the highest-centrality documents in the similarity graph. Use them to
find anchor documents for a topic area.

```sql
SELECT source_id, title, source_path, file_date, doc_type, centrality
FROM documents
WHERE is_hub = 1
ORDER BY centrality DESC
LIMIT 10;
```

Use `community_id` to restrict a search to a conceptual cluster discovered
during graph building.

```sql
SELECT v.score, s.source_id, s.title, s.section_title,
       substr(s.content, 1, 1000) AS content
FROM vec_ops(
  'similar:sql-first exploration diverse pool:80',
  'SELECT id FROM sections WHERE community_id = 124'
) v
JOIN sections s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 10;
```

## Preset Bias

Prefer presets when they fit:

- `@orient` for the live schema, doc types, section titles, hubs, and examples
- `@genealogy concept=...` to trace a concept's lineage across hubs and timeline
- `@health` for pipeline health: chunk/source counts, embedding coverage, graph freshness
- `@bridges` for cross-community connector documents

Use raw SQL when a preset is too broad or when you need a precise pre-filter
before semantic scoring.

## Reporting Results

For each result include:

- cell name
- `source_id` (enables stable drilldown)
- `file_date` if present
- `title` and `source_path`
- `doc_type` and `section_title`
- score when using `vec_ops()` or `keyword()`
- a compact content excerpt unless a full document was requested

When a result looks clipped, drilldown by `source_id` before concluding the
content is short.
