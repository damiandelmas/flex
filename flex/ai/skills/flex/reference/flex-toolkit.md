# Flex Toolkit

Use this as a public query cookbook for stable Flex cells. Start with `@orient`, inspect available columns, then choose the lightest retrieval mode that answers the question.

## Orientation

```sql
@orient
```

Follow with:

```sql
PRAGMA table_info(chunks);
SELECT type, COUNT(*) FROM chunks GROUP BY type ORDER BY COUNT(*) DESC;
```

## Exact Lookup

```sql
SELECT k.id, k.rank, k.snippet, c.file, c.content
FROM keyword('publish boundary') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

Use for names, paths, error strings, IDs, and other literals.

## Scoped Semantic Search

```sql
SELECT v.id, v.score, c.file, substr(c.content, 1, 260) AS preview
FROM vec_ops(
  'similar:release readiness and public packaging',
  'SELECT id FROM chunks WHERE type = ''file'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 12;
```

Put scope in the pre-filter. Use the outer SQL query for display, joins, grouping, and final sorting.

## Breadth Search

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:main decisions from this workstream diverse') v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 12;
```

Use `diverse` when repeated near-duplicates are less useful than coverage.

## Contrastive Search

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:deployment blocker suppress:successful deploy') v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 10;
```

Use `suppress:text` to downweight an obvious dominant theme and surface adjacent evidence.

## Freshness Search

```sql
SELECT v.id, v.score, c.created_at, c.content
FROM vec_ops('similar:current open release work decay:7') v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 10;
```

Use `decay:N` when the question is time-sensitive. Column names differ by cell, so confirm date fields with `PRAGMA table_info(chunks)`.

## Known Path Workflow

```sql
SELECT file, section, substr(content, 1, 300) AS preview
FROM chunks
WHERE type = 'file' AND file LIKE '%context/current%'
ORDER BY file
LIMIT 50;
```

Known path beats semantic search. Use SQL first, then semantic retrieval inside the scoped set if needed.

## Synthesis Pattern

1. Orient with `@orient`.
2. Inspect schema and counts.
3. Run one exact or scoped semantic query.
4. Read enough returned content to verify claims.
5. Pivot with SQL filters or a narrower query.
6. Qualify anything not directly supported by retrieved evidence.
