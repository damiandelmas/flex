# Flex Primitives

Public Flex retrieval is built from SQL plus two table functions: `vec_ops()` for semantic retrieval and `keyword()` for exact lexical retrieval. Start every unfamiliar cell with `@orient`, then inspect the schema before composing queries.

## Retrieval Pipeline

Every semantic query has three phases:

1. SQL pre-filter narrows candidate chunk IDs.
2. Vector scoring applies retrieval operations.
3. SQL compose joins, groups, filters, and paginates results.

Scores are ordinal within one query. Do not compare score magnitude across different token strings.

## vec_ops

```sql
SELECT v.id, v.score, c.content
FROM vec_ops(
  'similar:authentication middleware decisions diverse decay:14',
  'SELECT id FROM chunks WHERE type = ''file'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 10;
```

Use `vec_ops()` for fuzzy or conceptual retrieval. Put hard constraints in the second argument so the candidate pool is scoped before scoring. A `WHERE` clause after `vec_ops()` filters after selection and can starve sparse result sets.

Common public tokens:

| token | use |
| --- | --- |
| `similar:text` | embed text and rank nearby chunks |
| `diverse` | return a broader spread of related results |
| `suppress:text` | downweight an over-dominant theme |
| `decay:N` | favor newer chunks with an N-day half-life |
| `centroid` | anchor around the center of a result set |

## keyword

```sql
SELECT k.id, k.rank, k.snippet, c.content
FROM keyword(
  'WebSocketManager',
  'SELECT id FROM chunks WHERE file LIKE ''%.py'''
) k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

Use `keyword()` for exact strings: file names, symbols, error text, IDs, and quoted product terms. It uses the cell's FTS index and accepts an optional SQL pre-filter.

## SQL Shape First

Before semantic search, get the cheap shape:

```sql
PRAGMA table_info(chunks);

SELECT type, COUNT(*)
FROM chunks
GROUP BY type
ORDER BY COUNT(*) DESC;
```

Then narrow semantically:

```sql
SELECT v.id, v.score, c.file, substr(c.content, 1, 240) AS preview
FROM vec_ops(
  'similar:release checklist and publish boundary',
  'SELECT id FROM chunks WHERE type = ''file'' AND file LIKE ''%release%'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 12;
```

## File Retrieval

Use direct SQL when the path or filename is known:

```sql
SELECT file, section, ext, substr(content, 1, 240) AS preview
FROM chunks
WHERE type = 'file' AND file LIKE '%context/current/release%'
ORDER BY created_at DESC
LIMIT 20;
```

Use semantic retrieval when the wording is unknown:

```sql
SELECT v.id, v.score, c.file, c.section, substr(c.content, 1, 240) AS preview
FROM vec_ops(
  'similar:how the publish pipeline prevents leaks',
  'SELECT id FROM chunks WHERE type = ''file'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 12;
```
