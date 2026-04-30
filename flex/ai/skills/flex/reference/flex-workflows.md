# Flex Workflows

These workflows describe stable public retrieval patterns. They assume the cell is already installed and available through the Flex endpoint.

## Answer A Question From A Cell

1. Run `@orient`.
2. Inspect `PRAGMA table_info(chunks)`.
3. Use `keyword()` for exact terms or `vec_ops()` for conceptual search.
4. Read the retrieved rows.
5. Answer with evidence and note uncertainty.

Example:

```sql
SELECT v.id, v.score, c.file, substr(c.content, 1, 320) AS preview
FROM vec_ops(
  'similar:how install hooks register module assets',
  'SELECT id FROM chunks WHERE type = ''file'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 12;
```

## Trace A File Or Decision

Start exact:

```sql
SELECT k.id, k.rank, k.snippet, c.file, c.content
FROM keyword('install_presets.py') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 20;
```

Then pivot to semantic context:

```sql
SELECT v.id, v.score, c.file, substr(c.content, 1, 320) AS preview
FROM vec_ops(
  'similar:why install presets changed',
  'SELECT id FROM chunks WHERE file LIKE ''%install_presets.py%'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 10;
```

## Build A Workstream Digest

Use time and type constraints first:

```sql
SELECT type, COUNT(*)
FROM chunks
WHERE created_at >= datetime('now', '-14 days')
GROUP BY type
ORDER BY COUNT(*) DESC;
```

Then retrieve broad evidence:

```sql
SELECT v.id, v.score, c.created_at, substr(c.content, 1, 360) AS preview
FROM vec_ops(
  'similar:recent work completed blockers next steps diverse decay:14',
  'SELECT id FROM chunks WHERE created_at >= datetime(''now'', ''-14 days'')'
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 20;
```

## Find Release Risks

Search exact terms first:

```sql
SELECT k.id, k.rank, k.snippet, c.file
FROM keyword('TODO release boundary leak') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 20;
```

Then search conceptually:

```sql
SELECT v.id, v.score, c.file, substr(c.content, 1, 320) AS preview
FROM vec_ops('similar:public release risks leaks packaging blockers diverse') v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 20;
```

## Query Discipline

- Start with the cell shape.
- Prefer direct SQL for known paths and structured constraints.
- Prefer `keyword()` for exact strings.
- Prefer `vec_ops()` for concepts and fuzzy language.
- Put hard constraints in the pre-filter.
- Use `diverse`, `suppress:text`, and `decay:N` to adjust retrieval intent without changing the underlying SQL contract.
