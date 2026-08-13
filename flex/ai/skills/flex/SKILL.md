---
name: flex
description: Query any named self-describing Flex cell through its live SQL contract. Use when the user asks to search, inspect, traverse, compare, or recover knowledge from conversations, documents, code, context, wiki, Ledger, or another Flex cell.
allowed-tools:
  - mcp__flex__flex
user-invocable: true
argument-hint: "cell and request, e.g. 'ledger @orient', 'products about training', 'codex session 123'"
---

# Flex

Flex is a queryable world of registered knowledge cells. A cell is a
self-describing SQLite database that retains its own objects, schema, authority,
and lifecycle. Cells may expose captured source material, compiled navigation,
authored annotations, relationships, full-text retrieval, semantic retrieval, or
some subset of those capabilities.

Use this skill for the general case: query the named cell as it actually is.
Do not force a routing taxonomy, assume a universal schema, or flatten different
providers into one truth model. Narrower skills can add domain methodology when a
task clearly needs it; this remains the arbitrary-cell doorway.

The normal transport is `mcp__flex__flex` with:

- `cell`: the registered cell name;
- `query`: ordinary SQL or a cell-defined `@preset`.

## Start with the live contract

Run `@orient` before the first substantive query against a cell in a turn:

```text
cell=<name>
query=@orient
```

Orientation is the contract. It explains the live representation, scope,
capabilities, lifecycle/freshness, tables and views, retrieval functions, saved
queries, source-recovery route, and any cell-specific cautions. Do not guess a
table, column, preset, `keyword()`, `vec_ops()`, graph relation, or write surface
that `@orient` has not established.

`@orient` describes a cell; it is not a content dump. If a cell offers `@index`,
use it for its compact navigational map. Use SQL, FTS, or vectors to retrieve and
test content.

## Saved SQL

Reusable SQL is stored in the selected database's `_presets` relation. Files
may seed defaults while compiling a cell, but runtime discovery and editing are
ordinary SQL against the database—not skill frontmatter, filesystem scanning,
or precedence rules. The existing `@name` spelling remains a compatibility
invocation while Flex moves toward a single SQL execution surface.

## Choose the smallest honest query mode

1. **Known identity, path, date, type, or relation**: use ordinary SQL.
2. **Known literal**—an identifier, filename, error, UUID, name, or phrase: use
   `keyword()` if the cell advertises it.
3. **Conceptual or fuzzy question**: use `vec_ops()` only if the cell advertises
   semantic retrieval.
4. **Need both**: compose them with SQL.

Start structural, then retrieve meaning, then open the exact source object before
making a consequential claim. A query may combine those stages, but it should not
hide which evidence established the result.

## SQL first

Structural queries are inexpensive and establish the shape of the question. The
actual relation names are cell-specific; these are patterns only when `@orient`
advertises equivalent fields:

```sql
SELECT type, COUNT(*) AS n
FROM chunks
GROUP BY type
ORDER BY n DESC;
```

```sql
SELECT created_at, id, substr(content, 1, 500) AS preview
FROM chunks
WHERE created_at >= :since
ORDER BY created_at DESC
LIMIT 25;
```

Use explicit IDs, source addresses, provider identity, and ordering columns when
they exist. An empty result is an honest result; inspect the cell's scope and
lifecycle before treating it as evidence of absence.

## Exact retrieval with FTS

If `@orient` advertises `keyword()`, it is a table source that can be joined to
the cell's native objects. Use it for exact language and compose all known scope
constraints in SQL:

```sql
SELECT k.id, k.rank, k.snippet, c.content
FROM keyword(:terms) AS k
JOIN chunks AS c ON c.id = k.id
WHERE c.type = :type
ORDER BY k.rank
LIMIT 20;
```

Follow the cell's documented rank direction and query syntax; implementations may
differ. FTS-visible text and structured metadata should be available as soon as a
cell publishes them. Do not wait for embeddings when exact or structural evidence
answers the question.

## Semantic retrieval when available

`vec_ops()` is also a table source. It ranks objects in the shared embedding
space, but only where the cell advertises vectors. Scores are meaningful within
one query, not as absolute measurements or cross-query facts.

Push known constraints into its candidate-selection SQL rather than filtering a
small ranked result afterward:

```sql
SELECT v.id, v.score, c.content
FROM vec_ops(
  'similar:how the deployment design changed and why',
  'SELECT id FROM chunks WHERE created_at >= date(''now'', ''-30 days'')'
) AS v
JOIN chunks AS c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 15;
```

Use a natural-language query that states the question. Use any modulation tokens
only when that cell's `@orient` documents them. Vectors may converge after a
cell's text, metadata, FTS, and relations have already become queryable; report
that freshness boundary rather than silently substituting an older result.

## Compose retrieval modes

SQL composes the results; it is not a separate feature layer. For example, where
the cell supports both functions:

```sql
SELECT k.id, k.rank, v.score, c.content
FROM keyword(:exact_terms) AS k
JOIN vec_ops('similar:the surrounding architectural rationale') AS v
  ON v.id = k.id
JOIN chunks AS c ON c.id = k.id
ORDER BY v.score DESC
LIMIT 20;
```

If the intersection is empty, that says the two retrieval criteria did not
overlap. It does not prove either retrieval surface is broken.

## Compose cells with Flex Meta

When one question genuinely spans cells, use registered, query-local composition
rather than copying material into a new database. Attach by registered cell name,
not filesystem path, and keep every source's identity visible:

```sql
ATTACH 'ledger' AS ledger;

SELECT a.note, a.weight, m.position, m.content
FROM annotations AS a
JOIN main.messages AS m ON m.id = a.target_chunk_id
WHERE a.target_cell_id = :main_cell_id
ORDER BY m.position;
```

The primary cell remains `main`; attached cells are read-only and exist only for
the query. Their schemas, authority, and retrieval capabilities remain separate.
Use the live Meta/cell contract for attachment availability and aliases.

## Navigation, recovery, and proof

- `@index` is a cell-specific navigational projection when offered. It should
  return a map, not replace exact evidence.
- Follow documented native recovery routes—such as `@full id=...`, source IDs,
  file UUIDs, or cell-defined drill-down presets—to inspect the canonical object.
- State the cell/provider used and retain stable addresses in consequential
  answers.
- Preserve disagreements between providers instead of smoothing them into a
  synthetic consensus.

## Lifecycle and authority

Querying is not compilation. Do not manually rebuild a cell merely because a
result is empty or vectors are pending; first inspect its advertised scope,
freshness, and last-good state. For a requested mutation, use only the target
cell's live, documented SQL mutation contract and the authority the user granted.
Do not write raw projection tables or invent a generic write API.

## Working method

1. Identify the source cell that owns the object or claim.
2. `@orient` that cell.
3. Use structural SQL to establish scope.
4. Use `keyword()` for literals or `vec_ops()` for meaning only when advertised.
5. Join, group, compare, or attach cells through ordinary SQL as needed.
6. Recover the canonical source before asserting a decision, release, count,
   identity, or other consequential fact.
7. Report observations crisply; qualify interpretations and hypotheses.

Every Flex answer should leave a reader able to see what was queried, which cell
owned the evidence, and how to reopen the source object.
