---
name: flex:codegraph
description: Navigate a code repository as a queryable graph via a flex `codegraph` cell — symbols, containment (module ⊃ class ⊃ method), the call graph (callers/callees/impact), and the import graph. FTS5 + structural SQL + graph traversal, no embeddings. Use when asked to explore code structure, find a symbol's callers/callees, trace the blast-radius of a change, or map module dependencies.
allowed-tools:
  - Bash
user-invocable: true
---

# flex:codegraph

A flex **codegraph** cell is a repository compiled into one SQLite cell as a navigable
graph — a recursive node tree (`module ⊃ class ⊃ method`) with a **call graph** and an
**import graph** over it. Query it for code *navigation*: "who calls X", "what does X
call", "what breaks if I change X", "what imports this module", "show me this file's
structure". It is FTS5 + structural SQL + graph traversal — **no embeddings**, so it is
not for fuzzy/semantic recall.

`flex:codegraph` is the structural `filesystem` engine built with the call/import graph
turned on (`--code`), adding verbs the general filesystem surface lacks
(`@callers`/`@callees`/`@impact`/`@subtree`). It is a separate built surface, not a
scope — see **`flex:filesystem`** for the general folder/document query surface.

> No cell yet, or need to refresh one? Compile any repository into a codegraph cell:
> `flex init --module codegraph --path REPO` (name it with `--name`; refresh with
> `--regen NAME`). See `flex init --help` for all build flags.

## Endpoint

```bash
FLEX=$(command -v flex)     # or simply: FLEX=flex
$FLEX core search --cell <name> "<SQL | @preset>"
```

Codegraph cells are **unlisted** — reach them by exact `--name`. Every query is valid SQL
or an `@preset`. **Always start with `@orient`** for an un-oriented cell.

```bash
$FLEX core search --cell <name> "@orient"
```

## Naming

Build one codegraph cell per repository and name it with `--name`. Each cell carries
**Python (ast) and JS/TS (tree-sitter)** call graphs — in TS, `this.m()`/`super.m()`
resolve to the enclosing class's methods (that's where the OO graph lives). To (re)build,
point `flex init --module codegraph --path` at any repo.

## Query surface

- **`chunks`** (view) — one row per node. Key columns: `section_title` (the **symbol
  name**; `(module)` = file preamble) · `source_id` (file path) · `depth`
  (1=top-level, 2=method, …) · `container_id` (parent node id, or file path) ·
  `content` · `position`.
- **`_edges_call`** `(caller_id, callee_id, callee_name)` — the call graph
  (`callee_id` NULL = external/stdlib).
- **`_edges_import`** `(source_id, module, name)` — the import graph.
- **`_edges_tree`** `(id, parent_id, depth)` — containment.

## Presets — the navigation verbs

```text
@orient                         schema, presets, shape
@callers  symbol=NAME           who calls NAME
@callees  symbol=NAME           what NAME calls
@impact   symbol=NAME           transitive callers — blast radius of changing NAME
@subtree  root=ID_or_FILEPATH   recursive descendants (a file's tree, or a class's methods)
```

## How to query (structural & graph first — there is no `vec_ops`)

1. **Symbol → definition** (exact name): `keyword('NAME')` or `WHERE section_title = 'NAME'`.
2. **Navigate** (the graph): `@callers` / `@callees` / `@impact` / `@subtree`, or raw `_edges_*` SQL.
3. **Structure / hubs** (counts): `GROUP BY` over `chunks` / `_edges_call` / `_edges_import` — free.
4. **Full-text** (a literal, error, identifier): `keyword('term')` over `content`.

### Find a symbol's definition
```sql
SELECT source_id, depth, substr(content,1,400) AS body
FROM chunks WHERE section_title = 'resolve_cell';
```

### Walk the call graph
```bash
$FLEX core search --cell <name> "@callers symbol=open_cell"
$FLEX core search --cell <name> "@impact symbol=regenerate_views"     # blast radius
```

### A file or class as a tree
```bash
$FLEX core search --cell <name> "@subtree root=/abs/path/to/flex/core.py"
```
```sql
-- a class's methods (DISTINCT — the chunks view can fan out)
SELECT DISTINCT t.section_title FROM _types_instant t
WHERE t.container_id = (SELECT chunk_id FROM _types_instant WHERE section_title='VectorCache')
ORDER BY t.position;
```

### Dependencies
```sql
-- what imports a module
SELECT DISTINCT source_id, name FROM _edges_import WHERE module = 'flex.core';
-- dependency hubs
SELECT module, count(DISTINCT source_id) AS importers
FROM _edges_import WHERE module NOT LIKE '.%'
GROUP BY module ORDER BY importers DESC LIMIT 15;
```

## Recipes

**Hot internal symbols** (most-called):
```sql
SELECT callee_name, count(DISTINCT caller_id) AS callers
FROM _edges_call WHERE callee_id IS NOT NULL
GROUP BY callee_name ORDER BY callers DESC LIMIT 15;
```

**Blast radius before a change** — `@impact symbol=X`, then read each affected def with `WHERE section_title IN (...)`.

**Entry points / orphans** (top-level defs nothing calls internally):
```sql
SELECT DISTINCT t.section_title, es.source_id
FROM _types_instant t JOIN _edges_source es ON t.chunk_id = es.chunk_id
WHERE t.depth = 1 AND t.section_title NOT LIKE '(%'
  AND t.chunk_id NOT IN (SELECT callee_id FROM _edges_call WHERE callee_id IS NOT NULL)
LIMIT 20;
```

**Symbol then its subtree** — `WHERE section_title='X'` for its `chunk_id`/file, then `@subtree root=<chunk_id>`.

## Methodology

- **Structural first.** `GROUP BY`/`COUNT(DISTINCT …)` over the edges is free — get the shape before reading bodies.
- **Graph for navigation, FTS for strings.** Presets/`_edges_*` to walk relationships; `keyword()` for an exact identifier, literal, or error.
- **Resolve then read.** Cheap query to a `section_title`/`chunk_id`, then pull the body or `@subtree`.
- **No semantic search.** No `vec_ops` on a codegraph cell. A fuzzy "where do we handle auth-ish things" needs a Vector cell — see `flex:filesystem --embed`.

## Limits (qualify claims against these)

- **Graph: Python (`ast`) + JS/TS (`tree-sitter`)** — `.py` and `.ts/.tsx/.js/.jsx` get call + import edges and the `class ⊃ method` tree; other languages chunk flat, markdown nests by heading.
- **Call resolution** — Python: bare `foo()` only. JS/TS: bare `foo()` **plus** `this.m()`/`super.m()` (resolved to the enclosing class — where the OO graph lives). In both, other member calls (`obj.m()`) are not edges — name-only resolution can't disambiguate them.
- **Name collisions / external** — same-named defs or stdlib calls leave `callee_id` NULL (kept by `callee_name`).
- **`chunks`-view fan-out** — `DISTINCT` when listing; the `@callers`/`@callees`/`@impact` presets join `_types_instant` (1:1) and are clean.

Lead with `@orient`. Prefer presets and shape queries before reading bodies. Say what the graph does and does not cover rather than overclaiming.
