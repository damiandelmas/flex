---
name: flex:filesystem
description: Query (and build) a flex `filesystem` cell over any folder tree — two modes. Instant (--no-embed, the default): FTS5 + structural SQL, a recursive node tree, and the SOMA `file_uuid` hinge joining a file to "every coding-agent session that touched it", no embeddings. Vector (--embed): semantic embeddings over the same folders for fuzzy/meaning search. Use to find a file or doc, full-text- or semantic-search a folder, see what sessions touched a file, or navigate a corpus by path/section. A code repository's call graph has its own skill — flex:codegraph.
allowed-tools:
  - Bash
user-invocable: true
---

# flex:filesystem

`flex:filesystem` points flex at any folders and gives you one queryable cell. It has
**two modes**, chosen at build time:

- **Instant** (`--no-embed`, the **default**) — a **no-embed** structural cell: FTS5 +
  structural SQL, a recursive node tree, an optional call/import graph, and the SOMA
  `file_uuid` hinge. Free, fast, watched — no semantic recall.
- **Vector** (`--embed`) — a **semantic** cell: embeddings over the same folder tree,
  for fuzzy "docs about X-ish" meaning search. Auto-detects an Obsidian vault.

This skill is the general query surface for both. A code repository's symbol/call/import
graph has its own specialized skill — **`flex:codegraph`** (same Instant substrate,
code-specific verbs).

Cells are named per corpus (e.g. `myrepo-fs` for a `~/projects/myrepo` tree). In Instant
mode each file carries its machine-global **SOMA `file_uuid`**, so a file here joins to
the coding-agent session cells (`claude_code`/`codex`) — "what the file says" next to
"who touched it".

Use `flex:filesystem` to: find a file or doc, full-text- or semantic-search a folder
tree, list/navigate files by path or section, see **which sessions touched a file**, or
spot duplicate content. For a *code repository's* call/import graph, use
**`flex:codegraph`**.

> No cell yet, or need to grow/refresh one?
> - Instant (structural, default): `flex init --module filesystem --path FOLDER`
>   (grow with more `--path`; refresh with `--regen NAME`).
> - Vector (semantic): `flex init --module filesystem --embed` (auto-detects an
>   Obsidian vault; pass `--vault PATH` for a specific folder).
> - See `flex init --help` for all build flags.
> - Path-prefix scopes → `## Scopes` below.

## Endpoint

```bash
FLEX=$(command -v flex)     # or simply: FLEX=flex
$FLEX core search --cell <name> "<SQL | @preset>"
```

Cells are **unlisted** — reach them by exact `--name`. Every query is
SQL or an `@preset`. **Always start with `@orient`.**

```bash
$FLEX core search --cell <name> "@orient"
```

`@orient` reports the cell's real shape and which of the two modes it was built in — the
query surface below is written for an **Instant** cell (structural + FTS, no `vec_ops`).
A **Vector** cell adds semantic scoring; its `@orient` documents the `vec_ops()` tokens.

## Query surface (Instant mode)

- **`chunks`** (view) — one row per chunk/section. Key columns: `content` ·
  `source_id` (the **file path**) · `section_title` (markdown heading, when built with
  `--nest`) · `file_uuid` (SOMA) · `content_hash` · `position` · `depth`.
- **`sources`** (view) — one row per file: `source_id` (path) · `title` · `file_uuid`.
- **`_edges_fs_identity`** `(source_id, file_uuid)` — the SOMA bridge (machine-global).
- `keyword('term')` — FTS5 over content. (Instant has no `vec_ops`; a Vector cell does.)

## How to query (FTS + structural — the Instant default)

### Full-text search a folder
```sql
SELECT k.snippet, c.source_id, c.section_title
FROM keyword('retrieval scoring') k JOIN chunks c ON k.id = c.id
ORDER BY k.rank DESC LIMIT 10;
```

### Find / list files by path
```sql
SELECT DISTINCT source_id, title FROM sources
WHERE source_id LIKE '%/changes/code/%' ORDER BY source_id LIMIT 30;
```

### Read a file (its sections in order)
```sql
SELECT section_title, substr(content,1,500) AS body
FROM chunks WHERE source_id = '/abs/path/to/doc.md' ORDER BY position;
```

### THE HINGE — which sessions touched this file (cross-cell, by `file_uuid`)
Two steps: get the file's uuid from the fs cell, then query a session cell with it.
```bash
# 1. the file's SOMA uuid
$FLEX core search --cell <name> "SELECT DISTINCT file_uuid FROM sources WHERE source_id LIKE '%/myfile.md'"
# 2. who touched it (claude_code stores file_uuids per chunk as a string)
$FLEX core search --cell claude_code \
  "SELECT session_id, created_at, tool_name, target_file FROM messages
   WHERE file_uuids LIKE '%<uuid>%' ORDER BY created_at DESC LIMIT 20"
```
This bridges "what the file says" (fs cell) and "every session that touched it"
(sessions cell) on the machine-global SOMA identity. See `flex:sessions`
(`cell="claude_code"`) for the sessions side.

## Recipes

**Duplicate content across the tree** (same bytes in two places):
```sql
SELECT content_hash, count(*) AS copies, group_concat(DISTINCT substr(source_id,-40))
FROM chunks GROUP BY content_hash HAVING copies > 1 ORDER BY copies DESC LIMIT 15;
```

**Everything under a sub-tree** (a folder prefix):
```sql
SELECT DISTINCT source_id FROM sources WHERE source_id LIKE '%/src/engine/%' LIMIT 50;
```

**Section-grain search** (needs `--nest`): filter `WHERE section_title = 'Decisions'`
to scope FTS or listing to a specific heading across many docs.

**Drift check** — find a doc's claim in the fs cell, take its `file_uuid`, then in the
session cell read the most recent session that edited it (the hinge, step 2 ordered by
`created_at DESC`).

## Scopes

A **scope** is not a separate cell — it's a `WHERE source_id LIKE` filter composed over
one lossless cell. `flex:filesystem` runs over a whole tree (e.g. a `myrepo-fs` cell =
the whole `~/projects/myrepo` tree, lossless including archives); scopes narrow that same
cell to a sub-folder.

- **by folder** (where it lives): a top-level directory →
  `WHERE source_id LIKE '%/<dir>/%'` (e.g. `%/src/%`, `%/docs/%`).
- **by kind** (a repeated sub-folder convention): scope to a nested folder →
  `%/tests/%`, `%/notes/%`.
- **compose them**: chain `LIKE` filters —
  `WHERE source_id LIKE '%/docs/%' AND source_id LIKE '%/api/%'`.

`source_id` **is** the clean absolute file path — there is no separate stored `path`
column. Use `source_id AS path` as a query-local ergonomic alias, as in the templates
below.

**1. Scope a query**
```sql
SELECT source_id AS path, section_title, substr(content,1,200)
FROM chunks WHERE source_id LIKE '%/<scope>/%';
```

**2. `--live`** (exclude dead/archived — append to any scoped query):
```sql
SELECT source_id AS path, section_title, substr(content,1,200)
FROM chunks WHERE source_id LIKE '%/<scope>/%'
  AND source_id NOT LIKE '%/archive/%'
  AND source_id NOT LIKE '%/sandbox/%';
```

**3. `ls` a folder** (distinct paths under a scope):
```sql
SELECT DISTINCT source_id AS path FROM chunks
WHERE source_id LIKE '%/<dir>/%' ORDER BY path;
```

**4. Subtree (containment)**: for the recursive descendants of a container node —
rather than a plain path-prefix match — use the `subtree` preset over `_edges_tree`. It
walks containment edges, so it also catches nodes whose `source_id` doesn't literally
embed the ancestor's path segment.

**5. FTS scoped to a folder** (keyword match + path filter in one query):
```sql
SELECT k.snippet, c.source_id AS path, c.section_title
FROM keyword('retrieval scoring') k JOIN chunks c ON k.id = c.id
WHERE c.source_id LIKE '%/<dir>/%'
ORDER BY k.rank DESC LIMIT 10;
```

A scope over an Instant cell is the **complement**, not a replacement, for a Vector
cell: a raw `WHERE source_id LIKE` sweep is best when the question is cross-cutting
("does this exist anywhere in the tree") rather than "what does the meaning-based view
say" — for the latter, build the same folders `--embed` (Vector) instead. For a code
repository's call/import graph, reach for **`flex:codegraph`** — a separate built surface
(the `--code` profile) with graph verbs plain Instant lacks, not a `WHERE` clause over
this cell.

## Methodology

- **Pick the mode for the question.** Exact term or structure → Instant (`--no-embed`,
  default). Fuzzy meaning ("docs about X-ish") → Vector (`--embed`).
- **Structural first.** `GROUP BY`/`COUNT(DISTINCT …)` and `LIKE` over `source_id` are free — get the shape before reading bodies.
- **FTS for terms, path for location.** `keyword()` for an exact word/phrase; `source_id LIKE` for "where is this file".
- **The hinge is two cells.** Time and authorship live in the session cells, not here — bridge on `file_uuid`.

## Limits (qualify claims against these)

- **Instant has no embeddings, no live freshness.** Compile-time snapshot; `--regen` to refresh. No semantic recall — build `--embed` (Vector) for that.
- **Section-grain needs `--nest`.** Without it, chunks are coarse (whole-file or default split); `section_title` may be empty.
- **The cell has no time axis.** Its timestamps are compile time, identical — real change-time and authorship come from the session cells via the hinge.
- **`chunks`-view fan-out.** `DISTINCT` when listing.

Lead with `@orient`. Prefer shape queries before reading bodies. For "who/when touched a file" always cross to a session cell on `file_uuid` — don't infer it from this cell.
