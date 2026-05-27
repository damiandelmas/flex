# Claude Code Cell Instructions

This cell indexes Claude Code session history. It is a self-describing SQLite
knowledge cell with chunks, messages, sessions, files, ACP coverage views,
embeddings, graph intelligence, and source-recovery presets.

Use this cell when the question is about Claude Code coding-agent work:
prompts, assistant turns, tool calls, files touched, delegation/forks,
decisions, failures, handoffs, or session history.

A Claude Code skill is only an entry point. It should route to this cell, call
`@orient`, and then follow this cell manual.

First call:

```text
cell="claude_code" query="@orient"
```

Every Flex query must be valid SQL or a preset. Plain text is not accepted; wrap
it in `keyword()` or `vec_ops()`.

Native Claude Code session ids are unprefixed in `claude_code`. In combined
project-wide cells, the same provider ids may be prefixed as `claude:<id>`.

## Core Surfaces

`chunks` is the unified retrieval surface. Use it to find clues. Its `content`
may be clipped, especially for tool output.

`messages` is the ordered event surface. Use it once you have a session id,
position, or message id. `messages.file_body` often holds the full tool body,
file body, patch body, or captured stdout.

`sessions` is the source-level navigation surface. Use it for titles,
timestamps, projects, source counts, graph communities, hubs, and recency.

`agent_key_chunks` is the high-signal timeline. Prefer it when the user asks
what mattered, what changed, where the plan moved, or what the current state
was. It is better than a raw tail for intent and decision recovery.

`files` contains file-body chunks when file capture exists. It is useful for
file-body search, but terminal reads may only exist in `messages.file_body`.

## Retrieval Pipeline

Pipeline: SQL -> `vec_ops` -> SQL.

Phase 1 narrows with SQL. Phase 2 scores with embeddings. Phase 3 composes with
SQL.

Scores are ordinal within one query. Do not compare scores across queries with
different tokens.

## Phase 1: SQL Pre-Filter

The second argument to `vec_ops` narrows candidates before scoring. Push every
known constraint here.

A `WHERE` clause after `vec_ops` filters after the pool is filled; sparse
post-filters can starve the pool. Pre-filter instead.

```sql
SELECT id FROM chunks WHERE type = 'user_prompt'
SELECT id FROM chunks WHERE session_id LIKE 'abc123%'
SELECT id FROM chunks WHERE tool_name = 'Edit'
SELECT id FROM chunks WHERE created_at >= date('now', '-7 days')
SELECT id FROM chunks WHERE file LIKE '%/src/auth/%'
```

## Phase 2: Vector Operations

`vec_ops` scores candidates using embeddings. Tokens reshape scoring before
selection: spread across subtopics, suppress a dominant theme, weight recency,
search from examples, or trace a direction.

`vec_ops` is a table source; always use it after `FROM` or `JOIN`.

```sql
SELECT v.id, v.score, c.session_id, c.created_at, c.type,
       substr(c.content, 1, 1400) AS content
FROM vec_ops(
  'similar:key decisions tradeoffs and design choices this week diverse decay:7',
  'SELECT id FROM chunks WHERE type = ''user_prompt'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 15;
```

The `similar:` text is embedded. Bare keywords cast a wide net; natural
language, usually 5-15 words, narrows focus.

### Modulation Tokens

Tokens compose:

```text
similar:how we handle auth and token refresh diverse suppress:JWT rotation boilerplate decay:7
```

`diverse` applies MMR and penalizes similarity to already-selected results. Use
it for breadth across subtopics.

`suppress:TEXT` embeds `TEXT` and demotes chunks similar to it. Aim at the
dominant cluster theme, not the whole topic. Stack multiple suppressions when
needed.

`decay:N` applies temporal decay. `decay:7` is a weekly half-life. `decay:1` is
aggressive. `decay:0` disables decay.

`centroid:id1,id2,...` uses the mean embedding of example chunk ids as the
query. Use when examples define a concept better than words.

`from:TEXT to:TEXT` finds content along a conceptual arc. Anchors should be
contrasting concepts, such as `from:quick hacky prototype to:principled
production system`.

`pool:N` increases the candidate pool. Use it only when a broad query still
needs more candidates after a proper pre-filter.

Edge cases:

- `diverse` is boolean; no parameter.
- `decay:0` disables decay.
- `pool:0` falls back to the default pool.
- Use one `vec_ops` per query. For multiple, use a CTE.
- Some graph fields may be `NULL` before enrichment finishes. Use `COALESCE`.

## Phase 3: SQL Composition

Join scored results back to views, boost or filter with graph metadata, group,
filter, and paginate.

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:authentication patterns and middleware design') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC
LIMIT 10;
```

## Recipes

Structural shape, no embeddings:

```sql
SELECT project, COUNT(*) AS sessions
FROM sessions
GROUP BY project
ORDER BY sessions DESC;
```

Semantic search skeleton:

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:YOUR TOPIC IN NATURAL LANGUAGE', 'PRE_FILTER') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC
LIMIT 10;
```

Pre-filter examples:

- Session scope: `'SELECT id FROM chunks WHERE session_id LIKE ''abc%'''`
- User messages: `'SELECT id FROM chunks WHERE type = ''user_prompt'''`
- Recent: `'SELECT id FROM chunks WHERE created_at >= date(''now'', ''-7 days'')'`
- Project: `'SELECT id FROM chunks WHERE session_id IN (SELECT session_id FROM sessions WHERE project = ''myapp'')'`
- Files only: `'SELECT id FROM chunks WHERE type = ''file'''`
- No filter: omit it or use `''`

Exact term search:

```sql
SELECT k.id, k.rank, k.snippet, c.content
FROM keyword('term') k
JOIN chunks c ON k.id = c.id
ORDER BY k.rank DESC
LIMIT 10;
```

For multi-word names, brands, file titles, or phrases, quote the phrase inside
`keyword()` first:

```sql
SELECT k.id, k.rank, k.snippet, c.session_id, c.position, c.tool_name
FROM keyword('"Northstar Coffee"', 'SELECT id FROM chunks') k
JOIN chunks c ON k.id = c.id
ORDER BY k.rank DESC
LIMIT 10;
```

Unquoted multi-word FTS can match plausible noise when one token dominates or a
term is dropped by tokenization.

Scoped keyword:

```sql
SELECT k.id, k.rank, k.snippet, c.content
FROM keyword('authentication', 'SELECT id FROM chunks WHERE type = ''user_prompt''') k
JOIN chunks c ON k.id = c.id
ORDER BY k.rank DESC
LIMIT 10;
```

The second argument is pre-filter SQL. Without it, BM25 ranks globally.

Hybrid intersection:

```sql
SELECT k.id, k.rank, v.score, c.content
FROM keyword('sdk') k
JOIN vec_ops('similar:cell creation pipeline and programmatic ingest workflow') v
  ON k.id = v.id
JOIN chunks c ON k.id = c.id
ORDER BY v.score DESC
LIMIT 10;
```

Empty hybrid results mean no overlap, not broken syntax.

FTS as pre-filter:

```sql
SELECT v.id, v.score, c.content
FROM vec_ops(
  'similar:error handling and retry patterns',
  'SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid
   WHERE chunks_fts MATCH ''timeout'''
) v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC
LIMIT 10;
```

Hub navigation:

```sql
SELECT v.id, v.score, s.title, s.centrality
FROM vec_ops('similar:the main architectural decisions and system design') v
JOIN chunks c ON v.id = c.id
JOIN sessions s ON c.session_id = s.session_id
WHERE s.is_hub = 1
ORDER BY s.centrality DESC
LIMIT 5;
```

File path search:

```sql
SELECT file, section, ext, substr(content, 1, 200)
FROM chunks
WHERE type = 'file'
  AND file LIKE '%/changes/code/2603%'
ORDER BY created_at DESC
LIMIT 10;
```

Semantic search within files:

```sql
SELECT v.id, v.score, c.file, c.section, substr(c.content, 1, 200)
FROM vec_ops(
  'similar:how authentication middleware validates tokens',
  'SELECT id FROM chunks WHERE type = ''file'''
) v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC
LIMIT 10;
```

## Session Navigation

Use `@story` for one session before pulling a full transcript.

```text
@story session=<session_id>
```

Raw tail:

```sql
SELECT session_id, created_at, position, type, tool_name, file,
       substr(content, 1, 1800) AS content
FROM chunks
WHERE session_id = '<session_id>'
ORDER BY position DESC
LIMIT 20;
```

Important timeline:

```sql
SELECT session_id, created_at, position, type, tool_name, target_file,
       key_reason, key_weight, substr(content, 1, 1600) AS content
FROM agent_key_chunks
WHERE session_id = '<session_id>'
ORDER BY position
LIMIT 40;
```

Files touched:

```sql
SELECT session_id, target_file AS file, tool_name,
       COUNT(*) AS n,
       MAX(COALESCE(timestamp, created_at)) AS latest
FROM agent_key_chunks
WHERE session_id = '<session_id>'
  AND target_file IS NOT NULL
GROUP BY session_id, target_file, tool_name
ORDER BY latest DESC
LIMIT 100;
```

Activity density:

```sql
SELECT session_id,
       COUNT(id) AS chunk_count,
       SUM(CASE WHEN type = 'tool_call' THEN 1 ELSE 0 END) AS tool_chunks,
       SUM(CASE WHEN file IS NOT NULL THEN 1 ELSE 0 END) AS file_chunks,
       MIN(created_at) AS first_indexed,
       MAX(created_at) AS latest_indexed
FROM chunks
WHERE session_id = '<session_id>'
GROUP BY session_id;
```

## Source Recovery

Treat `chunks.content` as a retrieval clue, not necessarily the source body.
For tool output, stdout, and file captures, the full body usually lives in
`messages.file_body`.

Use these presets before writing custom SQL:

- `@full id=<message_or_chunk_id>` returns the best full body for an id. It uses
  `messages.file_body` when present. If the id is a clipped output row such as
  `Chunk ID: ... Original token count: ... Output:`, it looks for the nearby
  command/tool row with the same chunk marker and returns that full body.
- `@observed-file path=<path-fragment>` finds file observations across
  `target_file`, command text, and captured stdout/file bodies. Use this for
  `sed -n path`, `cat path`, `rg path`, generated heredocs, and terminal reads
  that do not populate `target_file`.
- `@file-history path=<path-fragment>` returns an ordered timeline of
  mutations, reads, target-file touches, and stdout observations.

Recovery ladder:

1. Use `keyword()` or `vec_ops()` to find candidate rows.
2. Once you have an id, stop searching and call `@full id=...`.
3. If recovering a path, call `@observed-file path=...` or
   `@file-history path=...`, then use the returned `fetch_full`.
4. When manually inspecting, compare `length(content)` and
   `length(file_body)` in `messages`; if `file_body` is longer, prefer it.
5. If a result body starts with `Chunk ID:` / `Original token count:` /
   `Output:`, assume the visible content may be clipped and fetch the source
   body.

Manual inspection:

```sql
SELECT id, session_id, position, type, tool_name, target_file,
       length(content) AS content_len,
       length(file_body) AS file_body_len,
       substr(content, 1, 300) AS preview
FROM messages
WHERE session_id = '<session_id>'
  AND position BETWEEN <pos_minus_5> AND <pos_plus_5>
ORDER BY position;
```

## Methodology

Choose the right mode:

- Known path/name: structural SQL such as `WHERE file LIKE '%pattern%'`
- Known exact term: `keyword('term')`
- Conceptual/fuzzy: `vec_ops('similar:...')`

Start with `@orient`. Use `PRAGMA table_info(...)` only when `@orient` does not
expose the column detail you need.

Structural first. `GROUP BY`, `COUNT(*)`, and `DISTINCT` are cheap. Get the
shape before going semantic.

Discover, then narrow. Run a broad `vec_ops`, find themes, then pre-filter the
next query with what you learned.

Push constraints into the pre-filter, not into a sparse `WHERE` after
`vec_ops`.

Pivot on mode shift. Theme becomes quantification with `GROUP BY`. ID becomes
exact retrieval with joins and `ORDER BY position`.

Cross-cell when needed. Different cells have different columns and date ranges.
Trust `@orient` first; use `PRAGMA` only as a fallback.

## Presets

Use presets when possible. `@orient` discovers the presets installed in this
cell.

Useful presets:

- `@orient`
- `@story session=<session_id>`
- `@digest days=<n>`
- `@file path=<path-fragment>`
- `@file-search query=<terms>`
- `@full id=<message_or_chunk_id>`
- `@observed-file path=<path-fragment>`
- `@file-history path=<path-fragment>`

## Provider Notes

Claude Code captures can appear as clean file bodies, patch bodies, command
outputs, tool wrappers, or search-result payloads. Prefer `tool_name`,
`target_file`, `type`, `position`, and `file_body` length to decide whether a
row is the source body or only a retrieval clue.

For artifact archaeology, path reads through shell output are first-class
evidence. If `@file` or `target_file` does not find a mutation, use
`@observed-file` or `@file-history` before concluding the path was absent.

## Reporting

Keep output compact and evidence-shaped. Include:

- cell name: `claude_code`
- session id
- timestamp or position
- row type, tool name, and file/path when relevant
- rank or vector score when using `keyword()` or `vec_ops()`
- compact content excerpt unless the user asked for the full body

Qualify claims when there is no explicit evidence. If you only found a clipped
chunk, say it is a clue and identify the next recovery query.
