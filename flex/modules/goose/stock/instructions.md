# Goose Cell Instructions

This cell indexes Goose `sessions.db` conversation history. It uses the shared
coding-agent substrate: `chunks`, `messages`, `sessions`, `files`, and
`agent_key_chunks` mean the same thing here as in the Codex and Claude Code
cells. Goose-specific session metadata lives in `sources` (extended view) and
is queryable directly via the `provider_name`, `goose_mode`, `model_config_json`,
`total_tokens`, `session_type`, and `working_dir` columns.

The cell is self-describing. Start here:

```text
cell="goose" query="@orient"
```

`@orient` returns the schema, presets, graph entry points, and examples. Every
Flex query must be valid SQL or a preset. Plain text is not accepted; wrap it
in `keyword()` or `vec_ops()`.

## Core Surfaces

`chunks` — unified retrieval surface. All chunk types: `user_prompt`,
`assistant`, `tool_call`, `file`. Content may be clipped for tool output.
Key columns: `id`, `content`, `type`, `session_id`, `position`, `tool_name`,
`file`, `ext`.

`messages` — ordered event surface. Use once you have a session id or
position. `messages.file_body` holds full tool bodies and file captures.
Key columns: `id`, `content`, `session_id`, `position`, `type`, `tool_name`,
`target_file`, `success`, `cwd`, `file_body`.

`sessions` — source-level navigation. Titles, timestamps, projects, graph
community, hubs, centrality. Key columns: `session_id`, `project`, `title`,
`message_count`, `started_at`, `ended_at`, `centrality`, `is_hub`,
`community_label`, `fork_count`.

`agent_key_chunks` — high-signal timeline. Prefer for intent and decision
recovery over a raw tail. Adds `key_reason`, `key_weight`, `target_file`.

`files` — file-body sub-chunks when Goose exposes file content in tool I/O.

`sources` — extended source view with all Goose-native metadata:
`provider_name`, `model_config_json`, `goose_mode`, `thread_id`,
`total_tokens`, `input_tokens`, `output_tokens`, `session_type`,
`working_dir`, `recipe_json`.

## Goose-Specific Metadata

Query provider, model, and mode across sessions:

```sql
SELECT session_id, provider_name, goose_mode,
       json_extract(model_config_json, '$.model_name') AS model,
       total_tokens
FROM sources
ORDER BY start_time DESC
LIMIT 20;
```

Sessions grouped by provider and mode:

```sql
SELECT provider_name, goose_mode, COUNT(*) AS sessions,
       SUM(total_tokens) AS total_tok
FROM sources
GROUP BY provider_name, goose_mode
ORDER BY sessions DESC;
```

## Choosing Search Mode

Use structural SQL first when you know ids, paths, projects, types, or dates.

```sql
SELECT session_id, title, started_at, ended_at, message_count
FROM sessions
WHERE title LIKE '%authentication%'
ORDER BY started_at DESC
LIMIT 20;
```

Use `keyword()` for exact terms, paths, error text, function names, and quoted
phrases.

```sql
SELECT k.id, k.rank, k.snippet, c.session_id, c.position, c.tool_name
FROM keyword('"tool execution failed"', 'SELECT id FROM chunks') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

Use `vec_ops()` for conceptual search. Push session, type, date, project, or
file constraints into the second argument.

```sql
SELECT v.id, v.score, c.session_id, c.created_at, c.type,
       substr(c.content, 1, 1400) AS content
FROM vec_ops(
  'similar:debugging tool calls and session state diverse decay:14',
  'SELECT id FROM chunks WHERE type = ''user_prompt'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 15;
```

Do not apply a sparse `WHERE` after a global vector search — that starves the
candidate pool. Put constraints inside the pre-filter SQL (second argument).

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

High-signal timeline:

```sql
SELECT session_id, created_at, position, type, tool_name, target_file,
       key_reason, key_weight, substr(content, 1, 1600) AS content
FROM agent_key_chunks
WHERE session_id = '<session_id>'
ORDER BY position
LIMIT 40;
```

## Source Recovery

Treat `chunks.content` as a retrieval clue, not necessarily the source body.
Full tool bodies and file captures usually live in `messages.file_body`.

Use presets before writing custom SQL:

```text
@full id=<message_or_chunk_id>
@file path=<path-fragment>
@file-provenance path=<path-fragment>
```

`@full` returns the best full body for an id. If the id points at a clipped
output row, it looks for the nearby tool row with the full `file_body`.

`@file` finds sessions that touched a file path.

`@file-provenance` returns a single-call lineage: current path, path history,
and key events.

Recovery ladder: (1) `keyword()` or `vec_ops()` to find ids. (2) `@full
id=...` to fetch the source body. (3) For paths: `@file path=...` or
`@file-provenance path=...`. (4) In `messages`, prefer `file_body` when it
is longer than `content`. (5) If a result starts with `Chunk ID:` or
`Original token count:`, it is clipped — fetch the source body.

## What Goose Lacks vs Claude Code

Goose does not capture pre-edit file backups or patch bodies. File content
reaches the cell only when Goose tool inputs or outputs include a full file
body. `file_uuid` and `content_hash` coverage is low (see `@orient` coverage
section). For mutation archaeology, `@file-provenance` and `agent_key_chunks`
are the first resort; do not expect the deep Edit-diff archaeology that Claude
Code cells support.

## Presets

Use presets when possible. `@orient` discovers the presets installed in this
cell.

Useful presets:

- `@orient`
- `@story session=<session_id>`
- `@digest days=<n>`
- `@file path=<path-fragment>`
- `@file-provenance path=<path-fragment>`
- `@file-search query=<terms>` / `@sprints` / `@genealogy concept=<concept>`
- `@bridges` / `@health`

## Refresh Model

Goose is a `watch` lifecycle cell. Refresh compares the current source
database byte size against the last recorded size and skips unchanged
databases. Use a copied database or fixture when validating public installs.

## Methodology

Start with `@orient`. Structural first — `GROUP BY`, `COUNT(*)`, `DISTINCT`
are cheap. Discover then narrow: broad `vec_ops` finds themes, pre-filter
the next query with those themes. Push constraints into the pre-filter, not a
sparse `WHERE` after `vec_ops`.

## Reporting

Keep output compact and evidence-shaped. Include:

- cell name: `goose`
- session id
- timestamp or position
- row type, tool name, and file/path when relevant
- rank or vector score when using `keyword()` or `vec_ops()`
- provider and model when session identity matters
- compact content excerpt unless the user asked for the full body

Qualify claims when there is no explicit evidence. If you only found a clipped
chunk, say it is a clue and identify the next recovery query.
