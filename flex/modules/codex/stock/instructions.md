# Codex Cell Instructions

This cell indexes Codex CLI session provenance. Each source is a Codex
session; each row is a prompt, assistant turn, tool call, tool result, file
capture, or derived high-signal event.

The cell is self-contained. Start here, not in the global `instructions` cell:

```text
cell="codex" query="@orient"
```

`@orient` returns the schema, presets, examples, coverage notes, graph entry
points, and these mounted instructions. A Codex skill is only an entry point:
it should route to `cell="codex"` and call `@orient`, then follow the cell.

## What This Cell Is For

Use the Codex cell when the question is about Codex work:

- user prompts, assistant turns, tool calls, tool output, and session tails
- which files Codex read, wrote, patched, or printed to stdout
- forks, delegation, Aura-launched Codex homes, and `.codex` source coverage
- semantic search over Codex workstreams, decisions, failures, and plans
- recovering exact source bodies from clipped search results

Codex uses the shared coding-agent substrate. The table and preset vocabulary is
intentionally close to the Claude Code cell: `chunks`, `messages`, `sessions`,
`files`, and `agent_key_chunks` mean the same kind of thing here.

## First Move

Call `@orient` once for the Codex cell in the current task. Use it as the live
manual because it shows the actual installed schema and presets.

```text
cell="codex" query="@orient"
```

Every Flex query must be SQL or a preset. Plain English is not a query. Use
`keyword()` for exact text and `vec_ops()` for semantic search.

## Core Surfaces

`chunks` is the unified retrieval surface. Use it to find clues. Its `content`
may be clipped, especially for tool output.

`messages` is the ordered event surface. Use it once you have a session id,
position, or message id. `messages.file_body` often holds the full tool body or
captured stdout.

`sessions` is the source-level navigation surface. Use it for titles,
timestamps, projects, source counts, graph communities, hubs, and recency.

`agent_key_chunks` is the high-signal timeline. Prefer it when the user asks
what mattered, what changed, where the plan moved, or what the current state
was. It is better than a raw tail for intent and decision recovery.

`files` contains file-body chunks when file capture exists. It is useful for
file-body search, but terminal reads may only exist in `messages.file_body`.

## Choosing Search Mode

Use structural SQL first when you know ids, paths, projects, types, or dates.

```sql
SELECT session_id, title, started_at, ended_at, message_count
FROM sessions
WHERE title LIKE '%release boundary%'
ORDER BY started_at DESC
LIMIT 20;
```

Use `keyword()` for exact terms, paths, ids, errors, function names, and
multi-word names. Quote multi-word phrases inside the term.

```sql
SELECT k.id, k.rank, k.snippet, c.session_id, c.position, c.tool_name
FROM keyword('"Northstar Coffee"', 'SELECT id FROM chunks') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

Unquoted multi-word search can return plausible noise if one token dominates or
a token is dropped by FTS tokenization.

Use `vec_ops()` for conceptual search. Put scope constraints in the second
argument so the candidate pool is narrowed before scoring.

```sql
SELECT v.id, v.score, c.session_id, c.created_at, c.type,
       substr(c.content, 1, 1400) AS content
FROM vec_ops(
  'similar:why codex ingestion changed to multisource .codex homes diverse decay:14',
  'SELECT id FROM chunks WHERE type = ''user_prompt'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 15;
```

Do not run a global vector search and then apply a sparse `WHERE` clause. That
starves the pool. Put session, type, date, project, or file constraints inside
the pre-filter SQL.

## Session Navigation

Use `@story` for one session before pulling a full transcript.

```text
@story session=<session_id>
```

For a raw tail:

```sql
SELECT session_id, created_at, position, type, tool_name, file,
       substr(content, 1, 1800) AS content
FROM chunks
WHERE session_id = '<session_id>'
ORDER BY position DESC
LIMIT 20;
```

For important events:

```sql
SELECT session_id, created_at, position, type, tool_name, target_file,
       key_reason, key_weight, substr(content, 1, 1600) AS content
FROM agent_key_chunks
WHERE session_id = '<session_id>'
ORDER BY position
LIMIT 40;
```

## Source Recovery

Treat search results as clues, not evidence. The exact body may live one layer
over.

Use the recovery presets before writing custom SQL:

```text
@full id=<message_or_chunk_id>
@observed-file path=<path-fragment>
@file-history path=<path-fragment>
```

`@full` returns the best full body for a message or chunk id. If the id points
at a clipped output row such as `Chunk ID: ... Original token count: ...`, it
tries to climb to the sibling tool row with the full `messages.file_body`.

`@observed-file` finds file observations across `target_file`, Bash command
text, and captured stdout/file bodies. Use it for `sed -n path`, `cat path`,
`rg path`, generated heredocs, and other terminal observations that do not
populate `target_file`.

`@file-history` returns an ordered timeline of reads, writes, target-file
touches, and stdout observations for a path fragment.

Manual recovery ladder:

1. Use `keyword()` or `vec_ops()` only to find candidate ids.
2. Once you have an id, stop searching and call `@full id=...`.
3. If recovering a path, call `@observed-file path=...` or `@file-history path=...`.
4. If inspecting manually, compare `length(content)` and `length(file_body)` in
   `messages`; prefer `file_body` when it is longer.
5. If a body starts with `Chunk ID:`, `Original token count:`, or `Output:`,
   assume the visible content may be an indexed shard.

Manual inspection example:

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

## Codex Source Coverage

The Codex cell can be built from more than global `~/.codex`. It may include
additional `.codex` homes discovered from Flex config and Aura registry/ledger
metadata. The intended shape is simple: every usable `.codex` home has the same
session layout and is scanned through the same Codex ingestion path.

When auditing coverage, distinguish:

- declared source homes
- usable source homes with a `sessions/` directory
- indexed sessions with searchable message chunks
- metadata-only or event-only stubs

Do not treat metadata-only stubs as searchable conversations. They can explain
provenance, but they should not be counted as recovered message history.

## Preset Bias

Prefer presets when they fit:

- `@orient` for the live manual
- `@story session=...` for one-session shape
- `@digest days=...` for recent activity
- `@file path=...` for file-touch provenance
- `@full id=...` for exact source body recovery
- `@observed-file path=...` for terminal-observed file bodies
- `@file-history path=...` for ordered path timelines

Use raw SQL when the question is structural, when a preset is too broad, or
when you need a precise pre-filter before semantic scoring.

## Reporting Results

Include enough evidence to make follow-up retrieval stable:

- cell name: `codex`
- session id
- timestamp or position
- row type, tool name, and file/path when relevant
- rank or vector score when using `keyword()` or `vec_ops()`
- a compact excerpt, unless `@full` was requested

When evidence is partial, say so. If you only found a clipped chunk, say it is a
clue and identify the next recovery query.
