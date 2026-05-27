---
name: flex:sessions
description: Search coding-agent session history through Flex MCP. Use for Claude Code and ACP-shaped coding-agent cells when the user asks what happened in sessions, wants last turns, tails, story, files, tool calls, recent messages, or scoped semantic search over coding-agent conversations.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "cell/session/topic, e.g. 'claude_code story abc123', 'claude_code tail abc123 10', 'claude_code query recent auth work'"
---

# flex:sessions

Use this for coding-agent session history. Flex session cells are searchable records of prompts, assistant turns, tool calls, tool results, file references, timestamps, and high-signal views such as `agent_key_chunks`.

Use the MCP server for skill-led retrieval. Do not use the local Flex CLI unless the user specifically asks for CLI output.

## Workflow

1. Choose the cell named by the user. For the public package this is usually `claude_code`.
2. Run `@orient` once per cell unless it has already been oriented in this turn.
3. Prefer presets first, especially `@story` for one session.
4. Use structural SQL for tails, last turns, files, and activity counts.
5. Use `keyword()` or `vec_ops()` only after scoping with SQL prefilters.
6. Return session ids and timestamps so follow-up searches can stay anchored.

## Common Modes

```text
orient CELL
find CELL "<topic or terms>"
story CELL SESSION
tail CELL SESSION [N]
last-turns CELL SESSION_OR_SCOPE
key CELL SESSION_OR_SCOPE [N]
files CELL SESSION_OR_SCOPE
activity CELL SESSION_OR_SCOPE
query CELL SESSION_OR_SCOPE "<sql or @preset>"
```

Default `N=10`.

## Presets First

Use cell presets when they fit:

```text
@orient
@health
@story session=<session_id>
@file path=<path>
@file-search query=<terms>
@digest days=<n>
@sprints
```

`@story` is the best first read for one session. It usually returns metadata, timeline, artifacts, and important turns without pulling a full transcript.

## Session Scope

One session:

```sql
session_id = '<session_id>'
```

Prefix:

```sql
session_id LIKE '<prefix>%'
```

Many sessions:

```sql
session_id IN ('abc...', 'def...')
```

For `vec_ops` and `keyword`, put the session scope inside the prefilter:

```sql
SELECT v.id, v.score, c.session_id, c.created_at, c.type,
       substr(c.content, 1, 1600) AS content
FROM vec_ops(
  'similar:<topic> diverse decay:7',
  'SELECT id FROM chunks WHERE session_id IN (''abc...'',''def...'')'
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 20;
```

Avoid post-filtering a global vector pool by session; sparse post-filters starve the pool.

## Recipes

### Last Turns

Latest user and assistant message per session:

```sql
WITH ranked AS (
  SELECT session_id, type, created_at, timestamp, content,
         ROW_NUMBER() OVER (
           PARTITION BY session_id, type
           ORDER BY COALESCE(timestamp, created_at) DESC
         ) AS rn
  FROM chunks
  WHERE <SESSION_SCOPE>
    AND type IN ('user_prompt', 'assistant')
)
SELECT session_id, type, created_at, substr(content, 1, 1800) AS content
FROM ranked
WHERE rn = 1
ORDER BY session_id, type;
```

### Tail

Last N events in one session, including tool calls/results:

```sql
SELECT session_id, created_at, type, tool_name, file,
       substr(content, 1, 1800) AS content
FROM chunks
WHERE <SESSION_SCOPE>
ORDER BY COALESCE(timestamp, created_at) DESC
LIMIT <N>;
```

### Key

High-signal timeline. Prefer `agent_key_chunks` when present:

```sql
SELECT session_id, created_at, type, tool_name, target_file,
       key_reason, key_weight,
       substr(content, 1, 1400) AS content
FROM agent_key_chunks
WHERE <SESSION_SCOPE>
ORDER BY COALESCE(timestamp, created_at) DESC
LIMIT <N>;
```

Use this instead of raw tail when the user wants important evidence rather than exact recency.

### Files

File evidence from high-signal chunks:

```sql
SELECT session_id, target_file AS file, tool_name,
       COUNT(*) AS n,
       MAX(COALESCE(timestamp, created_at)) AS latest
FROM agent_key_chunks
WHERE <SESSION_SCOPE>
  AND target_file IS NOT NULL
GROUP BY session_id, target_file, tool_name
ORDER BY latest DESC
LIMIT 100;
```

If `agent_key_chunks` is absent or sparse, use `chunks.file`:

```sql
SELECT session_id, file, tool_name, type,
       COUNT(*) AS n,
       MAX(COALESCE(timestamp, created_at)) AS latest
FROM chunks
WHERE <SESSION_SCOPE>
  AND file IS NOT NULL
GROUP BY session_id, file, tool_name, type
ORDER BY latest DESC
LIMIT 100;
```

### Activity

Per-session evidence density:

```sql
SELECT session_id,
       COUNT(id) AS chunk_count,
       SUM(CASE WHEN type = 'tool_call' THEN 1 ELSE 0 END) AS tool_chunks,
       SUM(CASE WHEN file IS NOT NULL THEN 1 ELSE 0 END) AS file_chunks,
       MIN(created_at) AS first_indexed,
       MAX(created_at) AS latest_indexed
FROM chunks
WHERE <SESSION_SCOPE>
GROUP BY session_id
ORDER BY latest_indexed DESC;
```

### Find Sessions

Find candidate sessions before drilling down:

```sql
SELECT s.session_id, s.project, s.title, s.started_at, s.message_count
FROM sessions s
WHERE s.title LIKE '%<term>%'
   OR s.project LIKE '%<term>%'
ORDER BY s.started_at DESC
LIMIT 20;
```

Semantic search across recent user prompts:

```sql
SELECT v.id, v.score, c.session_id, c.created_at,
       substr(c.content, 1, 1400) AS content
FROM vec_ops(
  'similar:<topic> diverse decay:14',
  'SELECT id FROM chunks WHERE type = ''user_prompt'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 20;
```

## ACP Compatibility Notes

Map current query concepts to shared coding-agent vocabulary:

- `session_id` -> native session id / ACP session id
- `chunks` or `messages` -> message/tool event stream
- `user_prompt` -> session prompt / user message chunk
- `assistant` -> agent message chunk
- `tool_call` / `tool_result` -> tool call/update/result
- `file` / `target_file` -> file read/write/diff evidence
- delegation/fork/session edges -> delegation or fork evidence

Do not drop provider-specific raw evidence. ACP is the comparison layer, not a reason to erase native fields.

## Presentation

Keep output compact and evidence-shaped. Include:

- cell
- session id
- timestamp
- type/tool/file where relevant
- score for semantic queries
- compact content excerpt

For multi-session output, group by session id.
