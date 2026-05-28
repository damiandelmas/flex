# Goose Cell Instructions

This cell indexes Goose `sessions.db` conversation history. It uses the shared
coding-agent query surface from `claude_code`, with Goose-specific session
metadata preserved in `_types_goose_session`.

First call:

```text
cell="goose" query="@orient"
```

Every Flex query must be valid SQL or a preset. Plain text is not accepted; wrap
free text in `keyword()` or `vec_ops()`.

## Core Surfaces

`chunks` is the unified retrieval surface for prompts, assistant turns, tool
calls, tool results, and file-body chunks.

`messages` is the ordered event surface. Use it when you have a `session_id`,
message id, position, tool name, target file, or `file_body`.

`sessions` is the source-level surface. Use it for session titles, timestamps,
working directories, graph communities, hubs, and fingerprints.

`files` contains file-body chunks when Goose tool inputs or outputs expose file
content.

`_types_goose_session` is the native Goose sidecar. Query it directly for
provider, model, mode, recipe, token, thread, and session-type metadata.

## Common Queries

Native Goose session metadata:

```sql
SELECT source_id, provider_name, goose_mode, model_config_json
FROM _types_goose_session
ORDER BY updated_at DESC
LIMIT 20;
```

Sessions that touched a file:

```sql
SELECT DISTINCT session_id, target_file, tool_name
FROM messages
WHERE target_file LIKE '%src/app.py%'
ORDER BY session_id DESC
LIMIT 20;
```

Semantic search over assistant messages:

```sql
SELECT v.score, m.session_id, substr(m.content, 1, 1200) AS content
FROM vec_ops(
  'similar:debugging tool calls and session state',
  'SELECT id FROM messages WHERE type = ''assistant'''
) v
JOIN messages m ON m.id = v.id
ORDER BY v.score DESC
LIMIT 10;
```

Keyword search scoped to Goose file bodies:

```sql
SELECT k.rank, f.file, substr(f.content, 1, 800) AS content
FROM keyword('authentication', 'SELECT id FROM files') k
JOIN files f ON f.id = k.id
ORDER BY k.rank DESC
LIMIT 10;
```

## Refresh Model

Goose is a local `watch` lifecycle cell. The install path records the source
database path in `_meta.goose_db_path` and its byte size in
`_meta.goose_db_size`. Refresh compares the current source size with the last
recorded size and skips unchanged databases.

Use a copied database or fixture with `--goose-db` when validating public
installs. Do not point tests at a live user database unless the run is already
intended to index that local data.
