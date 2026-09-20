# DeepSeek Harness Cell Instructions

This cell indexes DeepSeek Harness session history. The native source is
`~/.dsh/sessions/**/session.jsonl.zstd` (or `session.jsonl` when compression is
disabled). The native files remain authoritative; Flex provides the searchable
projection.

Start with the live contract:

```text
cell="deepseek" query="@orient"
```

Use the shared coding-agent surfaces for prompts, assistant turns, tool
activity, failures, file observations, and session timelines:

```sql
SELECT session_id, title, started_at, ended_at, message_count
FROM sessions
ORDER BY started_at DESC
LIMIT 20;
```

Provider-specific facts remain directly queryable:

```sql
SELECT source_id, session_path, workspace_path, provider, model,
       reasoning_effort, context_window, completed, terminal_reason
FROM _types_deepseek_session
ORDER BY created_at_ms DESC;
```

Every native event, including request headers, permission/sandbox policy,
stream chunks, and terminal errors, is retained in
`_types_deepseek_event.payload_json` with its original `native_seq` and
`event_type`. Use that sidecar when the shared compatibility projection cannot
name a provider-specific event.

Search with `keyword()` for exact paths, errors, models, and event text; use
`vec_ops()` for conceptual retrieval after checking semantic readiness in
`@orient`. Use `@story session=<id>` for one session and `@full id=<chunk-id>`
when a shared result points to a clipped tool body.

The source is a local `watch` cell. Structural publication and vector
convergence are separate: new DSH events are queryable through SQL and FTS even
when their embedding is still NULL.
