-- @name: story
-- @description: Session narrative with timeline, artifacts, agents
-- @params: session (required)
-- @multi: true

-- @query: meta
SELECT
    source_id as session,
    title,
    datetime(start_time, 'unixepoch', 'localtime') as started,
    message_count as ops,
    primary_cwd as cwd
FROM sessions
WHERE source_id LIKE '%' || :session || '%';

-- @query: timeline
SELECT
    action,
    COALESCE(target_file, substr(content, 1, 60)) as target,
    datetime(timestamp, 'unixepoch', 'localtime') as ts
FROM messages
WHERE source_id LIKE '%' || :session || '%'
ORDER BY position
LIMIT 100;

-- @query: artifacts
SELECT DISTINCT
    action,
    target_file
FROM messages
WHERE source_id LIKE '%' || :session || '%'
  AND action IN ('Write', 'Edit', 'MultiEdit')
  AND target_file IS NOT NULL;

-- @query: agents
SELECT
    d.child_doc_id as child_session,
    d.agent_type
FROM _edges_delegations d
JOIN _raw_chunks c ON d.chunk_id = c.id
JOIN _edges_source e ON c.id = e.chunk_id
WHERE e.source_id LIKE '%' || :session || '%'
