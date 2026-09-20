-- @name: story
-- @description: Session narrative with timeline, artifacts, agents
-- @params: session (required)
-- @multi: true

-- @query: meta
SELECT
    session_id as session,
    title,
    started_at as started,
    message_count as ops
FROM sessions
WHERE session_id LIKE '%' || :session || '%'
UNION ALL
SELECT 'NOT_FOUND', 'No session matching "' || :session || '"', NULL, NULL
WHERE NOT EXISTS (
    SELECT 1 FROM sessions WHERE session_id LIKE '%' || :session || '%'
);

-- @query: timeline
SELECT
    key_reason,
    tool_name,
    COALESCE(target_file, substr(content, 1, 60)) as target,
    created_at as ts
FROM agent_key_chunks
WHERE session_id LIKE '%' || :session || '%'
ORDER BY position
LIMIT 100;

-- @query: artifacts
SELECT DISTINCT
    tool_name,
    target_file
FROM messages
WHERE session_id LIKE '%' || :session || '%'
  AND tool_name IN ('Write', 'Edit', 'MultiEdit')
  AND target_file IS NOT NULL;

-- @query: agents
SELECT
    d.child_session_id as child_session,
    d.agent_type
FROM _edges_delegations d
JOIN _coding_agent_source_visibility vis
  ON vis.source_id = COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36))
 AND vis.visible = 1
JOIN _coding_agent_source_visibility child_vis
  ON child_vis.source_id = d.child_session_id
 AND child_vis.visible = 1
WHERE COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36)) LIKE '%' || :session || '%'
