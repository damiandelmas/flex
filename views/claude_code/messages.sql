-- @name: messages
-- @description: Chunk-level surface for claude_code cells. Tool ops, message type, delegation edges. Session title/message_count as breadcrumbs.
-- id: unique per message. Format: {session_id}_{line_num}. vec_ops JOIN: JOIN messages m ON v.id = m.id
-- session_id: the Claude Code session UUID. Same as sessions.session_id.
-- child_session_id: non-NULL on Task chunks — follow to sessions for the spawned agent.

DROP VIEW IF EXISTS messages;
CREATE VIEW messages AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch', 'localtime') AS created_at,
    s.source_id AS session_id,
    s.position AS position,
    src.project,
    src.title,
    src.message_count,
    t.tool_name,
    t.target_file,
    t.success,
    t.cwd,
    tp.type,
    d.child_doc_id AS child_session_id,
    d.agent_type
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
LEFT JOIN _types_message tp ON r.id = tp.chunk_id
LEFT JOIN (SELECT chunk_id, child_doc_id, agent_type FROM _edges_delegations GROUP BY chunk_id) d ON r.id = d.chunk_id;
