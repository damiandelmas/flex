-- @name: messages
-- @description: Chunk-level surface for claude_code cells. Tool ops, message type, delegation edges. Session title/message_count as breadcrumbs.
-- id: unique per message. Format: {session_id}_{line_num}. vec_ops JOIN: JOIN messages m ON v.id = m.id
-- session_id: the Claude Code session UUID. Same as sessions.session_id.
-- child_session_id: non-NULL on Task chunks — follow to sessions for the spawned agent.
-- file_uuids: JSON array of SOMA file UUIDs. COALESCE(json_extract(file_uuids, '$[0]'), target_file) for rename-safe dedup.
-- target_file: path of file operated on. NULL for non-tool messages. Use WHERE target_file LIKE '%name%' to find all sessions that touched a file.
-- content: tool call signature only (e.g. "Write /path/to/file"). NOT the file body.
-- file_body: actual file content for Write/Edit/Read/Bash chunks. NULL for non-file messages.
--            For BM25 search across file bodies use @file-search preset (faster than LIKE).
--            For inline inspection: WHERE file_body LIKE '%pattern%' AND tool_name IN ('Write', 'Edit')

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
    d.child_session_id,
    d.agent_type,
    fi.file_uuids,
    fb.file_body
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
LEFT JOIN _types_message tp ON r.id = tp.chunk_id
LEFT JOIN (SELECT chunk_id, child_session_id, agent_type FROM _edges_delegations GROUP BY chunk_id) d ON r.id = d.chunk_id
LEFT JOIN (SELECT chunk_id, json_group_array(file_uuid) AS file_uuids FROM _edges_file_identity GROUP BY chunk_id) fi ON r.id = fi.chunk_id
LEFT JOIN (SELECT erc.chunk_id, rc.content AS file_body FROM _edges_raw_content erc JOIN _raw_content rc ON erc.content_hash = rc.hash GROUP BY erc.chunk_id) fb ON r.id = fb.chunk_id;
