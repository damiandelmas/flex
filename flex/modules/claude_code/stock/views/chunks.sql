-- @name: chunks
-- @description: Unified chunk surface. Every chunk in one view — messages, file bodies, tool calls. Type is a column, not a boundary.
-- type: user_prompt | assistant | tool_call | file — use in pre-filter to narrow. No pre-filter = everything.
-- file: file path from tool_ops (messages) or file_body (sub-chunks). NULL for non-file chunks.
-- section: function name or heading for file body sub-chunks. NULL for messages.
-- vec_ops usage: FROM vec_ops('similar:query diverse') v JOIN chunks c ON v.id = c.id
-- Narrow by type:  pre_filter: 'SELECT id FROM chunks WHERE type = ''user_prompt'''
-- Narrow by files:  pre_filter: 'SELECT id FROM chunks WHERE type = ''file'''
-- Narrow by tool:  pre_filter: 'SELECT id FROM chunks WHERE tool_name = ''Write'''

DROP VIEW IF EXISTS chunks;
CREATE VIEW chunks AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch', 'localtime') AS created_at,
    cr.type,
    s.source_id AS session_id,
    s.position,
    src.project,
    t.tool_name,
    COALESCE(fb.target_file, t.target_file) AS file,
    fb.title AS section,
    cr.ext,
    cr.child_session_id,
    cr.agent_type,
    cr.file_uuids,
    tp.branch_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
LEFT JOIN _types_message tp ON r.id = tp.chunk_id
LEFT JOIN _types_file_body fb ON r.id = fb.chunk_id
LEFT JOIN _enrich_chunk_rollup cr ON r.id = cr.chunk_id;
