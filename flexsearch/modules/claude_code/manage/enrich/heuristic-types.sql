-- @name: heuristic-types
-- @description: Tool-based kind classification at confidence 0.5
-- @target: _enrich_types
INSERT OR IGNORE INTO _enrich_types (chunk_id, semantic_role, confidence)
SELECT c.id,
    CASE WHEN t.tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 'file_operation'
         WHEN t.tool_name = 'Task' THEN 'delegation'
         WHEN t.tool_name = 'Bash' THEN 'command'
         WHEN t.tool_name IN ('Glob', 'Grep') THEN 'search'
         WHEN t.tool_name = 'Read' THEN 'read'
         WHEN t.tool_name IS NULL AND tm.role = 'user' THEN 'prompt'
         WHEN t.tool_name IS NULL AND tm.role = 'assistant' THEN 'response'
         ELSE 'message'
    END, 0.5
FROM _raw_chunks c
LEFT JOIN _edges_tool_ops t ON c.id = t.chunk_id
LEFT JOIN _types_message tm ON c.id = tm.chunk_id;
