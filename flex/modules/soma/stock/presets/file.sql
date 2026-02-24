-- @name: file
-- @description: Find sessions that touched a file (SOMA-first, unified across renames)
-- @params: path (required), limit (default: 30)

-- Resolve file_uuid from any matching path, fan out to ALL paths
SELECT
    substr(es.source_id, 1, 8) as session,
    t.tool_name,
    t.target_file,
    datetime(c.timestamp, 'unixepoch', 'localtime') as ts
FROM _edges_tool_ops t
JOIN _raw_chunks c ON t.chunk_id = c.id
JOIN _edges_source es ON t.chunk_id = es.chunk_id
LEFT JOIN _edges_file_identity fi ON t.chunk_id = fi.chunk_id
WHERE (
    fi.file_uuid = (
        SELECT fi2.file_uuid FROM _edges_file_identity fi2
        JOIN _edges_tool_ops t2 ON fi2.chunk_id = t2.chunk_id
        WHERE t2.target_file LIKE '%' || :path || '%'
        LIMIT 1
    )
    OR (fi.file_uuid IS NULL AND t.target_file LIKE '%' || :path || '%')
)
ORDER BY c.timestamp DESC
LIMIT :limit
