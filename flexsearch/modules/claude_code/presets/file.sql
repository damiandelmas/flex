-- @name: file
-- @description: Find sessions that touched a file
-- @params: path (required), limit (default: 30)

SELECT
    substr(m.source_id, 1, 8) as session,
    m.action,
    m.target_file,
    datetime(m.timestamp, 'unixepoch', 'localtime') as ts
FROM messages m
WHERE m.target_file LIKE '%' || :path || '%'
ORDER BY m.timestamp DESC
LIMIT :limit
