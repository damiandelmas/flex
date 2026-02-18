-- @name: session
-- @description: All tool activity for a session
-- @params: session (required), limit (default: 50)

SELECT
    tool_name,
    target_file,
    substr(content, 1, 80) as preview,
    datetime(timestamp, 'unixepoch', 'localtime') as ts
FROM messages
WHERE source_id LIKE '%' || :session || '%'
ORDER BY message_number
LIMIT :limit
