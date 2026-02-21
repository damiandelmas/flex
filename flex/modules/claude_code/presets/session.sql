-- @name: session
-- @description: All tool activity for a session
-- @params: session (required), limit (default: 50)

SELECT
    tool_name,
    target_file,
    substr(content, 1, 80) as preview,
    created_at as ts
FROM messages
WHERE session_id LIKE '%' || :session || '%'
ORDER BY position
LIMIT :limit
