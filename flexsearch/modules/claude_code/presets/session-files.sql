-- @name: session-files
-- @description: All files touched in a session with operations
-- @params: session (required)

SELECT
    target_file,
    tool_name,
    COUNT(*) as ops,
    MIN(message_number) as first_touch,
    MAX(message_number) as last_touch
FROM messages
WHERE source_id LIKE '%' || :session || '%'
  AND target_file IS NOT NULL
GROUP BY target_file, tool_name
ORDER BY first_touch;
