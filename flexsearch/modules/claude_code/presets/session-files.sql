-- @name: session-files
-- @description: All files touched in a session with operations
-- @params: session (required)

SELECT
    target_file,
    action,
    COUNT(*) as ops,
    MIN(position) as first_touch,
    MAX(position) as last_touch
FROM messages
WHERE source_id LIKE '%' || :session || '%'
  AND target_file IS NOT NULL
GROUP BY target_file, action
ORDER BY first_touch;
