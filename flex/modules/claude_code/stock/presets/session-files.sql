-- @name: session-files
-- @description: All files touched in a session with operations (SOMA-aware dedup)
-- @params: session (required)

SELECT
    COALESCE(json_extract(file_uuids, '$[0]'), target_file) as file_key,
    target_file,
    tool_name,
    COUNT(*) as ops,
    MIN(position) as first_touch,
    MAX(position) as last_touch
FROM messages
WHERE session_id LIKE '%' || :session || '%'
  AND target_file IS NOT NULL
GROUP BY COALESCE(json_extract(file_uuids, '$[0]'), target_file), tool_name
ORDER BY first_touch;
