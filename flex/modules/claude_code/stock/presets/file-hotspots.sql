-- @name: file-hotspots
-- @description: Most-touched files across all sessions (SOMA-aware dedup)
-- @params: limit (default: 30)

SELECT
    COALESCE(json_extract(file_uuids, '$[0]'), target_file) as file_key,
    target_file,
    COUNT(*) as total_ops,
    COUNT(DISTINCT session_id) as session_count,
    GROUP_CONCAT(DISTINCT tool_name) as tools
FROM messages
WHERE target_file IS NOT NULL
GROUP BY COALESCE(json_extract(file_uuids, '$[0]'), target_file)
ORDER BY total_ops DESC
LIMIT :limit;
