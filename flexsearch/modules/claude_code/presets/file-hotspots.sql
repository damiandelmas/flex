-- @name: file-hotspots
-- @description: Most-touched files across all sessions
-- @params: limit (default: 30)

SELECT
    target_file,
    COUNT(*) as total_ops,
    COUNT(DISTINCT source_id) as sessions,
    GROUP_CONCAT(DISTINCT action) as tools
FROM messages
WHERE target_file IS NOT NULL
GROUP BY target_file
ORDER BY total_ops DESC
LIMIT :limit;
