-- @name: tool-usage
-- @description: Tool usage breakdown across sessions

SELECT
    action as tool,
    COUNT(*) as total,
    COUNT(DISTINCT source_id) as sessions,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT source_id), 1) as avg_per_session
FROM messages
WHERE action IS NOT NULL
GROUP BY action
ORDER BY total DESC;
