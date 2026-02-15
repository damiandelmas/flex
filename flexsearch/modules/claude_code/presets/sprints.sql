-- @name: sprints
-- @description: Work sprints detected by 6h gaps. Use start_ts/end_ts to drill into a sprint: WHERE start_time BETWEEN <start_ts> AND <end_ts>
-- @params: limit (default: 20), gap_hours (default: 6)

SELECT
    sprint_id,
    COUNT(*) as sessions,
    MIN(start_time) as start_ts,
    MAX(start_time) as end_ts,
    MIN(datetime(start_time, 'unixepoch', 'localtime')) as started,
    MAX(datetime(start_time, 'unixepoch', 'localtime')) as ended,
    ROUND((MAX(start_time) - MIN(start_time)) / 3600.0, 1) as duration_hours,
    SUM(message_count) as total_ops
FROM (
    SELECT source_id, start_time, message_count,
           SUM(new_sprint) OVER (ORDER BY start_time) as sprint_id
    FROM (
        SELECT source_id, start_time, message_count,
               CASE WHEN start_time - LAG(start_time) OVER (ORDER BY start_time) > :gap_hours * 3600
                    THEN 1 ELSE 0 END as new_sprint
        FROM _raw_sources
        WHERE start_time IS NOT NULL
          AND message_count >= 5
          AND source_id NOT LIKE 'agent-%'
    )
)
GROUP BY sprint_id
ORDER BY sprint_id DESC
LIMIT :limit
