-- @name: sessions
-- @description: Recent sessions (excludes warmups and micro-sessions)
-- @params: limit (default: 15)

SELECT
    substr(source_id, 1, 8) as session,
    substr(COALESCE(title, ''), 1, 80) as title,
    datetime(start_time, 'unixepoch', 'localtime') as started,
    message_count as ops,
    CASE WHEN is_hub = 1 THEN '*' ELSE '' END as hub
FROM sessions
WHERE message_count >= 5
  AND source_id NOT LIKE 'agent-%'
  AND title != 'Warmup'
ORDER BY start_time DESC
LIMIT :limit
