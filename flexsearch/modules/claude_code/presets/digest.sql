-- @name: digest
-- @description: Multi-day activity summary — sessions, tools, files touched
-- @params: days (default: 7)
-- @multi: true

-- @query: session_count
SELECT COUNT(DISTINCT source_id) as sessions
FROM messages
WHERE timestamp > strftime('%s', 'now', '-' || :days || ' days');

-- @query: active_projects
SELECT project, COUNT(DISTINCT source_id) as sessions
FROM sessions s
WHERE s.start_time > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY project
ORDER BY sessions DESC LIMIT 10;

-- @query: top_tools
SELECT action, COUNT(*) as ops
FROM messages
WHERE action IS NOT NULL
  AND timestamp > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY action
ORDER BY ops DESC LIMIT 10;

-- @query: hot_files
SELECT target_file, COUNT(*) as touches,
    COUNT(DISTINCT source_id) as sessions
FROM messages
WHERE target_file IS NOT NULL
  AND timestamp > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY target_file
ORDER BY touches DESC LIMIT 15;

-- @query: delegations
SELECT d.agent_type, COUNT(*) as spawned
FROM _edges_delegations d
JOIN _raw_chunks c ON d.chunk_id = c.id
WHERE c.timestamp > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY d.agent_type
ORDER BY spawned DESC;
