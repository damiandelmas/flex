-- @name: digest
-- @description: Multi-day activity summary — sessions, tools, files touched
-- @params: days (default: 7)
-- @multi: true

-- @query: session_count
SELECT COUNT(DISTINCT session_id) as sessions
FROM messages
WHERE timestamp > strftime('%s', 'now', '-' || :days || ' days');

-- @query: active_projects
SELECT project, COUNT(DISTINCT session_id) as sessions
FROM sessions s
WHERE s.started_at > datetime('now', '-' || :days || ' days', 'localtime')
GROUP BY project
ORDER BY sessions DESC LIMIT 10;

-- @query: top_tools
SELECT tool_name, COUNT(*) as ops
FROM messages
WHERE tool_name IS NOT NULL
  AND timestamp > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY tool_name
ORDER BY ops DESC LIMIT 10;

-- @query: hot_files
SELECT target_file, COUNT(*) as touches,
    COUNT(DISTINCT session_id) as sessions
FROM messages
WHERE target_file IS NOT NULL
  AND timestamp > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY target_file
ORDER BY touches DESC LIMIT 15;

-- @query: delegations
SELECT d.agent_type, COUNT(*) as spawned
FROM _edges_delegations d
WHERE d.created_at > strftime('%s', 'now', '-' || :days || ' days')
GROUP BY d.agent_type
ORDER BY spawned DESC;
