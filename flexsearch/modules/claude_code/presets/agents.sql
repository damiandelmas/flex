-- @name: agents
-- @description: Find spawned agents and their parents
-- @params: session (required)

SELECT
    substr(m.source_id, 1, 8) as parent,
    d.agent_type,
    substr(d.child_doc_id, 1, 12) as child,
    datetime(m.timestamp, 'unixepoch', 'localtime') as spawned_at
FROM _edges_delegations d
JOIN messages m ON d.chunk_id = m.id
WHERE m.source_id LIKE '%' || :session || '%'
   OR d.child_doc_id LIKE '%' || :session || '%'
ORDER BY m.timestamp
