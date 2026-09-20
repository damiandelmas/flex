-- @name: stats
-- @description: Exact live visible corpus shape. Runs the full count only when explicitly requested.
-- @multi: true

-- @query: shape
SELECT 'chunks' as what, COUNT(*) as n
FROM _raw_chunks c
JOIN _edges_source es ON es.chunk_id = c.id
JOIN _coding_agent_source_visibility vis
  ON vis.source_id = es.source_id AND vis.visible = 1
UNION ALL
SELECT 'sources', COUNT(*)
FROM _raw_sources src
JOIN _coding_agent_source_visibility vis
  ON vis.source_id = src.source_id AND vis.visible = 1;
