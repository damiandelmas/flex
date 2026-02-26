-- @name: bridges
-- @description: Cross-community connector sessions

SELECT
    substr(g.source_id, 1, 8) as session,
    s.title,
    g.community_id,
    ROUND(g.centrality, 4) as centrality
FROM _enrich_source_graph g
JOIN _raw_sources s ON g.source_id = s.source_id
WHERE g.is_bridge = 1
ORDER BY g.centrality DESC
LIMIT 20;
