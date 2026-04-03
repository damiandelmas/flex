-- @name: genealogy
-- @description: Trace a concept's lineage — timeline, hubs, key excerpts
-- @params: concept (required)
-- @multi: true

-- @query: timeline
SELECT DISTINCT src.source_id, src.file_date, src.title
FROM _raw_chunks c
JOIN _edges_source e ON c.id = e.chunk_id
JOIN _raw_sources src ON e.source_id = src.source_id
WHERE c.content LIKE '%' || :concept || '%'
ORDER BY src.file_date;

-- @query: hub_sources
SELECT DISTINCT src.source_id, src.title,
    ROUND(g.centrality, 4) as centrality
FROM _raw_chunks c
JOIN _edges_source e ON c.id = e.chunk_id
JOIN _raw_sources src ON e.source_id = src.source_id
JOIN _enrich_source_graph g ON src.source_id = g.source_id
WHERE c.content LIKE '%' || :concept || '%'
  AND (g.is_hub = 1 OR g.is_bridge = 1)
ORDER BY g.centrality DESC LIMIT 10;

-- @query: excerpts
SELECT substr(c.content, 1, 300) as excerpt, e.source_id
FROM _raw_chunks c
JOIN _edges_source e ON c.id = e.chunk_id
WHERE c.content LIKE '%' || :concept || '%'
ORDER BY c.timestamp DESC
LIMIT 5;
