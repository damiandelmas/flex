-- @name: orient
-- @description: Full cell orientation — shape, schema, graph intelligence, presets, samples
-- @multi: true

-- @query: now
SELECT datetime('now', 'localtime') as now;

-- @query: about
SELECT value as description FROM _meta WHERE key = 'description';

-- @query: shape
SELECT 'chunks' as what, COUNT(*) as n FROM _raw_chunks
UNION ALL
SELECT 'sources', COUNT(*) FROM _raw_sources;

-- @query: query_surface
-- Everything composable: views (primary), table functions, edge tables for explicit JOIN.
SELECT 'view' as kind, m.name as name, GROUP_CONCAT(p.name, ', ') as columns, '' as note
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
UNION ALL
SELECT 'table_function', 'vec_ops(''_raw_chunks'', ...)', 'id, score', 'Semantic retrieval — use after FROM/JOIN'
UNION ALL
SELECT 'table_function', 'chunks_fts', 'rowid, content', 'FTS5 keyword search (MATCH). Bridge to vec_ops via: SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid'
ORDER BY kind, name;

-- @query: hubs
SELECT g.source_id, src.title as label,
    ROUND(g.centrality, 4) as centrality, g.community_id
FROM _enrich_source_graph g
JOIN _raw_sources src ON g.source_id = src.source_id
WHERE g.is_hub = 1
ORDER BY g.centrality DESC LIMIT 10;

-- @query: communities
SELECT g.community_id, COUNT(*) as sources
FROM _enrich_source_graph g
GROUP BY g.community_id ORDER BY sources DESC LIMIT 8;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: retrieval
SELECT key, value FROM _meta WHERE key LIKE 'retrieval:%' ORDER BY key;

-- @query: sample
SELECT substr(content, 1, 150) as preview FROM _raw_chunks
WHERE length(content) > 100 ORDER BY RANDOM() LIMIT 3;
