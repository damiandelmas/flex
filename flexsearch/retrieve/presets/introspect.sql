-- @name: introspect
-- @description: Full cell orientation — shape, schema, samples
-- @multi: true

-- @query: about
SELECT value as description FROM _meta WHERE key = 'description';

-- @query: tables
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%fts%' AND name NOT LIKE '_qmem%' ORDER BY name;

-- @query: views
SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;

-- @query: shape
SELECT 'chunks' as what, COUNT(*) as n FROM _raw_chunks
UNION ALL
SELECT 'sources', COUNT(*) FROM _raw_sources;

-- @query: schema_chunks
SELECT name, type FROM pragma_table_info('_raw_chunks');

-- @query: edge_tables
SELECT name, (SELECT COUNT(*) FROM sqlite_master sm2 WHERE sm2.name = sm.name) as exists_flag
FROM sqlite_master sm WHERE type='table' AND name LIKE '_edges_%';

-- @query: enrich_tables
SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_enrich_%';

-- @query: sample
SELECT substr(content, 1, 150) as preview FROM _raw_chunks
WHERE length(content) > 100 ORDER BY RANDOM() LIMIT 3;
