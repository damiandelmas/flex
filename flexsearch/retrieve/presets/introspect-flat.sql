-- @name: introspect-flat
-- @description: Cell orientation for flat-table schema (pre chunk-atom)
-- @multi: true

-- @query: about
SELECT value as description FROM _meta WHERE key = 'description';

-- @query: tables
SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%fts%' AND name NOT LIKE '_qmem%' AND name NOT LIKE 'sqlite%' ORDER BY name;

-- @query: shape
SELECT 'chunks' as what, COUNT(*) as n FROM chunks UNION ALL SELECT 'sessions', COUNT(*) FROM docs;

-- @query: schema_docs
SELECT name, type FROM pragma_table_info('docs');

-- @query: schema_chunks
SELECT name, type FROM pragma_table_info('chunks');

-- @query: facets
SELECT facet, COUNT(*) as n FROM docs GROUP BY facet ORDER BY n DESC LIMIT 10;

-- @query: tools
SELECT tool_name, COUNT(*) as n FROM chunks WHERE tool_name IS NOT NULL GROUP BY tool_name ORDER BY n DESC LIMIT 10;

-- @query: hubs
SELECT substr(id,1,12) as session, ROUND(centrality,3) as cent FROM docs WHERE is_hub=1 ORDER BY centrality DESC LIMIT 10;

-- @query: communities
SELECT community_id, COUNT(*) as size FROM docs WHERE community_id IS NOT NULL GROUP BY community_id ORDER BY size DESC LIMIT 10;

-- @query: sample
SELECT role, substr(content,1,150) as preview FROM chunks WHERE length(content)>100 ORDER BY RANDOM() LIMIT 3;
