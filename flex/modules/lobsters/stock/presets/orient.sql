-- @name: orient
-- @description: Lobsters cell orientation - shape, docs, views, presets, samples
-- @multi: true

-- @query: now
SELECT datetime('now', 'localtime') AS now,
       'UTC' || printf('%+d', cast((julianday('now','localtime') - julianday('now')) * 24 as integer)) AS timezone;

-- @query: about
SELECT value AS description FROM _meta WHERE key = 'description';

-- @query: cell_docs
SELECT scope, name, path, mtime, chars, content
FROM _flex_docs
ORDER BY
    CASE scope
        WHEN 'cell_instructions' THEN 0
        WHEN 'local_notes' THEN 1
        ELSE 2
    END,
    name;

-- @query: shape
SELECT 'chunks' AS what, COUNT(*) AS n FROM _raw_chunks
UNION ALL
SELECT 'sources', COUNT(*) FROM _raw_sources;

-- @query: query_surface
SELECT 'view' AS kind, m.name AS name, GROUP_CONCAT(p.name, ', ') AS columns, '' AS note
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
UNION ALL
SELECT 'table_function', 'vec_ops table source', 'id, score', 'Semantic retrieval after FROM/JOIN'
UNION ALL
SELECT 'table_function', 'keyword table source', 'id, rank, snippet', 'FTS5 keyword retrieval after FROM/JOIN'
ORDER BY kind, name;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: sample
SELECT type, title, author, substr(content, 1, 180) AS preview
FROM chunks
WHERE length(content) > 40
ORDER BY RANDOM()
LIMIT 3;
