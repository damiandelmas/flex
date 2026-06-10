-- @name: orient
-- @description: Hacker News cell orientation — docs, views, presets, samples
-- @multi: true

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
SELECT 'threads', COUNT(*) FROM _raw_sources;

-- @query: views
SELECT m.name AS view_name, GROUP_CONCAT(p.name, ', ') AS columns
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
ORDER BY m.name;

-- @query: query_surface
SELECT 'chunks' AS surface,
       'chunk-level rows; type is story or comment; join keyword search results on chunks.id' AS use
UNION ALL
SELECT 'threads',
       'one row per HN story thread with score, comment count, URL, and graph columns'
UNION ALL
SELECT 'keyword search',
       'use the keyword table function in FROM or JOIN, then join chunks on id'
UNION ALL
SELECT 'semantic search',
       'use the semantic table function in FROM or JOIN, then join chunks on id';

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: top_threads
SELECT title, score, num_comments, url
FROM threads
ORDER BY score DESC
LIMIT 10;

-- @query: sample
SELECT created_at, type, author, score, title, substr(content, 1, 180) AS preview
FROM chunks
ORDER BY timestamp DESC
LIMIT 5;
