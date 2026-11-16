-- @name: orient
-- @description: Hacker News cell orientation — views, presets, samples
-- @multi: true

-- @query: about
SELECT value AS description FROM _meta WHERE key = 'description';

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
       'chunk-level rows; type is story or comment; join retrieval results on chunks.id' AS use
UNION ALL
SELECT 'threads',
       'one row per HN story thread with score, comment count, URL, and graph columns'
UNION ALL
SELECT 'keyword search',
       'use the keyword table function in FROM or JOIN, then join chunks on id'
WHERE NOT EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'semantic search',
       'use the vec_ops table function in FROM or JOIN, then join chunks on id'
WHERE NOT EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'keyword search',
       'chunks_fts MATCH ''term'' or ''"exact phrase"'' (FTS5); bm25(chunks_fts) ascending is best; bridge to chunks via _raw_chunks.rowid = chunks_fts.rowid'
UNION ALL
SELECT 'semantic search',
       'embed the question once in a MATERIALIZED CTE: vec_slice(llm_embed_generate(''search_query: '' || :q), 0, 256); score _raw_chunks.embedding with 1 - vec_distance_cosine(vec_slice(embedding, 0, 256), q), then join chunks on id'
WHERE EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine');

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
