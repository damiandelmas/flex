-- @name: orient
-- @description: Filesystem structural cell — the live contract for a recipe-driven, no-embed folder union.
-- @multi: true
--

-- @query: now
SELECT datetime('now','localtime') AS now;

-- @query: about
SELECT COALESCE((SELECT value FROM _meta WHERE key='description'),
                'Filesystem structural cell') AS description,
       'filesystem-structural' AS profile;

-- @query: embed_mode
SELECT 'embed-off — use keyword() plus structural SQL; vec_ops/similar are unavailable.' AS embed_mode;

-- @query: shape
SELECT 'source_rows' AS what, COUNT(*) AS n FROM _raw_sources
UNION ALL SELECT 'raw_chunk_rows', COUNT(*) FROM _raw_chunks
UNION ALL SELECT 'projected_chunk_rows', COUNT(*) FROM chunks
UNION ALL SELECT 'distinct_projected_chunk_ids', COUNT(DISTINCT id) FROM chunks
UNION ALL SELECT 'embedded_raw_chunks', COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL;

-- @query: columns
SELECT 'chunks' AS view, group_concat(name, ', ') AS columns FROM pragma_table_info('chunks')
UNION ALL
SELECT 'sources', group_concat(name, ', ') FROM pragma_table_info('sources');

-- @query: search_surface
SELECT 'keyword(''term'', ''SELECT id FROM chunks'')' AS how,
       'FTS5 exact-term/phrase search; scope with the second argument or source_id.' AS note
UNION ALL
SELECT 'source_id',
       'Absolute source path — the primary structural axis for path scoping.'
UNION ALL
SELECT 'file_uuid',
       'Optional stable file identity for joining exact file provenance to other cells.';

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: path_roots
SELECT value AS root
FROM json_each(COALESCE((SELECT value FROM _meta WHERE key='selections'), '[]'))
ORDER BY root;

-- @query: method
SELECT 'flex' AS skill,
       'inspect this contract first; use keyword() and path-scoped structural SQL for no-embed retrieval.';
