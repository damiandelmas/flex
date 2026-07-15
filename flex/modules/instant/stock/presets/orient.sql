-- @name: orient
-- @description: flex-fs (instant) cell — the LIVE query contract for a no-embed filesystem cell.
--   The agentic PRAGMA: what THIS cell actually is (columns, shape, presets, path roots, embed mode).
--   It COMPLEMENTS the flex:instant / flex:code skills — the query METHOD (scopes, the file_uuid
--   hinge, recipes) lives in the skill, not here. Columns are DISCOVERED (pragma_table_info) so this
--   one orient is honest on a flat cell and a --nest/--code cell alike.
-- @multi: true
--

-- @query: now
SELECT datetime('now','localtime') AS now;

-- @query: about
SELECT COALESCE((SELECT value FROM _meta WHERE key='description'), 'instant filesystem cell') AS description;

-- @query: embed_mode
-- The one honesty conditional. An instant fs cell is no-embed by default (no _meta.embed key),
-- so vec_ops/similar are unavailable — use keyword() + structural SQL. Flips only if built --embed.
SELECT CASE
  WHEN lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'false')) IN ('false','0','off','no')
  THEN 'embed-off — structural cell: keyword() FTS + path scoping + file identity (and node tree / call graph when built --nest/--code). vec_ops/similar are unavailable.'
  ELSE 'embed-on — this fs cell also carries embeddings: vec_ops/similar are available.'
END AS embed_mode;

-- @query: shape
SELECT 'source_rows' AS what, COUNT(*) AS n FROM _raw_sources
UNION ALL SELECT 'raw_chunk_rows', COUNT(*) FROM _raw_chunks
UNION ALL SELECT 'projected_chunk_rows', COUNT(*) FROM chunks
UNION ALL SELECT 'distinct_projected_chunk_ids', COUNT(DISTINCT id) FROM chunks
UNION ALL SELECT 'embedded_raw_chunks', COALESCE((SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL),0);

-- @query: columns
-- DISCOVERED from the live views — never hardcoded. This is the agentic PRAGMA: the exact
-- columns to query on THIS cell (a flat cell lacks section_title/etc.; a --nest/--code cell has them).
SELECT 'chunks' AS view, group_concat(name, ', ') AS columns FROM pragma_table_info('chunks')
UNION ALL
SELECT 'sources', group_concat(name, ', ') FROM pragma_table_info('sources');

-- @query: search_surface
-- Fixed for this engine: keyword() is the retrieval primitive; source_id IS the file path.
SELECT 'keyword(''term'', ''SELECT id FROM chunks'')' AS how, 'FTS5 exact-term/phrase search — the retrieval primitive (no vec_ops on a no-embed cell). Scope with the 2nd arg or a source_id LIKE filter.' AS note
UNION ALL
SELECT 'source_id', 'The clean absolute file path — the primary structural axis. WHERE source_id LIKE ''%/dir/%'' to scope; there is no separate path column.'
UNION ALL
SELECT 'file_uuid', 'SOMA identity — join OUT to a session cell (claude_code/codex) to see who touched a file. The two-step hinge is in the flex:instant skill.';

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: path_roots
-- A few coarse source_id prefixes so an agent sees the corpus's real path structure to scope against
-- (grounds the skill's generic `source_id LIKE` scoping to THIS cell).
SELECT value AS root
FROM json_each(COALESCE((SELECT value FROM _meta WHERE key='selections'), '[]'))
ORDER BY root;

-- @query: method
-- Pointer, not pedagogy: the query METHOD lives in the skill; this orient is the live contract.
SELECT 'flex:instant' AS skill, 'scopes (:teams/:runway/path), the file_uuid session hinge, recipes, limits' AS owns
UNION ALL
SELECT 'flex:code', 'code cells only (--code): @callers/@callees/@impact/@subtree, call/import graph';
