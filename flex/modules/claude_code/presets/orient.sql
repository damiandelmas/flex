-- @name: orient
-- @description: Full cell orientation — shape, schema, graph intelligence, presets, samples
-- @multi: true
-- NOTE: claude_code override — enhances hubs with terse fingerprint_index from _enrich_session_summary

-- @query: about
SELECT value as description FROM _meta WHERE key = 'description';

-- @query: shape
SELECT 'chunks' as what, COUNT(*) as n FROM _raw_chunks
UNION ALL
SELECT 'sources', COUNT(*) FROM _raw_sources;

-- @query: schema
-- Internal tables. Query VIEWS (messages, sessions) instead — they compose these into a clean surface.
SELECT name,
    CASE
        WHEN name LIKE '_raw_%' THEN 'raw (immutable, COMPILE)'
        WHEN name LIKE '_edges_%' THEN 'edges (relationships)'
        WHEN name LIKE '_types_%' THEN 'types (classification)'
        WHEN name LIKE '_enrich_%' THEN 'enrich (mutable, meditate)'
        WHEN name LIKE '_meta' OR name LIKE '_presets' OR name LIKE '_views' OR name LIKE '_ops' THEN 'infrastructure'
        ELSE 'other'
    END as lifecycle
FROM sqlite_master
WHERE type='table' AND name NOT LIKE '%fts%' AND name NOT LIKE '_qmem%'
ORDER BY lifecycle, name;

-- @query: views
SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;

-- @query: view_schemas
SELECT m.name as view_name, GROUP_CONCAT(p.name, ', ') as columns
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
ORDER BY m.name;

-- @query: hints
-- Key patterns for querying this cell. Query VIEWS (messages, sessions), not raw _ tables.
SELECT 'query surface' as pattern,
    'Use messages and sessions views. Tables starting with _ are internal.' as sql
UNION ALL
SELECT 'vec_ops → messages',
    'FROM vec_ops(''_raw_chunks'', ''your query'') v JOIN messages m ON v.id = m.id'
UNION ALL
SELECT 'filter by session',
    'WHERE m.session_id = ''d332a1a0-...'' OR WHERE m.session_id LIKE ''d332a1a0%'''
UNION ALL
SELECT 'vec_ops pre-filter (user prompts only)',
    'vec_ops(''_raw_chunks'', ''query'', ''diverse'', ''SELECT id FROM messages WHERE type = ''''user_prompt'''''')'
UNION ALL
SELECT 'file dedup (SOMA)',
    'GROUP BY COALESCE(json_extract(m.file_uuids, ''$[0]''), m.target_file)'
UNION ALL
SELECT 'session drill-down',
    '@session session=d332a1a0 OR @story session=d332a1a0';

-- @query: hubs
SELECT g.source_id AS session_id,
    COALESCE(substr(ess.fingerprint_index, 1, 120), src.title) as label,
    ROUND(g.centrality, 4) as centrality, g.community_id
FROM _enrich_source_graph g
JOIN _raw_sources src ON g.source_id = src.source_id
LEFT JOIN _enrich_session_summary ess ON g.source_id = ess.source_id
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

-- @query: coverage
-- Identity edges are PARTIAL BY DESIGN. <100% is normal.
-- Only count applicable chunks (e.g. file_uuid only applies to file tools).
SELECT 'file_uuid' as field,
    ROUND(100.0 * COUNT(DISTINCT fi.chunk_id) / MAX(COUNT(DISTINCT t.chunk_id), 1), 1) || '%' as coverage,
    'File tools only. Excludes /tmp/.' as note
FROM _edges_tool_ops t
LEFT JOIN _edges_file_identity fi ON t.chunk_id = fi.chunk_id
WHERE t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
  AND t.target_file NOT LIKE '/tmp/%'
UNION ALL
SELECT 'repo_root',
    ROUND(100.0 *
      (SELECT COUNT(DISTINCT ri.chunk_id) FROM _edges_repo_identity ri) /
      MAX((SELECT COUNT(DISTINCT t2.chunk_id) FROM _edges_tool_ops t2
       WHERE t2.target_file IS NOT NULL AND t2.target_file NOT LIKE '/tmp/%'), 1), 1) || '%',
    'Files outside git repos have no repo_root.'
UNION ALL
SELECT 'content_hash',
    ROUND(100.0 *
      (SELECT COUNT(DISTINCT ci.chunk_id) FROM _edges_content_identity ci) /
      MAX((SELECT COUNT(DISTINCT t3.chunk_id) FROM _edges_tool_ops t3
       WHERE t3.tool_name IN ('Write','Edit','MultiEdit') AND t3.target_file IS NOT NULL), 1), 1) || '%',
    'Mutations only. File must exist at capture time.'
UNION ALL
SELECT 'url_uuid',
    ROUND(100.0 *
      (SELECT COUNT(DISTINCT ui.chunk_id) FROM _edges_url_identity ui) /
      MAX((SELECT COUNT(DISTINCT t4.chunk_id) FROM _edges_tool_ops t4
       WHERE t4.tool_name = 'WebFetch'), 1), 1) || '%',
    'WebFetch only.'
UNION ALL
SELECT 'parent_uuid',
    ROUND(100.0 *
      (SELECT COUNT(*) FROM _types_message WHERE parent_uuid IS NOT NULL) /
      MAX((SELECT COUNT(*) FROM _types_message), 1), 1) || '%',
    'From JSONL files. Missing = JSONL deleted or pre-deploy.'
UNION ALL
SELECT 'raw_content',
    (SELECT COUNT(*) FROM _raw_content) || ' rows',
    'Tool inputs/outputs. JOIN via _edges_raw_content.';

-- @query: sample
SELECT substr(content, 1, 150) as preview FROM _raw_chunks
WHERE length(content) > 100 ORDER BY RANDOM() LIMIT 3;
