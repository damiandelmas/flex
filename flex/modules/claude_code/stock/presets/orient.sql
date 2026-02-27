-- @name: orient
-- @description: Full cell orientation — shape, schema, graph intelligence, presets, samples
-- @multi: true
-- NOTE: claude_code override — enhances hubs with terse fingerprint_index from _enrich_session_summary

-- @query: now
SELECT datetime('now', 'localtime') as now;

-- @query: about
SELECT value as description FROM _meta WHERE key = 'description';

-- @query: shape
SELECT 'chunks' as what, COUNT(*) as n FROM _raw_chunks
UNION ALL
SELECT 'sources', COUNT(*) FROM _raw_sources;

-- @query: query_surface
-- Everything composable in one section: views (primary), table functions, edge tables for explicit JOIN.
SELECT 'view' as kind, m.name as name, GROUP_CONCAT(p.name, ', ') as columns,
    CASE m.name
        WHEN 'messages' THEN 'type: user_prompt|assistant|tool_call. file_body: full file for Write, diff payload for Edit (NULL for non-file ops). Tool ops, identity, message type'
        WHEN 'sessions' THEN 'Sources with graph intelligence, fingerprints'
        ELSE ''
    END as note
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
UNION ALL
SELECT 'table_function', 'vec_ops [_raw_chunks]', 'id, score', 'Semantic retrieval — use after FROM/JOIN. Args: table, query, tokens, pre_filter_sql'
UNION ALL
SELECT 'table_function', 'chunks_fts', 'rowid, content', 'FTS5 keyword search (MATCH). Bridge to vec_ops via: SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid'
UNION ALL
SELECT 'edge_table', '_edges_raw_content', 'chunk_id, content_hash', 'Bridge to _raw_content(hash, content). Use file_body in messages view instead'
UNION ALL
SELECT 'edge_table', '_edges_delegations', 'chunk_id, child_session_id, agent_type, parent_source_id', 'Parent→child agent tree (recursive CTE)'
UNION ALL
SELECT 'edge_table', '_edges_content_identity', 'chunk_id, content_hash, blob_hash, old_blob_hash', 'Git content identity'
UNION ALL
SELECT 'edge_table', '_edges_repo_identity', 'chunk_id, repo_root', 'Repo root hash → _enrich_repo_identity lookup'
ORDER BY kind, name;


-- @query: hubs
SELECT g.source_id AS session_id,
    COALESCE(substr(ess.fingerprint_index, 1, 120), src.title) as label,
    ROUND(g.centrality, 4) as centrality,
    substr(g.community_label, 1, instr(g.community_label || ' ·', ' ·') - 1) AS community
FROM _enrich_source_graph g
JOIN _raw_sources src ON g.source_id = src.source_id
LEFT JOIN _enrich_session_summary ess ON g.source_id = ess.source_id
WHERE g.is_hub = 1
ORDER BY g.centrality DESC LIMIT 10;

-- @query: communities
SELECT
    g.community_id,
    substr(g.community_label, 1, instr(g.community_label || ' ·', ' ·') - 1) AS label,
    substr(g.community_label, instr(g.community_label, ' · ') + 3) AS sub_labels,
    COUNT(*) as sources
FROM _enrich_source_graph g
GROUP BY g.community_id ORDER BY sources DESC LIMIT 8;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

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
