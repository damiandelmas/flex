-- @name: orient
-- @description: Full cell orientation — shape, schema, graph intelligence, presets, samples
-- @multi: true
-- NOTE: module-specific orient override

-- @query: now
SELECT datetime('now', 'localtime') as now,
       'UTC' || printf('%+d', cast((julianday('now','localtime') - julianday('now')) * 24 as integer)) as timezone;

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
        WHEN 'chunks' THEN 'UNIFIED surface — all chunks (messages + files). type: user_prompt|assistant|tool_call|file. Narrow via pre-filter: SELECT id FROM chunks WHERE type = ''file''. Default vec_ops target.'
        WHEN 'messages' THEN 'Message chunks only (no file body sub-chunks). Tool ops, identity, delegation. Use chunks view for unified search.'
        WHEN 'agent_key_chunks' THEN 'High-signal agent timeline: prompts, plans, edits, delegation, failed tools, summaries/gaps. Best pre-filter for intent/state queries.'
        WHEN 'files' THEN 'File body sub-chunks only. file, section, ext columns. Use chunks view for unified search.'
        WHEN 'sessions' THEN 'Sources with graph intelligence, fingerprints'
        ELSE ''
    END as note
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
UNION ALL
SELECT 'table_function', 'vec_ops [_raw_chunks]', 'id, score', 'Semantic retrieval — use after FROM/JOIN. Args: table, query, tokens, pre_filter_sql'
UNION ALL
SELECT 'table_function', 'keyword', 'id, rank, snippet', 'FTS5 keyword search — use after FROM/JOIN. keyword(''term'', ''pre_filter_sql'') — optional 2nd arg scopes BM25 ranking'
UNION ALL
SELECT 'table_function', 'chunks_fts', 'rowid, content', 'Raw FTS5 table (prefer keyword() instead). Bridge to vec_ops via: SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid'
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
WITH ranked_key AS (
    SELECT
        session_id,
        substr(content, 1, 160) AS key_label,
        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY key_weight DESC, position ASC
        ) AS rn
    FROM agent_key_chunks
    WHERE length(trim(content)) > 20
)
SELECT g.source_id AS session_id,
    COALESCE(k.key_label, NULLIF(substr(src.title, 1, 160), ''), substr(ess.fingerprint_index, 1, 160)) as label,
    ROUND(g.centrality, 4) as centrality,
    substr(g.community_label, 1, instr(g.community_label || ' ·', ' ·') - 1) AS community
FROM _enrich_source_graph g
JOIN _raw_sources src ON g.source_id = src.source_id
LEFT JOIN _enrich_session_summary ess ON g.source_id = ess.source_id
LEFT JOIN ranked_key k ON k.session_id = g.source_id AND k.rn = 1
WHERE g.is_hub = 1
ORDER BY g.centrality DESC LIMIT 10;

-- @query: communities
SELECT * FROM (
    SELECT
        g.community_id,
        substr(g.community_label, 1, instr(g.community_label || ' ·', ' ·') - 1) AS label,
        substr(g.community_label, instr(g.community_label, ' · ') + 3) AS sub_labels,
        COUNT(*) as sources
    FROM _enrich_source_graph g
    WHERE g.community_label IS NOT NULL
    GROUP BY g.community_id ORDER BY sources DESC LIMIT 10
)
UNION ALL
SELECT NULL,
    (SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph) || ' total ('
    || (SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph WHERE community_label IS NOT NULL)
    || ' labeled)',
    NULL, NULL;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: coverage
-- Identity edges are PARTIAL BY DESIGN. <100% is normal.
-- Only count applicable chunks (e.g. file_uuid only applies to file tools).
WITH file_applicable AS (
    SELECT DISTINCT chunk_id
    FROM _edges_tool_ops
    WHERE tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
      AND target_file IS NOT NULL
      AND target_file NOT LIKE '/tmp/%'
),
target_applicable AS (
    SELECT DISTINCT chunk_id
    FROM _edges_tool_ops
    WHERE target_file IS NOT NULL
      AND target_file NOT LIKE '/tmp/%'
),
mutation_applicable AS (
    SELECT DISTINCT chunk_id
    FROM _edges_tool_ops
    WHERE tool_name IN ('Write','Edit','MultiEdit')
      AND target_file IS NOT NULL
),
webfetch_applicable AS (
    SELECT DISTINCT chunk_id
    FROM _edges_tool_ops
    WHERE tool_name = 'WebFetch'
)
SELECT 'file_uuid' as field,
    ROUND(100.0 * COUNT(DISTINCT fi.chunk_id) / MAX((SELECT COUNT(*) FROM file_applicable), 1), 1) || '%' as coverage,
    'File tools only. Excludes /tmp/.' as note
FROM file_applicable a
LEFT JOIN _edges_file_identity fi ON a.chunk_id = fi.chunk_id
UNION ALL
SELECT 'repo_root',
    ROUND(100.0 *
      (SELECT COUNT(DISTINCT ri.chunk_id) FROM _edges_repo_identity ri
       JOIN target_applicable a ON a.chunk_id = ri.chunk_id) /
      MAX((SELECT COUNT(*) FROM target_applicable), 1), 1) || '%',
    'Files outside git repos have no repo_root.'
UNION ALL
SELECT 'content_hash',
    ROUND(100.0 *
      (SELECT COUNT(DISTINCT ci.chunk_id) FROM _edges_content_identity ci
       JOIN mutation_applicable a ON a.chunk_id = ci.chunk_id) /
      MAX((SELECT COUNT(*) FROM mutation_applicable), 1), 1) || '%',
    'Mutations only. File must exist at capture time.'
UNION ALL
SELECT 'url_uuid',
    ROUND(100.0 *
      (SELECT COUNT(DISTINCT ui.chunk_id) FROM _edges_url_identity ui
       JOIN webfetch_applicable a ON a.chunk_id = ui.chunk_id) /
      MAX((SELECT COUNT(*) FROM webfetch_applicable), 1), 1) || '%',
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
SELECT key_reason, substr(content, 1, 180) as preview
FROM agent_key_chunks
WHERE length(content) > 100
ORDER BY RANDOM() LIMIT 3;
