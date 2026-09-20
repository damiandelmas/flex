-- @name: orient
-- @description: Full query orientation — instructions, schema, graph intelligence, presets, samples
-- @multi: true
-- NOTE: module-specific orient override

-- @query: now
SELECT datetime('now', 'localtime') as now,
       'UTC' || printf('%+d', cast((julianday('now','localtime') - julianday('now')) * 24 as integer)) as timezone;

-- @query: about
SELECT value as description FROM _meta WHERE key = 'description';

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

-- @query: query_surface
-- Everything composable in one section: views (primary), table functions, edge tables for explicit JOIN.
SELECT 'view' as kind, m.name as name, GROUP_CONCAT(p.name, ', ') as columns,
    CASE m.name
        WHEN 'chunks' THEN 'UNIFIED search surface. Treat content as retrieval clues; tool-output chunks may be clipped. Use @full id=... for source body recovery.'
        WHEN 'messages' THEN 'Message/tool surface. file_body holds full tool IO/file bodies when available. Compare length(content) vs length(file_body).'
        WHEN 'agent_key_chunks' THEN 'High-signal agent timeline: prompts, plans, edits, delegation, failed tools, summaries/gaps. Best pre-filter for intent/state queries.'
        WHEN 'files' THEN 'File body sub-chunks only. file, section, ext columns. Use chunks view for unified search.'
        WHEN 'sessions' THEN 'Sources with graph intelligence, fingerprints'
        WHEN 'session_repository_evidence' THEN 'Occurrence-grained session→repository/path evidence. Query repo_path when resolved; evidence_path always preserves the observed path.'
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

-- @query: source_recovery
SELECT 'full_body' AS mode,
       '@full id=<message_or_chunk_id>' AS query,
       'Recover best full body. Climbs from clipped Chunk ID output rows to sibling messages.file_body when possible.' AS note
UNION ALL
SELECT 'path_observation',
       '@observed-file path=<path-fragment>',
       'Find target_file hits plus Bash/stdout observations such as sed, cat, rg, or generated heredocs.'
UNION ALL
SELECT 'path_timeline',
       '@file-history path=<path-fragment>',
       'Ordered mutations, reads, target-file touches, and stdout observations.'
UNION ALL
SELECT 'exact_phrase',
       'keyword(''\"multi word phrase\"'', ''SELECT id FROM chunks'')',
       'Quote multi-word names/brands/titles inside keyword() to avoid tokenization noise.'
UNION ALL
SELECT 'manual_check',
       'SELECT id, length(content), length(file_body) FROM messages WHERE id = ...',
       'If file_body is longer than content, prefer file_body; chunks.content is the clue, not the source.';

-- @query: hubs
SELECT g.source_id AS session_id,
    COALESCE(NULLIF(substr(src.title, 1, 160), ''),
             substr(ess.fingerprint_index, 1, 160)) AS label,
    ROUND(g.centrality, 4) AS centrality,
    substr(g.community_label, 1,
           instr(g.community_label || ' ·', ' ·') - 1) AS community
FROM _enrich_source_graph g
JOIN _raw_sources src ON src.source_id = g.source_id
LEFT JOIN _types_source_warmup w ON w.source_id = src.source_id
LEFT JOIN _enrich_session_summary ess ON ess.source_id = src.source_id
WHERE g.is_hub = 1
  AND COALESCE(w.is_warmup_only, 0) = 0
  AND NOT EXISTS (
      SELECT 1 FROM _meta m, json_each(m.value) j
      WHERE m.key = 'exclude_sessions'
        AND src.source_id LIKE '%' || j.value || '%'
  )
ORDER BY g.centrality DESC
LIMIT 10;

-- @query: communities
SELECT * FROM (
    SELECT
        g.community_id,
        substr(g.community_label, 1, instr(g.community_label || ' ·', ' ·') - 1) AS label,
        substr(g.community_label, instr(g.community_label, ' · ') + 3) AS sub_labels,
        COUNT(*) as sources
    FROM _enrich_source_graph g
    JOIN _coding_agent_source_visibility vis ON vis.source_id = g.source_id AND vis.visible = 1
    WHERE g.community_label IS NOT NULL
    GROUP BY g.community_id ORDER BY sources DESC LIMIT 10
)
UNION ALL
SELECT NULL,
    (SELECT COUNT(DISTINCT g.community_id)
     FROM _enrich_source_graph g
     JOIN _coding_agent_source_visibility vis ON vis.source_id = g.source_id AND vis.visible = 1) || ' total ('
    || (SELECT COUNT(DISTINCT g.community_id)
        FROM _enrich_source_graph g
        JOIN _coding_agent_source_visibility vis ON vis.source_id = g.source_id AND vis.visible = 1
        WHERE g.community_label IS NOT NULL)
    || ' labeled)',
    NULL, NULL;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: sample
SELECT substr(content, 1, 180) as preview
FROM _raw_chunks c
JOIN _edges_source es ON es.chunk_id = c.id
JOIN _coding_agent_source_visibility vis ON vis.source_id = es.source_id AND vis.visible = 1
WHERE length(content) > 100
LIMIT 3;
