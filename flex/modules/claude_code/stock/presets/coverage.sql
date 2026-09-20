-- @name: coverage
-- @description: Exact live identity and provenance coverage audit. Expensive by design; run explicitly.
-- @multi: true

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
    ROUND(100.0 * COUNT(DISTINCT fi.chunk_id) /
          MAX((SELECT COUNT(*) FROM file_applicable), 1), 1) || '%' as coverage,
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
      (SELECT COUNT(*)
       FROM _types_message tm
       JOIN _edges_source es ON es.chunk_id = tm.chunk_id
       JOIN _coding_agent_source_visibility vis
         ON vis.source_id = es.source_id AND vis.visible = 1
       WHERE parent_uuid IS NOT NULL) /
      MAX((SELECT COUNT(*)
           FROM _types_message tm
           JOIN _edges_source es ON es.chunk_id = tm.chunk_id
           JOIN _coding_agent_source_visibility vis
             ON vis.source_id = es.source_id AND vis.visible = 1), 1), 1) || '%',
    'From JSONL files. Missing = JSONL deleted or pre-deploy.'
UNION ALL
SELECT 'raw_content',
    (SELECT COUNT(*) FROM _raw_content) || ' rows',
    'Tool inputs/outputs. JOIN via _edges_raw_content.'
UNION ALL
SELECT 'primary_cwd',
    ROUND(100.0 *
      (SELECT COUNT(*)
       FROM _raw_sources src
       JOIN _coding_agent_source_visibility vis
         ON vis.source_id = src.source_id AND vis.visible = 1
       WHERE primary_cwd IS NOT NULL AND primary_cwd != '') /
      MAX((SELECT COUNT(*)
           FROM _raw_sources src
           JOIN _coding_agent_source_visibility vis
             ON vis.source_id = src.source_id AND vis.visible = 1), 1), 1) || '%',
    'Session launch context. Distinct from repositories touched later.';
