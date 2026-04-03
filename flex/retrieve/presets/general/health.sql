-- @name: health
-- @description: Pipeline health check. Chunk/source counts, embedding coverage, graph freshness, recent ops.
-- @multi: true

-- @query: counts
SELECT
    (SELECT COUNT(*) FROM _raw_chunks) as total_chunks,
    (SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NULL) as unembedded_chunks,
    (SELECT COUNT(*) FROM _raw_sources) as total_sources,
    (SELECT COUNT(*) FROM _raw_sources WHERE embedding IS NULL) as unembedded_sources;

-- @query: graph
SELECT
    COALESCE(
        (SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_enrich_source_graph'),
        0
    ) as graph_table_exists,
    COALESCE(
        (SELECT COUNT(*) FROM _enrich_source_graph),
        0
    ) as enriched_sources,
    (SELECT datetime(MAX(timestamp), 'unixepoch', 'localtime') FROM _ops
     WHERE operation = 'build_similarity_graph') as last_graph_build,
    (SELECT COUNT(*) FROM _ops
     WHERE operation = 'incremental_index'
       AND timestamp > COALESCE(
           (SELECT MAX(timestamp) FROM _ops WHERE operation = 'build_similarity_graph'),
           0
       )) as sources_since_graph;

-- @query: queue
SELECT
    json_extract(params, '$.claude_code') as claude_code_pending,
    datetime(timestamp, 'unixepoch', 'localtime') as checked_at
FROM _ops
WHERE operation = 'queue_snapshot'
ORDER BY timestamp DESC
LIMIT 1;

-- @query: recent_ops
SELECT operation, target,
    datetime(timestamp, 'unixepoch', 'localtime') as when_run,
    rows_affected
FROM _ops
ORDER BY timestamp DESC
LIMIT 10;
