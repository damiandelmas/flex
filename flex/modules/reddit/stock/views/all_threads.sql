-- @name: all_threads
-- @description: Unfiltered source-level surface — every Reddit thread, no score/author filter. Use for exploration and audit.

DROP VIEW IF EXISTS all_threads;
CREATE VIEW all_threads AS
SELECT
    src.source_id,
    src.title,
    src.subreddit,
    src.author,
    src.score,
    src.num_comments,
    src.url,
    src.file_date,
    COUNT(DISTINCT s.chunk_id) AS chunk_count,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_sources src
LEFT JOIN _edges_source s ON src.source_id = s.source_id
LEFT JOIN _enrich_source_graph g ON src.source_id = g.source_id
GROUP BY src.source_id;
