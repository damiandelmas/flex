-- @name: threads
-- @description: Source-level surface for Reddit cells. Filtered by scope.posts.min_score from _meta. Use `all_threads` to bypass the filter.

DROP VIEW IF EXISTS threads;
CREATE VIEW threads AS
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
WHERE src.author NOT IN ('AutoModerator', '[deleted]')
  AND (
    -- Always include threads authored by us regardless of score
    src.author IN (
      SELECT value FROM json_each(
        COALESCE((SELECT value FROM _meta WHERE key = 'authors'), '[]')
      )
    )
    OR src.score >= CAST(
      COALESCE((SELECT value FROM _meta WHERE key = 'scope.posts.min_score'), '0')
      AS INTEGER)
  )
GROUP BY src.source_id;
