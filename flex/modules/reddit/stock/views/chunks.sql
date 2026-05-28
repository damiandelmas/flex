-- @name: chunks
-- @description: UNIFIED surface — Reddit chunks filtered by scope thresholds. type: post|comment. Use `all_chunks` to bypass the filter.

DROP VIEW IF EXISTS chunks;
CREATE VIEW chunks AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch') AS created_at,
    COALESCE(t.post_type, 'chunk') AS type,
    s.source_id,
    s.position,
    src.title,
    src.subreddit,
    src.url AS thread_url,
    src.score AS thread_score,
    src.num_comments AS thread_comments,
    t.author,
    t.score,
    t.parent_id,
    t.depth,
    t.permalink,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_reddit t ON r.id = t.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id
WHERE (t.author IS NULL OR t.author NOT IN ('AutoModerator', '[deleted]'))
  AND (
    -- Always include our own authored content regardless of scope thresholds
    t.author IN (
      SELECT value FROM json_each(
        COALESCE((SELECT value FROM _meta WHERE key = 'authors'), '[]')
      )
    )
    OR (
      src.score >= CAST(
        COALESCE((SELECT value FROM _meta WHERE key = 'scope.posts.min_score'), '0')
        AS INTEGER)
      AND CASE
        WHEN t.post_type = 'comment' THEN
          COALESCE(t.score, 0) >= CAST(
            COALESCE((SELECT value FROM _meta WHERE key = 'scope.comments.min_score'), '0')
            AS INTEGER)
          AND LENGTH(r.content) >= CAST(
            COALESCE((SELECT value FROM _meta WHERE key = 'scope.comments.min_chars'), '0')
            AS INTEGER)
        ELSE 1
      END
    )
  );
