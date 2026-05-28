-- @name: open-issues
-- @description: Open issues sorted by reactions (score). Pass days=N to filter recency (default 30), repo=owner/name to filter by repo.
-- @params: days, repo

SELECT
    src.source_id,
    src.title,
    src.repo,
    src.author,
    src.score,
    src.num_comments,
    src.url,
    src.labels,
    src.issue_number,
    datetime(MIN(r.timestamp), 'unixepoch') as created_at,
    g.community_id,
    g.is_hub
FROM _raw_sources src
LEFT JOIN _edges_source s ON src.source_id = s.source_id
LEFT JOIN _raw_chunks r ON s.chunk_id = r.id AND s.position = 0
LEFT JOIN _enrich_source_graph g ON src.source_id = g.source_id
WHERE src.state = 'open'
  AND r.timestamp >= (strftime('%s', 'now') - COALESCE(:days, 30) * 86400)
  AND (:repo IS NULL OR src.repo = :repo)
GROUP BY src.source_id
ORDER BY src.score DESC
LIMIT 25;
