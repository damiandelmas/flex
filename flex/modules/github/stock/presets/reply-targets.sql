-- @name: reply-targets
-- @description: Open issues with few comments that look like questions -- good candidates for engagement. Pass days=N (default 14).
-- @params: days

SELECT
    r.id,
    r.content,
    t.item_type,
    t.author,
    t.repo,
    t.issue_number,
    t.score,
    t.url,
    datetime(r.timestamp, 'unixepoch') as created_at,
    src.title AS issue_title,
    src.score AS issue_score,
    src.num_comments AS issue_comments,
    src.state
FROM _raw_chunks r
JOIN _types_github t ON r.id = t.chunk_id
JOIN _edges_source s ON r.id = s.chunk_id
JOIN _raw_sources src ON s.source_id = src.source_id
WHERE r.timestamp >= (strftime('%s', 'now') - COALESCE(:days, 14) * 86400)
  AND src.state = 'open'
  AND s.position = 0
  AND src.num_comments <= 3
  AND length(r.content) > 50
  AND (r.content LIKE '%?%' OR r.content LIKE '%how %' OR r.content LIKE '%why %'
       OR r.content LIKE '%issue%' OR r.content LIKE '%bug%' OR r.content LIKE '%feature%')
ORDER BY r.timestamp DESC
LIMIT 30;
