-- @name: me
-- @description: Dev.to articles + comments authored by us (from _meta.authors). Pass days=N for recency filter (default 30).
-- @params: days (default: 30)

SELECT
    c.id,
    c.content,
    c.created_at,
    c.type,
    c.title,
    c.author,
    c.score,
    c.url,
    c.article_url,
    c.article_score,
    c.article_comments,
    c.tags,
    c.reading_time
FROM chunks c
WHERE c.author IN (
    SELECT value FROM json_each(
        COALESCE((SELECT value FROM _meta WHERE key = 'authors'), '[]')
    )
)
AND c.created_at >= datetime('now', '-' || COALESCE(:days, 30) || ' days')
ORDER BY c.created_at DESC
LIMIT 200;
