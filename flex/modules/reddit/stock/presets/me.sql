-- @name: me
-- @description: Content authored by us (from _meta.authors). Posts + replies we've made. Pass days=N for recency filter (default 30).
-- @params: days (default: 30)

SELECT
    c.id,
    c.content,
    c.created_at,
    c.type,
    c.subreddit,
    c.thread_url,
    c.title as thread_title,
    c.author,
    c.score as my_score,
    c.thread_score,
    c.thread_comments,
    c.permalink
FROM chunks c
WHERE c.author IN (
    SELECT value FROM json_each(
        COALESCE((SELECT value FROM _meta WHERE key = 'authors'), '[]')
    )
)
AND c.created_at >= datetime('now', '-' || COALESCE(:days, 30) || ' days')
ORDER BY c.created_at DESC
LIMIT 200;
