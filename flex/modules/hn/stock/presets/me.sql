-- @name: me
-- @description: HN stories + comments authored by us (from _meta.authors). Pass days=N for recency filter (default 30).
-- @params: days (default: 30)

SELECT
    c.id,
    c.content,
    c.created_at,
    c.type,
    c.author,
    c.score,
    c.title as thread_title,
    c.thread_url,
    c.hn_url,
    c.thread_score,
    c.story_id,
    c.parent_id
FROM chunks c
WHERE c.author IN (
    SELECT value FROM json_each(
        COALESCE((SELECT value FROM _meta WHERE key = 'authors'), '[]')
    )
)
AND c.created_at >= datetime('now', '-' || COALESCE(:days, 30) || ' days')
ORDER BY c.created_at DESC
LIMIT 200;
