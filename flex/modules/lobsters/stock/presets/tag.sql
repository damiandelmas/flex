-- @name: tag
-- @description: Lobsters stories and comments from threads with a matching tag. Pass tag=python, days=N, limit=N.
-- @params: tag (required), days (default: 30), limit (default: 50)

SELECT
    id,
    type,
    created_at,
    title,
    author,
    score,
    thread_tags,
    thread_url,
    story_url,
    substr(content, 1, 500) AS preview
FROM chunks
WHERE EXISTS (
    SELECT 1
    FROM json_each(COALESCE(thread_tags, '[]')) tag_value
    WHERE tag_value.value = :tag
)
AND created_at >= datetime('now', '-' || COALESCE(:days, 30) || ' days')
ORDER BY timestamp DESC
LIMIT COALESCE(:limit, 50);
