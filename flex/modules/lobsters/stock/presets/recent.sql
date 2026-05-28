-- @name: recent
-- @description: Recent Lobsters stories and comments. Pass days=N and limit=N.
-- @params: days (default: 7), limit (default: 50)

SELECT
    id,
    type,
    created_at,
    title,
    author,
    score,
    thread_url,
    story_url,
    substr(content, 1, 500) AS preview
FROM chunks
WHERE created_at >= datetime('now', '-' || COALESCE(:days, 7) || ' days')
ORDER BY timestamp DESC
LIMIT COALESCE(:limit, 50);
