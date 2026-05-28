-- @name: top
-- @description: Highest-score Lobsters stories and comments. Pass days=N and limit=N.
-- @params: days (default: 30), limit (default: 50)

SELECT
    id,
    type,
    created_at,
    title,
    author,
    score,
    thread_score,
    thread_comments,
    thread_url,
    story_url,
    substr(content, 1, 500) AS preview
FROM chunks
WHERE created_at >= datetime('now', '-' || COALESCE(:days, 30) || ' days')
ORDER BY COALESCE(score, 0) DESC, timestamp DESC
LIMIT COALESCE(:limit, 50);
