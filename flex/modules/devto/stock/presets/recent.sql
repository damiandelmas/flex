-- @name: recent
-- @description: Recent Dev.to articles and comments. Params: limit.
-- @params: limit (default: 50)

SELECT
    id,
    created_at,
    type,
    title,
    author,
    score,
    article_url,
    tags,
    substr(content, 1, 500) AS preview
FROM chunks
ORDER BY timestamp DESC
LIMIT COALESCE(:limit, 50);
