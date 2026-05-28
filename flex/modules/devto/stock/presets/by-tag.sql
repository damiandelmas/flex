-- @name: by-tag
-- @description: Recent Dev.to chunks matching a tag. Params: tag, limit.
-- @params: tag, limit (default: 50)

SELECT
    c.id,
    c.created_at,
    c.type,
    c.title,
    c.author,
    c.score,
    c.article_url,
    c.tags,
    substr(c.content, 1, 500) AS preview
FROM chunks c
WHERE c.tags LIKE '%' || :tag || '%'
ORDER BY c.timestamp DESC
LIMIT COALESCE(:limit, 50);
