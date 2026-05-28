-- @name: tags
-- @description: Count indexed Dev.to articles by tag.

WITH article_tags AS (
    SELECT
        COALESCE(j.value, c.tags) AS tag,
        c.id
    FROM chunks c
    LEFT JOIN json_each(c.tags) j
    WHERE c.type = 'article'
)
SELECT
    tag,
    COUNT(DISTINCT id) AS articles
FROM article_tags
WHERE tag IS NOT NULL AND tag != ''
GROUP BY tag
ORDER BY articles DESC, tag
LIMIT 100;
