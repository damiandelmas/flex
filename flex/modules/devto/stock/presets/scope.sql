-- @name: scope
-- @description: Show the configured Dev.to pull scope stored in _meta.

SELECT
    key,
    value
FROM _meta
WHERE key IN (
    'scope',
    'tags',
    'authors',
    'since_days',
    'tag_limit',
    'comment_limit',
    'include_comments',
    'last_pull_ts',
    'last_pull_at'
)
ORDER BY
    CASE key
        WHEN 'scope' THEN 0
        WHEN 'tags' THEN 1
        WHEN 'authors' THEN 2
        ELSE 3
    END,
    key;
