-- @name: orient
-- @description: Dev.to cell orientation: scope, instructions, views, presets, and samples.
-- @multi: true

-- @query: about
SELECT
    COALESCE((SELECT value FROM _meta WHERE key = 'description'),
             'Dev.to articles and comments') AS description,
    'none' AS required_auth;

-- @query: cell_docs
SELECT scope, name, path, mtime, chars, content
FROM _flex_docs
ORDER BY
    CASE scope
        WHEN 'cell_instructions' THEN 0
        WHEN 'local_notes' THEN 1
        ELSE 2
    END,
    name;

-- @query: scope
SELECT key, value
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
ORDER BY key;

-- @query: shape
SELECT 'chunks' AS what, COUNT(*) AS n FROM _raw_chunks
UNION ALL
SELECT 'sources', COUNT(*) FROM _raw_sources;

-- @query: query_surface
SELECT 'view' AS kind, m.name AS name, GROUP_CONCAT(p.name, ', ') AS columns
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
UNION ALL
SELECT 'preset', name, params FROM _presets
ORDER BY kind, name;

-- @query: sample
SELECT
    id,
    created_at,
    type,
    title,
    author,
    tags,
    substr(content, 1, 300) AS preview
FROM chunks
ORDER BY timestamp DESC
LIMIT 3;
