-- @name: observed-file
-- @description: Find indexed file observations by normalized path, suffix, basename, or SOMA UUID
-- @params: path (required), limit (default: 30)

WITH matched AS (
    SELECT o.*
    FROM _enrich_observations o
    WHERE o.normalized_path = :path
       OR o.normalized_path LIKE '%/' || ltrim(:path, '/')
       OR o.path_basename = :path
       OR o.file_uuid = :path
    ORDER BY
        CASE WHEN o.normalized_path = :path OR o.file_uuid = :path THEN 0
             WHEN o.path_basename = :path THEN 1 ELSE 2 END,
        o.timestamp DESC, o.position DESC
    LIMIT :limit
)
SELECT
    m.chunk_id AS id,
    m.session_id,
    m.position,
    datetime(m.timestamp, 'unixepoch', 'localtime') AS created_at,
    m.operation_type AS observation_type,
    m.tool_name,
    m.target_file,
    m.normalized_path,
    m.file_uuid,
    m.cwd,
    CASE WHEN msg.file_body IS NOT NULL THEN 1 ELSE 0 END AS full_body_available,
    length(msg.content) AS content_len,
    length(msg.file_body) AS file_body_len,
    substr(replace(replace(msg.content, char(10), ' '), char(13), ' '), 1, 260) AS command_preview,
    substr(COALESCE(msg.file_body, msg.content), 1, 500) AS observed_preview,
    '@full id=' || m.chunk_id AS fetch_full
FROM matched m
JOIN messages msg ON msg.id = m.chunk_id
ORDER BY m.timestamp DESC, m.position DESC;
