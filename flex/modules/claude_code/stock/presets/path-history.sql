-- @name: file-history
-- @description: Ordered indexed captures, mutations, reads, and stdout observations for a path or SOMA UUID
-- @params: path (required), limit (default: 80)
-- @multi: true

-- @query: summary
WITH observations AS (
    SELECT * FROM _enrich_observations
    WHERE normalized_path = :path
       OR normalized_path LIKE '%/' || ltrim(:path, '/')
       OR path_basename = :path
       OR file_uuid = :path
)
SELECT
    :path AS query,
    count(*) AS observations,
    count(DISTINCT session_id) AS sessions,
    sum(operation_type = 'mutation') AS mutations,
    sum(operation_type = 'read') AS reads,
    sum(operation_type = 'stdout_observation') AS stdout_observations,
    min(datetime(timestamp, 'unixepoch', 'localtime')) AS first_seen,
    max(datetime(timestamp, 'unixepoch', 'localtime')) AS last_seen
FROM observations;

-- @query: timeline
WITH matched AS (
    SELECT * FROM _enrich_observations
    WHERE normalized_path = :path
       OR normalized_path LIKE '%/' || ltrim(:path, '/')
       OR path_basename = :path
       OR file_uuid = :path
    ORDER BY timestamp DESC, position DESC
    LIMIT :limit
)
SELECT
    datetime(m.timestamp, 'unixepoch', 'localtime') AS created_at,
    m.chunk_id AS id,
    m.session_id,
    m.position,
    m.operation_type AS observation_type,
    m.tool_name,
    m.target_file,
    m.normalized_path,
    m.file_uuid,
    m.cwd,
    CASE WHEN msg.file_body IS NOT NULL THEN 1 ELSE 0 END AS full_body_available,
    length(msg.file_body) AS file_body_len,
    substr(replace(replace(msg.content, char(10), ' '), char(13), ' '), 1, 260) AS command_preview,
    substr(COALESCE(msg.file_body, msg.content), 1, 500) AS observed_preview,
    '@full id=' || m.chunk_id AS fetch_full
FROM matched m
JOIN messages msg ON msg.id=m.chunk_id
ORDER BY m.timestamp ASC, m.position ASC;
