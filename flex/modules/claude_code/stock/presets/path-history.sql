-- @name: file-history
-- @description: Ordered captures, mutations, reads, and stdout observations for a file/path
-- @params: path (required), limit (default: 80)
-- @multi: true

-- @query: summary
WITH observations AS (
    SELECT
        id,
        session_id,
        timestamp,
        created_at,
        tool_name,
        target_file,
        content,
        file_body,
        CASE
            WHEN target_file LIKE '%' || :path || '%' AND tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 'mutation'
            WHEN target_file LIKE '%' || :path || '%' AND tool_name = 'Read' THEN 'read'
            WHEN target_file LIKE '%' || :path || '%' THEN 'target_file'
            WHEN tool_name = 'Bash' THEN 'stdout_observation'
            ELSE 'body_match'
        END AS observation_type
    FROM messages
    WHERE target_file LIKE '%' || :path || '%'
       OR (tool_name = 'Bash' AND content LIKE '%' || :path || '%')
       OR file_body LIKE '%' || :path || '%'
)
SELECT
    :path AS query,
    count(*) AS observations,
    count(DISTINCT session_id) AS sessions,
    sum(CASE WHEN observation_type = 'mutation' THEN 1 ELSE 0 END) AS mutations,
    sum(CASE WHEN observation_type = 'read' THEN 1 ELSE 0 END) AS reads,
    sum(CASE WHEN observation_type = 'stdout_observation' THEN 1 ELSE 0 END) AS stdout_observations,
    sum(CASE WHEN file_body IS NOT NULL THEN 1 ELSE 0 END) AS full_bodies,
    min(created_at) AS first_seen,
    max(created_at) AS last_seen
FROM observations;

-- @query: timeline
WITH observations AS (
    SELECT
        id,
        session_id,
        position,
        timestamp,
        created_at,
        tool_name,
        target_file,
        cwd,
        type,
        content,
        file_body,
        CASE
            WHEN json_valid(file_body) THEN COALESCE(
                json_extract(file_body, '$.command'),
                json_extract(file_body, '$.cmd')
            )
            ELSE NULL
        END AS body_command,
        CASE
            WHEN json_valid(file_body) THEN COALESCE(
                json_extract(file_body, '$.content'),
                json_extract(file_body, '$.output'),
                json_extract(file_body, '$.stdout'),
                json_extract(file_body, '$.text')
            )
            ELSE NULL
        END AS body_text
    FROM messages
    WHERE target_file LIKE '%' || :path || '%'
       OR (tool_name = 'Bash' AND content LIKE '%' || :path || '%')
       OR file_body LIKE '%' || :path || '%'
),
classified AS (
    SELECT
        *,
        CASE
            WHEN target_file LIKE '%' || :path || '%' AND tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 'mutation'
            WHEN target_file LIKE '%' || :path || '%' AND tool_name = 'Read' THEN 'read'
            WHEN target_file LIKE '%' || :path || '%' THEN 'target_file'
            WHEN tool_name = 'Bash' THEN 'stdout_observation'
            ELSE 'body_match'
        END AS observation_type,
        COALESCE(body_command, content) AS command_text,
        COALESCE(body_text, file_body, content) AS observed_text
    FROM observations
)
SELECT
    created_at,
    id,
    session_id,
    position,
    observation_type,
    tool_name,
    target_file,
    cwd,
    CASE WHEN file_body IS NOT NULL THEN 1 ELSE 0 END AS full_body_available,
    length(file_body) AS file_body_len,
    substr(replace(replace(command_text, char(10), ' '), char(13), ' '), 1, 260) AS command_preview,
    substr(observed_text, 1, 500) AS observed_preview,
    '@full id=' || id AS fetch_full
FROM classified
ORDER BY timestamp ASC, position ASC
LIMIT :limit;
