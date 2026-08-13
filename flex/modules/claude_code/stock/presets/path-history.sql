-- @name: file-history
-- @description: Ordered occurrence-grained captures, mutations, reads, stdout observations, and visibly inferred shell paths for a path or SOMA UUID
-- @params: path (required), limit (default: 80)
-- @multi: true

-- @query: summary
WITH soft_edges AS (
    SELECT
        MIN(id) AS edge_id,
        chunk_id,
        file_path,
        file_uuid,
        inferred_op,
        confidence
    FROM _edges_soft_ops
    WHERE file_path = :path
       OR file_path LIKE '%/' || ltrim(:path, '/')
       OR file_uuid = :path
    GROUP BY chunk_id, file_path, file_uuid, inferred_op, confidence
),
direct_observations AS (
    SELECT
        o.*,
        CASE
            WHEN o.target_file IS NOT NULL THEN 'direct_target'
            WHEN o.file_uuid IS NOT NULL THEN 'file_identity'
            ELSE 'indexed_observation'
        END AS evidence_basis,
        'captured' AS confidence,
        NULL AS inferred_op,
        0 AS evidence_rank,
        0 AS edge_id
    FROM _enrich_observations o
    WHERE (
        o.normalized_path = :path
        OR o.normalized_path LIKE '%/' || ltrim(:path, '/')
        OR o.path_basename = :path
        OR o.file_uuid = :path
    )
      AND (
        o.target_file IS NOT NULL
        OR o.file_uuid IS NOT NULL
        OR NOT EXISTS (
            SELECT 1 FROM _edges_soft_ops so WHERE so.chunk_id = o.chunk_id
        )
      )
),
soft_observations AS (
    SELECT
        so.chunk_id,
        COALESCE(o.session_id, msg.session_id) AS session_id,
        COALESCE(o.position, msg.position) AS position,
        COALESCE(o.timestamp, msg.timestamp) AS timestamp,
        COALESCE(o.tool_name, msg.tool_name) AS tool_name,
        NULL AS target_file,
        so.file_path AS normalized_path,
        NULL AS path_basename,
        so.file_uuid AS file_uuid,
        CASE
            WHEN COALESCE(o.tool_name, msg.tool_name) IN
                 ('Bash', 'shell', 'local_shell', 'exec')
                THEN 'stdout_observation'
            ELSE COALESCE(o.operation_type, 'observation')
        END AS operation_type,
        COALESCE(o.cwd, msg.cwd) AS cwd,
        COALESCE(o.message_type, msg.type) AS message_type,
        'soft_op' AS evidence_basis,
        so.confidence,
        so.inferred_op,
        1 AS evidence_rank,
        so.edge_id
    FROM soft_edges so
    JOIN messages msg ON msg.id = so.chunk_id
    LEFT JOIN _enrich_observations o ON o.chunk_id = so.chunk_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM _enrich_observations direct
        WHERE direct.chunk_id = so.chunk_id
          AND (direct.target_file IS NOT NULL OR direct.file_uuid IS NOT NULL)
          AND (
              direct.normalized_path = so.file_path
              OR direct.target_file = so.file_path
              OR (so.file_uuid IS NOT NULL AND direct.file_uuid = so.file_uuid)
          )
    )
),
observations AS (
    SELECT * FROM direct_observations
    UNION ALL
    SELECT * FROM soft_observations
),
matched AS (
    SELECT * FROM observations
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
FROM matched;

-- @query: timeline
WITH soft_edges AS (
    SELECT
        MIN(id) AS edge_id,
        chunk_id,
        file_path,
        file_uuid,
        inferred_op,
        confidence
    FROM _edges_soft_ops
    WHERE file_path = :path
       OR file_path LIKE '%/' || ltrim(:path, '/')
       OR file_uuid = :path
    GROUP BY chunk_id, file_path, file_uuid, inferred_op, confidence
),
direct_observations AS (
    SELECT
        o.*,
        CASE
            WHEN o.target_file IS NOT NULL THEN 'direct_target'
            WHEN o.file_uuid IS NOT NULL THEN 'file_identity'
            ELSE 'indexed_observation'
        END AS evidence_basis,
        'captured' AS confidence,
        NULL AS inferred_op,
        0 AS evidence_rank,
        0 AS edge_id
    FROM _enrich_observations o
    WHERE (
        o.normalized_path = :path
        OR o.normalized_path LIKE '%/' || ltrim(:path, '/')
        OR o.path_basename = :path
        OR o.file_uuid = :path
    )
      AND (
        o.target_file IS NOT NULL
        OR o.file_uuid IS NOT NULL
        OR NOT EXISTS (
            SELECT 1 FROM _edges_soft_ops so WHERE so.chunk_id = o.chunk_id
        )
      )
),
soft_observations AS (
    SELECT
        so.chunk_id,
        COALESCE(o.session_id, msg.session_id) AS session_id,
        COALESCE(o.position, msg.position) AS position,
        COALESCE(o.timestamp, msg.timestamp) AS timestamp,
        COALESCE(o.tool_name, msg.tool_name) AS tool_name,
        NULL AS target_file,
        so.file_path AS normalized_path,
        NULL AS path_basename,
        so.file_uuid AS file_uuid,
        CASE
            WHEN COALESCE(o.tool_name, msg.tool_name) IN
                 ('Bash', 'shell', 'local_shell', 'exec')
                THEN 'stdout_observation'
            ELSE COALESCE(o.operation_type, 'observation')
        END AS operation_type,
        COALESCE(o.cwd, msg.cwd) AS cwd,
        COALESCE(o.message_type, msg.type) AS message_type,
        'soft_op' AS evidence_basis,
        so.confidence,
        so.inferred_op,
        1 AS evidence_rank,
        so.edge_id
    FROM soft_edges so
    JOIN messages msg ON msg.id = so.chunk_id
    LEFT JOIN _enrich_observations o ON o.chunk_id = so.chunk_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM _enrich_observations direct
        WHERE direct.chunk_id = so.chunk_id
          AND (direct.target_file IS NOT NULL OR direct.file_uuid IS NOT NULL)
          AND (
              direct.normalized_path = so.file_path
              OR direct.target_file = so.file_path
              OR (so.file_uuid IS NOT NULL AND direct.file_uuid = so.file_uuid)
          )
    )
),
observations AS (
    SELECT * FROM direct_observations
    UNION ALL
    SELECT * FROM soft_observations
),
matched AS (
    SELECT * FROM observations
    WHERE normalized_path = :path
       OR normalized_path LIKE '%/' || ltrim(:path, '/')
       OR path_basename = :path
       OR file_uuid = :path
    ORDER BY timestamp DESC, position DESC, evidence_rank, edge_id
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
    m.evidence_basis,
    m.confidence,
    m.inferred_op,
    CASE WHEN msg.file_body IS NOT NULL THEN 1 ELSE 0 END AS full_body_available,
    length(msg.file_body) AS file_body_len,
    substr(replace(replace(msg.content, char(10), ' '), char(13), ' '), 1, 260) AS command_preview,
    substr(COALESCE(msg.file_body, msg.content), 1, 500) AS observed_preview,
    '@full id=' || m.chunk_id AS fetch_full
FROM matched m
JOIN messages msg ON msg.id=m.chunk_id
ORDER BY m.timestamp ASC, m.position ASC, m.evidence_rank, m.edge_id;
