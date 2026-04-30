-- @name: file-provenance
-- @description: Single-call file lineage: current path, path history, key events, origin
-- @params: path (required), limit (default: 12)
-- @multi: true

-- @query: summary
WITH matched_uuid AS (
    SELECT fi.file_uuid
    FROM _edges_file_identity fi
    JOIN _edges_tool_ops t ON fi.chunk_id = t.chunk_id
    WHERE t.target_file LIKE '%' || :path || '%'
    LIMIT 1
),
scoped AS (
    SELECT
        c.id,
        c.timestamp,
        es.source_id,
        t.tool_name,
        t.target_file,
        fi.file_uuid,
        ci.blob_hash,
        ci.old_blob_hash
    FROM _edges_tool_ops t
    JOIN _raw_chunks c ON c.id = t.chunk_id
    JOIN _edges_source es ON es.chunk_id = t.chunk_id
    LEFT JOIN _edges_file_identity fi ON fi.chunk_id = t.chunk_id
    LEFT JOIN _edges_content_identity ci ON ci.chunk_id = t.chunk_id
    WHERE t.target_file IS NOT NULL
      AND (
        fi.file_uuid = (SELECT file_uuid FROM matched_uuid)
        OR ((SELECT file_uuid FROM matched_uuid) IS NULL AND t.target_file LIKE '%' || :path || '%')
        OR t.target_file LIKE '%' || :path || '%'
      )
),
latest AS (
    SELECT * FROM scoped ORDER BY timestamp DESC LIMIT 1
),
first_mutation AS (
    SELECT *
    FROM scoped
    WHERE tool_name IN ('Write', 'Edit', 'MultiEdit')
    ORDER BY timestamp ASC
    LIMIT 1
),
counts AS (
    SELECT
        COUNT(DISTINCT target_file) AS path_count,
        SUM(CASE WHEN tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 1 ELSE 0 END) AS mutations,
        SUM(CASE WHEN tool_name = 'Read' THEN 1 ELSE 0 END) AS reads,
        COUNT(DISTINCT source_id) AS sessions,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_seen
    FROM scoped
)
SELECT
    :path AS query,
    (SELECT target_file FROM latest) AS current_path,
    substr((SELECT COALESCE(blob_hash, old_blob_hash, '') FROM latest), 1, 12) AS current_blob,
    path_count || ' paths, ' || mutations || ' mutations, ' || reads || ' reads, ' || sessions || ' sessions' AS footprint,
    datetime(first_seen, 'unixepoch', 'localtime') || ' -> ' ||
        datetime(last_seen, 'unixepoch', 'localtime') AS observed_window,
    CASE
        WHEN (SELECT timestamp FROM first_mutation) IS NULL THEN 'No mutation captured; read-only history.'
        ELSE 'First mutation: ' || datetime((SELECT timestamp FROM first_mutation), 'unixepoch', 'localtime') ||
             ' in session ' || substr((SELECT source_id FROM first_mutation), 1, 8)
    END AS origin_hint
FROM counts;

-- @query: lineage
WITH matched_uuid AS (
    SELECT fi.file_uuid
    FROM _edges_file_identity fi
    JOIN _edges_tool_ops t ON fi.chunk_id = t.chunk_id
    WHERE t.target_file LIKE '%' || :path || '%'
    LIMIT 1
),
scoped AS (
    SELECT
        c.timestamp,
        t.tool_name,
        t.target_file,
        ci.blob_hash,
        ci.old_blob_hash
    FROM _edges_tool_ops t
    JOIN _raw_chunks c ON c.id = t.chunk_id
    LEFT JOIN _edges_file_identity fi ON fi.chunk_id = t.chunk_id
    LEFT JOIN _edges_content_identity ci ON ci.chunk_id = t.chunk_id
    WHERE t.target_file IS NOT NULL
      AND (
        fi.file_uuid = (SELECT file_uuid FROM matched_uuid)
        OR ((SELECT file_uuid FROM matched_uuid) IS NULL AND t.target_file LIKE '%' || :path || '%')
        OR t.target_file LIKE '%' || :path || '%'
      )
),
homes AS (
    SELECT
        target_file,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_seen,
        COUNT(*) AS touches,
        SUM(CASE WHEN tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 1 ELSE 0 END) AS mutations,
        GROUP_CONCAT(DISTINCT tool_name) AS tools,
        substr(MAX(COALESCE(blob_hash, old_blob_hash, '')), 1, 12) AS blob
    FROM scoped
    GROUP BY target_file
)
SELECT
    ROW_NUMBER() OVER (ORDER BY first_seen) AS step,
    datetime(first_seen, 'unixepoch', 'localtime') AS first_seen,
    CASE
        WHEN length(target_file) > 86 THEN '...' || substr(target_file, -83)
        ELSE target_file
    END AS path,
    touches,
    mutations,
    tools,
    blob
FROM homes
ORDER BY first_seen
LIMIT :limit;

-- @query: events
WITH matched_uuid AS (
    SELECT fi.file_uuid
    FROM _edges_file_identity fi
    JOIN _edges_tool_ops t ON fi.chunk_id = t.chunk_id
    WHERE t.target_file LIKE '%' || :path || '%'
    LIMIT 1
),
scoped AS (
    SELECT
        c.id,
        c.timestamp,
        es.source_id,
        t.tool_name,
        t.target_file,
        ci.blob_hash,
        ci.old_blob_hash
    FROM _edges_tool_ops t
    JOIN _raw_chunks c ON c.id = t.chunk_id
    JOIN _edges_source es ON es.chunk_id = t.chunk_id
    LEFT JOIN _edges_file_identity fi ON fi.chunk_id = t.chunk_id
    LEFT JOIN _edges_content_identity ci ON ci.chunk_id = t.chunk_id
    WHERE t.target_file IS NOT NULL
      AND (
        fi.file_uuid = (SELECT file_uuid FROM matched_uuid)
        OR ((SELECT file_uuid FROM matched_uuid) IS NULL AND t.target_file LIKE '%' || :path || '%')
        OR t.target_file LIKE '%' || :path || '%'
      )
),
ordered AS (
    SELECT
        *,
        LAG(target_file) OVER (ORDER BY timestamp, id) AS previous_path
    FROM scoped
)
SELECT
    datetime(timestamp, 'unixepoch', 'localtime') AS ts,
    CASE
        WHEN previous_path IS NULL THEN 'first seen'
        WHEN previous_path != target_file THEN 'path changed'
        WHEN tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 'mutated'
        ELSE 'read'
    END AS event,
    tool_name,
    CASE
        WHEN length(target_file) > 86 THEN '...' || substr(target_file, -83)
        ELSE target_file
    END AS path,
    substr(COALESCE(blob_hash, old_blob_hash, ''), 1, 8) AS blob,
    substr(source_id, 1, 8) AS session
FROM ordered
WHERE previous_path IS NULL
   OR previous_path != target_file
   OR tool_name IN ('Write', 'Edit', 'MultiEdit')
ORDER BY timestamp
LIMIT :limit;

-- @query: origin
WITH matched_uuid AS (
    SELECT fi.file_uuid
    FROM _edges_file_identity fi
    JOIN _edges_tool_ops t ON fi.chunk_id = t.chunk_id
    WHERE t.target_file LIKE '%' || :path || '%'
    LIMIT 1
),
scoped AS (
    SELECT
        c.timestamp,
        es.source_id,
        t.tool_name,
        t.target_file,
        t.cwd
    FROM _edges_tool_ops t
    JOIN _raw_chunks c ON c.id = t.chunk_id
    JOIN _edges_source es ON es.chunk_id = t.chunk_id
    LEFT JOIN _edges_file_identity fi ON fi.chunk_id = t.chunk_id
    WHERE t.target_file IS NOT NULL
      AND (
        fi.file_uuid = (SELECT file_uuid FROM matched_uuid)
        OR ((SELECT file_uuid FROM matched_uuid) IS NULL AND t.target_file LIKE '%' || :path || '%')
        OR t.target_file LIKE '%' || :path || '%'
      )
),
origin AS (
    SELECT *
    FROM scoped
    WHERE tool_name IN ('Write', 'Edit', 'MultiEdit')
    ORDER BY timestamp ASC
    LIMIT 1
)
SELECT
    datetime(o.timestamp, 'unixepoch', 'localtime') AS ts,
    substr(o.source_id, 1, 8) AS session,
    o.tool_name,
    o.target_file AS created_or_changed_path,
    o.cwd,
    (
        SELECT substr(replace(replace(m.content, char(10), ' '), char(13), ' '), 1, 260)
        FROM messages m
        WHERE m.session_id = o.source_id
          AND m.type = 'user_prompt'
          AND m.timestamp <= o.timestamp
        ORDER BY m.timestamp DESC, m.position DESC
        LIMIT 1
    ) AS prior_prompt
FROM origin o;
