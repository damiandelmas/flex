-- @name: file-provenance
-- @description: Single-call file lineage: current path, path history, key events, origin
-- @params: path (required), limit (default: 12)
-- @multi: true

-- Provenance unifies two evidence tiers:
--   captured  — hard tool ops (Write/Edit/MultiEdit/Read) with target_file
--   inferred  — soft ops parsed from shell commands (cat >, tee, cp, mv, rm…)
-- Soft ops are mapped to canonical tool names so the mutation/read predicates
-- below work uniformly, and flagged via `inferred`/`confidence` so callers can
-- tell a parsed-from-shell candidate from a hard-captured mutation.

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
        ci.old_blob_hash,
        0 AS inferred,
        'captured' AS confidence
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
    UNION ALL
    SELECT
        c.id,
        c.timestamp,
        es.source_id,
        CASE so.inferred_op WHEN 'edit' THEN 'Edit' WHEN 'read' THEN 'Read'
                            WHEN 'delete' THEN 'Delete' ELSE 'Write' END AS tool_name,
        so.file_path AS target_file,
        NULL AS file_uuid,
        NULL AS blob_hash,
        NULL AS old_blob_hash,
        1 AS inferred,
        so.confidence AS confidence
    FROM _edges_soft_ops so
    JOIN _raw_chunks c ON c.id = so.chunk_id
    JOIN _edges_source es ON es.chunk_id = so.chunk_id
    WHERE so.file_path LIKE '%' || :path || '%'
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
        SUM(inferred) AS inferred_ops,
        COUNT(DISTINCT source_id) AS sessions,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_seen
    FROM scoped
)
SELECT
    :path AS query,
    (SELECT target_file FROM latest) AS current_path,
    substr((SELECT COALESCE(blob_hash, old_blob_hash, '') FROM latest), 1, 12) AS current_blob,
    path_count || ' paths, ' || mutations || ' mutations, ' || reads || ' reads, '
        || inferred_ops || ' inferred, ' || sessions || ' sessions' AS footprint,
    datetime(first_seen, 'unixepoch', 'localtime') || ' -> ' ||
        datetime(last_seen, 'unixepoch', 'localtime') AS observed_window,
    CASE
        WHEN (SELECT timestamp FROM first_mutation) IS NULL THEN 'No mutation captured; read-only history.'
        WHEN (SELECT inferred FROM first_mutation) = 1 THEN
            'First mutation (inferred from shell, ' || (SELECT confidence FROM first_mutation) ||
            '): ' || datetime((SELECT timestamp FROM first_mutation), 'unixepoch', 'localtime') ||
            ' in session ' || substr((SELECT source_id FROM first_mutation), 1, 8)
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
        ci.old_blob_hash,
        0 AS inferred
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
    UNION ALL
    SELECT
        c.timestamp,
        CASE so.inferred_op WHEN 'edit' THEN 'Edit' WHEN 'read' THEN 'Read'
                            WHEN 'delete' THEN 'Delete' ELSE 'Write' END AS tool_name,
        so.file_path AS target_file,
        NULL AS blob_hash,
        NULL AS old_blob_hash,
        1 AS inferred
    FROM _edges_soft_ops so
    JOIN _raw_chunks c ON c.id = so.chunk_id
    WHERE so.file_path LIKE '%' || :path || '%'
),
homes AS (
    SELECT
        target_file,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_seen,
        COUNT(*) AS touches,
        SUM(CASE WHEN tool_name IN ('Write', 'Edit', 'MultiEdit') THEN 1 ELSE 0 END) AS mutations,
        MAX(inferred) AS inferred,
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
    inferred,
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
        ci.old_blob_hash,
        0 AS inferred,
        'captured' AS confidence
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
    UNION ALL
    SELECT
        c.id,
        c.timestamp,
        es.source_id,
        CASE so.inferred_op WHEN 'edit' THEN 'Edit' WHEN 'read' THEN 'Read'
                            WHEN 'delete' THEN 'Delete' ELSE 'Write' END AS tool_name,
        so.file_path AS target_file,
        NULL AS blob_hash,
        NULL AS old_blob_hash,
        1 AS inferred,
        so.confidence AS confidence
    FROM _edges_soft_ops so
    JOIN _raw_chunks c ON c.id = so.chunk_id
    JOIN _edges_source es ON es.chunk_id = so.chunk_id
    WHERE so.file_path LIKE '%' || :path || '%'
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
    CASE WHEN inferred = 1 THEN tool_name || '~' || confidence ELSE tool_name END AS tool_name,
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
        t.cwd,
        0 AS inferred,
        'captured' AS confidence
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
    UNION ALL
    SELECT
        c.timestamp,
        es.source_id,
        CASE so.inferred_op WHEN 'edit' THEN 'Edit' WHEN 'read' THEN 'Read'
                            WHEN 'delete' THEN 'Delete' ELSE 'Write' END AS tool_name,
        so.file_path AS target_file,
        t.cwd,
        1 AS inferred,
        so.confidence AS confidence
    FROM _edges_soft_ops so
    JOIN _raw_chunks c ON c.id = so.chunk_id
    JOIN _edges_source es ON es.chunk_id = so.chunk_id
    LEFT JOIN _edges_tool_ops t ON t.chunk_id = so.chunk_id
    WHERE so.file_path LIKE '%' || :path || '%'
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
    CASE WHEN o.inferred = 1 THEN 'inferred (' || o.confidence || ')' ELSE 'captured' END AS evidence,
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
