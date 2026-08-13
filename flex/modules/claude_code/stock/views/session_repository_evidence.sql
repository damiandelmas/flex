-- @name: session_repository_evidence
-- @description: Occurrence-grained repository/path evidence for session composition. Exact paths remain visible when stable repo identity is unavailable.

DROP VIEW IF EXISTS session_repository_evidence;
CREATE VIEW session_repository_evidence AS
WITH occurrences AS (
    SELECT
        o.session_id,
        o.chunk_id,
        o.position,
        o.timestamp,
        o.operation_type,
        o.normalized_path AS evidence_path,
        o.path_basename,
        o.file_uuid,
        CASE
            WHEN o.target_file IS NOT NULL THEN 'direct_target'
            WHEN o.file_uuid IS NOT NULL THEN 'file_identity'
            ELSE 'compiled_observation'
        END AS evidence_basis
    FROM _enrich_observations o
    WHERE o.normalized_path IS NOT NULL

    UNION ALL

    SELECT
        es.source_id,
        so.chunk_id,
        es.position,
        r.timestamp,
        COALESCE(so.inferred_op, 'observation'),
        so.file_path,
        NULL,
        so.file_uuid,
        'soft_op'
    FROM _edges_soft_ops so
    JOIN _edges_source es ON es.chunk_id = so.chunk_id
    JOIN _raw_chunks r ON r.id = so.chunk_id
    WHERE so.file_path IS NOT NULL
      AND so.file_path != ''
      AND NOT EXISTS (
          SELECT 1
          FROM _enrich_observations direct
          WHERE direct.chunk_id = so.chunk_id
            AND direct.normalized_path = so.file_path
      )
), resolved AS (
    SELECT
        o.*,
        (SELECT eri.repo_root
         FROM _edges_repo_identity edge
         JOIN _enrich_repo_identity eri ON eri.repo_root = edge.repo_root
         WHERE edge.chunk_id = o.chunk_id
           AND o.evidence_basis != 'soft_op'
         ORDER BY length(eri.repo_path) DESC
         LIMIT 1) AS identity_repo_root,
        (SELECT eri.repo_path
         FROM _edges_repo_identity edge
         JOIN _enrich_repo_identity eri ON eri.repo_root = edge.repo_root
         WHERE edge.chunk_id = o.chunk_id
           AND o.evidence_basis != 'soft_op'
         ORDER BY length(eri.repo_path) DESC
         LIMIT 1) AS identity_repo_path,
        (SELECT eri.project
         FROM _edges_repo_identity edge
         JOIN _enrich_repo_identity eri ON eri.repo_root = edge.repo_root
         WHERE edge.chunk_id = o.chunk_id
           AND o.evidence_basis != 'soft_op'
         ORDER BY length(eri.repo_path) DESC
         LIMIT 1) AS identity_project,
        (SELECT eri.repo_root
         FROM _enrich_repo_identity eri
         WHERE o.evidence_path = eri.repo_path
            OR o.evidence_path LIKE eri.repo_path || '/%'
         ORDER BY length(eri.repo_path) DESC
         LIMIT 1) AS prefix_repo_root,
        (SELECT eri.repo_path
         FROM _enrich_repo_identity eri
         WHERE o.evidence_path = eri.repo_path
            OR o.evidence_path LIKE eri.repo_path || '/%'
         ORDER BY length(eri.repo_path) DESC
         LIMIT 1) AS prefix_repo_path,
        (SELECT eri.project
         FROM _enrich_repo_identity eri
         WHERE o.evidence_path = eri.repo_path
            OR o.evidence_path LIKE eri.repo_path || '/%'
         ORDER BY length(eri.repo_path) DESC
         LIMIT 1) AS prefix_project
    FROM occurrences o
)
SELECT
    session_id,
    chunk_id,
    position,
    timestamp,
    datetime(timestamp, 'unixepoch', 'localtime') AS observed_at,
    operation_type,
    evidence_path,
    path_basename,
    file_uuid,
    COALESCE(identity_repo_root, prefix_repo_root) AS repo_root,
    COALESCE(identity_repo_path, prefix_repo_path) AS repo_path,
    COALESCE(identity_project, prefix_project) AS project,
    evidence_basis,
    CASE
        WHEN identity_repo_path IS NOT NULL THEN 'identity'
        WHEN prefix_repo_path IS NOT NULL THEN 'path_prefix'
        WHEN identity_repo_root IS NOT NULL THEN 'identity_only'
        ELSE 'unresolved_path'
    END AS resolution
FROM resolved;
