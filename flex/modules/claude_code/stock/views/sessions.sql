-- @name: sessions
-- @description: Source-level surface for claude_code cells. Session metadata with graph intelligence and navigational fingerprint. Warmup sessions excluded.

DROP VIEW IF EXISTS sessions;
CREATE VIEW sessions AS
SELECT
    src.source_id AS session_id,
    src.project,
    src.title,
    src.message_count,
    src.start_time,
    src.duration_minutes AS duration,
    datetime(src.start_time, 'unixepoch', 'localtime') AS started_at,
    datetime(src.end_time, 'unixepoch', 'localtime') AS ended_at,
    COUNT(DISTINCT s.chunk_id) as chunk_count,
    ess.fingerprint_index,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id,
    g.community_label
FROM _raw_sources src
LEFT JOIN _edges_source s ON src.source_id = s.source_id
LEFT JOIN _types_source_warmup w ON src.source_id = w.source_id
LEFT JOIN _enrich_session_summary ess ON src.source_id = ess.source_id
LEFT JOIN _enrich_source_graph g ON src.source_id = g.source_id
WHERE COALESCE(w.is_warmup_only, 0) = 0
GROUP BY src.source_id;
