-- @name: messages
-- @description: Chunk-level surface for claude_code cells. Domain vocabulary with graph intelligence.

DROP VIEW IF EXISTS messages;
CREATE VIEW messages AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    s.source_id,
    s.position,
    src.project,
    src.title,
    src.message_count,
    t.tool_name,
    t.target_file,
    t.success,
    t.cwd,
    tp.role,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
LEFT JOIN _types_message tp ON r.id = tp.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
