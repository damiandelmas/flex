-- @name: messages
-- @description: Chunk-level surface for claude_chat cells. Conversation messages with role and graph intelligence.

DROP VIEW IF EXISTS messages;
CREATE VIEW messages AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    s.source_id,
    s.source_type,
    s.position AS message_number,
    src.title,
    src.model,
    src.message_count,
    t.role,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_message t ON r.id = t.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
