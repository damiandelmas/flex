-- @name: messages
-- @description: Chunk-level surface for claude_code cells. Domain vocabulary with graph intelligence.
-- child_session_id: non-NULL on Task chunks — follow to _raw_sources for the spawned agent session.

DROP VIEW IF EXISTS messages;
CREATE VIEW messages AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    s.source_id,
    s.position AS message_number,
    src.project,
    src.title,
    src.message_count,
    t.tool_name,
    t.target_file,
    t.success,
    t.cwd,
    tp.role,
    d.child_doc_id AS child_session_id,
    d.agent_type,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
LEFT JOIN _types_message tp ON r.id = tp.chunk_id
LEFT JOIN (SELECT chunk_id, child_doc_id, agent_type FROM _edges_delegations GROUP BY chunk_id) d ON r.id = d.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
