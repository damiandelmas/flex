-- @name: posts
-- @description: Chunk-level surface for GitHub Issues cells. Issues and comments with author, repo, state, labels, and graph intelligence.

DROP VIEW IF EXISTS posts;
CREATE VIEW posts AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    s.source_id,
    s.source_type,
    s.position,
    src.title,
    src.repo,
    src.url AS issue_url,
    src.score AS issue_score,
    src.num_comments AS issue_comments,
    src.state AS issue_state,
    src.labels AS issue_labels,
    t.item_type,
    t.author,
    t.score,
    t.repo AS chunk_repo,
    t.issue_number,
    t.state,
    t.labels,
    t.url,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_github t ON r.id = t.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
