-- @name: chunks
-- @description: UNIFIED surface — all HN chunks. type: story|comment. Use for all queries.

DROP VIEW IF EXISTS chunks;
CREATE VIEW chunks AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch') AS created_at,
    COALESCE(t.item_type, 'chunk') AS type,
    s.source_id,
    s.position,
    src.title,
    src.url AS thread_url,
    src.hn_url,
    src.score AS thread_score,
    src.num_comments AS thread_comments,
    t.author,
    t.score,
    t.story_id,
    t.parent_id,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_hn t ON r.id = t.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
