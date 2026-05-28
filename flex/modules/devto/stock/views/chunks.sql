-- @name: chunks
-- @description: UNIFIED surface — all Dev.to chunks. type: article|comment. Use for all queries.

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
    src.url AS article_url,
    src.score AS article_score,
    src.num_comments AS article_comments,
    t.author,
    t.score,
    t.url,
    t.tags,
    t.reading_time,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_devto t ON r.id = t.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
