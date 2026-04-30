-- @name: sections
-- @description: Chunk-level surface for markdown cells. Sections with heading hierarchy, folder, tags, and graph scores.

DROP VIEW IF EXISTS sections;
CREATE VIEW sections AS
SELECT
    c.id,
    c.content,
    es.source_id,
    c.embedding IS NOT NULL AS has_embedding,
    t.item_type,
    t.note_title,
    t.section_title,
    t.heading_depth,
    t.heading_chain,
    t.word_count,
    t.char_start,
    t.char_end,
    ts.folder,
    ts.tags,
    COALESCE(sg.centrality, 0.0) AS centrality,
    sg.community_id,
    COALESCE(sg.is_hub, 0) AS is_hub
FROM _raw_chunks c
LEFT JOIN _edges_source es ON es.chunk_id = c.id
LEFT JOIN _types_markdown t ON t.chunk_id = c.id
LEFT JOIN _types_markdown_source ts ON ts.source_id = es.source_id
LEFT JOIN _enrich_source_graph sg ON sg.source_id = es.source_id;
