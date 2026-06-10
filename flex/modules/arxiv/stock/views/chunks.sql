-- @name: chunks
-- @description: Primary arXiv chunk view. One row per paper section, with universal chunk columns plus paper metadata.

DROP VIEW IF EXISTS chunks;
CREATE VIEW chunks AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch') AS created_at,
    CASE
        WHEN t.heading_command = 'abstract' THEN 'abstract'
        WHEN LOWER(t.section_heading) LIKE '%introduction%' THEN 'introduction'
        WHEN LOWER(t.section_heading) LIKE '%related%work%' THEN 'related_work'
        WHEN LOWER(t.section_heading) LIKE '%background%' THEN 'background'
        WHEN LOWER(t.section_heading) LIKE '%method%' THEN 'methodology'
        WHEN LOWER(t.section_heading) LIKE '%approach%' THEN 'methodology'
        WHEN LOWER(t.section_heading) LIKE '%model%' THEN 'methodology'
        WHEN LOWER(t.section_heading) LIKE '%architecture%' THEN 'methodology'
        WHEN LOWER(t.section_heading) LIKE '%experiment%' THEN 'experiments'
        WHEN LOWER(t.section_heading) LIKE '%evaluation%' THEN 'experiments'
        WHEN LOWER(t.section_heading) LIKE '%result%' THEN 'results'
        WHEN LOWER(t.section_heading) LIKE '%discussion%' THEN 'discussion'
        WHEN LOWER(t.section_heading) LIKE '%conclusion%' THEN 'conclusion'
        WHEN LOWER(t.section_heading) LIKE '%future%' THEN 'conclusion'
        WHEN LOWER(t.section_heading) LIKE '%appendix%' THEN 'appendix'
        WHEN LOWER(t.section_heading) LIKE '%supplement%' THEN 'appendix'
        ELSE COALESCE(t.heading_command, 'section')
    END AS type,
    s.source_id,
    s.source_type,
    s.position,
    src.title AS paper_title,
    src.author AS authors,
    src.url AS abs_url,
    src.file_date,
    t.arxiv_id,
    t.section_heading,
    t.heading_command,
    t.heading_depth,
    t.primary_category,
    t.categories,
    t.published,
    t.doi,
    t.journal_ref,
    t.source_type AS content_source,
    tree.parent_id AS parent_chunk,
    tree.depth AS tree_depth,
    tree.relation AS tree_relation,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_arxiv t ON r.id = t.chunk_id
LEFT JOIN _edges_tree tree ON r.id = tree.id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
