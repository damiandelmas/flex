-- @name: sources
-- @description: Paper-level surface for arXiv cells. One row per paper with aggregated metadata.

DROP VIEW IF EXISTS sources;
CREATE VIEW sources AS
SELECT
    s.source_id,
    s.title,
    s.author AS authors,
    s.url AS abs_url,
    s.file_date,
    s.score AS citation_count,
    s.num_comments AS section_count,
    -- Primary category from first chunk's types
    (SELECT t.primary_category FROM _types_arxiv t
     JOIN _edges_source e ON t.chunk_id = e.chunk_id
     WHERE e.source_id = s.source_id LIMIT 1) AS primary_category,
    (SELECT t.categories FROM _types_arxiv t
     JOIN _edges_source e ON t.chunk_id = e.chunk_id
     WHERE e.source_id = s.source_id LIMIT 1) AS categories,
    (SELECT t.published FROM _types_arxiv t
     JOIN _edges_source e ON t.chunk_id = e.chunk_id
     WHERE e.source_id = s.source_id LIMIT 1) AS published,
    (SELECT t.doi FROM _types_arxiv t
     JOIN _edges_source e ON t.chunk_id = e.chunk_id
     WHERE e.source_id = s.source_id LIMIT 1) AS doi,
    (SELECT t.source_type FROM _types_arxiv t
     JOIN _edges_source e ON t.chunk_id = e.chunk_id
     WHERE e.source_id = s.source_id LIMIT 1) AS content_source,
    -- Has full LaTeX source?
    (SELECT COUNT(*) > 0 FROM _edges_raw_content rc
     WHERE rc.source_id = s.source_id) AS has_latex,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_sources s
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
