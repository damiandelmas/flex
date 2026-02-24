-- @name: sections
-- @description: Chunk-level surface for doc-pac cells. Document sections with graph intelligence.

DROP VIEW IF EXISTS sections;
CREATE VIEW sections AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    s.source_id AS doc_id,
    s.position,
    src.title AS doc_title,
    CASE
        WHEN src.file_date LIKE '____-__-%' THEN src.file_date
        WHEN LENGTH(src.file_date) >= 8 AND SUBSTR(src.file_date,1,2) = '20'
        THEN SUBSTR(src.file_date,1,4) || '-' || SUBSTR(src.file_date,5,2) || '-' || SUBSTR(src.file_date,7,2)
        WHEN LENGTH(src.file_date) = 6 AND SUBSTR(src.file_date,1,2) = '20'
        THEN SUBSTR(src.file_date,1,4) || '-' || SUBSTR(src.file_date,5,2)
        WHEN LENGTH(src.file_date) >= 11 AND SUBSTR(src.file_date,7,1) = '-'
        THEN '20' || SUBSTR(src.file_date,1,2) || '-' || SUBSTR(src.file_date,3,2) || '-' || SUBSTR(src.file_date,5,2)
             || 'T' || SUBSTR(src.file_date,8,2) || ':' || SUBSTR(src.file_date,10,2)
        WHEN LENGTH(src.file_date) >= 6
        THEN '20' || SUBSTR(src.file_date,1,2) || '-' || SUBSTR(src.file_date,3,2) || '-' || SUBSTR(src.file_date,5,2)
        ELSE src.file_date
    END AS file_date,
    tp.doc_type,
    tp.temporal,
    tp.facet,
    tp.section_title,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _types_docpac tp ON r.id = tp.chunk_id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
