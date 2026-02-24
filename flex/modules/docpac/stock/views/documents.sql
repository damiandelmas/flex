-- @name: documents
-- @description: Source-level surface for doc-pac cells. Document metadata with graph intelligence.

DROP VIEW IF EXISTS documents;
CREATE VIEW documents AS
SELECT
    src.source_id,
    src.title,
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
    src.temporal,
    src.doc_type,
    COUNT(DISTINCT s.chunk_id) as chunk_count,
    g.centrality,
    g.is_hub,
    g.is_bridge,
    g.community_id
FROM _raw_sources src
LEFT JOIN _edges_source s ON src.source_id = s.source_id
LEFT JOIN _enrich_source_graph g ON src.source_id = g.source_id
GROUP BY src.source_id;
