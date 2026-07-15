-- @name: sections
-- @description: Chunk-level surface for doc-pac cells. Document sections with graph intelligence.

DROP VIEW IF EXISTS sections;
CREATE VIEW sections AS
SELECT
    r.id,
    r.content,
    CASE
        WHEN r.content GLOB '#*' AND INSTR(r.content, CHAR(10)) > 0
        THEN LTRIM(SUBSTR(r.content, INSTR(r.content, CHAR(10)) + 1))
        ELSE r.content
    END AS body,
    r.timestamp,
    s.source_id AS doc_id,
    s.source_id AS source_id,
    s.position,
    COALESCE(tree.depth, 0) AS depth,
    src.title AS doc_title,
    src.title AS title,
    src.source_path,
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
    -- coordinate split: doc_type = "category.subtype" (flat → category NULL)
    CASE WHEN INSTR(tp.doc_type, '.') > 0
         THEN SUBSTR(tp.doc_type, 1, INSTR(tp.doc_type, '.') - 1) END AS category,
    CASE WHEN INSTR(tp.doc_type, '.') > 0
         THEN SUBSTR(tp.doc_type, INSTR(tp.doc_type, '.') + 1)
         ELSE tp.doc_type END AS subtype,
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
LEFT JOIN _edges_tree tree ON tree.id = r.id
LEFT JOIN _enrich_source_graph g ON s.source_id = g.source_id;
