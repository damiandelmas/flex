-- @name: docs
-- @description: README section view — one row per README span chunk with section_type canonicalization from raw headings.

DROP VIEW IF EXISTS docs;
CREATE VIEW docs AS
SELECT
    c.id,
    c.content,
    c.timestamp,
    es.source_id,
    t.tool_name,
    t.github_owner,
    t.github_repo,
    t.section_heading,
    t.heading_command,
    t.heading_depth,
    CASE
        WHEN LOWER(t.section_heading) LIKE '%install%' THEN 'installation'
        WHEN LOWER(t.section_heading) LIKE '%getting started%' THEN 'installation'
        WHEN LOWER(t.section_heading) LIKE '%setup%' THEN 'installation'
        WHEN LOWER(t.section_heading) LIKE '%quick start%' THEN 'installation'
        WHEN LOWER(t.section_heading) LIKE '%prerequisite%' THEN 'requirements'
        WHEN LOWER(t.section_heading) LIKE '%require%' THEN 'requirements'
        WHEN LOWER(t.section_heading) LIKE '%depend%' THEN 'requirements'
        WHEN LOWER(t.section_heading) LIKE '%usage%' THEN 'usage'
        WHEN LOWER(t.section_heading) LIKE '%how to%' THEN 'usage'
        WHEN LOWER(t.section_heading) LIKE '%example%' THEN 'examples'
        WHEN LOWER(t.section_heading) LIKE '%demo%' THEN 'examples'
        WHEN LOWER(t.section_heading) LIKE '%api%' THEN 'api'
        WHEN LOWER(t.section_heading) LIKE '%reference%' THEN 'api'
        WHEN LOWER(t.section_heading) LIKE '%config%' THEN 'configuration'
        WHEN LOWER(t.section_heading) LIKE '%option%' THEN 'configuration'
        WHEN LOWER(t.section_heading) LIKE '%setting%' THEN 'configuration'
        WHEN LOWER(t.section_heading) LIKE '%feature%' THEN 'features'
        WHEN LOWER(t.section_heading) LIKE '%overview%' THEN 'overview'
        WHEN LOWER(t.section_heading) LIKE '%about%' THEN 'overview'
        WHEN LOWER(t.section_heading) LIKE '%what is%' THEN 'overview'
        WHEN LOWER(t.section_heading) LIKE '%introduc%' THEN 'overview'
        WHEN LOWER(t.section_heading) LIKE '%contribut%' THEN 'contributing'
        WHEN LOWER(t.section_heading) LIKE '%license%' THEN 'license'
        WHEN LOWER(t.section_heading) LIKE '%troubleshoot%' THEN 'troubleshooting'
        WHEN LOWER(t.section_heading) LIKE '%faq%' THEN 'troubleshooting'
        WHEN LOWER(t.section_heading) LIKE '%changelog%' THEN 'changelog'
        WHEN LOWER(t.section_heading) LIKE '%roadmap%' THEN 'roadmap'
        WHEN LOWER(t.section_heading) LIKE '%architect%' THEN 'architecture'
        WHEN LOWER(t.section_heading) LIKE '%design%' THEN 'architecture'
        ELSE 'other'
    END AS section_type,
    tree.parent_id AS parent_chunk,
    tree.depth AS tree_depth
FROM _raw_chunks c
JOIN _types_skills t ON c.id = t.chunk_id
JOIN _edges_source es ON c.id = es.chunk_id
LEFT JOIN _edges_tree tree ON c.id = tree.id
WHERE t.chunk_type = 'readme_span';
