-- @name: landscape
-- @description: Research landscape overview — categories, paper counts, date range
-- @params:

SELECT
    'shape' AS query,
    (SELECT COUNT(*) FROM _raw_sources) AS papers,
    (SELECT COUNT(*) FROM _raw_chunks) AS sections,
    (SELECT MIN(published) FROM _types_arxiv) AS earliest,
    (SELECT MAX(published) FROM _types_arxiv) AS latest

UNION ALL

SELECT
    'categories' AS query,
    NULL, NULL, NULL,
    (SELECT GROUP_CONCAT(DISTINCT primary_category) FROM _types_arxiv WHERE primary_category != '')

UNION ALL

SELECT
    'section_types' AS query,
    NULL, NULL, NULL,
    (SELECT GROUP_CONCAT(DISTINCT heading_command) FROM _types_arxiv);
