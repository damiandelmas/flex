-- @name: orient
-- @description: Markdown/Obsidian cell orientation with docs, views, presets, and samples
-- @multi: true

-- @query: about
SELECT value AS description FROM _meta WHERE key = 'description';

-- @query: cell_docs
SELECT scope, name, path, mtime, chars, content
FROM _flex_docs
ORDER BY
    CASE scope
        WHEN 'cell_instructions' THEN 0
        WHEN 'local_notes' THEN 1
        ELSE 2
    END,
    name;

-- @query: guide
SELECT 'start' AS topic, 'Use sections for chunk-level reading and notes for file-level metadata.' AS guidance
UNION ALL
SELECT 'semantic_search', 'Join vec_ops results to sections; pre-filter with SELECT id FROM sections when scoping by folder or tags.'
UNION ALL
SELECT 'links', 'Use _edges_wikilink for resolved links and _edges_wikilink_unresolved for ghost notes.'
UNION ALL
SELECT 'fields', 'Use _fields_inline for Dataview inline fields such as status:: active.';

-- @query: shape
SELECT 'notes' AS what, COUNT(*) AS n FROM _raw_sources
UNION ALL
SELECT 'sections', COUNT(*) FROM _raw_chunks
UNION ALL
SELECT 'resolved_wikilinks', COUNT(*) FROM _edges_wikilink
UNION ALL
SELECT 'ghost_notes', COUNT(*) FROM _edges_wikilink_unresolved
UNION ALL
SELECT 'dataview_fields', COUNT(*) FROM _fields_inline;

-- @query: views
SELECT m.name AS view_name, GROUP_CONCAT(p.name, ', ') AS columns
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view' AND m.name IN ('notes', 'sections', 'chunks', 'sources')
GROUP BY m.name
ORDER BY m.name;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;

-- @query: samples
SELECT
    n.title,
    s.section_title,
    substr(s.content, 1, 180) AS preview
FROM sections s
JOIN notes n ON n.source_id = s.source_id
ORDER BY s.source_id, s.id
LIMIT 5;

-- @query: link_summary
SELECT title, outgoing_links, backlinks, unresolved_links
FROM notes
ORDER BY backlinks DESC, outgoing_links DESC, title
LIMIT 10;
