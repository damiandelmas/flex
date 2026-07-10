-- @name: orient
-- @description: Docpac/context cell orientation — schema, document surfaces, graph entry points, presets, and query examples
-- @multi: true

-- @query: now
SELECT datetime('now', 'localtime') AS now,
       'UTC' || printf('%+d', CAST((julianday('now','localtime') - julianday('now')) * 24 AS INTEGER)) AS timezone;

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

-- @query: shape
SELECT 'documents' AS what, COUNT(*) AS n FROM documents
UNION ALL
SELECT 'sections', COUNT(*) FROM sections
UNION ALL
SELECT 'embedded_sections', COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL;

-- @query: embed_mode
-- The cell's own declaration (_meta.embed). Embed-off cells carry no vectors:
-- the semantic surface is INERT and @orient advertises only the structural one.
SELECT CASE
    WHEN lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) IN ('false','0','off','no')
    THEN 'embed-off — structural surface (keyword() FTS + node tree + declared edges + identity); vec_ops/similar/centroid/near_type are INERT'
    ELSE 'embed-on — semantic + structural surface (vec_ops + keyword() + views)'
END AS embed_mode;

-- @query: views
SELECT
  m.name,
  GROUP_CONCAT(p.name, ', ') AS columns,
  CASE m.name
    WHEN 'sections' THEN 'Primary reading surface. One row per markdown section/chunk; use for semantic, keyword, recent, and drilldown queries.'
    WHEN 'documents' THEN 'Document-level surface. One row per source doc; use for hubs, corpus shape, and document navigation.'
    WHEN 'chunks' THEN 'Richer chunk-level compatibility surface; useful when a cell has extra source/type fields.'
    ELSE ''
  END AS note
FROM sqlite_master m, pragma_table_info(m.name) p
WHERE m.type = 'view'
GROUP BY m.name
ORDER BY
  CASE m.name WHEN 'sections' THEN 0 WHEN 'documents' THEN 1 WHEN 'chunks' THEN 2 ELSE 9 END,
  m.name;

-- @query: query_surface
-- The semantic row is gated on the cell's embed mode (_meta.embed). An embed-off
-- cell carries no vectors, so vec_ops/similar/centroid/near_type are INERT: the row
-- is replaced by an explicit INERT notice pointing at the structural surface. The
-- predicate set ('false','0','off','no') matches cell_is_no_embed() verbatim.
SELECT 'scoring' AS kind,
       'vec' || '_ops(''similar:topic diverse pool:100'', ''SELECT id FROM sections'')' AS name,
       'id, score' AS columns,
       'Semantic retrieval over scoped section ids; use after FROM/JOIN.' AS note
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
UNION ALL
SELECT 'scoring',
       'INERT — no embeddings in this cell',
       '(unavailable)',
       'Embed-off cell: vec_ops/similar/centroid/near_type do not apply. Use keyword() + structural SQL + the node tree (_edges_tree) instead.'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) IN ('false','0','off','no')
UNION ALL
SELECT 'table_function',
       'key' || 'word(''term'', ''SELECT id FROM sections'')',
       'id, rank, snippet',
       'FTS5 exact-term search over scoped section ids; snippets mark hits with >>>term<<< highlighting.'
UNION ALL
SELECT 'view', 'sections',
       'id, content, source_id, position, title, source_path, file_date, doc_type, temporal, facet, section_title, centrality, is_hub, is_bridge, community_id',
       'Default context reading surface. Read content for section text.'
UNION ALL
SELECT 'view', 'documents',
       'source_id, title, source_path, file_date, doc_type, temporal, chunk_count, centrality, is_hub, community_id',
       'Document-level navigation and hub surface.'
UNION ALL
SELECT 'structural', 'node tree (_edges_tree)',
       'id, parent_id, depth',
       'Recursive section containment; walk with a CTE or filter depth=N. Structural navigation, always available.'
UNION ALL
SELECT 'table', 'chunks_fts',
       'rowid, content',
       'Raw FTS5 table. Prefer keyword(); bridge manually only for advanced filters.';

-- @query: doc_types
-- doc_type is the compound 'category.subtype'; plan-support refs (slot/spec/shape)
-- have no category, i.e. no '.', and are folded into one 'plan-support' line —
-- they live inside their plan, not as top-level doc_types. (The docpac views expose
-- doc_type, not a split category column.)
SELECT CASE WHEN INSTR(doc_type, '.') = 0 THEN 'plan-support (inside plans)'
            ELSE doc_type END AS doc_type,
       temporal, COUNT(DISTINCT source_id) AS documents, COUNT(*) AS sections
FROM sections
GROUP BY 1, temporal
ORDER BY documents DESC, sections DESC
LIMIT 20;

-- @query: recent
SELECT
  source_id,
  title,
  source_path,
  file_date,
  doc_type,
  section_title,
  substr(content, 1, 500) AS preview
FROM sections
ORDER BY file_date DESC, source_id DESC, position ASC
LIMIT 12;

-- @query: hubs
SELECT
  source_id,
  title,
  source_path,
  file_date,
  doc_type,
  chunk_count,
  ROUND(centrality, 4) AS centrality,
  community_id
FROM documents
WHERE is_hub = 1
ORDER BY centrality DESC
LIMIT 12;

-- @query: communities
SELECT community_id, COUNT(DISTINCT source_id) AS documents, COUNT(*) AS sections
FROM sections
WHERE community_id IS NOT NULL
GROUP BY community_id
ORDER BY documents DESC
LIMIT 12;

-- @query: section_titles
SELECT section_title, COUNT(*) AS sections, COUNT(DISTINCT source_id) AS documents
FROM sections
WHERE section_title IS NOT NULL
GROUP BY section_title
ORDER BY sections DESC
LIMIT 30;

-- @query: presets
-- source labels cell-shipped (.flexpresets.json) vs stock presets; COALESCE covers
-- rows/cells written before the source column existed.
SELECT name, description, params, COALESCE(source, 'stock') AS source
FROM _presets ORDER BY source DESC, name;

-- @query: examples
SELECT 'recent_overviews' AS use_case,
       'SELECT source_id, title, source_path, file_date, doc_type, substr(content,1,1200) AS content FROM sections WHERE section_title = ''Overview'' ORDER BY file_date DESC LIMIT 10' AS sql
UNION ALL
SELECT 'recent_section',
       'SELECT source_id, title, source_path, file_date, doc_type, section_title, substr(content,1,1200) AS content FROM sections WHERE section_title = ''SECTION TITLE'' ORDER BY file_date DESC LIMIT 10'
UNION ALL
-- Semantic examples are embed-on only; an embed-off cell has no vectors to score.
SELECT 'semantic_sections',
       'SELECT v.score, s.source_id, s.title, s.source_path, s.file_date, s.section_title, substr(s.content,1,1200) AS content FROM vec' || '_ops(''similar:YOUR TOPIC diverse pool:100'', ''SELECT id FROM sections'') v JOIN sections s ON s.id = v.id ORDER BY v.score DESC LIMIT 10'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
UNION ALL
SELECT 'semantic_section',
       'SELECT v.score, s.source_id, s.title, s.source_path, s.file_date, s.doc_type, s.section_title, substr(s.content,1,1200) AS content FROM vec' || '_ops(''similar:YOUR TOPIC diverse pool:100'', ''SELECT id FROM sections WHERE section_title = ''''SECTION TITLE'''' '') v JOIN sections s ON s.id = v.id ORDER BY v.score DESC LIMIT 10'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
UNION ALL
SELECT 'tree_subtree',
       'SELECT t.depth, s.section_title, substr(s.content,1,800) AS content FROM _edges_tree t JOIN sections s ON s.id = t.id WHERE t.id LIKE ''SOURCE_ID%'' ORDER BY t.depth, s.position'
UNION ALL
SELECT 'exact_term',
       'SELECT k.rank, k.snippet, s.source_id, s.title, s.source_path, s.section_title, substr(s.content,1,1000) AS content FROM key' || 'word(''TERM'', ''SELECT id FROM sections'') k JOIN sections s ON s.id = k.id ORDER BY k.rank DESC LIMIT 10'
UNION ALL
SELECT 'type_recent',
       'SELECT source_id, title, source_path, file_date, doc_type, section_title, substr(content,1,1200) AS content FROM sections WHERE doc_type = ''DOC TYPE'' ORDER BY file_date DESC LIMIT 10'
UNION ALL
SELECT 'full_document',
       'SELECT section_title, substr(content,1,2200) AS content FROM sections WHERE source_id = ''SOURCE_ID'' ORDER BY position LIMIT 30'
UNION ALL
SELECT 'hub_documents',
       'SELECT source_id, title, source_path, file_date, doc_type, centrality FROM documents WHERE is_hub = 1 ORDER BY centrality DESC LIMIT 10';
