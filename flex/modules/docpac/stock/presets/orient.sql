-- @name: orient
-- @description: Docpac/context cell orientation — schema, document surfaces, graph entry points, presets, and query examples
-- @multi: true

-- @query: now
SELECT datetime('now', 'localtime') AS now,
       'UTC' || printf('%+d', CAST((julianday('now','localtime') - julianday('now')) * 24 AS INTEGER)) AS timezone;

-- @query: about
SELECT value AS description FROM _meta WHERE key = 'description';

-- @query: shape
SELECT 'documents' AS what, COUNT(*) AS n FROM documents
UNION ALL
SELECT 'sections', COUNT(*) FROM sections
UNION ALL
SELECT 'embedded_sections', COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL;

-- @query: embed_mode
-- The cell's own declaration (_meta.embed). Embed-off cells carry no vectors:
-- semantic scoring is unavailable and @orient advertises the structural surface.
SELECT CASE
    WHEN lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) IN ('false','0','off','no')
    THEN 'embed-off — structural surface (FTS5 + node tree + declared edges + identity); semantic scoring is unavailable'
    WHEN EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
    THEN 'embed-on — semantic + structural surface (llm_embed_generate + vec_distance_cosine + chunks_fts + views)'
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
-- cell carries no vectors, so vec_ops/similar/centroid/near_type are unavailable: the row
-- is replaced by an explicit notice pointing at the structural surface. The
-- predicate set ('false','0','off','no') matches cell_is_no_embed() verbatim.
-- Retrieval rows are also derived from the connection: the Flex engine (no sqlite-vec)
-- offers vec_ops/keyword(); a connection with sqlite-vec and sqlite-ai loaded offers llm_embed_generate/vec_distance_*. Both list chunks_fts.
SELECT 'scoring' AS kind,
       'vec' || '_ops(''similar:topic diverse pool:100'', ''SELECT id FROM sections'')' AS name,
       'id, score' AS columns,
       'Semantic retrieval over scoped section ids; use after FROM/JOIN.' AS note
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
  AND NOT EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'scoring', 'llm_embed_generate', 'text -> float32[768]',
       'Embed a question in SQL: vec_slice(llm_embed_generate(''search_query: '' || :q), 0, 256). Compute once in a MATERIALIZED CTE.'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
  AND EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'llm_embed_generate')
UNION ALL
SELECT 'scoring', 'vec_distance_cosine / vec_distance_l2', 'distance',
       'Exact semantic scoring over _raw_chunks.embedding (768 float32; score at 256 with vec_slice). score = 1 - vec_distance_cosine(...).'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
  AND EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'scoring',
       'UNAVAILABLE — no embeddings in this cell',
       '(unavailable)',
       'Embed-off cell: semantic scoring does not apply on either path. Use FTS5 (chunks_fts) + structural SQL + the node tree (_edges_tree) instead.'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) IN ('false','0','off','no')
UNION ALL
SELECT 'table_function',
       'key' || 'word(''term'', ''SELECT id FROM sections'')',
       'id, rank, snippet',
       'FTS5 exact-term search over scoped section ids; snippets mark hits with >>>term<<< highlighting.'
WHERE NOT EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
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
       'FTS5 index over _raw_chunks (rowid = _raw_chunks.rowid). chunks_fts MATCH ''term'' or ''"exact phrase"''; bm25(chunks_fts) ascending is best.';

-- @query: health_defects
-- Rejected metadata is visible rather than silently entering chronology.
SELECT kind, COUNT(*) AS affected
FROM _health_defects
GROUP BY kind
ORDER BY affected DESC, kind;

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
SELECT name, description, params FROM _presets ORDER BY name;

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
  AND NOT EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'semantic_section',
       'SELECT v.score, s.source_id, s.title, s.source_path, s.file_date, s.doc_type, s.section_title, substr(s.content,1,1200) AS content FROM vec' || '_ops(''similar:YOUR TOPIC diverse pool:100'', ''SELECT id FROM sections WHERE section_title = ''''SECTION TITLE'''' '') v JOIN sections s ON s.id = v.id ORDER BY v.score DESC LIMIT 10'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
  AND NOT EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'semantic_sections',
       'WITH q AS MATERIALIZED (SELECT vec_slice(llm_embed_generate(''search_query: '' || :q), 0, 256) AS v) SELECT 1 - vec_distance_cosine(vec_slice(c.embedding, 0, 256), q.v) AS score, s.source_id, s.title, s.source_path, s.file_date, s.section_title, substr(s.content,1,1200) AS content FROM q, _raw_chunks c JOIN sections s ON s.id = c.id WHERE c.embedding IS NOT NULL ORDER BY score DESC LIMIT 10'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
  AND EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'semantic_section',
       'WITH q AS MATERIALIZED (SELECT vec_slice(llm_embed_generate(''search_query: '' || :q), 0, 256) AS v) SELECT 1 - vec_distance_cosine(vec_slice(c.embedding, 0, 256), q.v) AS score, s.source_id, s.title, s.source_path, s.file_date, s.doc_type, s.section_title, substr(s.content,1,1200) AS content FROM q, _raw_chunks c JOIN sections s ON s.id = c.id WHERE s.section_title = ''SECTION TITLE'' AND c.embedding IS NOT NULL ORDER BY score DESC LIMIT 10'
WHERE lower(COALESCE((SELECT value FROM _meta WHERE key='embed'),'true')) NOT IN ('false','0','off','no')
  AND EXISTS (SELECT 1 FROM pragma_function_list WHERE name = 'vec_distance_cosine')
UNION ALL
SELECT 'tree_subtree',
       'WITH RECURSIVE subtree(id, depth) AS (SELECT id, 0 FROM sections WHERE id = ''ROOT_SECTION_ID'' UNION ALL SELECT e.id, subtree.depth + 1 FROM _edges_tree e JOIN subtree ON e.parent_id = subtree.id) SELECT subtree.depth, s.section_title, substr(s.content,1,800) AS content FROM subtree JOIN sections s ON s.id = subtree.id ORDER BY subtree.depth, s.position'
UNION ALL
SELECT 'exact_term',
       'SELECT bm25(chunks_fts) AS rank, s.source_id, s.title, s.source_path, s.section_title, substr(s.content,1,1000) AS content FROM chunks_fts f JOIN _raw_chunks c ON c.rowid = f.rowid JOIN sections s ON s.id = c.id WHERE chunks_fts MATCH ''TERM'' ORDER BY rank LIMIT 10'
UNION ALL
SELECT 'type_recent',
       'SELECT source_id, title, source_path, file_date, doc_type, section_title, substr(content,1,1200) AS content FROM sections WHERE doc_type = ''DOC TYPE'' ORDER BY file_date DESC LIMIT 10'
UNION ALL
SELECT 'full_document',
       'SELECT section_title, substr(content,1,2200) AS content FROM sections WHERE source_id = ''SOURCE_ID'' ORDER BY position LIMIT 30'
UNION ALL
SELECT 'hub_documents',
       'SELECT source_id, title, source_path, file_date, doc_type, centrality FROM documents WHERE is_hub = 1 ORDER BY centrality DESC LIMIT 10';
