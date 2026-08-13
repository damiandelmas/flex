-- @name: orient
-- @description: Live contract for one mixed filesystem cell.
-- @multi: true

-- @query: about
SELECT COALESCE((SELECT value FROM _meta WHERE key='description'), 'filesystem cell') AS description,
       (SELECT value FROM _meta WHERE key='source_path') AS root;

-- @query: configuration
SELECT COALESCE((SELECT value FROM _meta WHERE key='lifecycle'),'unknown') AS lifecycle,
       COALESCE((SELECT value FROM _meta WHERE key='obsidian'),'false') AS obsidian,
       COALESCE((SELECT value FROM _meta WHERE key='embed'),'false') AS embed;

-- @query: freshness
SELECT source_state, COUNT(*) AS files,
       datetime(MAX(mtime_ns) / 1000000000, 'unixepoch') AS newest_source_utc
FROM _filesystem_source_state
GROUP BY source_state ORDER BY source_state;

-- @query: shape
SELECT 'files' AS what, COUNT(*) AS n FROM _raw_sources
UNION ALL SELECT 'chunks', COUNT(*) FROM _raw_chunks
UNION ALL SELECT 'symbols', COUNT(*) FROM _symbols
UNION ALL SELECT 'call_edges', COUNT(*) FROM _edges_call
UNION ALL SELECT 'import_edges', COUNT(*) FROM _edges_import
UNION ALL SELECT 'wikilinks', COUNT(*) FROM _edges_wikilink;

-- @query: file_kinds
SELECT file_kind, COUNT(DISTINCT source_id) AS files, COUNT(*) AS chunks
FROM chunks GROUP BY file_kind ORDER BY files DESC, file_kind;

-- @query: embedding
SELECT COALESCE((SELECT value FROM _meta WHERE key='embed'),'false') AS enabled,
       (SELECT value FROM _meta WHERE key='vec:model') AS model,
       (SELECT value FROM _meta WHERE key='embedding_dim') AS stored_dim,
       (SELECT value FROM _meta WHERE key='vec:serve_dim') AS serve_dim;

-- @query: capabilities
SELECT 'keyword()' AS surface, 'FTS5 exact-term retrieval across every file kind' AS use
UNION ALL SELECT 'vec_ops()', 'semantic retrieval when embed=true'
UNION ALL SELECT '_edges_tree', 'heading/symbol containment via recursive SQL'
UNION ALL SELECT '_fields_inline/_edges_wikilink', 'Markdown metadata; vault links when obsidian=true';

-- @query: columns
SELECT 'chunks' AS view, group_concat(name, ', ') AS columns FROM pragma_table_info('chunks');
