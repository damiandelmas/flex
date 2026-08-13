-- @name: orient
-- @description: flex code cell — the LIVE query contract for a codegraph cell: a repository compiled
--   into a recursive symbol tree with a call graph + import graph. The agentic PRAGMA for code
--   navigation. It COMPLEMENTS the flex:code skill — the query METHOD (blast-radius workflow, recipes,
--   graph SQL) lives in the skill; this is the live per-cell contract (shape, columns, graph tables,
--   nav verbs, coverage). Code cells are ALWAYS no-embed → structural + graph only, no vec_ops, no
--   embed-mode branch. Columns DISCOVERED so it's honest whatever the build produced.
-- @multi: true
--

-- @query: now
SELECT datetime('now','localtime') AS now;

-- @query: about
SELECT COALESCE((SELECT value FROM _meta WHERE key='description'), 'code cell — a repository as a symbol/call/import graph') AS description,
       (SELECT value FROM _meta WHERE key='code_surface_version') AS surface_version;

-- @query: shape
-- Raw tables (always present). files · symbols · call edges · import edges.
SELECT 'files' AS what, COUNT(*) AS n FROM _raw_sources
UNION ALL SELECT 'symbols', COUNT(*) FROM _symbols
UNION ALL SELECT 'call_edges', COUNT(*) FROM _edges_call
UNION ALL SELECT 'import_edges', COUNT(*) FROM _edges_import;

-- @query: columns
-- The chunks view (one row per node; section_title = the symbol name, (module) = file preamble),
-- DISCOVERED so it reflects the actual build.
SELECT 'chunks' AS view, group_concat(name, ', ') AS columns FROM pragma_table_info('chunks');

-- @query: graph_surface
-- The code-cell tables that make it a graph, and what each carries. Query the graph via the nav
-- presets below (or these tables directly); see the flex:code skill for the SQL recipes.
SELECT '_symbols' AS tbl, 'the symbol index — name, kind, defining file (name, def_id, file_id, kind)' AS carries
UNION ALL SELECT '_types_instant', 'per-node metadata — section_title (symbol name), depth (1=top-level, 2=method), container_id (parent node), position'
UNION ALL SELECT '_edges_call', 'the call graph — caller_id -> callee_name (name-resolved; unresolved/external calls keep the name)'
UNION ALL SELECT '_edges_import', 'the import graph — source_id imports module (name)'
UNION ALL SELECT '_edges_tree', 'containment — id -> parent_id, depth (module > class > method)'
UNION ALL SELECT '_edges_source', 'node -> file provenance'
UNION ALL SELECT '_edges_fs_identity', 'SOMA file_uuid per file — the hinge to session cells';

-- @query: graph_sql
-- Traverse the graph directly with SQL over _edges_call, _edges_import, and _edges_tree.
SELECT '_edges_call' AS table_name, 'caller_id -> callee_name; preserve unresolved and ambiguous names' AS carries
UNION ALL SELECT '_edges_import', 'source_id -> imported module'
UNION ALL SELECT '_edges_tree', 'id -> parent_id containment';

-- @query: retrieval
-- Fixed posture for a code cell: structural + graph first; keyword() for exact strings; NO vec_ops.
SELECT 'graph' AS surface, 'navigation — SQL over _edges_call/_edges_import/_edges_tree. The primary way to move through code.' AS how
UNION ALL SELECT 'symbol', 'exact definition — keyword(''NAME'') or WHERE section_title = ''NAME'' on chunks'
UNION ALL SELECT 'keyword()', 'FTS5 over content — a literal, error string, or identifier. Scope with the 2nd arg.'
UNION ALL SELECT 'structural', 'GROUP BY / COUNT(DISTINCT) over chunks / _edges_* — free; get the shape before reading bodies'
UNION ALL SELECT 'vec_ops', 'UNAVAILABLE and rejected — a code cell has no embeddings. There is no semantic/fuzzy search; use keyword() + the graph.';

-- @query: coverage
-- Be honest about what the graph does and does not cover (qualify claims against this).
SELECT 'languages' AS aspect, 'call + import edges and the class > method tree: Python (ast) and JS/TS (tree-sitter). Other languages chunk flat; markdown nests by heading — no call graph.' AS detail
UNION ALL SELECT 'call resolution', 'Python: bare foo() only. JS/TS: bare foo() plus this.m()/super.m() (resolved to the enclosing class). Other member calls (obj.m()) are NOT edges — name-only resolution cannot disambiguate them.'
UNION ALL SELECT 'external / collisions', 'stdlib/external calls are unresolved. Same-named definitions are conservative candidate sets; @impact reports an ambiguous boundary but does not traverse through it.'
UNION ALL SELECT 'fan-out', 'chunks may fan out through optional enrichments. @subtree reads one-row structural tables; graph presets label any candidate expansion.';

-- @query: method
-- Pointer, not pedagogy: the query METHOD lives in the skill; this orient is the live contract.
SELECT 'flex:code' AS skill, 'blast-radius workflow, hot-symbol/orphan recipes, the graph SQL, methodology, limits' AS owns
UNION ALL SELECT 'flex:instant', 'the same substrate as a plain fs cell — path scoping, the file_uuid session hinge';
