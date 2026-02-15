-- @name: file-graph
-- @description: File co-edit graph — bipartite projection from shared file_uuids
-- @target: _enrich_file_graph
-- @script: flexsearch/modules/claude_code/manage/enrich_file_graph.py
-- @module: flexsearch.modules.claude_code.manage.file_graph
CREATE TABLE IF NOT EXISTS _enrich_file_graph (
    source_id TEXT PRIMARY KEY,
    file_community_id INTEGER,   -- Louvain community on file co-edit graph
    file_centrality REAL,        -- PageRank on file co-edit graph
    file_is_hub INTEGER DEFAULT 0,
    shared_file_count INTEGER    -- distinct files this session touched
);
-- Source data: _edges_file_identity + _edges_source
-- Edge: two sessions share a file_uuid -> weighted edge (weight = shared file count)
-- Noise: files touched by >200 sessions excluded (MAX_SESSIONS_PER_FILE in noise.py)
-- Communities = codebase regions. Hubs = sessions touching many shared files.
-- Orthogonal to similarity graph: debugging + implementation on same file are
-- neighbors here, strangers in embedding space.
-- Results (260214):
--   8,095 unique files, 2,411 sessions, 88,166 edges
--   418 communities, 593 hubs
--   85% of applicable sessions covered
--   Runs in ~1s
