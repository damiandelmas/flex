-- @name: source-graph
-- @description: Source-level similarity graph — Louvain + PageRank + hubs + bridges
-- @target: _enrich_source_graph
-- @script: flexsearch/modules/claude_code/manage/rebuild_all.py (rebuild_source_graph)
-- @module: flexsearch.manage.meditate (build_similarity_graph, compute_scores, persist)
-- @noise: flexsearch.modules.claude_code.manage.noise.graph_filter_sql()
CREATE TABLE IF NOT EXISTS _enrich_source_graph (
    source_id TEXT PRIMARY KEY,
    centrality REAL,             -- PageRank
    is_hub INTEGER DEFAULT 0,    -- top 10% by centrality
    is_bridge INTEGER DEFAULT 0, -- top 5% by betweenness (k=100 approx for >500 nodes)
    community_id INTEGER         -- Louvain community
);
-- Config:
--   threshold: 0.65 (corpus median pairwise sim = 0.61)
--   betweenness: approximate k=100 for graphs >500 nodes
-- Results (260214):
--   2,188 nodes, 928,750 edges (39% density)
--   6 communities, 218 hubs, 109 bridges
--   Built in 65s total
