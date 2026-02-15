-- @name: noise-filter-graph
-- @description: WHERE clause for thread source graph rebuild
-- @target: _enrich_source_graph
-- @module: flexsearch.modules.claude_code.manage.noise.graph_filter_sql()
-- @threshold: 0.65 (was 0.5 — median pairwise sim is 0.61, 0.5 gave 78% density)
--
-- Use as: build_similarity_graph(db, threshold=0.65, where=graph_filter_sql())
--
-- Filters:
--   min_chunks >= 20 (sessions with <20 chunks carry 4.7% of content)
--   no warmups (title = 'Warmup')
--   2,188 of 5,776 sources qualify (38% of total, 99% of applicable)
source_id IN (
    SELECT source_id FROM _edges_source
    GROUP BY source_id HAVING COUNT(*) >= 20
) AND source_id NOT IN (
    SELECT source_id FROM _raw_sources WHERE title = 'Warmup'
)
