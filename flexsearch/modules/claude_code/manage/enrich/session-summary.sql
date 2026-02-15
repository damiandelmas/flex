-- @name: session-summary
-- @description: Embedding-relative session summaries via HDBSCAN clustering
-- @target: _enrich_session_summary
-- @script: flexsearch/modules/claude_code/manage/enrich_summary.py
-- @module: flexsearch.modules.claude_code.manage.summary (label_cluster, short_session_label)
-- @noise: flexsearch.modules.claude_code.manage.noise.session_filter_sql()
CREATE TABLE IF NOT EXISTS _enrich_session_summary (
    source_id TEXT PRIMARY KEY,
    topic_clusters TEXT,      -- JSON: [{"label": "auth.py + router.ts", "pct": 65.2, "count": 45}, ...]
    community_label TEXT,     -- from project distribution in same community
    topic_summary TEXT        -- composed one-liner (named topic_summary to avoid _raw_sources.summary collision)
);
-- Config:
--   session filter: message_count >= 5, not agent-*, not Warmup
--   HDBSCAN: min_chunks=20, min_cluster_size=5, min_samples=3, metric=euclidean
--   short_session_label fallback for sessions with <20 chunks
-- Results (260214):
--   1,261 sessions (1,124 HDBSCAN, 137 short), 0 mixed fallbacks
--   52% of applicable sessions (>= 20 chunks)
--   Runs in ~31s
