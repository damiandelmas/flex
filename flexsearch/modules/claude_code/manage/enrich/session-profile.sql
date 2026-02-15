-- @name: session-profile
-- @description: Session keyword exhaust for source-level search
-- @target: _enrich_session_profile
-- @status: cleaned 260214 — killed session_shape, dominant_tool, tool_signature,
--          file_count, files_touched (all derivable from edge tables via SQL)
CREATE TABLE IF NOT EXISTS _enrich_session_profile (
    source_id TEXT PRIMARY KEY,
    keyword_exhaust TEXT        -- pipe-delimited: files | centroid_keywords | tool_sig | shape | prompt | community
);
-- keyword_exhaust is the source-level search surface.
-- Format: LIKE-searchable, embeddable. Future: ONNX re-embed into _raw_sources.embedding.
-- Results (260214):
--   1,250 rows (from old enrichment run, pre-noise-filter)
--   Downstream pipeline (corpus clusters → differential → exhaust → re-embed) deferred
