-- @name: warmup-classification
-- @description: Structural warmup detection — sessions with <50 tool ops and no modifications
-- @target: _types_source_warmup
--
-- Warmup: a session where ALL tool calls are exploration (Glob/Read/Bash)
-- and total tool-op chunks < 50. No Write/Edit/Task/MultiEdit.
-- Replaces brittle title = 'Warmup' filter.
--
-- Run context: rebuild_all.py Step 0 (before graph build).
-- The table has source_id PK so it auto-JOINs into sessions view.

CREATE TABLE IF NOT EXISTS _types_source_warmup (
    source_id TEXT PRIMARY KEY,
    is_warmup_only INTEGER DEFAULT 0
);

DELETE FROM _types_source_warmup;

INSERT INTO _types_source_warmup (source_id, is_warmup_only)
SELECT es.source_id, 1
FROM _edges_source es
JOIN _edges_tool_ops t ON es.chunk_id = t.chunk_id
GROUP BY es.source_id
HAVING COUNT(*) < 50
   AND SUM(CASE WHEN t.tool_name IN ('Write', 'Edit', 'Task', 'MultiEdit')
                THEN 1 ELSE 0 END) = 0;
