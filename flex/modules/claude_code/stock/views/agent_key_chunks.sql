-- @name: agent_key_chunks
-- @description: High-signal agent timeline chunks for coding-agent cells.
-- Use as a pre-filter for intent, edits, delegation, failures, and summary/gap retrieval.

DROP VIEW IF EXISTS agent_key_chunks;
CREATE VIEW agent_key_chunks AS
WITH classified AS (
    SELECT
        m.*,
        CASE
            WHEN LOWER(LTRIM(m.content)) LIKE '<environment_context>%' THEN 1
            WHEN LOWER(LTRIM(m.content)) LIKE '<turn_aborted>%' THEN 1
            WHEN LOWER(LTRIM(m.content)) LIKE '# agents.md instructions%' THEN 1
            WHEN LOWER(LTRIM(m.content)) LIKE '<instructions>%' THEN 1
            WHEN LOWER(LTRIM(m.content)) LIKE 'chunk id:%' THEN 1
            WHEN LOWER(m.content) LIKE '%startup_timeout_sec%' THEN 1
            WHEN LOWER(m.content) LIKE '%mcp client for%timed out%' THEN 1
            WHEN LOWER(m.content) LIKE '%shells still running%' THEN 1
            ELSE 0
        END AS is_boilerplate
    FROM messages m
)
SELECT
    m.*,
    CASE
        WHEN m.type = 'user_prompt' THEN 'user_prompt'
        WHEN m.tool_name = 'TodoWrite' THEN 'plan'
        WHEN m.tool_name IN ('Edit', 'Write', 'MultiEdit') THEN 'file_change'
        WHEN m.tool_name = 'Task' OR m.child_session_id IS NOT NULL THEN 'delegation'
        WHEN m.success = 0 THEN 'failed_tool'
        WHEN m.type = 'assistant'
          AND (
              LOWER(m.content) LIKE '%remaining%'
              OR LOWER(m.content) LIKE '%unresolved%'
              OR LOWER(m.content) LIKE '%blocked%'
              OR LOWER(m.content) LIKE '%verified%'
              OR LOWER(m.content) LIKE '%not tested%'
              OR LOWER(m.content) LIKE '%next%'
          )
          THEN 'summary_or_gap'
        ELSE 'context'
    END AS key_reason,
    CASE
        WHEN m.type = 'user_prompt' THEN 100
        WHEN m.tool_name = 'TodoWrite' THEN 90
        WHEN m.success = 0 THEN 85
        WHEN m.tool_name IN ('Edit', 'Write', 'MultiEdit') THEN 80
        WHEN m.tool_name = 'Task' OR m.child_session_id IS NOT NULL THEN 75
        WHEN m.type = 'assistant' THEN 55
        ELSE 10
    END AS key_weight
FROM classified m
WHERE (m.type = 'user_prompt' AND m.is_boilerplate = 0)
   OR m.tool_name = 'TodoWrite'
   OR m.tool_name IN ('Edit', 'Write', 'MultiEdit')
   OR m.tool_name = 'Task'
   OR m.child_session_id IS NOT NULL
   OR m.success = 0
   OR (
       m.type = 'assistant'
       AND m.is_boilerplate = 0
       AND (
           LOWER(m.content) LIKE '%remaining%'
           OR LOWER(m.content) LIKE '%unresolved%'
           OR LOWER(m.content) LIKE '%blocked%'
           OR LOWER(m.content) LIKE '%verified%'
           OR LOWER(m.content) LIKE '%not tested%'
           OR LOWER(m.content) LIKE '%next%'
       )
   );
