-- @name: delegation-graph
-- @description: Directed parent->child agent delegation graph
-- @target: _enrich_delegation_graph
-- @script: flexsearch/modules/claude_code/manage/enrich_delegation.py
-- @module: flexsearch.modules.claude_code.manage.delegation_graph
CREATE TABLE IF NOT EXISTS _enrich_delegation_graph (
    source_id TEXT PRIMARY KEY,
    agents_spawned INTEGER,        -- out-degree (how many agents this session spawned)
    is_orchestrator INTEGER DEFAULT 0,  -- 1 if agents_spawned > ORCHESTRATOR_THRESHOLD (5)
    delegation_depth INTEGER,      -- BFS depth from root (0 = root, 1 = child)
    parent_session TEXT            -- source_id of parent (NULL for roots)
);
-- Source data: _edges_delegations + _edges_source
-- Directed graph: parent_source_id -> child_doc_id
-- Threshold: ORCHESTRATOR_THRESHOLD = 5 (from noise.py)
-- Results (260214):
--   1,732 sessions, 1,443 edges, 58 orchestrators
--   Max depth: 1 (flat delegation — no grandchildren observed)
--   55% of applicable sessions covered (structural limit — not all sessions spawn)
--   Runs in ~0.1s
