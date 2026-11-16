-- @name: delegation-tree
-- @description: Recursive delegation tree from a parent session
-- @params: session (required)

WITH RECURSIVE tree AS (
    SELECT
        d.child_session_id,
        d.agent_type,
        1 as depth
    FROM _edges_delegations d
    JOIN _coding_agent_source_visibility vis
      ON vis.source_id = COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36))
     AND vis.visible = 1
    JOIN _coding_agent_source_visibility child_vis
      ON child_vis.source_id = d.child_session_id
     AND child_vis.visible = 1
    WHERE COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36)) LIKE '%' || :session || '%'

    UNION ALL

    -- CROSS JOIN fixes the order: tree, then its child edges, then visibility
    -- by primary key. Left to the planner, both visibility lookups started
    -- from the `visible` index (~28k rows each) and the step never finished.
    SELECT
        d2.child_session_id,
        d2.agent_type,
        t.depth + 1
    FROM tree t
    CROSS JOIN _edges_delegations d2
      ON COALESCE(d2.parent_source_id, substr(d2.chunk_id, 1, 36)) = t.child_session_id
    CROSS JOIN _coding_agent_source_visibility vis
      ON vis.source_id = COALESCE(d2.parent_source_id, substr(d2.chunk_id, 1, 36))
     AND vis.visible = 1
    CROSS JOIN _coding_agent_source_visibility child_vis
      ON child_vis.source_id = d2.child_session_id
     AND child_vis.visible = 1
    WHERE t.depth < 5
)
SELECT child_session_id as session, agent_type, depth
FROM tree
ORDER BY depth, child_session_id;
