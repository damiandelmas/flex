-- @name: delegation-tree
-- @description: Recursive delegation tree from a parent session
-- @params: session (required)

WITH RECURSIVE tree AS (
    SELECT
        d.child_doc_id,
        d.agent_type,
        1 as depth
    FROM _edges_delegations d
    JOIN _edges_source e ON d.chunk_id = e.chunk_id
    WHERE e.source_id LIKE '%' || :session || '%'

    UNION ALL

    SELECT
        d2.child_doc_id,
        d2.agent_type,
        t.depth + 1
    FROM _edges_delegations d2
    JOIN _edges_source e2 ON d2.chunk_id = e2.chunk_id
    JOIN tree t ON e2.source_id = t.child_doc_id
    WHERE t.depth < 5
)
SELECT child_doc_id as session, agent_type, depth
FROM tree
ORDER BY depth, child_doc_id;
