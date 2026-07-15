-- @name: subtree
-- @description: Recursive descendants of a heading or symbol chunk.
-- @params: root (required)
WITH RECURSIVE sub(id, depth) AS (
  SELECT id, depth FROM _edges_tree WHERE parent_id=:root
  UNION ALL
  SELECT e.id, e.depth FROM _edges_tree e JOIN sub ON e.parent_id=sub.id
)
SELECT c.* FROM chunks c JOIN sub ON c.id=sub.id ORDER BY sub.depth, c.position;
