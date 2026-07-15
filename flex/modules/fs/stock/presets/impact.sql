-- @name: impact
-- @description: Transitive callers of a symbol.
-- @params: symbol (required)
WITH RECURSIVE up(id) AS (
  SELECT caller_id FROM _edges_call WHERE callee_name=:symbol
  UNION
  SELECT e.caller_id FROM _edges_call e
  JOIN _symbols s ON s.name=e.callee_name
  JOIN up ON up.id=s.def_id
)
SELECT DISTINCT t.section_title AS affected, up.id AS affected_id, es.source_id AS affected_file
FROM up JOIN _types_filesystem t ON t.chunk_id=up.id
JOIN _edges_source es ON es.chunk_id=up.id;
