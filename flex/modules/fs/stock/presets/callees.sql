-- @name: callees
-- @description: Calls made by a named definition; ambiguity is a candidate set.
-- @params: symbol (required)
WITH resolution AS (SELECT name, COUNT(*) AS candidate_count FROM _symbols GROUP BY name)
SELECT DISTINCT e.callee_name AS callee, s.def_id AS callee_id, e.caller_id,
       cs.source_id AS caller_file, ds.source_id AS callee_file,
       CASE WHEN COALESCE(r.candidate_count,0)=0 THEN 'unresolved'
            WHEN r.candidate_count=1 THEN 'unique' ELSE 'ambiguous' END AS resolution_state,
       COALESCE(r.candidate_count,0) AS candidate_count
FROM _edges_call e
JOIN _types_filesystem t ON t.chunk_id=e.caller_id
JOIN _edges_source cs ON cs.chunk_id=e.caller_id
LEFT JOIN resolution r ON r.name=e.callee_name
LEFT JOIN _symbols s ON s.name=e.callee_name
LEFT JOIN _edges_source ds ON ds.chunk_id=s.def_id
WHERE t.section_title=:symbol;
