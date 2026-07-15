-- @name: callers
-- @description: Definitions that call a symbol; candidate resolution remains explicit.
-- @params: symbol (required)
WITH resolution AS (SELECT name, COUNT(*) AS candidate_count FROM _symbols GROUP BY name)
SELECT DISTINCT t.section_title AS caller, e.caller_id,
       s.def_id AS candidate_def_id, es.source_id AS candidate_file,
       CASE WHEN COALESCE(r.candidate_count,0)=0 THEN 'unresolved'
            WHEN r.candidate_count=1 THEN 'unique' ELSE 'ambiguous' END AS resolution_state,
       COALESCE(r.candidate_count,0) AS candidate_count
FROM _edges_call e
JOIN _types_filesystem t ON t.chunk_id=e.caller_id
LEFT JOIN resolution r ON r.name=e.callee_name
LEFT JOIN _symbols s ON s.name=e.callee_name
LEFT JOIN _edges_source es ON es.chunk_id=s.def_id
WHERE e.callee_name=:symbol;
