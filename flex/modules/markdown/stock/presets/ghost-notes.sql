-- @name: ghost-notes
-- @description: Wikilink targets that don't exist yet — notes to write, ranked by demand

SELECT
    raw_target,
    COUNT(*) as referenced_by,
    GROUP_CONCAT(DISTINCT from_path) as referencing_notes
FROM _edges_wikilink_unresolved
GROUP BY raw_target
ORDER BY referenced_by DESC
LIMIT 20
