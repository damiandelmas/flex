-- @name: communities
-- @description: Note communities with hub notes and member counts

SELECT
    community_id,
    COUNT(*) as notes,
    GROUP_CONCAT(CASE WHEN is_hub = 1 THEN title END) as hub_notes,
    GROUP_CONCAT(CASE WHEN is_bridge = 1 THEN title END) as bridge_notes
FROM notes
WHERE community_id IS NOT NULL
GROUP BY community_id
ORDER BY notes DESC
