-- @name: hubs
-- @description: Structurally important notes by type (authority=many backlinks, connector=many outgoing, bridge=holds graph together)

SELECT
    title,
    hub_type,
    centrality,
    outgoing_links,
    backlinks
FROM notes
WHERE is_hub = 1
ORDER BY centrality DESC
LIMIT 20
