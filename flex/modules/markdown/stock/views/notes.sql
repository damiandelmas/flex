-- @name: notes
-- @description: Source-level surface for markdown cells. One row per note with metadata, link counts, and graph intelligence.

DROP VIEW IF EXISTS notes;
CREATE VIEW notes AS
SELECT
    s.source_id,
    s.title,
    ts.folder,
    ts.tags,
    ts.aliases,
    ts.file_modified,
    ts.note_created,
    (SELECT COUNT(DISTINCT wl.to_path)
     FROM _edges_wikilink wl
     WHERE wl.from_path = s.source_id) AS outgoing_links,
    (SELECT COUNT(DISTINCT wl.from_path)
     FROM _edges_wikilink wl
     WHERE wl.to_path = s.source_id) AS backlinks,
    (SELECT COUNT(*)
     FROM _edges_wikilink_unresolved wu
     WHERE wu.from_path = s.source_id) AS unresolved_links,
    COALESCE(sg.centrality, 0.0) AS centrality,
    sg.community_id,
    COALESCE(sg.is_hub, 0) AS is_hub,
    sg.hub_type,
    COALESCE(sg.is_bridge, 0) AS is_bridge
FROM _raw_sources s
LEFT JOIN _types_markdown_source ts ON ts.source_id = s.source_id
LEFT JOIN _enrich_source_graph sg ON sg.source_id = s.source_id;
