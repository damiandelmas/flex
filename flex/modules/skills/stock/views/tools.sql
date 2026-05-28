-- @name: tools
-- @description: Catalog view — one row per tool with GitHub metadata, tool_class canonicalization, and graph intelligence.

DROP VIEW IF EXISTS tools;
CREATE VIEW tools AS
SELECT
    c.id,
    c.content,
    c.timestamp,
    es.source_id,
    t.tool_name,
    t.github_owner,
    t.github_repo,
    t.tool_url,
    t.stars,
    t.language,
    t.license,
    t.topics,
    t.last_commit,
    t.open_issues,
    t.category,
    t.subcategory,
    t.tool_type,
    t.source_registry,
    t.quality_score,
    t.emoji_badges,
    t.install_command,
    -- tool_class: derived from category (raw awesome-list heading) when tool_type is NULL.
    -- tool_type takes precedence when populated (e.g. from registry enrichment).
    CASE
        WHEN t.tool_type IN ('mcp_server', 'mcp') THEN 'mcp'
        WHEN t.tool_type IN ('skill', 'slash_command', 'skills') THEN 'skill'
        WHEN t.tool_type = 'hook' THEN 'hook'
        WHEN t.tool_type IN ('agent', 'subagent') THEN 'agent'
        WHEN t.tool_type IN ('cli', 'tool', 'library', 'framework') THEN 'tool'
        WHEN t.tool_type IN ('memory', 'context', 'knowledge') THEN 'memory'
        WHEN t.tool_type IN ('guide', 'tutorial', 'reference') THEN 'guide'
        -- Fallback: derive from category (awesome-list heading)
        WHEN LOWER(t.category) LIKE '%hook%' THEN 'hook'
        WHEN LOWER(t.category) LIKE '%slash%command%' THEN 'skill'
        WHEN LOWER(t.category) LIKE '%agent%skill%' THEN 'skill'
        WHEN LOWER(t.category) LIKE '%claude.md%' THEN 'config'
        WHEN LOWER(t.category) LIKE '%status%line%' THEN 'tool'
        WHEN LOWER(t.category) LIKE '%workflow%' OR LOWER(t.category) LIKE '%guide%' OR LOWER(t.category) LIKE '%documentation%' THEN 'guide'
        WHEN LOWER(t.category) LIKE '%client%' THEN 'tool'
        WHEN LOWER(t.category) LIKE '%tooling%' THEN 'tool'
        WHEN LOWER(t.category) LIKE '%mcp%' THEN 'mcp'
        ELSE 'other'
    END AS tool_class,
    e.centrality,
    e.is_hub,
    e.is_bridge,
    e.community_id,
    eci.content_hash,
    eci.blob_hash,
    eri.repo_root,
    t.github_id
FROM _raw_chunks c
JOIN _types_skills t ON c.id = t.chunk_id
JOIN _edges_source es ON c.id = es.chunk_id
LEFT JOIN _enrich_source_graph e ON es.source_id = e.source_id
LEFT JOIN _edges_content_identity eci ON c.id = eci.chunk_id
LEFT JOIN _edges_repo_identity eri ON c.id = eri.chunk_id
WHERE t.chunk_type = 'catalog';
