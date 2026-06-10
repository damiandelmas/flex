-- @name: skills
-- @description: Skill artifact view — one row per Claude Code skill artifact (SKILL.md, agents, hooks) with parsed frontmatter metadata.

DROP VIEW IF EXISTS skills;
CREATE VIEW skills AS
SELECT
    c.id,
    c.content,
    c.timestamp,
    es.source_id,
    t.tool_name,
    t.github_owner,
    t.github_repo,
    t.chunk_type AS artifact_type,
    t.skill_name,
    t.skill_description,
    t.allowed_tools,
    t.disallowed_tools,
    t.skill_model,
    t.permission_mode,
    t.user_invocable,
    t.argument_hint,
    t.skill_context,
    t.max_turns,
    t.preloaded_skills,
    t.artifact_path,
    t.stars
FROM _raw_chunks c
JOIN _types_skills t ON c.id = t.chunk_id
JOIN _edges_source es ON c.id = es.chunk_id
WHERE t.chunk_type IN ('skill', 'agent', 'hook', 'command', 'manifest');
