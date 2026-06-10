# Tools Cell Instructions

Public name: `tools`. Implementation module: `flex.modules.skills`.

This cell catalogs the Claude Code ecosystem: MCP servers, Claude Code skills,
agents, hooks, commands, and related tooling gathered from public GitHub
repositories and registries. Each source is one repository; chunks are catalog
entries, README sections, and parsed skill artifacts.

Always start with:

```text
cell="tools" query="@orient"
```

`@orient` returns the live schema, views, graph intelligence, and presets. Call
it once per task before any other query.

## What This Cell Is For

Use the tools cell when the question is about the ecosystem:

- finding MCP servers by capability, language, or topic
- reading skill READMEs, install instructions, and usage examples
- comparing tools by stars, last commit, license, or category
- discovering Claude Code skills and their frontmatter (allowed tools, model,
  permission mode, user-invocable flag)
- structural catalog queries: how many MCP servers, top by stars, by language

At ~114K chunks across ~3,900 sources, this is the primary ecosystem catalog.

## Core Surfaces

`tools` is the catalog surface. One row per tool with GitHub metadata,
`tool_class` canonicalization, and graph intelligence. Use it for structural
queries and ecosystem-level counts.

Key columns: `source_id`, `tool_name`, `github_owner`, `github_repo`,
`tool_url`, `stars`, `language`, `license`, `topics`, `last_commit`,
`open_issues`, `category`, `subcategory`, `tool_type`, `source_registry`,
`quality_score`, `install_command`, `tool_class`, `is_mcp`, `centrality`,
`is_hub`, `is_bridge`, `community_id`.

`tool_class` values: `mcp`, `skill`, `hook`, `agent`, `tool`, `memory`,
`guide`, `config`, `other`. Use `tool_class = 'mcp'` or `is_mcp = 1` to
filter to MCP servers; `is_mcp` is the tighter signal where populated.

`docs` is the README section surface. One row per README span with
`section_heading`, `heading_depth`, `section_type` (canonicalized:
`installation`, `usage`, `examples`, `api`, `configuration`, `features`,
`architecture`, `troubleshooting`, `overview`, etc.), and `tree_depth`. Use it
to read install instructions, usage examples, and API references.

`skills` is the skill artifact surface. One row per SKILL.md, agent, hook, or
command with parsed frontmatter: `skill_name`, `skill_description`,
`allowed_tools`, `disallowed_tools`, `skill_model`, `permission_mode`,
`user_invocable`, `argument_hint`, `skill_context`, `max_turns`,
`preloaded_skills`, `artifact_path`, `stars`.

`chunks` is the unified surface across all chunk types. Use it for hybrid
queries that span catalog, README, and skill artifact rows.

## Choosing Search Mode

**Structural first.** Counts, rankings, and filters cost nothing. Ecosystem breakdown: `SELECT tool_class, COUNT(*) AS n FROM tools GROUP BY tool_class ORDER BY n DESC`.

Top MCP servers by stars, filtered to a language:

```sql
SELECT tool_name, github_owner, github_repo, stars, language, topics, install_command
FROM tools
WHERE tool_class = 'mcp'
  AND language = 'TypeScript'   -- omit to see all
ORDER BY stars DESC
LIMIT 20;
```

**Exact keyword** for package names, protocol names, known tool names, or
quoted phrases. Scope to `docs` or `tools` to avoid ranked noise from
unrelated chunk types.

```sql
SELECT k.id, k.rank, k.snippet, d.tool_name, d.section_heading, d.section_type
FROM keyword('"model context protocol"',
  'SELECT id FROM docs') k
JOIN docs d ON k.id = d.id
ORDER BY k.rank DESC
LIMIT 15;
```

**Semantic** for capability descriptions. Pre-filter by `chunk_type` or
`is_mcp` inside the second arg to prevent pool starvation on a ~114K chunk
corpus.

```sql
SELECT v.id, v.score, c.tool_name, c.github_repo,
       substr(c.content, 1, 500) AS excerpt
FROM vec_ops(
  'similar:MCP server for web scraping and browser automation diverse',
  'SELECT id FROM chunks WHERE is_mcp = 1'
) v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC
LIMIT 15;
```

Search skill READMEs for a capability:

```sql
SELECT v.id, v.score, d.tool_name, d.section_heading, d.section_type,
       substr(d.content, 1, 600) AS excerpt
FROM vec_ops(
  'similar:autonomous coding agent that delegates subagents and fans out tasks diverse',
  'SELECT id FROM docs'
) v
JOIN docs d ON v.id = d.id
ORDER BY v.score DESC
LIMIT 12;
```

Suppress dominant signal to find niche tools:

```sql
SELECT v.id, v.score, c.tool_name, c.stars,
       substr(c.content, 1, 400) AS excerpt
FROM vec_ops(
  'similar:database query and migration MCP servers suppress:PostgreSQL suppress:SQLite diverse',
  'SELECT id FROM chunks WHERE is_mcp = 1'
) v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC
LIMIT 12;
```

**Skill artifact search** — find user-invocable skills by description:

```sql
SELECT v.id, v.score, s.tool_name, s.skill_name, s.skill_description,
       s.allowed_tools, s.user_invocable, s.stars
FROM vec_ops(
  'similar:skill for running tests verifying fixes and checking app behavior',
  'SELECT id FROM skills'
) v
JOIN skills s ON v.id = s.id
ORDER BY v.score DESC
LIMIT 10;
```

Filter `skills` by invocability: `SELECT skill_name, skill_description, user_invocable, permission_mode, allowed_tools, stars FROM skills WHERE user_invocable = 1 ORDER BY stars DESC LIMIT 20`.

**Hybrid** (exact term, ranked semantically):

```sql
SELECT k.id, k.rank, v.score, c.tool_name, c.github_repo,
       substr(c.content, 1, 400) AS excerpt
FROM keyword('"stdio"') k
JOIN vec_ops('similar:MCP server transport stdio versus SSE tradeoffs') v ON k.id = v.id
JOIN chunks c ON k.id = c.id
ORDER BY v.score DESC LIMIT 10;
```

## GitHub Enrichment and Token Guidance

GitHub metadata (stars, language, topics, last\_commit, open\_issues) is
populated at refresh. `GITHUB_TOKEN` is optional; without it, unauthenticated
limits (60 req/h) may cut refresh short. Token needs only public read access.

Refresh is intentionally bounded by default:

```text
FLEX_SKILLS_SEARCH_PAGES=1
FLEX_SKILLS_ENRICH_LIMIT=50
FLEX_SKILLS_README_LIMIT=25
FLEX_SKILLS_ARTIFACT_LIMIT=25
FLEX_GITHUB_RATE_WAIT_MAX_SEC=0
FLEX_GITHUB_MAX_RETRIES=1
```

Increase these for broader refresh runs.

## Preset Bias

Use presets when they fit:

- `@orient` — live schema, views, graph, presets
- `@health` — chunk/source counts, embedding coverage, graph freshness
- `@genealogy concept=...` — trace a concept's lineage through hubs and key excerpts
- `@bridges` — cross-community connector tools (hub tools by centrality)

Use raw SQL when the question is structural, when a preset is too broad, or
when you need a tight pre-filter before semantic scoring. For hub tools by
centrality: `SELECT tool_name, github_repo, stars, centrality, community_id FROM tools WHERE is_hub = 1 ORDER BY centrality DESC LIMIT 10`.

## Reporting Results

Include: cell name `tools`; `source_id` (`github_owner/github_repo`),
`tool_name`, `tool_class`, `stars`, `language`, `license`; `skill_name`,
`user_invocable`, `permission_mode` for skill artifacts; vector score or
keyword rank; `section_heading` and `section_type` for README excerpts;
a compact excerpt with truncation noted.
