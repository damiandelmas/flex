# Tools Cell Instructions

Public name: `tools`.

Implementation module: `flex.modules.skills`.

Use this cell to find AI development tools, MCP servers, Claude Code skills,
agents, hooks, commands, plugin manifests, and related README documentation.

Start every session with `@orient`. Query the stock views first:

```sql
SELECT tool_name, tool_class, stars, source_id
FROM tools
ORDER BY stars DESC
LIMIT 20;
```

Use `docs` for README sections and `skills` for executable artifact text.
Use `keyword()` for exact terms such as package names, tool names, or protocol
names. Use `vec_ops()` when the user describes a capability semantically.

`GITHUB_TOKEN` is optional. Without it, refresh uses unauthenticated public API
limits and may stop after a small slice. With it, refresh can make broader
progress against public repositories. The token only needs public repository
read access.

Refresh is intentionally bounded by default:

```text
FLEX_SKILLS_SEARCH_PAGES=1
FLEX_SKILLS_ENRICH_LIMIT=50
FLEX_SKILLS_README_LIMIT=25
FLEX_SKILLS_ARTIFACT_LIMIT=25
FLEX_GITHUB_RATE_WAIT_MAX_SEC=0
FLEX_GITHUB_MAX_RETRIES=1
```
