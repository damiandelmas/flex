# Tools Cell

The public cell name is `tools`. The implementation directory remains
`flex/modules/skills` because the compiler and schema were first built around
Claude skill artifacts.

`tools` is an index of the Claude Code / AI dev ecosystem: MCP servers,
skills, agents, hooks, slash commands, CLI tools, and plugin manifests — plus
their README documentation. Each GitHub repo is one source (`owner/repo`).

## Get it (build locally)

Bootstrap an empty cell and populate it from GitHub:

```bash
flex core init --module tools
```

That creates an empty queryable cell, installs the stock views and general
presets, registers the cell as `tools`, and stores `cell_type=tools`. No network
work runs at init time. The cell registers with `lifecycle=refresh` and a 6h
interval; the refresh module is `flex.modules.skills.compile.refresh`, run by the
registry scheduler or manually. Run a first refresh right away to start
filling the cell:

```bash
python -m flex.modules.skills.compile.refresh --cell tools
flex core search --cell tools "@orient"
```

### GitHub funnel

Refresh runs five idempotent phases (dedup by `source_id`, NULL checks, NOT IN):

```text
search   GitHub Search API for new repos (primary source, stars:>=100)
catalog  awesome-list re-crawl (seed/vocabulary only)
enrich   backfill stars, language, topics, license, last_commit
readme   fetch + split READMEs into section span chunks + tree edges
skills   discover Claude artifacts (.claude/skills/, agents, hooks, manifests)
```

```bash
python -m flex.modules.skills.compile.refresh --cell tools
python -m flex.modules.skills.compile.refresh --cell tools --since 7d
python -m flex.modules.skills.compile.refresh --cell tools --mode search,enrich
```

Graph rebuilds automatically at ≥50 new sources. Negative probes are durable in
`_skills_probe_status`, so repos with no README/artifacts are not rechecked.

GitHub access is optional. Without `GITHUB_TOKEN`, the GitHub API path runs with
public unauthenticated limits and may make only partial progress. With
`GITHUB_TOKEN` (read access to public repos only), refresh gets the normal
authenticated quota. Bounded daemon defaults keep work small:

```text
FLEX_SKILLS_SEARCH_PAGES=1
FLEX_SKILLS_ENRICH_LIMIT=50
FLEX_SKILLS_README_LIMIT=25
FLEX_SKILLS_ARTIFACT_LIMIT=25
FLEX_GITHUB_RATE_WAIT_MAX_SEC=0
FLEX_GITHUB_MAX_RETRIES=1
```

## Cell shape

Span-level tree per repo; `owner/repo` is the dedup key (same tool in N awesome
lists → one source). Raw README kept pristine in `_raw_content` (re-parseable
without re-download).

```text
owner/repo:0       catalog entry      (depth 0, root)
owner/repo:1       full README        (depth 1)
owner/repo:1:N     README section     (depth 2-3 spans)
owner/repo:2       SKILL.md
owner/repo:3       agent .md
owner/repo:4       hook config
owner/repo:5       plugin manifest
```

## Query surface

```text
tools   catalog rows, GitHub metadata, tool_class, graph columns
docs    README section spans with section_type
skills  skill, agent, hook, command, and manifest artifacts (parsed frontmatter)
chunks  unified surface across all of the above
```

`tool_class` canonicalizes raw `tool_type` into `mcp` / `skill` / `hook` /
`agent` / `tool` / `memory` / `other`. Start with `@orient`; it includes these
instructions when document mounts are available.

```sql
-- top tools by stars
SELECT tool_name, stars FROM tools ORDER BY stars DESC LIMIT 10;

-- semantic: capability described in natural language
SELECT v.score, t.tool_name, t.stars
FROM vec_ops('similar:database access MCP server', 'SELECT id FROM tools') v
JOIN tools t ON t.id = v.id
ORDER BY v.score DESC LIMIT 10;

-- exact: package / protocol / tool name
SELECT k.snippet, t.tool_name
FROM keyword('"context7"', 'SELECT id FROM tools') k
JOIN tools t ON t.id = k.id;
```
