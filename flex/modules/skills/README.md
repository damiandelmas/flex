# Tools Cell

The public cell name is `tools`. The implementation directory remains
`flex/modules/skills` because the compiler and schema were first built around
Claude skill artifacts.

Use `tools` in user-facing commands, registry rows, docs, and MCP examples:

```bash
flex core search --cell tools "@orient"
flex core search --cell tools "SELECT tool_name, stars FROM tools ORDER BY stars DESC LIMIT 10"
```

The source module can bootstrap the cell with:

```bash
flex core init --module tools
```

That creates an empty queryable cell, installs the stock views and general
presets, registers the cell as `tools`, and stores `cell_type=tools`. The
refresh module is `flex.modules.skills.compile.refresh`; it can be run by the
registry refresh scheduler or manually.

GitHub access is optional. Without `GITHUB_TOKEN`, the GitHub API path runs
with public unauthenticated limits and may make only partial progress. With
`GITHUB_TOKEN`, refresh gets the normal authenticated public-repository quota.
The token only needs read access to public repositories. Bounded daemon defaults
keep work small:

```text
FLEX_SKILLS_SEARCH_PAGES=1
FLEX_SKILLS_ENRICH_LIMIT=50
FLEX_SKILLS_README_LIMIT=25
FLEX_SKILLS_ARTIFACT_LIMIT=25
FLEX_GITHUB_RATE_WAIT_MAX_SEC=0
FLEX_GITHUB_MAX_RETRIES=1
```

The stock query surface is:

```text
tools   catalog rows, GitHub metadata, tool_class, graph columns
docs    README section spans with section_type
skills  skill, agent, hook, command, and manifest artifacts
```

Start with `@orient`; it includes these instructions when document mounts are
available.
