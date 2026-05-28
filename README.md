<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

**fastest way to get knowledge and memory for every Claude Code session**

flex compiles your Claude Code session history into a queryable SQLite database
with vector and hybrid retrieval. your AI agent connects through MCP, discovers
the schema at runtime, and writes SQL against your history.

**install:**

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

---

## what's different

**most memory systems start working after you install them.**

flex works retroactively. as soon as you install, your existing sessions become
queryable through the MCP tool. you can ask how you installed the cloudflare
tunnel yesterday, why a release script changed, or what session created a file.

**retrieval tools typically have minimal metadata to filter on.**

flex captures the exact session, what files and repos it touched, which project
it belongs to, and whether it spawned agents. filtering happens before scoring:
the vector engine only scores what survives. flex also builds graph structure
for hub sessions, file co-edit patterns, project attribution, and source
recovery.

**vector search typically surfaces similar content and stops there.**

flex lets the agent combine SQL, presets, semantic search, suppression,
diversity, recency weighting, and trajectory search in one local query surface.

## what can you do?

### file lineage

flex tracks sessions, messages, tool operations, and file evidence. ask:

```text
"Use flex: what's the history of worker.py?"
```

### decision archaeology

the hardest question in software is why something was done. ask:

```text
"Use flex: how did we create the curl install script?"
```

flex finds the session where the decision happened and reconstructs the path:
which approaches were considered, which failed, and why you landed there.

### weekly digest

sessions are grouped by projects, touched files, and key decisions. ask:

```text
"Use flex: what did we build this week?"
```

### semantic search

semantic search can be composed with filters and operators. ask:

```text
"Use flex: 5 things we talked about this week outside the main project"
```

## codex

Codex CLI sessions use the same coding-agent substrate:

```bash
flex init --module codex
flex core search --cell codex "@orient"
```

The Codex cell indexes local Codex CLI sessions, messages, tool operations,
file evidence, and source recovery so they can be queried through the same MCP
surface.

## obsidian

flex can also turn an Obsidian vault or Markdown folder into a local cell:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

or point directly at a vault:

```bash
VAULT=/path/to/vault curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

Then ask through MCP:

```text
"Use flex: what notes are connected to project planning?"
"Use flex: find orphaned notes in my vault"
"Use flex: summarize the notes that mention release planning"
```

The Obsidian/Markdown cell indexes notes, sections, frontmatter tags, aliases,
wikilinks, ghost notes, and heading hierarchy without modifying your source
files.

## raw cli access

MCP is the normal interface. use raw CLI access when an agent or operator needs
direct terminal queries, debugging, or scripting:

```bash
flex core search --cell claude_code "@digest days=3"
flex core search --cell claude_code "@file path=worker.py"
flex core search --cell claude_code "SELECT COUNT(*) FROM sessions"
```

same query surface, direct from the terminal.

## local-first

your knowledge base is one SQLite file on your machine. flex registers the cell
name and stores the database under `~/.flex/cells/`.

```bash
flex core search --cell claude_code "SELECT COUNT(*) FROM sessions"
```

everything runs locally. no hosted database required.

## what's inside

- **MCP server**: one read-only query tool, `flex_search`; this is the primary
  interface for agents.
- **SQLite cells**: sessions, messages, chunks, source rows, views, presets, and
  `@orient` docs.
- **CLI**: initialize cells, run raw terminal queries, and inspect health.
- **[flexvec](https://github.com/damiandelmas/flexvec)**: SQL vector retrieval
  kernel with suppression, diversification, decay, and trajectory operators.
  [paper](https://arxiv.org/abs/2603.22587)
- **worker**: background refresh for local coding-agent memory.

---

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

MIT · Python 3.12 · SQLite · [getflex.dev](https://getflex.dev) · [paper](https://arxiv.org/abs/2603.22587) · [x](https://x.com/damian_delmas)
