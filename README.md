<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

**fastest way to get knowledge and memory for every Claude Code session**

flex compiles your Claude Code session history into a queryable SQLite database
with vector and hybrid retrieval. your AI agent connects via MCP, discovers the
schema at runtime, and writes SQL against your history.

**install:**

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

---

## what's different

**most memory systems start working after you install them.**

flex works retroactively. as soon as you install, all of your sessions become
queryable via the MCP tool. you can ask claude how you installed the cloudflare
tunnel yesterday, why you made a specific edit to your landing page, and much
more.

**retrieval tools typically have minimal metadata to filter on.**

flex captures the exact session, what files and repos it touched, which project
it belongs to, and whether it spawned agents. filtering happens before scoring:
the vector engine only scores what survives. on top of that, flex builds
knowledge graphs automatically: hub sessions, file co-edit patterns, project
attribution, and session structure.

**vector search typically surfaces similar content and stops there.**

flex lets you suppress a topic, weight by recency, diversify across subtopics,
and trace a direction through embedding space. if you want architecture
documents but not changelogs, just ask claude; it'll use the search operators
and SQL surface together.

## what can you do?

### file lineage

flex tracks every session's messages and operations. get information on what
session created a file, what prompts were used to create it, and why it was
changed last. just ask:

```text
"Use flex: what's the history of worker.py?"
```

the history of `worker.py` is followed beyond file moves and renames. never
lose track of your reasoning.

### decision archaeology

the hardest question in software: why was it done this way? flex finds the
session where the decision happened and reconstructs the path: which approaches
were considered, which failed, and why you landed here. just ask:

```text
"Use flex: how did we create the curl install script?"
```

this works for technical questions, practical concerns, and logistical setups
as well. it makes it easy to grab the runbook for setting up your website from
three weeks ago.

### weekly digest

sessions are grouped by the projects they touch, the files that got the most
edits, and key decisions. just ask:

```text
"Use flex: what did we build this week?"
```

claude runs a series of queries behind the scenes and comes back with an
overview of your recent work.

### semantic search

semantic search just surfaces similar content. flex can search for one topic
while suppressing another. just ask:

```text
"Use flex: 5 things we talked about this week outside the main project"
```

it can also retrieve a diverse sample instead of ten copies of the same answer.

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

Module docs: [`flex/modules/markdown/README.md`](flex/modules/markdown/README.md).

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

your knowledge base is one SQLite file on your machine. flex registers the
cell name and stores the database under `~/.flex/cells/`.

```bash
flex core search --cell claude_code "SELECT COUNT(*) FROM sessions"
```

everything runs locally. no hosted database required.

## what's inside

- **MCP server**: one read-only query tool, `flex_search`; this is the primary
  interface for agents.
- **CLI**: initialize cells, run raw terminal queries, and inspect health.
- **SQLite cells**: sessions, messages, chunks, source rows, views, presets, and
  `@orient` docs.
- **[flexvec](https://github.com/damiandelmas/flexvec)**: SQL vector retrieval
  kernel with suppression, diversification, decay, and trajectory operators.
  [paper](https://arxiv.org/abs/2603.22587)
- **worker**: background refresh for local coding-agent memory.

---

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

MIT · Python 3.12 · SQLite · [getflex.dev](https://getflex.dev) · [paper](https://arxiv.org/abs/2603.22587) · [x](https://x.com/damian_delmas)
