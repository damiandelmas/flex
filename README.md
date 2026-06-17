<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

**Composable search and retrieval for AI agents**

Retrieval was built for a human at a search box — hide the complexity, return ten
links. Your agent is a different consumer: it can read structure, write queries,
and compose operations. flex gives it a knowledge base shaped for that consumer
instead of the old one.

flex compiles coding-agent sessions, markdown vaults, and other sources into local
SQLite databases, then exposes them through one MCP tool with keyword search,
semantic search, and SQL. Your whole knowledge base is one file on your machine —
no hosted service, no new tool per source, just `flex_search`.

## install

Claude Code:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

Codex CLI:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- codex
```

Obsidian / Markdown:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

## coding-agent memory

Claude Code and Codex sessions become searchable through one MCP query surface.
flex indexes session history, tool calls, file edits, source evidence, repo
context, and sub-agent traces — and keeps updating as you work.

### what's different

**most memory systems start working *after* you install them.** flex works
retroactively — the moment you install, your existing sessions are queryable. Ask
how you set up the Cloudflare tunnel yesterday, why a release script changed, or
what session created a file.

**sessions aren't plain documents.** They have prompts, replies, tool calls, file
edits, repos, projects, and sub-agents. flex keeps that structure, so your agent
filters *before* semantic scoring instead of asking vector search to guess what
matters. Every answer stays attached to its source evidence — the session it came
from, the files and repos it touched, and where to go next for the full trace.

**vector search usually returns similar content and stops.** flex lets your agent
compose SQL, semantic, and keyword search with operators for suppression,
diversity, recency, and trajectory — architecture work but not changelogs, recent
auth work but not oauth docs, a diverse sample instead of ten near-duplicates.

### what you can ask

**file lineage** — flex tracks sessions, messages, tool calls, and file edits:

```text
"Use flex: what's the history of worker.py?"
```

What session created it, what prompts shaped it, what changed later, and why.

**decision archaeology** — the hardest question in software is *why*:

```text
"Use flex: why did we create registry.py?"
```

flex finds the session where the decision happened and reconstructs the path —
which approaches were tried, which failed, and why you landed where you did.

**weekly digest** — grouped by project, touched files, and key decisions:

```text
"Use flex: what did we build this week?"
```

Already installed? Run `flex init --module claude-code` (or `--module codex`),
then ask: `"Use flex: orient me to my Claude Code memory."`

## beyond coding agents

Coding memory is the sharpest use of flex, not its edge. Underneath, flex is a
substrate: any source that compiles into the cell format becomes queryable through
the same MCP tool, and adding a source never adds a new tool.

Obsidian and Markdown ship as a ready-made module today:

```bash
VAULT=/path/to/vault curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

flex indexes notes, sections, frontmatter, aliases, wikilinks, ghost notes, and
heading hierarchy without touching your files, then exposes backlinks and note
communities as queryable columns. New sources arrive the same way Claude Code,
Codex, and Obsidian do — compiled into a cell behind the one query surface.

### source modules

**Core**

| module | what it indexes |
|---|---|
| [`claude-code`](https://github.com/damiandelmas/flex/blob/main/flex/modules/claude_code/README.md) | Claude Code sessions: prompts, tool calls, file evidence |
| [`codex`](https://github.com/damiandelmas/flex/blob/main/flex/modules/codex/README.md) | Codex CLI sessions, same surface |
| [`obsidian`](https://github.com/damiandelmas/flex/blob/main/flex/modules/markdown/README.md) | vaults and markdown trees: sections, wikilinks, backlinks |
| [`tools`](https://github.com/damiandelmas/flex/blob/main/flex/modules/skills/README.md) | the agentic ecosystem catalog: skills, MCP servers, frameworks |

**Beta**

| module | install |
|---|---|
| [`goose`](https://github.com/damiandelmas/flex/blob/main/flex/modules/goose/README.md) | `flex init --module goose` |
| [`github`](https://github.com/damiandelmas/flex/blob/main/flex/modules/github/README.md) | `flex init --module github --github-repos owner/repo` |
| [`reddit`](https://github.com/damiandelmas/flex/blob/main/flex/modules/reddit/README.md) | `flex init --module reddit --subreddits ClaudeCode,LocalLLaMA --since 30d` |
| [`hn`](https://github.com/damiandelmas/flex/blob/main/flex/modules/hn/README.md) | `flex init --module hn --hn-queries "claude code,mcp server"` |
| [`arxiv`](https://github.com/damiandelmas/flex/blob/main/flex/modules/arxiv/README.md) | `flex init --module arxiv --arxiv-query "all:retrieval augmented generation"` |

## extension modules

Extension modules enrich any cell with shared structure — they don't add a source
or a tool.

### SOMA

Stable identity for files, repos, content, and URLs, so flex follows the same file
across renames, moves, and repo relocations. That's what makes file history work as
lineage instead of path search. Ships with Claude Code and Codex.

### knowledge graphs

Hubs, bridges, communities, centrality, and co-edit relationships over sessions and
files — and backlinks, ghost notes, and hub notes over a Markdown vault. Your agent
queries them as ordinary SQL columns, not through a separate graph tool.

## how retrieval works

Every query runs three phases in one SQL statement.

```text
SQL pre-filter  ->  Search  ->  SQL compose
```

1. **SQL pre-filter** narrows what enters scoring — by date, source, type, length,
   project, path, or any SQL expression.
2. **Search** runs vector, keyword, or hybrid retrieval over the filtered set.
3. **SQL compose** joins results back to your tables for grouping, filtering,
   reranking, or source recovery.

The retrieval engine bridges vector scoring, keyword search, and hybrid retrieval
into SQL.

## flexvec

Most vector systems return the nearest chunks and stop. flexvec exposes the score
array so retrieval becomes programmable.

Local memory and knowledge bases are small enough that brute-force similarity is
practical. Approximate indexes help at huge scale, but they hide the full score
array. flexvec keeps that array available, which lets your agent suppress a topic,
diversify results, weight by recency, or search along a conceptual direction before
selecting rows.

Tokens compose in one query string:

| token | what it does |
|---|---|
| `similar:TEXT` | search for this concept |
| `suppress:TEXT` | push this topic out of results |
| `diverse` | spread across subtopics instead of ten versions of the same answer |
| `decay:N` | favor recent content with an N-day half-life |
| `centroid:id1,id2` | search from the average of examples |
| `from:A to:B` | find content along a conceptual arc |
| `pool:N` | set how many candidates to score |

Example:

```sql
SELECT v.id, v.score, m.session_id, m.content
FROM vec_ops(
  'similar:how the system works architecture
   diverse
   suppress:website landing page design tagline',
  'SELECT id FROM messages
   WHERE type = ''assistant'''
) v
JOIN messages m ON v.id = m.id
ORDER BY v.score DESC
LIMIT 5
```

This finds architecture messages while suppressing landing-page drafts. Standard
semantic search usually does only the first half.

## architecture

flex has one shape and a small set of conventions. Every source compiles into the
same kind of artifact — a **cell** — and an agent learns to query any cell by
reading its schema, not by learning a new API.

### cells

A cell is a portable SQLite database for one knowledge source: Claude Code
sessions, Codex sessions, a Markdown vault, project history, or another structured
corpus. Source modules are adapters that read a source format and compile it into
the shared cell shape.

Cells are the same shape at every level — chunks with edges, types, enrichments,
and views. A registry at `~/.flex/registry.db` catalogs them by UUID, so names
resolve to paths and renaming is a single update. Adding a new source adds a new
cell behind the same query surface — never a new tool.

### the cell shape

One node type sits at the center: `_raw_chunks` — content, embedding, timestamp.
Everything else orbits it as tables keyed by `chunk_id`. A document is a grouping
edge. A module is a table. An enrichment is a score. A view composes them into a
flat surface the agent queries.

The table prefix *is* the lifecycle declaration:

```text
_raw_*      immutable facts        written at compile time   never wiped
_edges_*    relationships          re-derived on ingest
_types_*    classification         re-derived on ingest
_enrich_*   computed structure     always safe to wipe
(no prefix) views                  composed from the tables above
```

Reading a table name tells an agent what it's looking at: `_enrich_*` is
recomputable and safe to drop, while `_raw_*` is the durable record that survives
everything.

### self-describing

There is no manifest and no external config. A cell describes itself through
`sqlite_master`, `PRAGMA table_info`, and a single entry point: `@orient` returns
the cell's shape, schema, views, presets, and sample content in one call. Agents
discover view columns instead of hardcoding them, so a cell stays queryable as
modules add tables and columns.

### the lifecycle

Three write paths feed one read surface:

```text
compile  (facts)      ─→  _raw_*, _edges_*, _types_*  ─┐
                                                       ├─→  views  ─→  agent queries SQL
enrich   (structure)  ─→  _enrich_*  ──────────────────┘
```

Compile is deterministic — the same source always produces the same chunks, with
no interpretation. Enrichment runs offline, reading what compile wrote and
computing structure (graphs, fingerprints, communities) into `_enrich_*`. Views
regenerate from whatever tables exist. Content is fact; labels are hypothesis; raw
data survives everything; and every mutation logs itself to `_ops`, so each cell
carries its own provenance.

### modules are tables

A module installs by creating tables with the convention prefixes and uninstalls
by dropping them — no registration, no coupling. A cell without a given module
still has full retrieval; those columns are simply absent. SOMA and the graph
enrichments are modules in exactly this sense — ordinary SQLite tables and columns,
queried as SQL, never a separate tool.

### one interface, local-first

MCP is transport, not topology. The agent sees a single read-only tool —
`flex_search` against a named cell — and retrieval happens inside the cell (see
[how retrieval works](#how-retrieval-works)). The `flex core` CLI is for
installation and operations: initialize sources, rebuild cells, inspect health.
The durable artifact is the cell itself — one local SQLite file under
`~/.flex/cells/`, with no hosted service in the query path.

## what's inside

- **MCP server**: one read-only query tool, `flex_search` — the primary interface
  for agents.
- **local SQLite databases**: source-specific tables, views, saved queries, and
  runtime docs your agent can inspect.
- **CLI**: initialize sources and inspect health.
- **[flexvec](https://github.com/damiandelmas/flexvec)**: SQL vector retrieval
  kernel with suppression, diversification, decay, and trajectory operators.
- **worker**: background refresh for local coding-agent memory.

## paper

The retrieval kernel is described in the flexvec paper:

[flexvec: SQL Vector Retrieval with Programmatic Embedding Modulation](https://arxiv.org/abs/2603.22587)

---

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- codex
```

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

MIT · Python 3.12 · SQLite · [getflex.dev](https://getflex.dev) · [paper](https://arxiv.org/abs/2603.22587) · [x](https://x.com/damian_delmas)
