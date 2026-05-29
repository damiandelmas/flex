<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

**fastest way to get knowledge and memory for your coding agent**

flex compiles your coding agent session history into a queryable SQLite database
with vector and hybrid retrieval. your AI agent connects through MCP, discovers
the schema at runtime, and writes SQL against your history.

**install one source:**

```bash
# Claude Code
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code

# Codex CLI
curl -sSL https://getflex.dev/install.sh | bash -s -- codex

# Obsidian / Markdown
curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

---

## what's different

**most memory systems start working after you install them.**

flex works retroactively. as soon as you install, your existing sessions become
queryable through the MCP tool. you can ask how you installed the Cloudflare
tunnel yesterday, why a release script changed, or what session created a file.

**retrieval tools typically have minimal metadata to filter on.**

coding-agent sessions are not plain documents. they have prompts, replies, tool
calls, file edits, repos, projects, and sub-agents. flex keeps that structure,
so the agent can filter before semantic scoring instead of asking vector search
to guess what matters.

flex also keeps the answer attached to source evidence: the session it came
from, what files and repos it touched, what project it belongs to, and where to
go next if you need the full trace.

**vector search typically surfaces similar content and stops there.**

flex lets the agent combine SQL, semantic search, keyword search, suppression,
diversity, recency weighting, and trajectory search in one local query
interface.

that means you can ask for architecture work but not changelogs, recent auth
work but not oauth docs, or a diverse sample instead of ten copies of the same
answer.

## what can you do?

ask things like:

- how did we set up the docker environment for this?
- what session did we edit `registry.py` last and what was my reason?
- what are the wildest moments in our entire session history?

### file lineage

flex tracks sessions, messages, tool calls, file edits, and source evidence.
ask:

```text
"Use flex: what's the history of worker.py?"
```

flex can find what session created a file, what prompts were used to create it,
what changed later, and why.

### decision archaeology

the hardest question in software is why something was done. ask:

```text
"Use flex: why did we create registry.py?"
```

flex finds the session where the decision happened and reconstructs the path:
which approaches were considered, which failed, and why you landed there.

### weekly digest

sessions are grouped by projects, touched files, and key decisions. ask:

```text
"Use flex: what did we build this week?"
```

your agent can run the queries behind the scenes and come back with an overview
of recent work.

### semantic search (and more)

semantic search can be composed with filters and operators. ask:

```text
"Use flex: 5 things we talked about this week outside the main project"
```

flex can search for one topic while suppressing another, or retrieve a diverse
sample instead of ten copies of the same answer.

## sources

flex is one MCP search tool for local sources. each source has its own install
path, but the agent uses the same query model across them.

### Claude Code

Fresh install:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

If flex is already installed:

```bash
flex init --module claude-code
flex core search --cell claude_code "@orient"
```

Claude Code sessions become searchable through MCP. flex indexes local session
history, tool calls, file edits, and sub-agent traces, then keeps updating as
you work.

### Codex

Fresh install:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- codex
```

If flex is already installed:

```bash
flex init --module codex
flex core search --cell codex "@orient"
```

Codex CLI sessions use the same coding-agent model: messages, tool calls, file
evidence, repo context, and source recovery through the same MCP interface.

### Obsidian and Markdown

Fresh install:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

Point directly at a vault:

```bash
VAULT=/path/to/vault curl -sSL https://getflex.dev/install.sh | bash -s -- obsidian
```

If flex is already installed:

```bash
flex init --module obsidian --vault /path/to/vault --name obsidian
flex core search --cell obsidian "@orient"
```

Then ask through MCP:

```text
"Use flex: what notes are connected to project planning?"
"Use flex: find orphaned notes in my vault"
"Use flex: summarize the notes that mention release planning"
```

flex indexes notes, sections, frontmatter tags, aliases, wikilinks, ghost
notes, and heading hierarchy without modifying your source files.

## how retrieval works

every query runs three phases in one SQL statement.

```text
SQL pre-filter  ->  score modulation  ->  SQL compose
```

1. **SQL pre-filter** narrows what enters scoring by date, source, type, length,
   project, path, or any SQL expression.
2. **score modulation** reshapes semantic scores with operators such as
   suppression, diversity, recency, and trajectory.
3. **SQL compose** joins results back to your tables for grouping, filtering,
   reranking, or source recovery.

the retrieval engine bridges vector scoring into SQL. the agent writes
`FROM vec_ops(...)` as if it were a table; flex runs the scoring work, writes the
results back into SQLite, and lets the rest of the query continue normally.

## tokens

tokens reshape scores. they compose freely in a single string.

| token | what it does |
|---|---|
| `similar:TEXT` | search for this concept |
| `suppress:TEXT` | push this topic out of results |
| `diverse` | spread across subtopics instead of ten versions of the same answer |
| `decay:N` | favor recent content with an N-day half-life |
| `centroid:id1,id2` | search from the average of examples |
| `from:A to:B` | find content along a conceptual arc |
| `pool:N` | set how many candidates to score |

`'similar:auth diverse suppress:oauth decay:7'` is four operations in one query.

## example

the kind of query an agent can write:

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

this finds the 5 assistant messages that are most similar to `how the system
works architecture`, while suppressing messages that are similar to `website
landing page design tagline`. that is how you get the old architecture thread
instead of five landing page drafts.

most vector search tools can only do the former, not the latter. since the tool
is in SQL, the MCP instructions are pretty light: coding agents already know
how to use the mechanics.

## local-first

your knowledge base is one SQLite file on your machine. flex stores local
databases under `~/.flex/cells/`.

```bash
flex core search --cell claude_code "SELECT COUNT(*) FROM sessions"
```

everything runs locally. no hosted database required.

## raw cli access

MCP is the normal interface. use raw CLI access when an agent or operator needs
direct terminal queries, debugging, or scripting:

```bash
flex core search --cell claude_code "@digest days=3"
flex core search --cell claude_code "@file path=worker.py"
flex core search --cell claude_code "SELECT COUNT(*) FROM sessions"
```

same query interface, direct from the terminal.

## what's inside

- **MCP server**: one read-only query tool, `flex_search`; this is the primary
  interface for agents.
- **local SQLite databases**: source-specific tables, views, saved queries, and
  runtime docs the agent can inspect.
- **CLI**: initialize sources, run raw terminal queries, and inspect health.
- **[flexvec](https://github.com/damiandelmas/flexvec)**: SQL vector retrieval
  kernel with suppression, diversification, decay, and trajectory operators.
  [paper](https://arxiv.org/abs/2603.22587)
- **worker**: background refresh for local coding-agent memory.

## on flexvec

the architecture of the retrieval kernel, [flexvec](https://github.com/damiandelmas/flexvec), and a practical evaluation is available as an arXiv preprint: how the score array becomes a programmable surface instead of just a sorting criterion.

here: [paper](https://arxiv.org/abs/2603.22587)

---

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

MIT · Python 3.12 · SQLite · [getflex.dev](https://getflex.dev) · [paper](https://arxiv.org/abs/2603.22587) · [x](https://x.com/damian_delmas)
