<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

Vector and hybrid retrieval for structured data. Flex compiles any data source into a SQLite database with embeddings, knowledge graphs, and structured views. Installing flex registers an MCP endpoint with a single tool — the AI agent reads the schema and writes SQL against the database.

```bash
pip install getflex
```

## what's inside

- **[flexvec](https://github.com/damiandelmas/flexvec)** — SQL vector retrieval kernel. suppress, diversify, decay, trajectory — composable operations on the score array before selection. [paper](https://arxiv.org/abs/2603.22587).
- **MCP server** — a single read-only tool (`flex_search`). The agent discovers the schema at runtime and writes SQL.
- **worker** — a background service that detects new data and embeds it within seconds.
- **modules** — data source adapters. `claude_code` ships built-in.

## claude code

The claude_code module indexes your entire Claude Code session history — file lineage, decision archaeology, weekly digests. One install command.

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

The installer scans existing sessions, embeds everything, starts a background worker, and registers the MCP server.

```
"Use flex: what's the history of worker.py?"
"Use flex: what did we build this week?"
"Use flex: how did we create the curl install script?"
```

→ [claude code docs](docs/claude_code/)

## how it works

```
data source
       │
  [compile]  parse → chunks + metadata + embeddings
       │
       ▼
  SQLite database
       │
  [manage]  knowledge graph, fingerprints, project attribution
       │
  [MCP server]  read-only SQL surface
       │
       ▼
  AI agent writes SQL
```

Each database is a single `.db` file with the same schema — chunks, edges, enrichments, and views. The database describes itself; the agent discovers what's available at query time.

## local-first

```bash
ls ~/.flex/cells/
claude_code.db    284M

sqlite3 claude_code.db "SELECT COUNT(*) FROM sessions"
4547
```

Everything runs in-process. No external services, no cloud dependency.

---

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

MIT · Python 3.12 · SQLite · [getflex.dev](https://getflex.dev) · [paper](https://arxiv.org/abs/2603.22587) · [x](https://x.com/damian_delmas)
