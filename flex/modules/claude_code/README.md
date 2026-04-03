<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

**fastest way to get knowledge and memory for every Claude Code session**

flex compiles your Claude Code session history into a queryable SQLite database with vector and hybrid retrieval. your AI agent connects via MCP, discovers the schema at runtime, and writes SQL against your history.

**install:**

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

---

## what's different

**most memory systems start working after you install them.**

flex works retroactively — as soon as you install, all of your sessions become queryable via the MCP tool. you can ask claude how you installed the cloudflare tunnel yesterday, why you made a specific edit to your landing page, and much more.

**retrieval tools typically have minimal metadata to filter on.**

flex captures everything: the exact session, whether it spawned agents, what files and repos it touched, which project it belongs to. filtering happens before scoring — the vector engine only scores what survives. on top of that, flex builds knowledge graphs automatically — community structure, hub sessions, file co-edit patterns, project attribution.

**vector search typically surfaces similar content and stops there.**

flex lets you suppress a topic, weight by recency, diversify across subtopics, trace a direction through embedding space — all composable in one query. if you want "architecture documents" but not "changelogs", just ask claude; it'll use the `similar:architecture` and `suppress:changelog` tokens.

## what can you do?

### file lineage

flex tracks every session's messages and operations. get information on what session created a file, what prompts were used to create it, and why it was changed last. just ask:

```
"Use flex: what's the history of worker.py?"
```

the history of worker.py is followed beyond file moves and renames. never lose track of your reasoning.

### decision archaeology

the hardest question in software: *why was it done this way?* flex finds the session where the decision happened and reconstructs the path — which approaches were considered, which failed, and why you landed here. just ask:

```
"Use flex: how did we create the curl install script?"
```

this works for technical questions, practical concerns and logistical setups as well. makes it easy to grab the runbook for setting up your website from three weeks ago.

### weekly digest

sessions are grouped by the projects they touch, the files that got the most edits, and key decisions. just ask:

```
"Use flex: what did we build this week?"
```

claude runs a series of queries behind the scenes and comes back with an overview of your recent week.

### semantic search

semantic search just surfaces similar content. flex can search for one topic while suppressing another. just ask:

```
"Use flex: 5 things we talked about this week outside the main project"
```

it can also get a diverse sample of information.

---

## local-first

your knowledge base is one file on your machine.

```bash
ls ~/.flex/cells/
claude_code.db    284M

# back it up
rsync -av ~/.flex/cells/ backup:~/

# query it directly — open format
sqlite3 claude_code.db "SELECT COUNT(*) FROM sessions"
4547
```

everything runs in-process. no external services, no cloud dependency.

---

## cli access

query from the terminal without claude:

```bash
flex search "@digest days=3"
flex search "@file path=src/worker.py"
flex search "SELECT COUNT(*) FROM sessions WHERE project = 'myapp'"
```

same queries, no MCP required.

---

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

MIT · Python 3.12 · SQLite · [getflex.dev](https://getflex.dev) · [x](https://x.com/damian_delmas)
