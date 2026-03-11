<p align="center">
  <img src="../../assets/banner.png" alt="flex" width="100%">
</p>

# flex

AI was trained on SQL — it doesn't need another retrieval API.

Flex compiles your Claude Code sessions into one SQLite file. Claude queries it directly — no SDK, no cloud, no vector database.

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

<details><summary>other install methods</summary>

```bash
python -m flex init                       # if GNU flex shadows the binary
```
</details>

One command. Indexes your full history on first run, then stays current automatically.

---

## what can you ask?

### weekly digest

```
"Use flex: what did we build this week?"
```

Sessions grouped by project, files that got the most edits, key decisions. A weekly digest runs about 16 queries behind the scenes.

### file lineage

```
"Use flex: what's the history of worker.py?"
```

Every session that touched the file — who created it, why, what changed, what it became. Tracks files across renames automatically.

### decision archaeology

```
"Use flex: how did we create the curl install script?"
```

The hardest question in software: *why was it done this way?* Flex finds the session where the decision happened and reconstructs the path — which approaches were considered, which failed, and why you landed here.

### semantic search

```
"Use flex: 5 things we talked about this week outside the main project"
```

Search by meaning, not keywords. Modulation tokens reshape the search per query — `diverse` spreads across subtopics, `suppress:oauth` suppresses a dominant theme, `decay:7` weights toward last week.

---

## what happens when you install

```
Claude Code tool use
       ↓
  [hooks]  notify on every prompt and tool call
       ↓
  [worker]  parses, embeds, writes to cell — 2s latency
       ↓
  ~/.flex/cells/claude_code.db
       ↓
  [MCP server]  Claude writes SQL against your history
```

`flex init` scans your existing sessions (~2-20 min depending on history size), downloads an 87 MB embedding model, and installs hooks + services. After that, everything is automatic. Restart Claude Code, type `/mcp` to verify.

---

## presets

Claude discovers these automatically via `@orient` — you rarely need to name them directly.

```
@orient          schema, views, presets, graph topology
@digest          multi-day activity summary
@file            every session that touched a file, across renames
@story           session narrative — timeline, artifacts, agents
@sprints         work sprints detected by time gaps
@genealogy       trace a concept's lineage across sessions
@health          pipeline freshness, embedding coverage
```

---

## local-first

Your entire knowledge base is one file on your machine.

```bash
ls ~/.flex/cells/
claude_code.db    284M

# back it up
rsync -av ~/.flex/cells/ backup:~/

# query it directly — open format
sqlite3 claude_code.db "SELECT COUNT(*) FROM sessions"
3337
```

No cloud. No vendor. No rate limits. One file you can `scp` anywhere.

---

## under the hood

The database describes itself. Run `@orient` and the agent gets the schema, graph topology, communities, hubs, and every available preset in one call.

```
shape:        148K chunks · 3,337 sources
views:        messages (id, content, type, session_id, tool_name, target_file, ...)
              sessions (session_id, project, centrality, is_hub, community_label, ...)
functions:    vec_ops('similar:QUERY TOKENS', pre_filter_sql) → (id, score)
              keyword(term) → (id, rank, snippet)
communities:  flexsearch (71 sessions) · thread (111) · npta (106) · website (55)
hubs:         9d1e3f3d "FlexSearch SQL engine" (centrality: 0.0045)
presets:      @digest @story @genealogy @file @sprints @bridges ...
```

Every query runs three phases. The agent writes the full query.

```
SQL pre-filter  →  Vector Operations  →  SQL compose
```

```sql
SELECT v.id, v.score, m.content
FROM vec_ops(
    'similar:implementation summary what changed
     diverse suppress:plan suppress:design suppress:architecture',
    'SELECT id FROM messages
     WHERE type = ''assistant'' AND length(content) > 300') v
JOIN messages m ON v.id = m.id
ORDER BY v.score DESC LIMIT 10
```

Pre-filter narrows the corpus with SQL, vector ops scores with embeddings + modulation tokens, SQL compose joins back to views with graph intelligence.

---

## ecosystem

Each knowledge domain gets its own cell. The schema is the protocol — ATTACH any two cells and JOIN them directly.

| cell | what's in it |
|---|---|
| `claude_code` | Every coding session — decisions, bugs, architecture choices |
| `reddit` | Community pulse — r/ClaudeCode, r/Python, r/LocalLLaMA, and more |
| `project-docs` | Your documentation corpus — specs, changelogs, design docs |
| yours | anything — one compile adapter, two tables |

---

## terminal access

Query without Claude:

```bash
flex search "@digest days=3"
flex search "@file path=src/worker.py"
flex search "SELECT COUNT(*) FROM sessions WHERE project = 'myapp'"
```

---

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

MIT · Python 3.12 · SQLite · ONNX · [getflex.dev](https://getflex.dev) · [github](https://github.com/damiandelmas/flex) · [x](https://x.com/damian_delmas)
