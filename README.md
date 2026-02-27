# flex

AI was trained on SQL. It doesn't need another retrieval API.

Every `memory.search()` call is a dumbed-down SELECT. Every `memory.add()` is a dumbed-down INSERT. Abstractions exist to make things easier for humans. Your agent isn't human.

Flex gives your agent the schema and gets out of the way. Your agent reads what exists and composes with what's available.

```bash
pip install getflex && flex init
```

---

## Ecosystem

Unify your knowledge. Each domain has its own cell. The schema is the protocol — ATTACH any two cells and JOIN them directly.

| cell | what's in it | |
|---|---|---|
| `claude_code` | Every coding session. Decisions, bugs, architecture choices across all projects. | 138K chunks · 3,175 sessions |
| `vector-project-docs` | Spec-driven development docs, architecture, patterns, changelogs. | 4.6K chunks · 747 docs |
| `infra-project-docs` | Architecture documents, changelogs, agentic memories. | 2.4K chunks · 341 docs |
| yours | anything | one compile adapter |

Query between cells:

```sql
ATTACH 'project-docs' AS docs;
SELECT m.content, d.title
FROM messages m
JOIN docs.chunks d ON d.content LIKE '%authentication%'
WHERE m.type = 'user_prompt'
LIMIT 10
```

---

## The Cell

The prefix declares the lifecycle, the mutability, the writer. No manifest. No config. The database describes itself.

```
_raw_*      immutable     content, embeddings      written by compile
_edges_*    append-only   relationships             written by compile or modules
_types_*    immutable     classification            written by compile
_enrich_*   mutable       graph scores              written by manage
```

Graph intelligence — centrality, hub status, community membership — lives in `_enrich_*` columns.

---

## The Pipeline

Every query runs three steps. Each one does exclusively what the others can't.

```
SQL pre-filter  →  NumPy vector operations  →  SQL compose
```

**SQL pre-filter.** Any SQL whose first column returns chunk IDs. `WHERE type = 'user_prompt'`. `WHERE session_id LIKE 'abc%'`. `JOIN _edges_file_identity ON file_uuid = ?`. Every table in the database is pre-filter vocabulary.

**NumPy vector operations.** Cosine similarity across the candidate set. Modulation tokens reshape the landscape before selection.

**SQL compose.** Full SQL on 500 candidates. Hub boost. Community filter. JOINs against edge tables. Graph arithmetic. All composable.

```sql
SELECT v.id, v.score, m.content
FROM vec_ops('_raw_chunks', 'why did we switch to postgres',
  'diverse recent:14',
  'SELECT id FROM messages WHERE type = ''user_prompt''') v
JOIN messages m ON v.id = m.id
JOIN sessions s ON m.session_id = s.session_id
ORDER BY v.score * (1 + COALESCE(s.centrality, 0)) DESC
LIMIT 10
```

### Optimization

The pre-filter runs first. If your WHERE clause returns 12,000 chunks, NumPy operates on 12,000 vectors — not 148K. Full-corpus matmul is the worst case, not the default.

---

## Modulation Tokens

Vector search returns the same results every time. Tokens reshape the embedding landscape per query.

| token | operation |
|---|---|
| `diverse` | MMR — spread across subtopics |
| `recent:N` | temporal decay — N-day half-life |
| `unlike:TEXT` | contrastive — push away from a concept |
| `like:id1,id2` | centroid — search from a synthetic vantage |
| `from:T to:T` | trajectory — direction through embedding space |

They compose freely. `diverse unlike:oauth recent:7` — three landscape operations, one query. The embeddings aren't static. They're modulated per question.

## Structure Tokens

Run after selection — on the 500 candidates. Add columns SQL compose can use.

`local_communities` runs ephemeral Louvain on the candidate set, adds `_community` to each result.

```sql
SELECT _community, COUNT(*) as n, MIN(m.content) as sample
FROM vec_ops('_raw_chunks', 'authentication', 'local_communities') v
JOIN messages m ON v.id = m.id
GROUP BY _community
```

---

## Modules

A module is tables. Install by creating them with convention prefixes. Uninstall by dropping them. The system discovers what exists.

### Source Modules

Compile raw artifacts into chunks. One adapter per format.

A source module has four domains:

```
compile/    parse format → chunk-atom tables
manage/     offline enrichment → _enrich_* columns
retrieve/   presets — named queries shipped with the module
structure/  query-time topology tokens (optional)
```

The compile contract: parse your format, write two tables, embed, views rebuild automatically.

```
# minimal adapter
_raw_chunks   (id, content, embedding, timestamp)
_edges_source (chunk_id, source_id)
```

Everything else is additive. Add `_types_message` for classification. Add `_edges_tool_ops` if your format has file operations. Call `soma_enrich(chunk)` inline and four identity edge tables appear automatically.

**claude_code** — the reference implementation. Indexes your full session history on first run, then stays current. Hook → queue → daemon (2s ingest). Writes 8 table types. SOMA inline. Enrichment every 30 minutes.

```
3,337 sessions  ·  148K chunks  ·  284MB  ·  one file

file_uuid      100%      every file tool call, unified across renames
content_hash   81.9%     file content at capture time
url_uuid       99.3%     WebFetch operations
```

### Extension Modules

Attach intelligence to existing chunks with `CREATE TABLE`. SOMA is the native extension — stable identity for files, repos, content, and URLs across renames, moves, and deleted worktrees.

```sql
-- SOMA: stable file UUID across renames, moves, repo migrations
CREATE TABLE _edges_file_identity (
  chunk_id  TEXT,
  file_uuid TEXT NOT NULL
);
```

The view generator discovers it. Views update. AI can JOIN on it immediately. No registration. No base class. No interface. A cell without SOMA has full retrieval — identity edges are simply absent.

---

## Presets

Capture any query as a preset. Ship with your module. Discoverable via `@orient`.

### claude_code examples

```
@orient          schema, views, presets, graph topology
@health          pipeline freshness, queue depth, embedding coverage
@digest          multi-day activity summary — sessions, tools, files touched
@sprints         work periods detected by 6h gaps, with op counts
@story           session narrative — timeline, artifacts, agents
@file            every session that touched a file, unified across renames
@genealogy       concept lineage — timeline, hubs, key excerpts
@delegation-tree recursive sub-agent tree from any parent session
@bridges         cross-community connector sessions
```

`@file` resolves a file UUID from any path, fans out to every session that ever touched it — across renames, moves, worktrees — and falls back to path matching for older data. One MCP call.

---

## Local-First

Own your data.

```bash
# it's a file
ls ~/.flex/cells/
claude_code.db    284M

# back it up
rsync -av ~/.flex/cells/ backup:~/

# ship it
scp claude_code.db prod:~/

# query it directly — open format
sqlite3 claude_code.db "SELECT COUNT(*) FROM sessions"
3337
```

No cloud account. No API key. No vendor. No rate limits.

---

```bash
pip install getflex
```

MIT · Python 3.12 · SQLite · ONNX · numpy · networkx

[getflex.dev](https://getflex.dev) · [github](https://github.com/getflex) · [x](https://x.com/damian_delmas)
