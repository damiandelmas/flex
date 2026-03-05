# flex

AI was trained on SQL — it doesn't need another retrieval API.

flex compiles AI agent conversations and knowledge bases into SQLite. Each knowledge base is a self-describing cell — chunks, embeddings, graph intelligence, and extensible functionality.

Instead of encoding retrieval intelligence into the pipeline flex exposes the schema and lets the agent compose its own queries over a rich data surface.

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

<details><summary>other install methods</summary>

```bash
pip install getflex && flex init          # manage your own environment
python -m flex init                       # if GNU flex shadows the binary
```
</details>

---

## ecosystem

Unify your knowledge. Each domain has its own cell. The schema is the protocol — ATTACH any two cells and JOIN them directly.

| cell | what's in it |
|---|---|
| `claude_code` | Every coding session. Decisions, bugs, architecture choices across all projects. |
| `vector-project-docs` | Spec-driven development docs, architecture, patterns, changelogs. |
| `infra-project-docs` | Architecture documents, changelogs, agentic memories. |
| yours | anything — one compile adapter |

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

## the pipeline

Flex keeps embeddings as numpy arrays you can modulate — diverse, unlike, trajectory, contrastive, temporal decay — operations on the embedding space itself.

The retrieval engine is published independently as [flexvec](https://github.com/damiandelmas/flexvec) for use in any SQLite database.

Every query runs three steps. The agent writes the query.

```
SQL pre-filter  →  NumPy vector operations  →  SQL compose
```

**SQL pre-filter.** Any SQL whose first column returns chunk IDs. `WHERE type = 'user_prompt'`. `WHERE session_id LIKE 'abc%'`. `JOIN _edges_file_identity ON file_uuid = ?`. Every table in the database is pre-filter vocabulary. If your WHERE returns 12,000 chunks, NumPy operates on 12,000 vectors — not the full corpus.

**NumPy vector operations.** Cosine similarity across the candidate set. Modulation tokens reshape the landscape before selection. `unlike:oauth` penalizes similarity to a concept in embedding space. `diverse` runs MMR. `recent:7` applies temporal decay.

**SQL compose.** Full SQL on 500 candidates. Hub boost. Community filter. JOINs against edge tables. Graph arithmetic. Add a column to your chunks — sentiment, classification, anything — and compose it into queries immediately. For cells with curated views, add the column to the view `.sql` file and run `flex sync`. For auto-generated views, it appears automatically.

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

---

## @orient

The database describes itself. Run `@orient` and the agent gets the schema, graph topology, communities, hubs, and every available preset in one call. The agent reads what exists and knows what to ask.

```
shape:        148K chunks · 3,337 sources
views:        messages (id, content, type, session_id, tool_name, target_file, ...)
              sessions (session_id, project, centrality, is_hub, community_label, ...)
functions:    vec_ops(table, query, tokens, pre_filter_sql) → (id, score)
              keyword(term) → (id, rank, snippet)
communities:  flexsearch (71 sessions) · thread (111) · npta (106) · website (55)
hubs:         9d1e3f3d "FlexSearch SQL engine" (centrality: 0.0045)
presets:      @digest @story @genealogy @file @sprints @bridges ...
```

---

## modulation tokens

| token | operation |
|---|---|
| `diverse` | MMR — spread across subtopics |
| `recent:N` | temporal decay — N-day half-life |
| `unlike:TEXT` | contrastive — push away from a concept |
| `like:id1,id2` | centroid — search from a synthetic vantage |
| `from:T to:T` | trajectory — direction through embedding space |

They compose freely. `diverse unlike:oauth recent:7` — three operations on the embedding space, one query. The embeddings aren't static. They're modulated per question.

## structure tokens

Run after selection on the 500 candidates. Add columns that SQL compose can use.

`local_communities` runs ephemeral Louvain on the candidate set, adds `_community` to each result.

```sql
SELECT _community, COUNT(*) as n, MIN(m.content) as sample
FROM vec_ops('_raw_chunks', 'authentication', 'local_communities') v
JOIN messages m ON v.id = m.id
GROUP BY _community
```

---

## modules

A module is tables. Install by creating them with convention prefixes. Uninstall by dropping them. The system discovers what exists.

### source modules

Compile raw artifacts into chunks. One adapter per format.

**claude_code** — the reference implementation. Indexes your full session history on first run, then stays current. Hook → queue → daemon (2s ingest). SOMA inline. Enrichment every 30 minutes.

Run `@health` on your cell for current coverage. Fresh installs achieve 100% on all SOMA dimensions.

<details><summary>writing a source module</summary>

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

</details>

### extension modules

Attach intelligence to existing chunks with `CREATE TABLE`. SOMA is the native extension — stable identity for files, repos, content, and URLs across renames, moves, and deleted worktrees.

```sql
-- SOMA: stable file UUID across renames, moves, repo migrations
CREATE TABLE _edges_file_identity (
  chunk_id  TEXT,
  file_uuid TEXT NOT NULL
);
```

The view generator discovers it. For tables with a PK on `chunk_id`, auto-generated views include it automatically. Curated views require adding the JOIN manually. All shipped modules use curated views. No registration. No base class. No interface. A cell without SOMA has full retrieval — identity edges are simply absent.

---

## presets

Capture any query as a preset. Ship with your module. Discoverable via `@orient`.

### claude_code examples

```
@orient          schema, views, presets, graph topology
@health          pipeline freshness, queue depth, embedding coverage
@digest          multi-day activity summary — sessions, tools, files touched
@sprints         work periods detected by 6h gaps, with op counts
@story           session narrative — timeline, artifacts, agents
@genealogy       concept lineage — timeline, hubs, key excerpts
@delegation-tree recursive sub-agent tree from any parent session
@bridges         cross-community connector sessions
```

**@file** resolves a file UUID from any path, fans out to every session that ever touched it — across renames, moves, worktrees — and falls back to path matching for older data. One MCP call.

---

<details><summary>cell internals</summary>

The prefix convention declares the intended lifecycle. Not enforced — semantic contract.

```
_raw_*      write-once    content, embeddings      written by compile
_edges_*    append-only   relationships             written by compile or modules
_types_*    write-once    classification            written by compile
_enrich_*   rebuildable   graph scores              written by manage
```

Graph intelligence — centrality, hub status, community membership — lives in `_enrich_*` columns.

</details>

---

## local-first

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
curl -sSL https://getflex.dev/install.sh | bash
```

MIT · Python 3.12 · SQLite · ONNX · numpy · networkx

[getflex.dev](https://getflex.dev) · [github](https://github.com/getflex) · [x](https://x.com/damian_delmas)