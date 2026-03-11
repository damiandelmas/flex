<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

AI was trained on SQL — it doesn't need another retrieval API.

flex compiles your session history, memory and knowledge into SQLite. Your AI agent connects, discovers the schema, and starts querying. The fastest way to make your structured data searchable.

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

## How it works

Flex compiles your data into a SQLite file called a cell — chunks, embeddings, graph intelligence, full-text index. Your AI agent connects via MCP, discovers the schema, and writes SQL.

Every query runs three phases. **SQL pre-filter** narrows what enters scoring — by date, project, message type, or any SQL expression. **Modulation** reshapes results — suppress a topic, weight by recency, diversify across subtopics. **SQL compose** joins scored results back to metadata, graph intelligence, and any table in the database.

In one statement:

```sql
SELECT v.id, v.score, c.content
FROM vec_ops(
    'similar:authentication patterns
     diverse suppress:deployment suppress:testing',
    'SELECT id FROM chunks WHERE length(content) > 200') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

This searches for "authentication patterns," spreads results across subtopics (`diverse`), pushes deployment and testing content out of the results (`suppress:`), and only scores chunks longer than 200 characters.

## Modulation tokens

Tokens reshape how search results are scored. They compose freely.

| token | what it does |
|---|---|
| `suppress:TEXT` | push a topic out of results |
| `diverse` | spread results across subtopics instead of clustering on the strongest match |
| `decay:N` | weight recent content — N-day half-life |
| `centroid:id1,id2` | "more like these" — search from the average of example chunks |
| `from:T to:T` | find content along a conceptual direction (e.g. `from:prototype to:production`) |

```
'diverse suppress:oauth decay:7'
```

Three operations, one query. They compose freely.

## Cells

Each knowledge domain gets its own SQLite file called a cell. The schema is the protocol — ATTACH any two cells and JOIN them.

| cell | what's in it |
|---|---|
| [`claude_code`](flex/modules/claude_code/) | Every coding session — decisions, tool calls, file operations, agent delegations |
| `reddit` | Community activity — posts, comments, signal scoring |
| `arxiv` | Paper corpus with semantic retrieval over the research landscape |
| `project-docs` | Your documentation corpus — specs, changelogs, design docs |
| yours | anything — one compile adapter, two tables |

## Presets

Named queries stored in the database. The agent discovers them via `@orient`.

```
@orient          schema, views, presets, graph topology
@digest          multi-day activity summary
@file            every session that touched a file, across renames
@story           session narrative — timeline, artifacts, agents
@genealogy       trace a concept's lineage across sessions
```

## Local-first

Your knowledge base is one file on your machine.

```bash
ls ~/.flex/cells/
claude_code.db    284M

sqlite3 claude_code.db "SELECT COUNT(*) FROM sessions"
3337
```

No cloud. No vendor. No rate limits. Back up with `cp`. Ship with `scp`. Inspect with any SQLite client.

---

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

MIT · Python 3.12 · SQLite · numpy · ONNX · NetworkX

[getflex.dev](https://getflex.dev) · [claude_code module](flex/modules/claude_code/) · [x](https://x.com/damian_delmas)
