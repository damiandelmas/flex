<p align="center">
  <img src="../../assets/banner.png" alt="flex" width="100%">
</p>

# flex MCP server

Give your AI agent queryable memory over its own conversation history.

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

Installs hooks, indexes your sessions, and registers the MCP server. Restart Claude Code, type `/mcp` to verify.

---

## What the agent can do

One MCP endpoint. The agent writes SQL against your indexed history.

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

Vector search, keyword search, structural SQL, and graph intelligence — all composable in one query.

## Presets

The agent discovers these automatically via `@orient`.

```
@orient          schema, views, presets, graph topology
@digest          multi-day activity summary
@file            every session that touched a file, across renames
@story           session narrative — timeline, artifacts, agents
@genealogy       trace a concept's lineage across sessions
```

## Modulation tokens

Operations on the score array before selection. Compose freely.

| token | what it does |
|---|---|
| `diverse` | MMR — spread results across subtopics |
| `decay:N` | temporal decay with N-day half-life |
| `suppress:TEXT` | push results away from a concept |
| `centroid:id1,id2` | search from the mean of example chunks |
| `from:T to:T` | direction vector through embedding space |

## Local-first

Your data stays on your machine. One SQLite file, no cloud, no API keys.

```bash
ls ~/.flex/cells/
claude_code.db    284M
```

---

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

MIT · [getflex.dev](https://getflex.dev) · [main repo](../../) · [x](https://x.com/damian_delmas)
