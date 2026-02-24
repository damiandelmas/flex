# Flex

Your AI sessions, searchable forever.

Flex captures every Claude Code session and makes it queryable. Ask Claude "what did I decide about auth last month?" — it searches your history and answers.

## Install

```
pip install getflex
flex init
```

That's it. Hooks capture your sessions automatically. Claude queries them via MCP.

## Search

```bash
flex search "SELECT COUNT(*) FROM sessions"
flex search "@health"
flex search "SELECT v.score, m.content FROM vec_ops('_raw_chunks', 'authentication') v JOIN messages m ON v.id = m.id LIMIT 5"
```

Or just ask Claude directly — the MCP server is wired automatically.

## How it works

- **Hooks** capture every tool use and user prompt
- **Worker daemon** indexes them into a local SQLite cell with 128-dim embeddings (Matryoshka-truncated from Nomic embed-text-v1.5 768d)
- **MCP server** exposes the cell to Claude as a SQL endpoint
- **vec_ops** enables semantic search: `diverse`, `recent`, `unlike`, `like`, `from/to` trajectory

All data stays local at `~/.flex/`. Nothing leaves your machine.

## Architecture

```
Claude Code tool use → hooks → queue.db → worker → cell.db → MCP → Claude
```

The cell is a self-describing SQLite database. Claude reads the schema and writes its own queries. No SDK, no API client — just SQL.

## License

MIT
