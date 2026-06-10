# Goose Module

The Goose module indexes a local Goose `sessions.db` into a Flex coding-agent
cell. It reads the native SQLite store at
`~/.local/share/goose/sessions/sessions.db`, transpiles sessions and messages
into the shared coding-agent substrate, and registers a local `watch` lifecycle
cell named `goose`.

```bash
flex init --module goose
flex core search --cell goose "@orient"
```

Use `--goose-db` to point at a copied database or fixture:

```bash
flex init --module goose --goose-db /tmp/goose-fixture/sessions.db
```

## Public Surface

The public module surface is intentionally small:

- `install.py` declares the module spec and calls the shared coding-agent
  install runner.
- `refresh.py` polls the recorded Goose database path and resyncs only when the
  source file grows.
- `compile/worker.py` is the Goose-specific transpiler. It reads native Goose
  rows and writes `_raw_chunks`, `_raw_sources`, `_edges_source`,
  `_edges_tool_ops`, `_types_message`, `_types_file_body`, and related shared
  coding-agent tables.
- `stock/instructions.md` explains the query surface that appears through
  `@orient` document mounts.

The benchmark/source corpus directory `_corpus/` is not part of the public
module. It contained private benchmark prompts, generated results, fixtures,
and Goose-native parity adapter source. Those artifacts are excluded from this
public-ready surface.

## Sidecar

Goose keeps provider-specific session facts in `_types_goose_session`.
Canonical coding-agent tables stay shared, while native fields remain directly
queryable:

```sql
SELECT source_id, provider_name, goose_mode, model_config_json
FROM _types_goose_session
ORDER BY updated_at DESC
LIMIT 20;
```

Important sidecar columns include `provider_name`, `model_config_json`,
`goose_mode`, `session_type`, `working_dir`, token counters, recipe JSON, and
thread metadata.

## Refresh

The install path records `goose_db_path` and `goose_db_size` in `_meta`. Refresh
opens that recorded database path, compares the current byte size with the last
recorded size, and skips work when unchanged. This keeps the daemon path safe
for local Goose stores and makes fixture/copy validation possible without
mutating the live database.
