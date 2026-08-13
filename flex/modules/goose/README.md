# Goose Module

The Goose module indexes a local Goose `sessions.db` into a Flex coding-agent
cell. It reads the native SQLite store at
`~/.local/share/goose/sessions/sessions.db`, transpiles sessions and messages
into the shared coding-agent substrate, and registers a local `watch` lifecycle
cell named `goose`.

```bash
flex init --module goose
flex search --cell goose "@orient"
```

Use `--goose-db` to point at a copied database or fixture:

```bash
flex init --module goose --goose-db /tmp/goose-fixture/sessions.db
```

## Public Surface

The public module surface is intentionally small:

- `install.py` declares the module spec and calls the shared coding-agent
  install runner.
- `refresh.py` provides WAL-aware structural capture and drains semantic debt
  independently.
- `compile/worker.py` is the Goose-specific transpiler. It reads native Goose
  rows and writes `_raw_chunks`, `_raw_sources`, `_edges_source`,
  `_edges_tool_ops`, `_types_message`, `_types_file_body`, and related shared
  coding-agent tables.
- `stock/instructions.md` explains the query surface that appears through
  `@orient` document mounts.

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

The install path records `goose_db_path`, `goose_source_signature`, and the
compatibility `goose_db_size` receipt in `_meta`. Watch events on the database
or its WAL publish text, metadata, FTS, and relations in one structural
transaction. Embeddings remain nullable debt and converge through the separate
semantic refresh lane. Periodic signature comparison remains the missed-event
correctness floor.
