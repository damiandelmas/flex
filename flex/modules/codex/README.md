# Codex Module

The Codex module indexes Codex CLI session provenance into a Flex coding-agent
cell. It reads per-session JSONL rollouts from `~/.codex/sessions/`
(`YYYY/MM/DD/rollout-*.jsonl`), transpiles them into the shared coding-agent
substrate, and registers a local `watch` lifecycle cell named `codex`.

```bash
flex init --module codex
flex core search --cell codex "@orient"
```

Use `--codex-dir` to point at a copied sessions root or fixture:

```bash
flex init --module codex --codex-dir /tmp/codex-fixture/sessions
```

## What Gets Indexed

Each source is a Codex session; each chunk is a prompt, assistant turn, tool
call, tool result, file capture, or derived high-signal event. The transpiler:

- reads `session_meta`, `event_msg`, and `response_item` events from each
  rollout file (`turn_context` is skipped)
- recovers `apply_patch` diffs from sibling `patch_apply_end` events keyed by
  `call_id` (the function call itself carries empty arguments)
- normalizes tool names to the Claude Code canonical vocabulary where the
  function matches (`exec_command` → Bash, `apply_patch` → Edit,
  `spawn_agent` → Task, `update_plan` → TodoWrite, `view_image` → Read);
  other tools pass through verbatim, with the raw Codex name preserved in
  `_raw_content.tool_name`
- infers soft file-ops from shell command text, so `cat`, `sed -n`, heredoc
  writes, and similar terminal observations land in `_edges_soft_ops` even
  when no canonical file tool ran
- records delegation (`spawn_agent` items) and fork lineage from
  `session_meta.forked_from_id` as fork edges between sessions
- pulls session titles from the Codex `state_5.sqlite` threads table when
  available, so `@story` and `@orient` show titles instead of raw UUIDs

Everything downstream — cell bootstrap, embedding, enrichment, stock views,
presets, lifecycle — is handled by the Claude Code substrate. The query
vocabulary is intentionally shared: `chunks`, `messages`, `sessions`, `files`,
and `agent_key_chunks` mean the same kind of thing in both cells.

## Public Surface

- `install.py` declares the module spec and calls the shared coding-agent
  install runner.
- `sources.py` resolves which Codex homes refresh should scan.
- `refresh.py` rescans the resolved homes and resyncs changed rollouts.
- `compile/worker.py` is the Codex-specific transpiler. It writes
  `_raw_chunks`, `_raw_sources`, `_edges_source`, `_edges_tool_ops`,
  `_types_message`, `_types_file_body`, and related shared tables.
- `stock/instructions.md` explains the query surface mounted through
  `@orient`.

## Multi-Home Sources

The cell can be built from more than the global `~/.codex`. Additional
`.codex` homes can be declared in Flex config or cell metadata; every usable
home is expected to have the same `sessions/` layout and is scanned through
the same ingestion path. Per-session provenance lives in the
`_types_codex_source` sidecar:

```sql
SELECT session_id, source_kind, codex_home, rollout_path
FROM _types_codex_source
LIMIT 20;
```

Columns include `source_kind`, `codex_home`, `sessions_dir`, `state_db`, and
`rollout_path`, so coverage audits can distinguish declared homes, usable
homes, and indexed sessions.

## ACP Views

Like other coding-agent cells, the cell exposes shared agent-context-protocol
views: `acp_sessions` and `acp_events` (plus `acp_category_coverage`), with
`provider = 'codex'`. These give cross-agent consumers a common recall surface
with drillback pointers into the native rows.

## Querying

Start with `@orient` — it returns the live schema, presets, and mounted
instructions. Use structural SQL against `chunks`, `messages`, and `sessions`
when you know ids, paths, or dates; use `keyword()` for exact terms and
`vec_ops()` for semantic search with a pre-filter in the second argument.
Recovery presets (`@full`, `@observed-file`, `@file-history`) climb from
clipped search results to full message bodies.

## Refresh

The cell registers a `watch` lifecycle on `**/rollout-*.jsonl`. Refresh
computes a directory signature (total size and file count) over the resolved
homes and skips work when the source has not changed.
