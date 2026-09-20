# Pi Coding-Agent Module

The Pi module indexes local Pi coding-agent sessions into Flex's shared
coding-agent substrate. Pi's authority is the versioned JSONL tree under
`~/.pi/agent/sessions/`; the SQLite cell is a rebuildable query projection.

```bash
flex init --module pi
flex search --cell pi "@orient"
```

Use `--pi-sessions-dir` for a copied corpus or fixture:

```bash
flex init --module pi --pi-sessions-dir /tmp/pi-sessions
```

Only files whose first parsed record is a valid Pi `session` header are
ingested. This deliberately excludes derivative extension transcripts such as
`subagent-artifacts/*_transcript.jsonl`, while including canonical nested
subagent `run-*/session.jsonl` files.

Every native entry is retained in `_types_pi_entry`, including its parent edge,
file position, active-branch membership, depth, raw payload, model/configuration
changes, compactions, labels, and extension state. `_types_pi_session` records
the header, current leaf, title, model/provider, thinking level, branch counts,
usage, and error state. Model-visible content is projected into the shared
`chunks`, `messages`, `sessions`, and operation/file surfaces.

Structural rows and FTS commit before optional embedding and enrichment. Watch
refresh uses a signature reconciliation across header-valid session files as
the missed-event correctness floor.
