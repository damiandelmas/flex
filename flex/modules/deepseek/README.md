# DeepSeek Harness Module

The DeepSeek module indexes a local DeepSeek Harness (`dsh`) session home into
the shared Flex coding-agent substrate. DSH's source of truth is one append-only
`session.jsonl.zstd` artifact per session; the Flex SQLite cell is a queryable
projection and never replaces those artifacts.

```bash
flex init --module deepseek
flex search --cell deepseek "@orient"
```

Use `--dsh-dir` to point at a copied harness home or fixture:

```bash
flex init --module deepseek --dsh-dir /tmp/dsh
```

The transpiler preserves every native event in `_types_deepseek_event`, keeps
session/request/model metadata in `_types_deepseek_session`, and projects user
messages, assistant text/reasoning, tool blocks, configuration changes, and
terminal errors into the shared `chunks`, `messages`, `sessions`, and `files`
surfaces. Both zstd-compressed and plaintext `session.jsonl` artifacts are
accepted; compressed sources use the system `zstd` executable when the optional
Python zstandard package is absent.

Structural rows and FTS are committed before optional embedding and enrichment.
The cell registers as a local `watch` source, and a signature reconciliation is
the missed-event correctness floor.
