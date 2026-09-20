---
name: flex-sessions-pi
description: Compatibility alias for Pi coding-agent session history. Route messages, reasoning, tools, branches, compactions, and native entry recovery through the shared Flex sessions contract.
allowed-tools:
  - mcp__flex__flex
---

# flex-sessions-pi

Use the Pi coding-agent cell through the shared session query contract. Start
with `cell="pi" query="@orient"`, then use the shared `@story`, `@digest`,
`@file`, `@full`, `keyword()`, and `vec_ops()` surfaces.

Pi-native tree entries remain in `_types_pi_entry`; header, active-leaf,
branch, model, thinking-level, usage, and session-name metadata lives in
`_types_pi_session`. Follow those sidecars whenever a compatibility view cannot
express Pi's branch or compaction semantics.
