---
name: flex:sessions:deepseek
description: Compatibility alias for DeepSeek Harness session history. Route turns, tools, files, failures, and native event recovery through the shared Flex sessions contract.
allowed-tools:
  - mcp__flex__flex
---

# flex:sessions:deepseek

Use the DeepSeek Harness coding-agent cell through the shared session query
contract. Start with `cell="deepseek" query="@orient"`, then use the shared
`@story`, `@digest`, `@file`, `@full`, `keyword()`, and `vec_ops()` surfaces.

DeepSeek-native request headers, streaming chunks, policy changes, and terminal
events remain in `_types_deepseek_event`; session/model/provider metadata lives
in `_types_deepseek_session`. Follow those sidecars when the compatibility
views cannot name a DSH-specific event.
