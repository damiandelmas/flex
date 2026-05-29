# Changelog

Public changes to **flex** ([getflex.dev](https://getflex.dev)).

---

## 0.30.0 — May 27, 2026

**Codex As A First-Class Source**

Codex sessions index alongside Claude Code through one query surface: session history, prompts, assistant turns, tool calls, file evidence, and stock presets. Codex refresh scans multiple declared Codex homes instead of only the global `~/.codex/sessions`, with per-session provenance recorded in `_types_codex_source`. It keeps one canonical `codex` cell and deliberately avoids broad filesystem crawling.

**Shared Coding-Agent Views**

Claude Code and Codex now expose shared `acp_sessions`, `acp_events`, and `acp_category_coverage` views, so different runtimes are inspected through one vocabulary. Each runtime's `@orient` is more self-contained — an agent can read the storage model and evidence path directly from the cell instead of needing external documentation.

**Source Recovery Presets**

Added shared `@full`, `@observed-file`, and `@file-history` presets for session cells. These solve the case where search finds a clipped `chunks.content` row while the full body lives in `messages.file_body`. `@observed-file` and `@file-history` also treat terminal reads such as `sed`, `cat`, and `rg` as first-class source observations, not only explicit file edits, making artifact recovery direct.

**Orient Instruction Mounts**

Added `_flex_docs`, a read-time temp table that lets `@orient` surface packaged cell instructions and optional local notes. Docs load from controlled Markdown paths — not indexed into the cell, not exposed as arbitrary filesystem reads. A static `instructions` cell gives agents a fallback query when packaged skills are unavailable: `{"cell":"instructions","query":"@orient"}`.

**MCP-First Agent Surface**

The overloaded CLI skill was replaced by a clearer split: a public `flex` retrieval skill plus session-specific `flex-sessions-claudecode` and `flex-sessions-codex` skills. Agent guidance now points at MCP search first, with the tool description explaining the four retrieval modes — SQL, presets, `keyword()`, and `vec_ops()`. Raw diagnostics live under `flex core`; top-level `flex search` is intentionally redirected there.

**Focused Discovery And Health**

Default MCP discovery is smaller: active listed sources show by default, active unlisted sources stay queryable by exact name, and inactive sources stay unavailable. `flex status`, `flex status --problems`, and `flex health` report local source health. Worker retry on repeatedly-failing unchanged session files is quieter.

**Runtime And Install Reliability**

`flex core sync` repairs supervised Linux service installs instead of replacing them with unmanaged background processes, and restarts respect systemd-owned installs. Local worker services and remote refresh scheduling are separated, so a service restart no longer triggers unrelated refresh work. Public install paths exist for `claude-code`, `codex`, and `obsidian`.

## 0.10.0 — March 30, 2026

**Hybrid Retrieval**

`keyword()` gained scoped filtering, better natural-language handling, and rank normalization, so it composes with `vec_ops()` in one SQL statement for hybrid search. Install split into base Flex and source-specific setup paths, and local embedding via `flex-embed` replaced the previous cloud embedding path. Public delivery moved to a wheel-first install aligned around `getflex.dev`.

## 0.8.1 — March 13, 2026

**Unified Query Surface**

A single `chunks` view made content queryable across source types. `vec_ops()` tokens were renamed around retrieval behavior (`similar:`, `suppress:`, `centroid:`, `pool:`) and gained `decay:` for temporal score decay. File-body content — generated and edited files, split along Markdown and code structural boundaries — became queryable alongside session messages. Graph and similarity work got faster on larger sources, and embedding writes became safe under concurrent reads.

## 0.3.0 — March 2, 2026

**Keyword Search**

`keyword()` was added for full-text search alongside `vec_ops()`, combinable in one query, with `flex sync` checking and repairing the keyword index. The `curl | bash` installer was rewritten for interrupted downloads, reinstall, uninstall, and shell PATH setup, and detects and re-downloads corrupt model files. Flex stopped colliding with GNU flex, the lexer generator.

## 0.2.x — February 2026

**One-Line Install And Mac Support**

`curl -sSL https://getflex.dev/install.sh | bash` handles Python venvs, PATH, and the GNU flex name collision. `flex init` recovers from interrupted or corrupt model downloads and resumes instead of restarting; `flex sync` repairs broken installs. Flex runs on macOS, supports multiple simultaneous Claude Code sessions, and on systems without a service manager the MCP server can handle indexing itself. Plain-text `flex search` input returns suggestions instead of a raw SQL error.

