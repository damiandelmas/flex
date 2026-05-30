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

## 0.20.0 — April 29, 2026

**Published To PyPI**

flex became installable from PyPI as `getflex`, published through GitHub Actions Trusted Publishing from the public repository. The `getflex.dev` installer resolves the published wheel and checksum, so `pip install getflex` and `curl -sSL https://getflex.dev/install.sh | bash` both serve the released version.

## 0.10.0 — April 3, 2026

**Flex SDK**

Introduced the SDK for building a cell from any source without writing view SQL or importing module internals. `index()` indexes a text list or folder in one line; the structured path — `create`, `source`, `ingest`, `link`, `embed`, `graph`, `register` — adds typed metadata, tree edges, and graph intelligence. `create()` reuses an existing cell path instead of orphaning databases, and `register()` carries lifecycle and refresh controls (`static` / `refresh` / `watch`).

**Lifecycle And Status**

Cell lifecycle moved into the registry as a single control plane. `flex status` reports lifecycle, last refresh, and state across cells.

**Hybrid Retrieval Hardening**

`keyword()` gained a scoped pre-filter so BM25 ranks within a subset instead of the global index, plus FTS5 sanitization for natural-language input and rank normalization that makes `keyword()`/`vec_ops()` score fusion meaningful.

**Install Paths**

Install split into base flex (`flex index` ready) and a full `claude-code` pipeline (session scanning, worker, services, MCP). Wheels are hosted on `getflex.dev`.

## 0.9.0 — March 14, 2026

**Indexing And Scale**

Added file-body indexing — content from Write, Read, and Edit tool results is chunked by language and embedded alongside session messages. Session parsing became fork-aware, code files split along structural boundaries via tree-sitter, and graph builds scaled to large cells through FAISS nearest-neighbor search with NetworKit graph algorithms.

## 0.8.0 — March 13, 2026

**Public Release**

The public repository was released under MIT with public install artifacts.

**Unified Query Surface**

A single `chunks` view made content queryable across source types, with `type` as a column rather than a separate view per substrate. `vec_ops()` tokens were renamed around retrieval behavior (`similar:`, `suppress:`, `centroid:`, `pool:`) and gained `decay:` for temporal score decay.

## 0.7.0 — March 10, 2026

**arXiv Source**

arXiv became the first non-conversation source — searchable papers with research-oriented views, proving the cell shape generalizes beyond coding-agent sessions.

## 0.5.0 — March 7, 2026

**Scoring Engine And Worker**

The scoring engine moved to a compiled implementation while keeping query compatibility. Session indexing moved to size-based polling, which reliably captures sub-agent sessions and any still-growing session instead of marking partial syncs done.
