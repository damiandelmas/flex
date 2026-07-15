# Changelog

Public changes to **flex** ([getflex.dev](https://getflex.dev)).

---

## 0.52.0 — July 14, 2026

This release makes a cell's declared contracts executable: one reproducible
embedding space, committed freshness receipts, scale-safe evidence recovery,
and query surfaces that expose ambiguity or unavailability instead of guessing.

### Changes

- **One portable embedding space** — newly embedded cells use the official
  Nomic v1.5 fp32 ONNX model at native 768-dimensional storage and per-cell
  Matryoshka serving at 256 dimensions. `flex init` downloads the ~522 MiB
  artifact and tokenizer from a revision-pinned upstream URL, verifies both by
  SHA-256, and installs them atomically. Existing pre-0.52 cells remain pinned
  to their retained legacy model until `flex reembed` safely migrates them by
  copy, verification, and atomic swap; Flex never silently interprets stored
  vectors as a different space.
- **Production-scale coding-agent recovery** — Claude Code and Codex gain an
  indexed observation projection for `@full`, observed-file, path-history, and
  bounded session-tail reads. Structural recovery no longer warms multi-GB
  vector matrices. Exact duplicate vectors are collapsed before MMR selection.
- **Freshness is a committed fact** — multi-root event watches and fair,
  size+mtime reconciliation cover every declared selection. Registry health now
  distinguishes dispatch, committed generations, source high-water marks, and
  pending reconciliation instead of treating activity as completion.
- **Honest code graphs** — same-named callees are labeled candidate sets rather
  than fabricated certain edges; callers can qualify by definition or file.
  Structural subtree reads use one-row node tables and avoid view fan-out.
- **Profile-owned document surfaces** — markdown, Obsidian, and structured-doc
  profiles own their views, presets, and refresh behavior. Malformed dates are
  rejected into queryable health defects. Obsidian cells expose notes, tags,
  Dataview fields, wikilinks, hubs, orphans, and ghost-note presets.
- **Fail-loud Hub transfers** — a named-cell miss, incomplete object-store
  credentials, provenance mismatch, or non-public bucket without an explicit
  base URL now aborts instead of reporting a successful no-op or publishing a
  manifest URL that cannot resolve. Registry `origin` records whether a cell is
  public or private rather than inferring provenance from its URL.
- **Retrieval scores keep their meaning** — MMR uses diversity only to select
  candidates and publishes each result's true relevance. Exact duplicate
  vectors collapse before selection, decay follows its documented exponential
  half-life, and natural-language FTS input is sanitized consistently.
- **Structural invariants heal forward** — edge tables enforce duplicate-safe
  uniqueness, chunk rollups and presentation views are regenerated from shared
  builders, and refresh paths upgrade compatible older schemas before writing.
- **Interface closure** — active unlisted cells remain hidden from discovery but
  are callable by exact MCP name; retired cell names refuse rebuild advice.
  Filesystem orientation labels raw versus projected counts, reports full roots
  and freshness, handles punctuation as FTS token boundaries, and uses uniform
  JSON success envelopes.

## 0.51.0 — July 2026

flex compiles any folder, repository, or document tree into a portable, queryable local **cell** — and now
keeps it always current. This release brings the filesystem and code-graph compilers, a hub for pulling and
publishing cells, and a per-file incremental engine that retires the old full-tree regen. Identity, capture,
and serving are hardened underneath.

### What's New

#### flex filesystem — a no-embed compiler for any folder tree

Point flex at any folders and get one SQLite cell you query by FTS5 and structural SQL — a recursive node
tree (`module ⊃ section`), path/section navigation — with **no embeddings** (free, fast, watched by
default). It compiles in seconds and every query is plain SQL or a preset, not an approximate-nearest-
neighbor lookup. `--embed` builds a vector cell over the same folders when you want semantic retrieval. The
payoff is the **SOMA `file_uuid` hinge**: every file carries a machine-global identity, so a filesystem cell
joins to the coding-agent session cells on that identity — "what a file says" next to "who touched it."

#### flex codegraph — a repository as a queryable graph

The code profile compiles a repository into one cell as a navigable graph: a symbol tree
(`module ⊃ class ⊃ method`) with a **call graph** and an **import graph**. Ask who calls a symbol
(`@callers`), what it calls (`@callees`), the transitive blast radius of a change (`@impact`), or a file's
subtree (`@subtree`) — in plain SQL, exact not fuzzy. Python (`ast`) and JS/TS (`tree-sitter`). No
embeddings, no running daemon.

#### flex hub — pull and publish prebuilt cells

`flex hub pull <cell>` fetches a prebuilt, checksum-verified cell and registers it — search runs entirely on
your machine against the local file, no ongoing network dependency. `flex hub view` lists what's available;
`flex hub status` shows connection and publish state; `flex hub push` (optional) publishes your own.

#### One compile path for markdown, Obsidian, and structured docs

One auto-routing pipeline replaces three parallel implementations; structured-doc cells gain a
`category × subtype` coordinate and section-grain retrieval, and a corpus can opt into declarative chunking
per folder or file type (`.flexchunk.json`), lossless by default.

#### Always-current cells — incremental refresh, no regen burn

Filesystem and code cells re-index **per file** instead of regenerating the whole tree on a timer. The
periodic reconcile walk that guarantees correctness prunes `.git`/`node_modules`/`venv`/build dirs at
traversal (682ms → 43ms on a large markdown tree; 698ms → 7.5ms on a code repo) and runs as a cheap
correctness floor (~0.76% CPU) with delete-authority — a removed file's rows are pruned. The full-tree
regen and its CPU burn are retired; `last_refresh` now reports real index activity.

### Changes

- **Event-driven capture replaces the poll loop** — the Claude Code worker wakes on filesystem events
  (watchdog/inotify) instead of a fixed two-second timer, with a 60-second reconciliation backstop,
  automatic fallback to polling, and a per-tick time budget on the embedding sweep.
- **Capture daemon reliability** — fixed a crash-loop where a test fixture could kill live capture/MCP
  services; enrichment cadence persists across restarts behind a startup grace; `flex init` no longer
  bounces the daemon on a lightweight `--regen`/`--path`/`--remove`; service kills are scoped to this
  `FLEX_HOME`.
- **SOMA file identity — hardened, and unified across every build path** — read-path identity resolution no
  longer takes a write lock (fixing stalls under load), stamping batches through a single writer, heal skips
  identity collisions instead of clobbering. `_edges_fs_identity` (one row per file) is now minted by all
  three filesystem build paths through one shared pattern, with a single-sourced exclusion for ephemeral
  corpora and delete-time pruning; `flex soma backfill-identity` stamps existing cells (and creates the
  table on cells that predate it) without a rebuild — verified full coverage across all filesystem cells.
- **Deep-heading content no longer lost** — heading-tree slots are sized to a document's actual maximum
  depth instead of a fixed cap, closing a content-loss class on deeply nested documents.
- **Code cells: multi-directory + build-output pruned** — a code cell indexes multiple directories from its
  stored selections, and the walk prunes build output (`out/`, `_next/`, `.output/`, `target/`, `*.min.js`)
  so a repo cell indexes roughly its tracked source.
- **Embed-off cells tell the truth** — a structural (no-embed) cell serves its own lean `@orient` describing
  its real, discovered columns (not a template referencing vector operators it lacks), skips vector-cache
  warmup it does not need, and mounts structural instructions. `@orient` is an embed-aware, per-cell contract.
- **Presentation views self-heal**, and **cell-shipped presets survive a rebuild** (`.flexpresets.json` is
  read, cascaded, and re-inserted on regen).
- **Lower memory, more headroom** — memory-mapped vector matrices (`FLEX_VEC_MEMMAP`, reclaimable without
  swap) and configurable ONNX inference threads.
- **flex-mcp steadier under load** — independent HTTP-connection and query-execution limits, vector-cache
  refresh moved off the query thread (serve-stale + background rebuild + atomic swap), and `flex warm` for
  eager-vs-lazy cell loading.
- **Retrieval honesty and freshness** — keyword search reports terms it couldn't match, semantic search
  exposes a result's distance above its pool's noise floor, undated docs get an effective date backfilled
  from git/filesystem, and cells declaring a refresh module get auto-refresh wired up.
- **Retired the experimental multi-model reembed apparatus** — the `flex reembed` command and the
  model-registry path are removed from the public surface; cells build against a single default embedder.

*Consolidation note: the filesystem engines are unified at the compile/index layer; the user-facing install
verbs (`filesystem` / `codegraph`) route to them but are not yet collapsed into a single routing surface —
that consolidation is in progress.*

## 0.40.0 — June 2026

### Changes

#### Shell Operations Become File History

Files created or modified through shell commands — `cat > file`, `tee`, heredocs — now appear in `@file` and `@file-provenance` lineage alongside structured edits. The detector rejects shell syntax that only looks like a path, so inferred operations stay clean. Provenance answers "where did this file come from" even when no editor ever touched it.

#### Recovery Presets

`@full id=...` climbs from a clipped search hit to the exact full source body. `@observed-file path=...` and `@file-history path=...` treat terminal reads — `sed`, `cat`, `rg` — as first-class file observations, returning ordered timelines for any path fragment. Search results are clues; these presets are the evidence.

#### Every Cell Teaches Its Own Retrieval

All shipping modules now carry a full retrieval manual mounted into `@orient`: the three-phase query pipeline (SQL pre-filter → vector scoring → SQL composition), modulation tokens, exact-term vs semantic guidance, and runnable examples written against that cell's real columns and presets. `@orient` itself is never truncated — orientation arrives whole.

#### Four New Sources (Beta Tier)

`flex init --module reddit | hn | github | arxiv` — subreddit-scoped Reddit via Arctic Shift (no auth), Hacker News via Algolia (no auth), GitHub Issues (works unauthenticated with honest rate-limit warnings; `GITHUB_TOKEN` for 5000 req/h), and arXiv (no auth, rate-respectful defaults). Beta tier: CI-tested, issue-driven fixes.

#### flex-labs: The Public Workshop

Experimental modules and skills now live at [github.com/damiandelmas/flex-labs](https://github.com/damiandelmas/flex-labs) — installable over a stable flex via `~/.flex/modules/`. Day one: devto and lobsters source modules, aider and opencode coding-agent memory adapters, and three skills (flex-topology, flex-archeology, flex-digest). Labs modules graduate to the wheel through field reports plus a cold-start test matrix.

#### Multi-Runtime Memory, Honest Labels

Codex cells read declared `.codex` homes beyond the global default, with per-session source provenance. Shared ACP views (`acp_sessions`, `acp_events`, `acp_category_coverage`) now label each row with its true provider. Goose cells preserve provider/model/mode metadata.

#### Memory Footprint Fixed

The MCP server's vector cache now appends incrementally instead of rebuilding, under an LRU byte budget with a hard service memory cap. Long-running servers no longer ratchet from gigabytes to tens of gigabytes. Documentation/context cells skip unchanged files by content hash, so daemon restarts stop re-embedding entire corpora.

#### Cold-Start Test Matrix

Every public module's documented setup path now runs in CI from a fresh install against real APIs — including the documented refusal behaviors. The matrix caught and fixed real bugs before this release shipped them.

## 0.30.0 — May 27, 2026

### Changes

#### Codex As A First-Class Source

Codex sessions index alongside Claude Code through one query surface: session history, prompts, assistant turns, tool calls, file evidence, and stock presets. Codex refresh scans multiple declared Codex homes instead of only the global `~/.codex/sessions`, with per-session provenance recorded in `_types_codex_source`. It keeps one canonical `codex` cell and deliberately avoids broad filesystem crawling.

#### Shared Coding-Agent Views

Claude Code and Codex now expose shared `acp_sessions`, `acp_events`, and `acp_category_coverage` views, so different runtimes are inspected through one vocabulary. Each runtime's `@orient` is more self-contained — an agent can read the storage model and evidence path directly from the cell instead of needing external documentation.

#### Source Recovery Presets

Added shared `@full`, `@observed-file`, and `@file-history` presets for session cells. These solve the case where search finds a clipped `chunks.content` row while the full body lives in `messages.file_body`. `@observed-file` and `@file-history` also treat terminal reads such as `sed`, `cat`, and `rg` as first-class source observations, not only explicit file edits, making artifact recovery direct.

#### Orient Instruction Mounts

Added `_flex_docs`, a read-time temp table that lets `@orient` surface packaged cell instructions and optional local notes. Docs load from controlled Markdown paths — not indexed into the cell, not exposed as arbitrary filesystem reads. A static `instructions` cell gives agents a fallback query when packaged skills are unavailable: `{"cell":"instructions","query":"@orient"}`.

#### MCP-First Agent Surface

The overloaded CLI skill was replaced by a clearer split: a public `flex` retrieval skill plus session-specific `flex-sessions-claudecode` and `flex-sessions-codex` skills. Agent guidance now points at MCP search first, with the tool description explaining the four retrieval modes — SQL, presets, `keyword()`, and `vec_ops()`. Raw diagnostics live under `flex core`; top-level `flex search` is intentionally redirected there.

#### Focused Discovery And Health

Default MCP discovery is smaller: active listed sources show by default, active unlisted sources stay queryable by exact name, and inactive sources stay unavailable. `flex status`, `flex status --problems`, and `flex health` report local source health. Worker retry on repeatedly-failing unchanged session files is quieter.

## 0.20.0 — April 29, 2026

### Changes

#### Published To PyPI

flex became installable from PyPI as `getflex`, published through GitHub Actions Trusted Publishing from the public repository. The `getflex.dev` installer resolves the published wheel and checksum, so `pip install getflex` and `curl -sSL https://getflex.dev/install.sh | bash` both serve the released version.

## 0.10.0 — April 3, 2026

### Changes

#### Flex SDK

Introduced the SDK for building a cell from any source without writing view SQL or importing module internals. `index()` indexes a text list or folder in one line; the structured path — `create`, `source`, `ingest`, `link`, `embed`, `graph`, `register` — adds typed metadata, tree edges, and graph intelligence. `create()` reuses an existing cell path instead of orphaning databases, and `register()` carries lifecycle and refresh controls (`static` / `refresh` / `watch`).

#### Lifecycle And Status

Cell lifecycle moved into the registry as a single control plane. `flex status` reports lifecycle, last refresh, and state across cells.

#### Hybrid Retrieval Hardening

`keyword()` gained a scoped pre-filter so BM25 ranks within a subset instead of the global index, plus FTS5 sanitization for natural-language input and rank normalization that makes `keyword()`/`vec_ops()` score fusion meaningful.

#### Install Paths

Install split into base flex (`flex index` ready) and a full `claude-code` pipeline (session scanning, worker, services, MCP). Wheels are hosted on `getflex.dev`.

## 0.9.0 — March 14, 2026

### Changes

#### Indexing And Scale

Added file-body indexing — content from Write, Read, and Edit tool results is chunked by language and embedded alongside session messages. Session parsing became fork-aware, code files split along structural boundaries via tree-sitter, and graph builds scaled to large cells through FAISS nearest-neighbor search with NetworKit graph algorithms.

## 0.8.0 — March 13, 2026

### Changes

#### Public Release

The public repository was released under MIT with public install artifacts.

#### Unified Query Surface

A single `chunks` view made content queryable across source types, with `type` as a column rather than a separate view per substrate. `vec_ops()` tokens were renamed around retrieval behavior (`similar:`, `suppress:`, `centroid:`, `pool:`) and gained `decay:` for temporal score decay.

## 0.7.0 — March 10, 2026

### Changes

#### arXiv Source

arXiv became the first non-conversation source — searchable papers with research-oriented views, proving the cell shape generalizes beyond coding-agent sessions.

## 0.5.0 — March 7, 2026

### Changes

#### Scoring Engine And Worker

The scoring engine moved to a compiled implementation while keeping query compatibility. Session indexing moved to size-based polling, which reliably captures sub-agent sessions and any still-growing session instead of marking partial syncs done.
