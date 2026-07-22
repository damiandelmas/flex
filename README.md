<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

**Composable retrieval for AI agents.**

flex compiles coding-agent sessions, folders, repositories, and supported data
sources into self-describing SQLite **cells**. An agent can inspect a cell, narrow
its data with SQL, search it by keywords or meaning, and follow results back to
source evidence — all through one MCP tool.

A cell is a portable knowledge artifact, not a pile of flattened chunks. It keeps
the schema, relationships, identity, provenance, and retrieval operations that its
source can support. flex runs locally by default; a hosted retrieval service is not
required.

## quick start

Install flex for Claude Code:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code
```

Or for Codex CLI:

```bash
curl -sSL https://getflex.dev/install.sh | bash -s -- codex
```

The installer creates a cell from your existing session history, configures the
MCP server, and starts background refresh. Start a new client session so it loads
the new MCP configuration, then ask:

```text
Use flex to orient me to my coding history, then find the decisions that shaped
this project in the last week.
```

The agent's first tool call should be the cell's own orientation preset:

```json
{"cell":"claude_code","query":"@orient"}
```

Use `codex` instead of `claude_code` when you installed the Codex module.

To verify the installation from the shell:

```bash
flex health --json
flex core search --cell claude_code "@orient"
```

### let your coding agent install it

Paste this into Claude Code or Codex CLI:

```text
Install flex for this client. Use the installer at https://getflex.dev: pass
`claude-code` when this is Claude Code or `codex` when this is Codex CLI.
Preserve existing flex cells and configuration. Treat any non-zero command or
unhealthy service as a blocking error. Run `flex health --json`, inspect the JSON,
and require `status` to be `ok`; the command can report degraded state without a
non-zero exit. Then verify the query path with `flex core search --cell CELL
"@orient"`. MCP configuration is loaded at client startup, so do not claim the
flex MCP tool is available in this running session; tell me to start a new session
and give me the exact first prompt to run.
```

## compile a folder

Turn a mixed folder or repository into one watched cell:

```bash
flex init --module filesystem --path /path/to/folder
```

The filesystem compiler understands Markdown headings and metadata, Python and
JS/TS symbols, calls and imports, and readable text. These coexist in one cell and
remain distinguishable by file kind and source path.

```bash
# Add Obsidian aliases, tags, Dataview fields, and wikilinks to Markdown files
flex init --module filesystem --path /path/to/vault --obsidian

# Keep the same source structure but omit semantic vectors
flex init --module filesystem --path /path/to/folder --no-embed

# Build once instead of watching the folder
flex init --module filesystem --path /path/to/folder --no-watch
```

Embeddings are enabled by default. New embedded cells use the revision-pinned
Nomic v1.5 fp32 model locally, with 768-dimensional storage and a 256-dimensional
Matryoshka query surface. No embedding API key is required.

Refresh is atomic per file: a failed or unreadable update preserves the last good
indexed version. Filesystem events provide low-latency updates; reconciliation
catches missed events, restarts, deletions, and edits made while flex was stopped.

After creating a cell, start here:

```bash
flex core search --cell CELL "@orient"
```

`@orient` reports the cell's root, source types, schema, embedding mode, freshness,
presets, and runnable query examples. Trust it over generic documentation: it is
generated for the cell you are actually querying.

## what flex makes possible

### coding history as a database

Claude Code and Codex sessions retain prompts, replies, tool calls, file reads and
edits, repositories, projects, and sub-agent traces. flex indexes history that
already exists and keeps it current as you work.

Ask questions such as:

```text
Use flex to find the history of worker.py. Which sessions read or changed it,
what decisions were made, and where is the full source evidence?
```

```text
Use flex to reconstruct why registry.py was introduced. Separate proposed ideas,
failed approaches, and the implementation that shipped.
```

```text
Use flex to summarize what we built this week, grouped by project and touched
files. Recover the source turns for every major claim.
```

Search results are entry points, not the final evidence. Session cells ship
presets such as `@full`, `@observed-file`, and `@file-history` for moving from a
matching chunk to the complete turn, file observation, or ordered lineage.

### one folder, several retrieval modes

A filesystem cell can answer structural and semantic questions without forcing
every problem through vector similarity.

Exact terms and identifiers:

```sql
SELECT k.snippet, c.source_id, c.section_title
FROM keyword('refresh failure') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC
LIMIT 12
```

Semantic retrieval with source fields:

```sql
SELECT v.score, c.source_id, c.section_title, c.content
FROM vec_ops('durable reconciliation after restart') v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC
LIMIT 12
```

Ordinary structural SQL:

```sql
SELECT file_kind,
       count(DISTINCT source_id) AS files,
       count(*) AS chunks
FROM chunks
GROUP BY file_kind
ORDER BY files DESC
```

Code-aware cells also expose presets for callers, callees, imports, symbol
subtrees, and impact analysis. Obsidian-enabled cells add notes, tags, aliases,
wikilinks, hubs, orphans, and ghost targets without narrowing the rest of the
folder to Markdown.

## retrieval is a composition

Every retrieval query has three possible stages:

```text
SQL pre-filter  ->  keyword / semantic scoring  ->  SQL composition
```

1. **Pre-filter** the candidate set by date, source, type, project, path, or any
   other SQL predicate.
2. **Search** the eligible rows with full-text, semantic, or hybrid retrieval.
3. **Compose** results with the rest of the cell: join source records, group by
   session, recover files, apply graph features, or rerank.

The important boundary is that filtering happens before scoring. An agent can ask
for recent assistant turns from one repository and then search that set, instead
of retrieving globally and hoping a post-filter leaves useful results.

### programmatic vector retrieval

[flexvec](https://github.com/damiandelmas/flexvec) exposes the score array inside
SQLite so semantic retrieval can be shaped before rows are selected.

| token | operation |
|---|---|
| `similar:TEXT` | search toward a concept |
| `suppress:TEXT` | push a concept out of the result set |
| `diverse` | select across subtopics rather than near-duplicates |
| `decay:N` | favor recent content with an N-day half-life |
| `centroid:id1,id2` | search from the average of examples |
| `from:A to:B` | search along a conceptual direction |
| `pool:N` | control the scored candidate pool |

For example:

```sql
SELECT v.id, v.score, m.session_id, m.content
FROM vec_ops(
  'similar:system architecture
   diverse
   suppress:landing page copy',
  'SELECT id FROM messages WHERE type = ''assistant'''
) v
JOIN messages m ON m.id = v.id
ORDER BY v.score DESC
LIMIT 5
```

The diversity objective selects rows but does not replace their relevance scores;
published scores retain their similarity meaning.

## source modules

Stable sources:

| module | source |
|---|---|
| [`claude-code`](flex/modules/claude_code/README.md) | Claude Code sessions, tool calls, and file evidence |
| [`codex`](flex/modules/codex/README.md) | Codex CLI sessions and file evidence |
| [`filesystem`](flex/modules/fs) | mixed Markdown, code, and text folders; optional Obsidian semantics |
| [`tools`](flex/modules/skills/README.md) | skills, MCP servers, frameworks, and agent tools |

`instant`, `markdown`, `obsidian`, `codegraph`, and `code` remain compatibility
aliases for earlier narrow workflows. New folder workflows should use
`filesystem` with `--obsidian` or `--no-embed` where needed.

Beta sources:

| module | example |
|---|---|
| [`goose`](flex/modules/goose/README.md) | `flex init --module goose` |
| [`github`](flex/modules/github/README.md) | `flex init --module github --github-repos owner/repo` |
| [`reddit`](flex/modules/reddit/README.md) | `flex init --module reddit --subreddits ClaudeCode,LocalLLaMA --since 30d` |
| [`hn`](flex/modules/hn/README.md) | `flex init --module hn --hn-queries "claude code,mcp server"` |
| [`arxiv`](flex/modules/arxiv/README.md) | `flex init --module arxiv --arxiv-query "all:retrieval augmented generation"` |

Adding a supported source creates another cell behind the same query interface. It
does not add another MCP search tool.

## architecture

flex has one shape and a small set of conventions. Every source compiles into the
same kind of artifact — a **cell** — and an agent learns to query any cell by
reading its schema, not by learning a new API.

### cells

A cell is a portable SQLite database for one knowledge source: Claude Code
sessions, Codex sessions, a mixed folder, project history, or another structured
corpus. Source modules are adapters that read a source format and compile it into
the shared cell shape.

Cells share the same conventions at every level — chunks with edges, types,
enrichments, and views. A registry at `~/.flex/registry.db` catalogs them by UUID,
so names resolve to paths and renaming is a single update. Adding a new source adds
a new cell behind the same query surface — never a new search tool.

### the cell shape

One node type sits at the center: `_raw_chunks` — content, embedding, timestamp.
Everything else orbits it as tables keyed by `chunk_id`. A document is a grouping
edge. An enrichment is a score. A view composes them into a flat surface the agent
queries.

The table prefix is a lifecycle declaration:

```text
_raw_*      source-derived facts      owned by compile and refresh
_edges_*    relationships             derived from source facts
_types_*    classification            source or derived labels
_enrich_*   computed structure        safe to regenerate
(no prefix) views                     composed query surfaces
```

Reading a table name tells an agent what kind of data it is looking at.
`_enrich_*` is recomputable analytical structure; `_raw_*` is the compiler-owned
source record that enrichment must not overwrite.

### self-describing

A cell exposes its query contract through `sqlite_master`, `PRAGMA table_info`,
and a single entry point: `@orient` returns its shape, schema, views, presets, and
sample content in one call. Agents discover view columns instead of hardcoding
them, so a cell stays queryable as modules add tables and columns. The local
registry and service configuration locate and operate cells; they do not define a
cell's query schema.

### the lifecycle

Two write paths feed one read surface:

```text
compile / refresh  ─→  _raw_*, _edges_*, _types_*  ─┐
                                                     ├─→  views  ─→  agent queries SQL
enrich             ─→  _enrich_*  ──────────────────┘
```

Compile and refresh translate the source into facts, relationships, and source
types. Enrichment reads those facts and computes structure such as graphs,
fingerprints, and communities into `_enrich_*`. Views regenerate from the tables
that exist. Source content and derived interpretation stay visibly separate.

### extensions are tables

Table-level extensions add capabilities by creating convention-prefixed tables;
removing a recomputable extension removes those tables without changing the query
transport. SOMA and graph enrichments work in this sense — ordinary SQLite tables
and columns queried as SQL, never separate MCP tools. Source modules additionally
own compilation and lifecycle configuration because they connect a cell to an
external source.

### one interface, local-first

MCP is transport, not topology. The agent sees a single read-only tool —
`flex_search` against a named cell — and retrieval happens inside the cell (see
[retrieval is a composition](#retrieval-is-a-composition)). The `flex core` CLI is
for installation and operations: initialize sources, inspect cells, and diagnose
health. The durable artifact is the cell itself — one local SQLite file, normally
under `~/.flex/cells/`.

## local-first, optionally networked

Queries run against local SQLite files. The optional hub can distribute prebuilt,
checksum-verified cells:

```bash
flex hub view
flex hub pull CELL
flex hub status
```

A pulled cell remains locally queryable without an ongoing network dependency.
Publishing is explicit and provenance-checked; private local cells are not
implicitly published.

## upgrade older embedding cells

Cells created before 0.52 retain their original embedding model until explicitly
migrated. flex never infers a vector space from byte width or silently relabels
stored vectors.

Preview and run the migration:

```bash
flex reembed --dry-run
flex reembed
flex reembed --dry-run
flex health --json
```

For a non-empty cell, migration works on a copy while the original remains
available in its old vector space. The copy is swapped in only after database
integrity, vector width, and sampled embedding reproducibility checks pass. Its
backup is retained under `~/.flex/backups/reembed-nomic/`. An eligible empty cell
needs no vector rewrite and is stamped directly; already-migrated and
structural-only cells are skipped.

For larger local migrations, increase ONNX CPU parallelism explicitly:

```bash
FLEX_ONNX_THREADS=8 flex reembed
```

## operational commands

```bash
flex health --json                         # services, cells, refresh, watchers
flex status                               # cell lifecycle and freshness
flex core search --cell CELL "@orient"    # inspect a cell's live contract
flex sync --cell CELL                     # bring one cell into parity
flex warm                                 # inspect or manage the vector warm set
```

## benchmarks

Measured, not claimed. On [LongMemEval-S](https://arxiv.org/abs/2410.10813) (470
questions, session-level), scored with the paper's own evaluation code on the
identical question set — same tier, same ground truth, no cross-tier comparison:

| retriever | recall@5 | nDCG@10 |
|---|---|---|
| **flex hybrid** (`keyword() ∩ vec_ops()`) | **0.860** | **0.913** |
| paper's best (Contriever + LLM fact-expansion) | 0.774 | 0.837 |
| Contriever | 0.753 | 0.827 |
| BM25 | 0.638 | 0.708 |

flex's composed hybrid beats the paper's strongest published technique on its
own yardstick. On [BEIR](https://github.com/beir-cellar/beir), flexvec's
modulation properties pass 52/52 tests; on SciFact, `diverse` cuts intra-list
similarity 13% while keeping 93% of baseline nDCG@10. At the kernel level, a
composed query (pre-filter → score → rerank) over a 1M-chunk federated corpus
runs in 82ms with no ANN index — a pre-filtered 3K-chunk scope runs in 0.4ms.

Re-verified on the current default embedder (Nomic v1.5 fp32, 768d) after the
0.52 migration: hybrid retrieval holds at 0.857 recall@5 / 0.910 nDCG@10 — flat
versus the original measurement above, because keyword search already saturates
this benchmark's fact-lookup questions. The embedding upgrade shows up where it
matters for a weaker retriever: measured in isolation (vector similarity alone,
no keyword component), the same embedding swap moves recall@1 from 0.43 to 0.82
and nDCG@10 from 0.67 to 0.90 on identical questions. Full methodology, caveats,
and what remains unrun or CLAIM-only (not VERIFIED) is in the retrieval kernel
paper.

## paper

The retrieval kernel is described in:

[flexvec: SQL Vector Retrieval with Programmatic Embedding Modulation](https://arxiv.org/abs/2603.22587)

## license

MIT · Python 3.12+ · SQLite · [getflex.dev](https://getflex.dev) ·
[GitHub](https://github.com/damiandelmas/flex) ·
[paper](https://arxiv.org/abs/2603.22587)
