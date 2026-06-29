---
name: flex:dev:modules
description: Flex module system — how modules declare themselves, the MODULE spec dict, worker patterns, transpilers, install flow, substrate types, and the watch/refresh lifecycle. Load when creating a new module, modifying an existing module's worker or install, or working with the substrate extraction layer.
user-invocable: false
---

# flex:dev:modules

Reference for the flex module system. Load this before creating or modifying
modules under `flex/modules/`, working with MODULE specs, transpilers,
install flows, or the substrate layer.

## Module Directory Structure

Every module lives under `flex/modules/<name>/` with a standard layout:

```
modules/<name>/
├── __init__.py              # Usually empty or exports constants
├── install.py               # MODULE dict + register_args() + run()
├── compile/
│   ├── __init__.py
│   └── worker.py            # transpile() and/or scan functions
└── stock/
    ├── instructions.md      # Cell-bundled instructions for agents
    ├── views/*.sql           # SQL view definitions
    └── presets/*.sql          # Named query presets
```

Optional files: `contract.py` (validation), `refresh.py` (remote refresh),
`README.md` (module docs).

## Current Modules (v0.40.0+)

| Module | cell_type | substrate | Pattern | Purpose |
|--------|-----------|-----------|---------|---------|
| `claude_code` | `claude_code` | — | Scanner + daemon | CC session ingestion, master daemon loop |
| `codex` | `codex` | `claude_code` | Transpiler | Codex rollout JSONL → CC schema |
| `goose` | `goose` | `claude_code` | Transpiler | Goose SQLite → CC schema |
| `markdown` | `markdown` | — | Scanner | Obsidian/vault markdown files |
| `arxiv` | `arxiv` | — | Transpiler | arXiv papers via API |
| `github` | `github` | — | Transpiler | GitHub issues/discussions |
| `hn` | `hn` | — | Transpiler | Hacker News threads |
| `reddit` | `reddit` | — | Transpiler | Reddit threads via Arctic Shift |
| `skills` | `skills` | — | Transpiler | Public AI skill/tool catalog |
| `soma` | — | — | Library | Identity module (no install) |

## The MODULE Spec Dict

Each module's `install.py` exports a `MODULE` dict. This is the module's
self-declaration — discovered by `registry.py:discover_module_specs()`.

### Required Fields

```python
MODULE = {
    "cell_type": "mymodule",        # unique identifier, used in registry
}
```

### Common Fields

```python
MODULE = {
    "cell_type": "codex",
    "substrate": "claude_code",                           # schema inheritance
    "default_cell_name": "codex",                         # registry name
    "transpile": "flex.modules.codex.compile.worker:transpile",  # dotted ref
    "signature": "flex.modules.codex.compile.worker:compute_dir_signature",
    "refresh_module": "flex.modules.codex.refresh",       # for lifecycle=refresh
    "watch_path": "/path/to/watch",                       # for lifecycle=watch
    "watch_pattern": "**/*.jsonl",                        # glob pattern
    "views_from": ("claude_code",),                       # reuse views
    "presets_from": ("claude_code", "soma"),               # reuse presets
    "enrichment_stubs_from": "claude_code",               # reuse enrichment
    "default_source": {"path": "...", "type": "dir"},     # install source
}
```

### Substrate Field

The `substrate` field controls which schema tables are created at install time.

| Value | Effect |
|-------|--------|
| `"base"` | Base tables only (chunks, sources, metadata, cells, fts) |
| `"claude_code"` | Base + CC tables (messages, sessions, files, types, tools) + SOMA |
| _(absent)_ | Module handles its own schema |

Substrate extraction (`contract.py`) defines:
- `REQUIRED_BASE_TABLES`: chunks, sources, source_graph, chunk_metadata, cells
- `REQUIRED_TABLES`: base + messages, sessions, files (coding-agent cells)
- `validate_base_cell(conn)` / `validate_coding_agent_cell(conn)` → `ContractReport`

## install.py Pattern

Every module's `install.py` exports three things:

```python
MODULE = { ... }  # spec dict

def register_args(sub):
    """Register CLI subcommand arguments."""
    sub.add_argument("source", nargs="?", help="Source path or URL")
    sub.add_argument("--name", help="Cell name override")
    # ... module-specific args

def run(args, console):
    """Install entry point. Called by `flex install <module>`."""
    from flex.modules.claude_code.coding_agent_install import run_from_spec
    run_from_spec(args, console, MODULE)
```

Most modules delegate to `run_from_spec()` which handles:
1. Cell creation with correct substrate schema
2. Transpiler loading via `_load_ref(spec["transpile"])`
3. Transpile → embed → enrichment pipeline
4. Registry registration with lifecycle kwargs

## Worker Patterns

### Pattern A: Transpiler (codex, goose, arxiv, github, hn, reddit, skills)

`compile/worker.py` exports a `transpile(source_path, conn)` function.
Called on-demand by `run_from_spec()` or `refresh_cell()`. One-shot processing.

```python
def transpile(source_path: str | Path, conn: sqlite3.Connection,
              progress=None, encode_fn=None) -> dict:
    """Transpile source data into cell chunks.
    
    Returns:
        dict with stats like {'sessions': N, 'chunks': N, 'skipped': N}
    """
```

Transpilers read external data (JSONLs, SQLite, APIs), map it to the cell's
chunk schema, and insert via `insert_base_chunk()` or `insert_chunk_atom()`.

### Pattern B: Scanner (claude_code, markdown)

`compile/worker.py` exports a daemon-callable scan function that maintains
in-memory caches and is called every tick from the daemon loop.

```python
def scan_sessions(conn, size_cache, error_cache=None) -> dict:
    """Stat-based polling scan. Returns {'synced': N, 'chunks': N}."""

def scan_markdown_cells() -> dict:
    """Walk vault directories, sync changed files. Returns {'indexed': N}."""
```

Scanners own their caches (`size_cache`, `_hash_cache`) and are designed
for repeated invocation.

### claude_code Is Both

The `claude_code` module is unique — it contains the master `daemon_loop()`
AND is a scanner for its own cell type. All other scanners and transpilers
are called from within `claude_code`'s daemon loop.

## Cell Lifecycle

Modules participate in the daemon via lifecycle registration:

| Lifecycle | How the daemon handles it |
|-----------|--------------------------|
| `static` | Ignored by daemon — install-time only |
| `watch` | `discover_watched()` returns the cell; daemon monitors `watch_path` for changes via inotify or polling |
| `refresh` | `run_due_refreshes()` checks `refresh_interval` and calls `refresh_module` when due |

### Watch Lifecycle (Event-Driven)

When a cell has `lifecycle='watch'`:
1. `discover_watched()` finds it in the registry
2. `daemon.py:_build_watcher()` registers its `watch_path` + `watch_pattern` with `FlexWatcher`
3. inotify detects file changes → `drain()` returns paths → daemon syncs
4. Integrity scan (60s) catches anything inotify missed

### Refresh Lifecycle (Timer-Driven)

When a cell has `lifecycle='refresh'`:
1. `flex/refresh.py:run_due_refreshes()` checks if `refresh_interval` has elapsed
2. Loads `refresh_module` → calls its `refresh(cell_name)` function
3. Updates `last_refresh_at` and `refresh_status` in registry

## Key Functions for Module Authors

### Schema Bootstrap

| Function | File | Purpose |
|----------|------|---------|
| `_ensure_core_tables(conn)` | worker.py | CC-specific tables |
| `_ensure_content_tables(conn)` | worker.py | Content/FTS tables |
| `bootstrap_cell(name, cell_type, substrate)` | coding_agent_install.py | Create cell with substrate-appropriate schema |

### Chunk Insertion

| Function | File | Purpose |
|----------|------|---------|
| `insert_base_chunk(conn, ...)` | worker.py | Insert into base schema (any substrate) |
| `insert_chunk_atom(conn, ...)` | worker.py | Insert into full CC schema (messages, sessions, files) |
| `sync_session_messages(session_id, conn)` | worker.py | Sync a CC JSONL file into chunks |

### Discovery

| Function | File | Purpose |
|----------|------|---------|
| `discover_module_specs()` | specs.py | Load all MODULE dicts |
| `module_spec_for(cell_type)` | specs.py | Get spec for a cell type |
| `discover_install_modules()` | specs.py | Find all installable modules |
| `discover_watched()` | registry.py | Cells with watch lifecycle |

## Module Search Paths

Modules are discovered from three locations (in order):

1. **Packaged:** `flex/modules/*/install.py` (in the repo)
2. **User-installed:** `~/.flex/modules/*/install.py`
3. **FLEX_MODULE_PATH:** Additional directories (colon-separated env var)

## Creating a New Module

1. Create `flex/modules/<name>/` with the standard layout
2. Define `MODULE` dict in `install.py` with at minimum `cell_type`
3. Set `substrate` if inheriting schema (e.g. `"claude_code"` or `"base"`)
4. Implement `compile/worker.py` with `transpile()` function
5. Add `stock/views/*.sql` for queryable surfaces
6. Add `stock/instructions.md` for agent-facing documentation
7. Delegate `run()` to `run_from_spec(args, console, MODULE)`
8. For watch cells: set `watch_path`, `watch_pattern` in MODULE dict
9. For refresh cells: implement `refresh.py` with `refresh(cell_name)` function

## File Map

```
flex/modules/
├── claude_code/
│   ├── __init__.py          # BASE_ENRICHMENT_STUBS, ENRICHMENT_STUBS
│   ├── contract.py          # Schema validation (base vs full)
│   ├── coding_agent_install.py  # run_from_spec() — generic install pipeline
│   ├── coding_agent_watch.py    # scan_coding_agent_cells() for watch lifecycle
│   └── compile/
│       ├── worker.py        # daemon_loop(), scan_sessions(), sync, embed
│       └── soft_detect.py   # Session type detection heuristics
├── codex/                   # Codex rollout ingestion
├── goose/                   # Goose session ingestion
├── markdown/                # Vault/markdown ingestion
├── arxiv/                   # arXiv paper ingestion
├── github/                  # GitHub issue/discussion ingestion
├── hn/                      # Hacker News thread ingestion
├── reddit/                  # Reddit thread ingestion
├── skills/                  # Public skill/tool catalog
├── soma/                    # Identity module (library, no install)
└── specs.py                 # Module discovery + spec resolution
```
