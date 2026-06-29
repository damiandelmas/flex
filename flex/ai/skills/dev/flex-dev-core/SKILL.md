---
name: flex:dev:core
description: Flex daemon architecture, event-driven watcher, registry, engine internals. Load when modifying the daemon loop, watcher, cell lifecycle, registry, or refresh system. Not for module-level work — use flex:dev:modules for that.
user-invocable: false
---

# flex:dev:core

Reference for the flex daemon runtime, registry, and engine internals.
Load this before touching daemon.py, watcher.py, registry.py, engine.py,
refresh.py, or the systemd integration.

## Daemon Architecture

The flex daemon (`flex/daemon.py`) is the local capture/watch process. Entry
points:

| Path | Command | What runs |
|------|---------|-----------|
| Unified daemon | `python -m flex.daemon` | All loops (main + background + refresh) |
| Worker-only | `python -m flex.daemon --no-refresh --no-background` | Main loop only |
| Direct worker | `python -m flex.modules.claude_code.compile.worker --daemon` | Polling-only fallback |
| Systemd | `flex-worker.service` | Runs worker-only |
| Refresh timer | `flex-refresh.timer` → `python -m flex.refresh` | 30min refresh cycle |

### Thread Model

`daemon.py:main()` starts up to 3 threads:

| Thread | Function | Default interval | Purpose |
|--------|----------|-----------------|---------|
| Main | `daemon_loop()` from worker.py | 0.25s (inotify) / 2s (polling) | File change detection + sync |
| Background | `_background_tick_loop()` | 60s | Hook-driven tasks (`daemon_tick` hook) |
| Refresh | `_refresh_loop()` | 30min (30s startup delay) | Remote cell refresh via `run_due_refreshes()` |

CLI flags: `--interval`, `--remote-interval`, `--refresh-interval`,
`--no-background`, `--no-refresh`, `--no-inotify`.

### Instance Locking

`daemon.py:main()` acquires `fcntl.LOCK_EX` on `~/.flex/daemon.lock`.
Only one daemon instance runs at a time.

## Event-Driven Watcher

`flex/watcher.py` provides `FlexWatcher` — a watchdog-based inotify wrapper
with debounced drain.

### How It Works

```
Observer thread (inotify)          Main thread (daemon_loop)
─────────────────────────          ─────────────────────────
file modified/created  ──►  _pending dict  ◄──  drain() every 0.25s
                            {path: timestamp}    returns paths where
                            (lock-protected)     now - ts >= debounce
```

- Observer thread does NO SQLite work. It only records `(path, monotonic_time)`.
- Main thread calls `drain()` → gets debounced paths → calls `sync_session_messages()`.
- Debounce window: 500ms default (`FLEX_DEBOUNCE_MS`). Starts from first event per path (prevents starvation during sustained writes).

### FlexWatcher API

| Method | Purpose |
|--------|---------|
| `watch(path, pattern, recursive)` | Register directory for inotify |
| `start() -> bool` | Start observer thread |
| `stop()` | Join observer thread |
| `drain() -> list[Path]` | Return debounced paths, clear buffer |
| `active: bool` | Whether observer is running |
| `status() -> dict` | Diagnostics: watches, pending count, debounce |

### Watcher Setup in daemon.py

`_build_watcher()` handles all setup:

1. Check `FLEX_DISABLE_INOTIFY` env → skip if set
2. Import `FlexWatcher` → graceful ImportError if watchdog missing
3. Register `~/.claude/projects/` for `*.jsonl` (Claude Code sessions)
4. Register `discover_watched()` cells (markdown vaults, coding-agent dirs)
5. `start()` → returns watcher or None on failure

### Fallback Behavior

If watcher is None (watchdog missing, inotify failed, `--no-inotify`):
- `daemon_loop` runs in polling mode — identical to pre-watcher behavior
- 2-second tick, full `rglob("*.jsonl")` + stat() scan every tick
- Zero behavioral change from the legacy path

## The Daemon Loop

`flex/modules/claude_code/compile/worker.py:daemon_loop(interval, watcher)`

### Two Modes

**Event-driven (watcher active):**
- Tick: 0.25s
- Each tick: `watcher.drain()` → sync only changed files
- Every 60s (`FLEX_INTEGRITY_INTERVAL`): full `scan_sessions()` as safety net
- Embed sweep: only on changes or integrity tick

**Polling (watcher is None):**
- Tick: 2s (configurable via `--interval`)
- Each tick: full `scan_sessions()` — rglob + stat() of every JSONL
- Embed sweep: every tick

### Per-Tick Phases (both modes)

| Phase | What | Cadence |
|-------|------|---------|
| 0 | Session sync (inotify drain OR polling scan) | Every tick |
| 1 | Embed orphan chunks (`_batch_embed_chunks`, batch=64) | Every tick |
| 2 | Corpus document indexing (`_corpus_drainer`) | Every tick |
| 3 | Secondary cell drain (`_secondary_cell_drainer`) | Every tick |
| 4 | Markdown vault scan (`_markdown_scanner`) | Every tick |
| 5 | Coding-agent watch scan (`_coding_agent_scanner`) | 30s throttle |
| 6 | SOMA heal + eternity backup | 24h |
| 7 | Enrichment cycle + corpus graph refresh | 30min |

Phases 2-5 are guarded by `try/except ImportError` — missing modules are skipped.

## scan_sessions (The Filebeat Pattern)

`worker.py:scan_sessions(conn, size_cache, error_cache)`

Pure stat()-based polling. The inotify watcher replaces this as the primary
detection mechanism but it remains as the integrity scanner.

- Walks `~/.claude/projects/**/*.jsonl` via `rglob()`
- Compares `stat().st_size` to `size_cache[session_id]`
- If file grew → `sync_session_messages(session_id, conn)`
- On error → exponential backoff (5s, 10s, 20s... up to 300s) per session
- Rejects symlinks

## Registry

`flex/registry.py` — SQLite database at `~/.flex/registry.db`.

### Key Functions

| Function | Purpose |
|----------|---------|
| `register_cell(name, path, cell_type, ...)` | UPSERT cell into registry |
| `resolve_cell(name_or_type)` | Find cell path by name or cell_type |
| `discover_watched()` | Cells with `lifecycle='watch'` + `watch_path` |
| `discover_install_modules()` | Scan for module `install.py` files |
| `discover_module_specs()` | Load MODULE dicts from install modules |
| `module_spec_for(cell_type)` | Resolve spec by cell type |
| `get_hook(name)` | Get registered hook function |
| `load_plugins()` | Load plugin modules from `~/.flex/plugins/` |

### Cell Lifecycle Values

| Value | Meaning |
|-------|---------|
| `static` | No refresh (default) |
| `refresh` | Periodic refresh via `refresh_module` or `refresh_script` |
| `watch` | Daemon monitors `watch_path` for file changes |

### Registry Schema (cells table)

Key columns: `name`, `path`, `cell_type`, `lifecycle`, `refresh_module`,
`refresh_script`, `refresh_interval`, `watch_path`, `watch_pattern`,
`last_refresh_at`, `refresh_status`, `active`, `unlisted`, `substrate`.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `FLEX_HOME` | `~/.flex` | Flex home directory |
| `FLEX_DISABLE_INOTIFY` | unset | Force polling mode |
| `FLEX_DEBOUNCE_MS` | `500` | inotify debounce window (ms) |
| `FLEX_INTEGRITY_INTERVAL` | `60` | Seconds between full stat() scans |
| `FLEX_CODING_AGENT_WATCH_INTERVAL_SEC` | `30` | Coding-agent scan throttle |
| `FLEX_MODULE_PATH` | unset | Additional module search paths |

## File Map

```
flex/
├── daemon.py          ← unified daemon entry point, watcher setup
├── watcher.py         ← FlexWatcher (inotify + debounce)
├── registry.py        ← cell registry, module discovery
├── engine.py          ← query engine (run_from_spec substrate dispatch)
├── refresh.py         ← remote cell refresh orchestrator
├── health.py          ← daemon health checks
├── cli.py             ← CLI entry point (flex command)
├── core.py            ← shared utilities
├── serve.py           ← MCP server launcher
├── mcp_server.py      ← MCP tool implementations
├── mcp_core.py        ← MCP transport core
├── sdk.py             ← public SDK
├── views.py           ← view installation
├── secrets.py         ← ~/.flex/secrets loader
├── instructions.py    ← cell instruction generator
└── modules/           ← see flex:dev:modules
```
