"""
Flex daemon — local capture/watch process plus optional background loops.

Usage:
    python -m flex.daemon                                  # all loops
    python -m flex.daemon --interval 5                     # custom local scan interval
    python -m flex.daemon --no-background                  # disable background tasks
    python -m flex.daemon --no-refresh                     # disable refresh cycle
    python -m flex.daemon --no-refresh --no-background     # local worker service

Systemd:
    flex-worker.service runs local-only.
    flex-refresh.timer runs python -m flex.refresh on a schedule.
"""

import argparse
import os
import sys
import threading
import time


def _load_secrets():
    """Load ~/.flex/secrets into environment (KEY=VALUE format)."""
    import os, stat
    from pathlib import Path
    secrets_path = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")) / "secrets"
    if secrets_path.exists():
        # Fix permissions if world-readable (should be 600)
        mode = secrets_path.stat().st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            try:
                secrets_path.chmod(0o600)
            except OSError:
                pass
        for line in secrets_path.read_text().splitlines():
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, _, val = line.partition('=')
                os.environ.setdefault(key.strip(), val.strip())


def _background_tick_loop(interval: int = 60):
    """Background task loop. Hook-driven — no-op if no hook registered."""
    while True:
        try:
            from flex.registry import get_hook
            _poll = get_hook("daemon_tick")
            if _poll:
                _poll()
        except Exception as e:
            print(f"[background] Error: {e}", file=sys.stderr)
        time.sleep(interval)


def _refresh_loop(interval: int = 60):
    """Refresh cells on a timer. Reads the registry, runs what's due.

    Ticks every `interval` seconds (default 60). Each tick calls
    run_due_refreshes() which checks per-cell intervals and only
    runs cells that are actually due.
    """
    # Wait 30s on startup to let the local scan get ahead
    time.sleep(30)

    while True:
        try:
            from flex.refresh import run_due_refreshes

            results = run_due_refreshes()

            if results:
                ok = sum(1 for v in results.values() if v == 'ok')
                errors = sum(1 for v in results.values() if v.startswith('error'))
                print(f"[refresh] {ok} ok, {errors} errors: "
                      f"{', '.join(results.keys())}", file=sys.stderr)

                # Post-refresh sync hook
                any_hooked = any(
                    'hooked' in str(v) for v in results.values()
                )
                if any_hooked:
                    from flex.registry import get_hook
                    _sync = get_hook("post_refresh_hook")
                    if _sync:
                        try:
                            _sync()
                        except Exception as e:
                            print(f"[refresh] Post-refresh hook failed: {e}", file=sys.stderr)

        except Exception as e:
            print(f"[refresh] Error: {e}", file=sys.stderr)

        time.sleep(interval)


def _module_watch_loop(interval: float = 60):
    """Run one Registry-owned local reconciliation pass per interval.

    The coordinator is the only scheduling owner. Provider refresh modules are
    still isolated by :func:`flex.refresh.refresh_cell` when materialization is
    required; no second ``flex.refresh --watches`` CLI process is created.
    """
    print(f"[flex-daemon] Module reconciliation: {interval:g}s", file=sys.stderr)
    while True:
        try:
            from flex.refresh import run_due_watches
            run_due_watches()
        except Exception as e:
            print(f"[watch] Error: {e}", file=sys.stderr)
        time.sleep(interval)


def _codex_structural_tick_loop(interval: float = 2.0) -> None:
    """Publish growth in known active Codex rollouts independently of corpus work.

    Discovery and deletion truth remain in the ordinary watcher/reconciliation
    lanes. This loop follows already-receipted provider addresses only, so a
    large repository or Markdown walk cannot delay live conversation text,
    metadata, relationships, and FTS.
    """
    from flex.lifecycle import coordinator
    from flex.modules.codex.compile.worker import scan_codex_cells

    while True:
        started = time.time()
        try:
            stats = coordinator(lambda *_a, **_k: None).active_append_pass(
                lambda names: scan_codex_cells(
                    deadline=started + max(1.0, interval),
                    embed=False,
                    discover=False,
                    cell_names=names,
                )
            ) or {}
            if stats.get("indexed", 0):
                print(
                    f"[flex-daemon] codex structural indexed={stats['indexed']}",
                    file=sys.stderr,
                )
        except Exception as exc:
            print(f"[flex-daemon] Codex structural tick error: {exc}", file=sys.stderr)
        elapsed = time.time() - started
        time.sleep(max(0.1, interval - elapsed))


def _build_claude_watcher():
    """Phase 1 composition: build the Claude Code filesystem observer.

    Returns (queue, watcher, config). queue/watcher are None when events are
    explicitly disabled (FLEX_WATCH_DISABLE), the Claude projects directory
    doesn't exist yet, or the watchdog backend fails to start — daemon_loop()
    falls back to its original every-tick polling behavior whenever queue is
    None, so a watch failure here degrades latency, never correctness.
    """
    from flex.watch import WatchRegistration, InvalidationQueue, Watcher, load_watch_config

    config = load_watch_config()
    if config.disabled:
        print("[flex-daemon] Filesystem events disabled via FLEX_WATCH_DISABLE — polling fallback",
              file=sys.stderr)
        return None, None, config

    try:
        from flex.modules.claude_code.compile.worker import CLAUDE_PROJECTS
    except ImportError:
        return None, None, config

    if not CLAUDE_PROJECTS.exists():
        print("[flex-daemon] Claude projects dir not found — filesystem events unavailable, polling fallback",
              file=sys.stderr)
        return None, None, config

    registration = WatchRegistration(
        cell_name='claude_code',
        root=CLAUDE_PROJECTS,
        pattern='**/*.jsonl',
        recursive=True,
    )
    queue = InvalidationQueue(
        quiet_window=config.quiet_window,
        max_latency=config.max_latency,
        max_size=config.queue_max,
    )
    watcher = Watcher(registration, queue)
    if not watcher.start():
        print(
            f"[flex-daemon] Filesystem watcher failed to start ({watcher.last_error}) — polling fallback",
            file=sys.stderr,
        )
        return None, None, config

    print(f"[flex-daemon] Filesystem events enabled (backend={watcher.backend})", file=sys.stderr)
    return queue, watcher, config


def _build_local_watchers():
    """Build one observer per declared local root, sharing one bounded queue."""
    from flex.registry import list_cells
    from flex.watch import (
        InvalidationQueue, WatchRegistration, WatcherSet,
        load_watch_config, registrations_for_cells,
    )

    config = load_watch_config()
    if config.disabled:
        return None, None, config
    registrations = registrations_for_cells(list_cells())

    # Claude's legacy registry row may not carry lifecycle/watch_path even though
    # its source authority is known. Preserve that first-party registration while
    # the registry metadata converges.
    try:
        from flex.modules.claude_code.compile.worker import CLAUDE_PROJECTS
        root = CLAUDE_PROJECTS.resolve()
        if root.is_dir() and not any(
            r.cell_name == "claude_code" and r.root == root for r in registrations
        ):
            registrations.append(WatchRegistration(
                "claude_code", root, "**/*.jsonl", recursive=True,
            ))
    except (ImportError, OSError):
        pass

    if not registrations:
        print("[flex-daemon] No local watch roots — polling fallback", file=sys.stderr)
        return None, None, config
    queue = InvalidationQueue(
        quiet_window=config.quiet_window,
        max_latency=config.max_latency,
        max_size=config.queue_max,
    )
    watchers = WatcherSet(registrations, queue)
    # Recursive observer setup can take minutes across a large registry. Event
    # callbacks may begin filling the shared queue as individual roots come
    # online, while the worker immediately starts structural reconciliation and
    # capture instead of waiting for the whole watch topology.
    watchers.start_background()
    print(
        f"[flex-daemon] Filesystem event initialization started for "
        f"{len(registrations)} roots",
        file=sys.stderr,
    )
    return queue, watchers, config


def _run_local_worker(*, interval, queue, watcher, reconcile_interval):
    """Choose the coding-session owner when present, otherwise filesystem-only."""
    from flex.registry import resolve_cell

    if resolve_cell("claude_code"):
        from flex.modules.claude_code.compile.worker import daemon_loop
    else:
        from flex.modules.fs.compile.worker import daemon_loop
    daemon_loop(
        interval=interval,
        invalidation_queue=queue,
        watcher=watcher,
        reconcile_interval=reconcile_interval,
    )


def main():
    _load_secrets()
    from flex.registry import load_plugins
    load_plugins()

    # Prevent duplicate daemon instances
    import fcntl
    from pathlib import Path as _Path
    _lock_path = _Path(os.environ.get("FLEX_HOME", _Path.home() / ".flex")) / "daemon.lock"
    _lock_path.parent.mkdir(parents=True, exist_ok=True)
    _lock_fd = open(_lock_path, 'w')
    try:
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[flex-daemon] Another instance is already running. Exiting.", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Flex daemon — local capture/watch lifecycle")
    parser.add_argument("--interval", type=int, default=2,
                        help="Local cell scan interval in seconds (default: 2)")
    parser.add_argument("--remote-interval", type=int, default=60,
                        help="Remote poll interval in seconds (default: 60)")
    parser.add_argument("--refresh-interval", type=int, default=1800,
                        help="Remote-pull refresh interval in seconds (default: 1800 = 30min)")
    parser.add_argument("--no-background", action="store_true",
                        help="Disable background tasks")
    parser.add_argument("--no-refresh", action="store_true",
                        help="Disable refresh cycle")
    args = parser.parse_args()

    print("[flex-daemon] Starting unified daemon", file=sys.stderr)
    print(f"  Local scan:      {args.interval}s", file=sys.stderr)
    print(f"  Background:      {'disabled' if args.no_background else f'{args.remote_interval}s'}",
          file=sys.stderr)
    print(f"  Refresh:         {'disabled' if args.no_refresh else f'{args.refresh_interval}s'}",
          file=sys.stderr)

    # Thread 1: background tasks (plugin-driven)
    if not args.no_background:
        t = threading.Thread(
            target=_background_tick_loop,
            kwargs={"interval": args.remote_interval},
            daemon=True,
        )
        t.start()

    # Thread 2: refresh cycle (installed modules)
    if not args.no_refresh:
        t = threading.Thread(
            target=_refresh_loop,
            kwargs={"interval": args.refresh_interval},
            daemon=True,
        )
        t.start()

    # Watch reconciliation is distinct from lifecycle='refresh' pulls.  Keep
    # it alive for the installed ``--no-refresh`` local worker, but make its
    # thread a subprocess-only supervisor so it never imports refresh modules.
    from flex.watch import load_watch_config
    _watch_config = load_watch_config()
    t = threading.Thread(
        target=_module_watch_loop,
        kwargs={"interval": _watch_config.reconcile_interval},
        daemon=True,
    )
    t.start()

    # Structural coding-session capture is independent of general corpus and
    # semantic scheduling. It opens its own provider connections each tick.
    t = threading.Thread(
        target=_codex_structural_tick_loop,
        name="codex-structural-tick",
        daemon=True,
    )
    t.start()

    # Typed local observers are built before the worker loop and stopped on
    # every exit path, including signal-driven shutdown.
    queue, watcher, _watch_config = _build_local_watchers()

    def _shutdown_watcher(signum=None, frame=None):
        if watcher is not None:
            watcher.stop()
        if signum is not None:
            sys.exit(0)

    import signal
    signal.signal(signal.SIGTERM, _shutdown_watcher)
    signal.signal(signal.SIGINT, _shutdown_watcher)

    # Main thread: local cell scan (blocks if module available)
    try:
        _run_local_worker(
            interval=args.interval,
            queue=queue,
            watcher=watcher,
            reconcile_interval=_watch_config.reconcile_interval if queue else None,
        )
    except ImportError:
        print("[flex-daemon] no local worker installed — running background services only",
              file=sys.stderr)
        # Block main thread so daemon stays alive for background services
        import time
        while True:
            time.sleep(60)
    finally:
        _shutdown_watcher()


if __name__ == "__main__":
    main()
