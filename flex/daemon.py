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

    # Main thread: local cell scan (blocks if module available)
    try:
        from flex.modules.claude_code.compile.worker import daemon_loop
        daemon_loop(interval=args.interval)
    except ImportError:
        print("[flex-daemon] claude_code module not installed — running background services only",
              file=sys.stderr)
        # Block main thread so daemon stays alive for background services
        import time
        while True:
            time.sleep(60)


if __name__ == "__main__":
    main()
