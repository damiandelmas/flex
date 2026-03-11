"""
Unified flex daemon — single process for all cell lifecycle.

Threads:
  - main:           2s local cell scan (claude_code, claude_chat, docpac)
                    + 30min enrichment cycle + 24h SOMA heal
  - remote-poll:    60s manifest poll, re-download stale published cells
  - remote-refresh: 30min remote-pull cell refresh (reddit, hn, skills, etc.)
                    + optional auto-push to R2

No cron needed. One systemd service does everything.

Usage:
    python -m flex.daemon                   # foreground
    python -m flex.daemon --interval 5      # custom local scan interval
    python -m flex.daemon --no-remote-poll  # disable manifest polling
    python -m flex.daemon --no-refresh      # disable remote-pull refresh

Systemd:
    ExecStart=/path/to/venv/bin/python -m flex.daemon
"""

import argparse
import sys
import threading
import time


def _load_secrets():
    """Load ~/.flex/secrets into environment (KEY=VALUE format)."""
    import os
    from pathlib import Path
    secrets_path = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")) / "secrets"
    if secrets_path.exists():
        for line in secrets_path.read_text().splitlines():
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, _, val = line.partition('=')
                os.environ.setdefault(key.strip(), val.strip())


def _remote_poll_loop(interval: int = 60):
    """Poll manifest, re-download stale remote cells.

    Only acts if there are installed remote cells (source_url IS NOT NULL).
    Failures are logged but never crash the daemon.
    """
    while True:
        try:
            from flex.registry import list_cells, register_cell, CELLS_DIR
            from flex.distribute.manifest import fetch_manifest, diff_manifest, download_cell

            local = list_cells()
            installed_remote = [c for c in local if c.get("source_url")]

            if not installed_remote:
                time.sleep(interval)
                continue

            remote = fetch_manifest()
            diffs = diff_manifest(remote, installed_remote)

            for name, status in diffs.items():
                if status == "stale":
                    entry = remote.get(name)
                    if not entry:
                        continue
                    try:
                        dest = download_cell(entry, CELLS_DIR)
                        register_cell(
                            name=name,
                            path=str(dest),
                            checksum=entry.checksum,
                            source_url=entry.url,
                        )
                        print(f"[remote-poll] Updated {name}", file=sys.stderr)
                    except Exception as e:
                        print(f"[remote-poll] Failed {name}: {e}", file=sys.stderr)

        except Exception as e:
            print(f"[remote-poll] Error: {e}", file=sys.stderr)

        time.sleep(interval)


def _remote_refresh_loop(interval: int = 1800):
    """Refresh remote-pull cells (reddit, hn, skills, etc.) on a timer.

    Replaces the cron-based flex.refresh invocation.
    Runs all discovered remote-pull cells, then optionally pushes to R2.
    """
    # Wait 30s on startup to let the local scan get ahead
    time.sleep(30)

    while True:
        try:
            from flex.refresh import discover_cells, refresh_cell, _should_push

            cell_names = discover_cells()
            if not cell_names:
                time.sleep(interval)
                continue

            print(f"[refresh] Starting refresh for {len(cell_names)} cells: "
                  f"{', '.join(cell_names)}", file=sys.stderr)

            t0 = time.time()
            any_pushed = False

            for cell_name in cell_names:
                try:
                    stats = refresh_cell(cell_name)
                    if stats and stats.get('pushed'):
                        any_pushed = True
                except Exception as e:
                    print(f"[refresh] {cell_name} error: {e}", file=sys.stderr)

            # Push updated manifest after all cells refreshed
            if any_pushed:
                try:
                    from flex.distribute.manifest import fetch_manifest
                    from flex.distribute.push import push_manifest
                    remote = fetch_manifest()
                    manifest_data = {}
                    for n, entry in remote.items():
                        manifest_data[n] = {
                            "url": entry.url, "checksum": entry.checksum,
                            "size": entry.size, "updated_at": entry.updated_at,
                            "description": entry.description, "cell_type": entry.cell_type,
                            "freshness": entry.freshness,
                            "chunk_count": entry.chunk_count,
                            "source_count": entry.source_count,
                        }
                    push_manifest(manifest_data)
                except Exception as e:
                    print(f"[refresh] Manifest push failed: {e}", file=sys.stderr)

            elapsed = time.time() - t0
            print(f"[refresh] Done in {elapsed:.1f}s", file=sys.stderr)

        except Exception as e:
            print(f"[refresh] Error: {e}", file=sys.stderr)

        time.sleep(interval)


def main():
    _load_secrets()

    parser = argparse.ArgumentParser(description="Flex daemon — unified cell lifecycle")
    parser.add_argument("--interval", type=int, default=2,
                        help="Local cell scan interval in seconds (default: 2)")
    parser.add_argument("--remote-interval", type=int, default=60,
                        help="Remote manifest poll interval in seconds (default: 60)")
    parser.add_argument("--refresh-interval", type=int, default=1800,
                        help="Remote-pull refresh interval in seconds (default: 1800 = 30min)")
    parser.add_argument("--no-remote-poll", action="store_true",
                        help="Disable background remote cell polling")
    parser.add_argument("--no-refresh", action="store_true",
                        help="Disable background remote-pull refresh")
    args = parser.parse_args()

    print("[flex-daemon] Starting unified daemon", file=sys.stderr)
    print(f"  Local scan:      {args.interval}s", file=sys.stderr)
    print(f"  Manifest poll:   {'disabled' if args.no_remote_poll else f'{args.remote_interval}s'}",
          file=sys.stderr)
    print(f"  Remote refresh:  {'disabled' if args.no_refresh else f'{args.refresh_interval}s'}",
          file=sys.stderr)

    # Thread 1: remote manifest poll (downloaded cell freshness)
    if not args.no_remote_poll:
        t = threading.Thread(
            target=_remote_poll_loop,
            kwargs={"interval": args.remote_interval},
            daemon=True,
        )
        t.start()

    # Thread 2: remote-pull refresh (API pulls for reddit, hn, etc.)
    if not args.no_refresh:
        t = threading.Thread(
            target=_remote_refresh_loop,
            kwargs={"interval": args.refresh_interval},
            daemon=True,
        )
        t.start()

    # Main thread: local cell scan (blocks)
    try:
        from flex.modules.claude_code.compile.worker import daemon_loop
    except ImportError as e:
        print(f"[flex-daemon] Import error: {e}", file=sys.stderr)
        print("[flex-daemon] Is the flex engine installed?", file=sys.stderr)
        sys.exit(1)

    daemon_loop(interval=args.interval)


if __name__ == "__main__":
    main()
