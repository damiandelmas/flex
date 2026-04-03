"""
Unified refresh for all refreshable cells.

Discovers cells with refresh modules, runs them in sequence.
Designed for cron or manual invocation.

Usage:
    python -m flex.refresh                    # all refreshable cells
    python -m flex.refresh --cells name1,name2  # specific cells
    python -m flex.refresh --dry-run          # check for new data only
    python -m flex.refresh --graph            # force graph rebuild
    python -m flex.refresh --since 7d         # override cursors (all cells)

Cron:
    0 */6 * * * /path/to/venv/bin/python -m flex.refresh >> ~/.flex/refresh.log 2>&1
"""

import argparse
import importlib
import sys
import time
from datetime import datetime, timezone

import json as _json
from pathlib import Path

from flex.registry import resolve_cell


_EXT_MODULES = {}
try:
    from flex.modules.registry_ext import _EXT_MODULES
except ImportError:
    pass


def run_due_refreshes(force: bool = False) -> dict:
    """The entire refresh lifecycle in one function.

    Reads the registry, finds cells due for refresh, runs each one,
    updates status. This is what the daemon calls on every tick.

    Args:
        force: If True, refresh all cells regardless of interval.

    Returns:
        Dict of {cell_name: 'ok' | 'error: ...'} for cells that ran.
    """
    from flex.registry import discover_refreshable, update_refresh_status

    results = {}
    cells = discover_refreshable()
    if not cells:
        return results

    now = time.time()

    for cell in cells:
        name = cell['name']
        interval = cell.get('refresh_interval') or 1800
        last = cell.get('last_refresh_at')

        # Check if due
        if not force and last:
            try:
                last_ts = datetime.fromisoformat(last).timestamp()
                if now - last_ts < interval:
                    continue  # not due yet
            except (ValueError, TypeError):
                pass  # bad timestamp, refresh anyway

        # Run it
        try:
            stats = refresh_cell(name)
            results[name] = 'ok' if stats is not None else 'error: no stats'
        except Exception as e:
            results[name] = f'error: {e}'

    return results


def _should_sync(cell_name: str) -> bool:
    """Check if a cell has a post-refresh sync hook configured."""
    from flex.registry import get_hook
    return get_hook("post_refresh_cell_hook") is not None


def discover_cells():
    """Return list of cell names that have refresh capability.

    Merges two sources:
    1. Legacy _EXT_MODULES dict (built-in modules with refresh.py)
    2. Registry cells with lifecycle='refresh' (SDK-built cells with refresh_script)

    Deduplicated by name.
    """
    available = set()

    # Legacy path: hardcoded module dict
    for cell_name, module_path in _EXT_MODULES.items():
        cell_path = resolve_cell(cell_name)
        if not cell_path:
            continue
        try:
            importlib.import_module(module_path)
            available.add(cell_name)
        except ImportError:
            pass

    # Registry path: cells with lifecycle='refresh'
    try:
        from flex.registry import discover_refreshable
        for cell in discover_refreshable():
            available.add(cell['name'])
    except Exception:
        pass

    return sorted(available)


def refresh_cell(cell_name, graph=False, dry_run=False, since_days=None):
    """Refresh a single cell.

    Three paths in priority order:
    1. Registry refresh_module (importable Python module with refresh())
    2. Registry refresh_script (subprocess — for SDK-built cells)
    3. Legacy _EXT_MODULES dict (fallback during transition)

    Returns stats dict or None on error.
    """
    from flex.registry import update_refresh_status

    cell_path = resolve_cell(cell_name)
    if not cell_path:
        print(f"[{cell_name}] Cell not found in registry", file=sys.stderr)
        return None

    # Resolve refresh method: registry first, legacy fallback
    refresh_module = None
    refresh_script = None

    try:
        from flex.registry import list_cells
        for c in list_cells():
            if c['name'] == cell_name:
                refresh_module = c.get('refresh_module')
                refresh_script = c.get('refresh_script')
                break
    except Exception:
        pass

    # Fallback to legacy dict
    if not refresh_module and not refresh_script:
        refresh_module = _EXT_MODULES.get(cell_name)

    if not refresh_module and not refresh_script:
        print(f"[{cell_name}] No refresh method registered", file=sys.stderr)
        return None

    update_refresh_status(cell_name, 'running')
    t0 = time.time()

    try:
        stats = None

        if refresh_script:
            # SDK-built cell: run the build script as subprocess
            import subprocess as _sp
            result = _sp.run(
                [sys.executable, refresh_script],
                capture_output=True, text=True, timeout=600,
            )
            if result.returncode != 0:
                raise RuntimeError(f"Script exited {result.returncode}: {result.stderr[:200]}")
            stats = {'script': refresh_script, 'returncode': 0}

        elif refresh_module:
            # Module-based: import and call refresh()
            try:
                mod = importlib.import_module(refresh_module)
            except ImportError as e:
                raise RuntimeError(f"Import failed: {e}")

            refresh_fn = getattr(mod, 'refresh', None)
            if not refresh_fn:
                raise RuntimeError(f"No refresh() in {refresh_module}")

            import inspect
            sig = inspect.signature(refresh_fn)
            kwargs = dict(graph=graph, dry_run=dry_run)
            if 'since_days' in sig.parameters:
                kwargs['since_days'] = since_days

            stats = refresh_fn(str(cell_path), **kwargs)

        elapsed = time.time() - t0
        print(f"[{cell_name}] Done in {elapsed:.1f}s", file=sys.stderr)
        update_refresh_status(cell_name, 'ok')

        # Post-refresh hook
        if not dry_run and _should_sync(cell_name):
            from flex.registry import get_hook
            _hook_fn = get_hook("post_refresh_cell_hook")
            if _hook_fn:
                try:
                    _hook_fn(cell_name)
                    print(f"[{cell_name}] Synced", file=sys.stderr)
                    stats = stats or {}
                    stats["hooked"] = True
                except Exception as e:
                    print(f"[{cell_name}] Hook failed: {e}", file=sys.stderr)

        return stats

    except Exception as e:
        elapsed = time.time() - t0
        print(f"[{cell_name}] ERROR after {elapsed:.1f}s: {e}", file=sys.stderr)
        update_refresh_status(cell_name, f'error: {str(e)[:100]}')
        return None


def _load_secrets():
    """Load ~/.flex/secrets into environment (KEY=VALUE format)."""
    import os
    secrets_path = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")) / "secrets"
    if secrets_path.exists():
        for line in secrets_path.read_text().splitlines():
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, _, val = line.partition('=')
                os.environ.setdefault(key.strip(), val.strip())


def main():
    _load_secrets()

    parser = argparse.ArgumentParser(
        description='Unified refresh for refreshable cells')
    parser.add_argument('--cells', default=None,
                        help='Comma-separated cell names (default: all discovered)')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild on all cells')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new data without ingesting')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 7d). Overrides cursors.')
    parser.add_argument('--list', action='store_true',
                        help='List available cells and exit')
    args = parser.parse_args()

    # Parse --since
    since_days = None
    if args.since:
        since_days = int(args.since.strip().lower().rstrip('d'))

    # Discover or filter cells
    if args.cells:
        cell_names = [c.strip() for c in args.cells.split(',')]
    else:
        cell_names = discover_cells()

    if args.list:
        all_known = list(_EXT_MODULES.keys())
        available = discover_cells()
        for name in all_known:
            status = "OK" if name in available else "NOT FOUND"
            print(f"  {name:15s} {status}")
        return

    if not cell_names:
        print("No cells to refresh.", file=sys.stderr)
        return

    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"===== {ts} =====", file=sys.stderr)
    print(f"Cells: {', '.join(cell_names)}", file=sys.stderr)
    if args.dry_run:
        print("Mode: dry-run", file=sys.stderr)
    print(file=sys.stderr)

    t_total = time.time()
    results = {}

    for cell_name in cell_names:
        print(f"{'=' * 50}", file=sys.stderr)
        stats = refresh_cell(
            cell_name,
            graph=args.graph,
            dry_run=args.dry_run,
            since_days=since_days,
        )
        results[cell_name] = stats

    elapsed = time.time() - t_total
    print(f"\n{'=' * 50}", file=sys.stderr)
    print(f"Total: {elapsed:.1f}s across {len(cell_names)} cells", file=sys.stderr)

    # Summary
    any_hooked = False
    for name, stats in results.items():
        if stats is None:
            print(f"  {name:15s} ERROR", file=sys.stderr)
        elif stats.get('dry_run'):
            print(f"  {name:15s} dry-run", file=sys.stderr)
        else:
            chunks = stats.get('chunks', 0)
            sources = stats.get('sources', 0)
            extra = " [synced]" if stats.get('hooked') else ""
            print(f"  {name:15s} {sources} sources, {chunks} chunks{extra}", file=sys.stderr)
            if stats.get('hooked'):
                any_hooked = True

    # Post-refresh sync hook
    if any_hooked:
        from flex.registry import get_hook
        _sync = get_hook("post_refresh_hook")
        if _sync:
            try:
                _sync()
            except Exception as e:
                print(f"  Post-refresh hook failed: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()
