"""
Unified refresh for all remote-pull cells.

Discovers cells with refresh modules, runs them in sequence.
Designed for cron or manual invocation.

Usage:
    python -m flex.refresh                    # all remote-pull cells
    python -m flex.refresh --cells reddit,hn  # specific cells
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


# Module path → cell name mapping for all remote-pull cells.
# Each module must expose a main() function or a refresh() function.
REMOTE_PULL_MODULES = {
    'reddit':      'flex.modules.reddit.compile.refresh',
    'hn':          'flex.modules.hn.compile.refresh',
    'lobsters':    'flex.modules.lobsters.compile.refresh',
    'bluesky':     'flex.modules.bluesky.compile.refresh',
    'devto':       'flex.modules.devto.compile.refresh',
    'x':           'flex.modules.x.compile.refresh',
    'arxiv':       'flex.modules.arxiv.compile.refresh',
    'skills-test': 'flex.modules.skills.compile.refresh',
    'people':      'flex.modules.people.compile.refresh',
}


def _load_publish_config() -> dict:
    """Load publish config from ~/.flex/config.json."""
    import os
    config_path = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")) / "config.json"
    if config_path.exists():
        try:
            return _json.loads(config_path.read_text()).get("publish", {})
        except Exception:
            pass
    return {}


def _should_push(cell_name: str) -> bool:
    """Check if a cell should be auto-pushed after refresh.

    If auto_push is true and no explicit cells list, defaults to all
    REMOTE_PULL_MODULES. The cells list acts as a filter, not a second registry.
    """
    config = _load_publish_config()
    if not config.get("auto_push", False):
        return False
    publish_cells = config.get("cells", None)
    if publish_cells is None:
        # Default: all remote-pull cells are publishable
        return cell_name in REMOTE_PULL_MODULES
    return cell_name in publish_cells


def discover_cells():
    """Return list of cell names that have refresh modules and exist in registry."""
    available = []
    for cell_name, module_path in REMOTE_PULL_MODULES.items():
        # Check cell exists in registry
        cell_path = resolve_cell(cell_name)
        if not cell_path:
            continue

        # Check module is importable
        try:
            importlib.import_module(module_path)
            available.append(cell_name)
        except ImportError:
            pass

    return available


def refresh_cell(cell_name, graph=False, dry_run=False, since_days=None):
    """Refresh a single cell using its module's refresh() function.

    Returns stats dict or None on error.
    """
    module_path = REMOTE_PULL_MODULES.get(cell_name)
    if not module_path:
        print(f"[{cell_name}] No refresh module registered", file=sys.stderr)
        return None

    cell_path = resolve_cell(cell_name)
    if not cell_path:
        print(f"[{cell_name}] Cell not found in registry", file=sys.stderr)
        return None

    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        print(f"[{cell_name}] Import failed: {e}", file=sys.stderr)
        return None

    # All refresh modules expose refresh(cell_path, ...) with compatible kwargs
    refresh_fn = getattr(mod, 'refresh', None)
    if not refresh_fn:
        print(f"[{cell_name}] No refresh() function in module", file=sys.stderr)
        return None

    # Build kwargs — only pass since_days if the function accepts it
    import inspect
    sig = inspect.signature(refresh_fn)
    kwargs = dict(graph=graph, dry_run=dry_run)
    if 'since_days' in sig.parameters:
        kwargs['since_days'] = since_days

    t0 = time.time()
    try:
        stats = refresh_fn(str(cell_path), **kwargs)
        elapsed = time.time() - t0
        print(f"[{cell_name}] Done in {elapsed:.1f}s", file=sys.stderr)

        # Post-refresh push hook
        if not dry_run and _should_push(cell_name):
            try:
                from flex.distribute.push import push_cell
                push_cell(cell_name)
                print(f"[{cell_name}] Pushed to R2", file=sys.stderr)
                stats = stats or {}
                stats["pushed"] = True
            except Exception as e:
                print(f"[{cell_name}] Push failed: {e}", file=sys.stderr)

        return stats
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[{cell_name}] ERROR after {elapsed:.1f}s: {e}", file=sys.stderr)
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
        description='Unified refresh for remote-pull cells')
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
        all_known = list(REMOTE_PULL_MODULES.keys())
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
    any_pushed = False
    for name, stats in results.items():
        if stats is None:
            print(f"  {name:15s} ERROR", file=sys.stderr)
        elif stats.get('dry_run'):
            print(f"  {name:15s} dry-run", file=sys.stderr)
        else:
            chunks = stats.get('chunks', 0)
            sources = stats.get('sources', stats.get('posts', 0))
            pushed = " [pushed]" if stats.get('pushed') else ""
            print(f"  {name:15s} {sources} sources, {chunks} chunks{pushed}", file=sys.stderr)
            if stats.get('pushed'):
                any_pushed = True

    # Push updated manifest after all cells are refreshed
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
                    "chunk_count": entry.chunk_count, "source_count": entry.source_count,
                }
            push_manifest(manifest_data)
        except Exception as e:
            print(f"  Manifest push failed: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()
