"""
Unified refresh for all refreshable cells.

Discovers cells with refresh modules, runs them in sequence.
Designed for cron or manual invocation.

Usage:
    python -m flex.refresh                    # all refreshable cells
    python -m flex.refresh --due              # scheduled: refresh due cells only
    python -m flex.refresh --cells name1,name2  # specific cells
    python -m flex.refresh --cells name1 name2  # same, shell-friendly
    python -m flex.refresh --dry-run          # check for new data only
    python -m flex.refresh --graph            # force graph rebuild
    python -m flex.refresh --since 7d         # override cursors (all cells)

Cron:
    0 */6 * * * /path/to/venv/bin/python -m flex.refresh --due >> ~/.flex/refresh.log 2>&1
"""

import argparse
import importlib
import multiprocessing
import os
import re
import sys
import time
from datetime import datetime, timezone

import json as _json
from pathlib import Path

from flex.registry import resolve_cell


_EXT_MODULES = {}


# C3 — untrusted-input validation for registry.refresh_script / refresh_module.
# Both fields originate from the registry, which is user-controlled under
# normal operation but reachable via poisoned cell downloads, SDK-registered
# refresh scripts, or direct SQL. Treat as untrusted on every refresh cycle.

_REFRESH_MODULE_RE = re.compile(r'^flex(\.[a-zA-Z0-9_]+)+$')
DEFAULT_REFRESH_TIMEOUT = 1800
DEFAULT_HEAVY_ADMISSION_WAIT_S = 180


def _refresh_timeout() -> int:
    """Return per-cell refresh timeout in seconds."""
    raw = os.environ.get("FLEX_REFRESH_TIMEOUT_SEC", "")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return DEFAULT_REFRESH_TIMEOUT


def _heavy_admission_wait_s() -> float:
    """Bound external refresh queueing without ever blocking capture work."""
    raw = os.environ.get("FLEX_HEAVY_ADMISSION_WAIT_S", "")
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return float(DEFAULT_HEAVY_ADMISSION_WAIT_S)


def _run_module_refresh_child(
    module_path: str,
    cell_path: str,
    kwargs: dict,
    conn,
) -> None:
    """Run module refresh in a child process and send stats/error to parent."""
    try:
        if module_path.startswith("flex.modules."):
            try:
                from flex.modules.specs import discover_install_modules
                discover_install_modules()
            except Exception:
                pass
        mod = importlib.import_module(module_path)
        refresh_fn = getattr(mod, 'refresh', None)
        if not refresh_fn:
            raise RuntimeError(f"No refresh() in {module_path}")
        stats = refresh_fn(cell_path, **kwargs)
        conn.send({"ok": True, "stats": stats})
    except Exception as e:
        conn.send({"ok": False, "error": str(e)})
    finally:
        conn.close()


def _run_module_refresh_with_timeout(
    module_path: str,
    cell_path: str,
    kwargs: dict,
    timeout: int | None = None,
) -> dict | None:
    """Run a registry refresh_module in a bounded child process."""
    timeout_s = timeout or _refresh_timeout()
    ctx = multiprocessing.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(
        target=_run_module_refresh_child,
        args=(module_path, cell_path, kwargs, child_conn),
    )
    proc.start()
    child_conn.close()

    try:
        if parent_conn.poll(timeout_s):
            try:
                result = parent_conn.recv()
            except EOFError:
                result = {"ok": False, "error": "child exited without result"}
        else:
            proc.terminate()
            proc.join(5)
            if proc.is_alive():
                proc.kill()
                proc.join()
            raise TimeoutError(f"refresh_module timed out after {timeout_s}s")
    finally:
        parent_conn.close()

    proc.join()
    if proc.exitcode and not result.get("ok"):
        raise RuntimeError(result.get("error") or f"child exited {proc.exitcode}")
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "refresh failed")
    return result.get("stats")


def _flex_home() -> Path:
    return Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")).resolve()


def _is_safe_refresh_script(path_str: str) -> tuple[bool, str]:
    """Verify a refresh_script path is safe to execute via subprocess.

    Returns ``(ok, reason)``. Safe when the path resolves to an existing
    ``.py`` file inside ``FLEX_HOME`` with no symlink escape.
    """
    if not path_str:
        return False, "empty path"
    try:
        p = Path(path_str)
        if not p.is_absolute():
            return False, "must be an absolute path"
        p = p.resolve()
    except Exception as e:
        return False, f"invalid path: {e}"
    if not p.exists():
        return False, "does not exist"
    if not p.is_file():
        return False, "not a file"
    if p.suffix != '.py':
        return False, "not a .py file"
    flex_home = _flex_home()
    try:
        rel_ok = p.is_relative_to(flex_home)
    except AttributeError:  # py<3.9 guard
        rel_ok = str(p).startswith(str(flex_home))
    if not rel_ok:
        return False, f"outside FLEX_HOME ({flex_home})"
    return True, "ok"


def _is_safe_refresh_module(mod_str: str) -> tuple[bool, str]:
    """Verify a refresh_module import path is safe (must match flex.*)."""
    if not mod_str:
        return False, "empty module"
    if '..' in mod_str:
        return False, "double dot in module path"
    if not _REFRESH_MODULE_RE.match(mod_str):
        return False, "must match flex.*"
    return True, "ok"


def _reload_external_refresh_module(module_path: str, mod):
    """Reload one external module package before an in-process watch probe.

    Watch dry-runs execute inside the long-lived daemon, unlike real refreshes,
    which already run in a fresh spawned process.  Reloading only ``refresh.py``
    is insufficient because its imported compiler/signature helpers remain in
    ``sys.modules``.  Evict loaded implementation files under the same external
    root, then import the refresh module once from a clean package cache.

    Only files inside a discovered external module root are eligible; packaged
    Flex modules retain ordinary Python import semantics.
    """
    if not module_path.startswith("flex.modules."):
        return mod

    module_file = getattr(mod, "__file__", None)
    if not module_file:
        return mod

    try:
        from flex.modules.specs import discover_install_modules

        loaded_path = Path(module_file).resolve()
        installed = discover_install_modules()
        entries = installed.values() if isinstance(installed, dict) else installed
        external_roots = [
            Path(entry["root"]).resolve()
            for entry in entries
            if entry.get("source") == "external" and entry.get("root")
        ]
        module_root = next(
            (root for root in external_roots if loaded_path.is_relative_to(root)),
            None,
        )
        if module_root is None:
            return mod
    except (ImportError, OSError, TypeError, ValueError):
        return mod

    importlib.invalidate_caches()
    package_name = ".".join(module_path.split(".")[:3])
    stale = []
    for name, cached in list(sys.modules.items()):
        cached_file = getattr(cached, "__file__", None)
        if not cached_file or name == package_name:
            continue
        try:
            if Path(cached_file).resolve().is_relative_to(module_root):
                stale.append((name, cached))
        except (OSError, TypeError, ValueError):
            continue

    for name, cached in sorted(stale, reverse=True):
        parent_name, _, child_name = name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None and getattr(parent, child_name, None) is cached:
            try:
                delattr(parent, child_name)
            except AttributeError:
                pass
        sys.modules.pop(name, None)
    return importlib.import_module(module_path)


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
    from flex.lifecycle import coordinator
    return coordinator(refresh_cell).remote_refresh_pass(force=force)


def run_due_watches() -> dict:
    """Poll lifecycle='watch' cells; regen only when the source signature changed.

    discover_watched() returns instant cells that carry a refresh_module. A cheap
    dry-run probes the msize signature (selection size + count + newest mtime); a
    real regen runs only on change, so this is safe to call every refresh tick.
    Fixes the wiring gap: discover_watched() existed but had no caller, so watched
    cells never auto-refreshed.
    """
    from flex.lifecycle import coordinator
    return coordinator(refresh_cell).local_watch_pass()


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


def refresh_cell(cell_name, graph=False, dry_run=False, since_days=None, quiet=False,
                 scheduled: bool = False, _heavy_admitted: bool = False):
    """Refresh one cell, deferring real work when semantic admission is busy.

    A command which owns the lease passes ``_heavy_admitted`` for each cell so
    its spawned module workers remain covered by the same parent lease.  Dry
    probes intentionally remain lock-free: they do not perform enrichment.
    """
    if dry_run or _heavy_admitted:
        return _refresh_cell(
            cell_name, graph=graph, dry_run=dry_run, since_days=since_days,
            quiet=quiet, scheduled=scheduled,
        )
    from flex.admission import try_heavy_lease
    with try_heavy_lease(
        detail=f"refresh:{cell_name}", timeout_s=_heavy_admission_wait_s(),
    ) as lease:
        if not lease.acquired:
            if not quiet:
                print(f"[{cell_name}] Deferred: semantic work already active", file=sys.stderr)
            return {"deferred": True}
        return _refresh_cell(
            cell_name, graph=graph, dry_run=False, since_days=since_days,
            quiet=quiet, scheduled=scheduled,
        )


def _refresh_cell(cell_name, graph=False, dry_run=False, since_days=None, quiet=False,
                  scheduled: bool = False):
    """Refresh a single cell.

    Three paths in priority order:
    1. Registry refresh_module (importable Python module with refresh())
    2. Registry refresh_script (subprocess — for SDK-built cells)
    3. Legacy _EXT_MODULES dict (fallback during transition)

    Returns stats dict or None on error.
    """
    from flex.registry import mark_refresh_committed, update_refresh_status
    from flex.secrets import check_secret_specs

    cell_path = resolve_cell(cell_name)
    if not cell_path:
        if not quiet:
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
        if not quiet:
            print(f"[{cell_name}] No refresh method registered", file=sys.stderr)
        return None

    if not dry_run:
        update_refresh_status(cell_name, 'running')
    t0 = time.time()

    try:
        stats = None

        if refresh_script:
            # C3 — validate path before subprocess execution
            ok, reason = _is_safe_refresh_script(refresh_script)
            if not ok:
                raise RuntimeError(f"unsafe refresh_script ({reason}): {refresh_script}")

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
            # C3 — validate module path before import
            ok, reason = _is_safe_refresh_module(refresh_module)
            if not ok:
                raise RuntimeError(f"unsafe refresh_module ({reason}): {refresh_module}")
            if refresh_module.startswith("flex.modules."):
                try:
                    from flex.modules.specs import discover_install_modules
                    discover_install_modules()
                except Exception:
                    pass
            # Module-based: import and call refresh()
            try:
                mod = importlib.import_module(refresh_module)
            except ImportError as e:
                raise RuntimeError(f"Import failed: {e}")
            if dry_run:
                mod = _reload_external_refresh_module(refresh_module, mod)

            missing = check_secret_specs(
                getattr(mod, 'REQUIRES_SECRETS', None),
                set_env=True,
            )
            if missing:
                raise RuntimeError("; ".join(missing))

            refresh_fn = getattr(mod, 'refresh', None)
            if not refresh_fn:
                raise RuntimeError(f"No refresh() in {refresh_module}")

            import inspect
            sig = inspect.signature(refresh_fn)
            kwargs = dict(graph=graph, dry_run=dry_run)
            if 'since_days' in sig.parameters:
                kwargs['since_days'] = since_days
            # Keep scheduling backward compatible: installed modules often
            # expose a deliberately narrow refresh() signature.  A module
            # explicitly accepting ``scheduled`` (or **kwargs) can opt into
            # bounded scheduler work without breaking the rest.
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in sig.parameters.values()
            )
            if scheduled and ('scheduled' in sig.parameters or accepts_kwargs):
                kwargs['scheduled'] = True

            if dry_run:
                stats = refresh_fn(str(cell_path), **kwargs)
            else:
                stats = _run_module_refresh_with_timeout(
                    refresh_module,
                    str(cell_path),
                    kwargs,
                )

        elapsed = time.time() - t0
        if not quiet:
            print(f"[{cell_name}] Done in {elapsed:.1f}s", file=sys.stderr)
        if not dry_run:
            receipt = stats if isinstance(stats, dict) else {}
            mark_refresh_committed(
                cell_name,
                source_signature=receipt.get('source_signature'),
                source_high_water=receipt.get('source_high_water'),
                pending=_receipt_pending(receipt),
            )

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
        if not quiet:
            print(f"[{cell_name}] ERROR after {elapsed:.1f}s: {e}", file=sys.stderr)
        if not dry_run:
            update_refresh_status(cell_name, f'error: {str(e)[:100]}')
        return None


def _receipt_pending(receipt: dict) -> int:
    """Normalize an optional module pending-work receipt for the registry."""
    value = receipt.get('refresh_pending', receipt.get('pending', 0))
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _load_secrets():
    """Load ~/.flex/secrets into environment (KEY=VALUE format)."""
    from flex.secrets import load_secrets_file
    load_secrets_file()


def _parse_cells_arg(values: list[str] | None) -> list[str]:
    """Parse --cells values, accepting comma-separated or space-separated names."""
    if not values:
        return []
    cells = []
    for value in values:
        cells.extend(c.strip() for c in value.split(',') if c.strip())
    return cells


def main():
    _load_secrets()

    parser = argparse.ArgumentParser(
        description='Unified refresh for refreshable cells')
    parser.add_argument('--cells', nargs='+', default=None,
                        help='Cell names, comma- or space-separated (default: all discovered)')
    parser.add_argument('--graph', action='store_true',
                        help='Force graph rebuild on all cells')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check for new data without ingesting')
    parser.add_argument('--since', default=None, type=str,
                        help='Pull this many days back (e.g. 7d). Overrides cursors.')
    parser.add_argument('--list', action='store_true',
                        help='List available cells and exit')
    parser.add_argument('--due', action='store_true',
                        help='Run one scheduled pass for cells due to refresh')
    parser.add_argument('--watches', action='store_true',
                        help='Run one isolated lifecycle=watch reconciliation pass')
    args = parser.parse_args()

    if args.due:
        results = run_due_refreshes()
        if results:
            detail = ', '.join(f"{name}={status}" for name, status in results.items())
            print(f"[refresh] {detail}", file=sys.stderr)
        return

    if args.watches:
        results = run_due_watches()
        if results:
            detail = ', '.join(f"{name}={status}" for name, status in results.items())
            print(f"[watch] {detail}", file=sys.stderr)
        return

    # Parse --since
    since_days = None
    if args.since:
        since_days = int(args.since.strip().lower().rstrip('d'))

    # Discover or filter cells
    if args.cells:
        cell_names = _parse_cells_arg(args.cells)
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
    lease = None
    if not args.dry_run:
        from flex.admission import try_heavy_lease
        lease = try_heavy_lease(
            detail="refresh CLI: " + ",".join(cell_names),
            timeout_s=_heavy_admission_wait_s(),
        )
        if not lease.acquired:
            print("[refresh] Deferred: semantic work already active; timer will retry.",
                  file=sys.stderr)
            return
    try:
        for cell_name in cell_names:
            print(f"{'=' * 50}", file=sys.stderr)
            stats = refresh_cell(
                cell_name,
                graph=args.graph,
                dry_run=args.dry_run,
                since_days=since_days,
                _heavy_admitted=bool(lease and lease.acquired),
            )
            results[cell_name] = stats
    finally:
        if lease is not None:
            lease.close()

    elapsed = time.time() - t_total
    print(f"\n{'=' * 50}", file=sys.stderr)
    print(f"Total: {elapsed:.1f}s across {len(cell_names)} cells", file=sys.stderr)

    # Summary
    any_hooked = False
    for name, stats in results.items():
        if stats is None:
            print(f"  {name:15s} ERROR", file=sys.stderr)
        elif stats.get('deferred'):
            print(f"  {name:15s} DEFERRED (semantic work active)", file=sys.stderr)
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
