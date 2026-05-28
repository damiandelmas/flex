"""Cell distribution — public download commands plus private publish hooks."""

from flex.registry import register_hook
from flex.distribute.manifest import fetch_manifest, diff_manifest, download_cell


def _push_api():
    """Return private publisher functions when the private package is present."""
    try:
        from flex.distribute.push import push_cell, push_manifest
    except ImportError:
        return None, None
    return push_cell, push_manifest


def _post_refresh_cell_hook(cell_name):
    push_cell, _ = _push_api()
    if push_cell is None:
        return
    push_cell(cell_name)


def _post_refresh_hook():
    _, push_manifest = _push_api()
    if push_manifest is None:
        return
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


def _daemon_tick():
    from flex.registry import list_cells, register_cell, CELLS_DIR
    local = list_cells()
    installed_remote = [c for c in local if c.get("source_url")]
    if not installed_remote:
        return
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
                    name=name, path=str(dest),
                    checksum=entry.checksum, source_url=entry.url,
                )
            except Exception:
                pass


def _register_cli_commands(sub):
    """Register catalog/add/update CLI subcommands."""
    import sys

    def cmd_catalog(args):
        from flex.registry import list_cells
        remote = fetch_manifest()
        local = list_cells()
        diffs = diff_manifest(remote, local)
        if args.json:
            import json
            out = {n: {"url": e.url, "size": e.size, "status": diffs.get(n, "new")}
                   for n, e in remote.items()}
            print(json.dumps(out, indent=2))
            return
        for name in sorted(remote):
            entry = remote[name]
            status = diffs.get(name, "new")
            print(f"  {name:<20s} {status}")

    def cmd_add(args):
        from flex.registry import register_cell, CELLS_DIR
        remote = fetch_manifest()
        names = list(remote.keys()) if args.all else args.cells
        for name in names:
            entry = remote.get(name)
            if not entry: continue
            dest = download_cell(entry, CELLS_DIR)
            register_cell(name=name, path=str(dest), cell_type=entry.cell_type,
                          source_url=entry.url, checksum=entry.checksum)
            print(f"  {name}: installed")

    def cmd_update(args):
        from flex.registry import list_cells, register_cell, CELLS_DIR
        remote = fetch_manifest()
        local = list_cells()
        installed = [c for c in local if c.get("source_url")]
        diffs = diff_manifest(remote, installed)
        for name, status in diffs.items():
            if status == "stale":
                entry = remote.get(name)
                if entry:
                    dest = download_cell(entry, CELLS_DIR)
                    register_cell(name=name, path=str(dest), source_url=entry.url, checksum=entry.checksum)
                    print(f"  {name}: updated")

    cat_p = sub.add_parser("catalog", help="List available cells")
    cat_p.add_argument("--json", action="store_true")
    cat_p.set_defaults(func=cmd_catalog)

    add_p = sub.add_parser("add", help="Install cells")
    add_p.add_argument("cells", nargs="*")
    add_p.add_argument("--all", action="store_true")
    add_p.set_defaults(func=cmd_add)

    upd_p = sub.add_parser("update", help="Refresh installed cells")
    upd_p.add_argument("cells", nargs="*", default=None)
    upd_p.set_defaults(func=cmd_update)


# Register public download hooks on import. Publisher hooks are registered only
# in private builds that include flex.distribute.push.
_push_cell, _push_manifest = _push_api()
if _push_cell is not None and _push_manifest is not None:
    register_hook("post_refresh_cell_hook", _post_refresh_cell_hook)
    register_hook("post_refresh_hook", _post_refresh_hook)
register_hook("daemon_tick", _daemon_tick)
register_hook("register_cli_commands", _register_cli_commands)
