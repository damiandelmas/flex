"""Cell distribution — download commands plus optional publish hooks."""

import os
import sys

from flex.registry import register_hook
from flex.hub.manifest import fetch_manifest, diff_manifest, download_cell


def _push_api():
    """Return publisher functions when publishing support is installed."""
    try:
        from flex.hub.push import push_cell, push_manifest, push_all
    except ImportError:
        return None, None, None
    return push_cell, push_manifest, push_all


def _post_refresh_cell_hook(cell_name):
    push_cell, _, _ = _push_api()
    if push_cell is None:
        return
    # Auto-push runs inside the refresh loop, whose generic catch would swallow
    # a bare EnvironmentError as an opaque failure. Skip cleanly when the bucket
    # is unconfigured instead of raising into that catch.
    if not os.environ.get("FLEX_R2_BUCKET"):
        print("[hub] FLEX_R2_BUCKET not set — skipping hub push", file=sys.stderr)
        return
    push_cell(cell_name)


def _post_refresh_hook():
    _, push_manifest, _ = _push_api()
    if push_manifest is None:
        return
    if not os.environ.get("FLEX_R2_BUCKET"):
        print("[hub] FLEX_R2_BUCKET not set — skipping hub push", file=sys.stderr)
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


# `flex hub view|pull|push|status` is registered directly in flex/cli.py
# (cmd_hub() and friends), mirroring the `soma`/`module` subparser groups —
# no dynamic register_cli_commands hook needed here. This module only
# registers the non-interactive hooks below: daemon refresh + publish-on-
# refresh hooks. Publisher hooks are registered only when publishing
# support (flex.hub.push) is installed.
_push_cell, _push_manifest, _push_all = _push_api()
if _push_cell is not None and _push_manifest is not None:
    register_hook("post_refresh_cell_hook", _post_refresh_cell_hook)
    register_hook("post_refresh_hook", _post_refresh_hook)
register_hook("daemon_tick", _daemon_tick)
