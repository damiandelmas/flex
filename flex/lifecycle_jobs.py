"""Scoped provider jobs. Scheduling lives in lifecycle_worker, never here.

Each job runs in a disposable process, uses the common physical-cell lease,
and delegates interpretation and publication to the existing provider writer.
"""
from __future__ import annotations

import importlib
import inspect
import json
import os
import sqlite3
import sys
import time
from pathlib import Path


DOCUMENT_TYPES = {"filesystem", "code", "docpac", "markdown", "obsidian"}
SESSION_TYPES = {"claude_code", "claude-code", "codex", "goose", "pi"}
# Document cells that may hold NULL vectors in _raw_chunks; _semantic admits one
# only if it actually has the table, the column and a vector tag.
SEMANTIC_DOCUMENT_TYPES = {"filesystem", "markdown", "docpac"}
SEMANTIC_TYPES = SESSION_TYPES | SEMANTIC_DOCUMENT_TYPES
SEMANTIC_BUDGET_S = 20
SEMANTIC_MORE_EXIT = 76     # budget spent with debt left: run again soon
ENRICHMENT_INTERVAL_S = 86400


def _ext():
    """Optional extension point: extra cell types, reconcilers and a
    post-job hook. When no extension is installed each use below is a no-op."""
    try:
        from flex.modules import lifecycle_ext
    except ImportError:
        return None
    return lifecycle_ext


_X = _ext()
_EXTRA_DOCS = set(getattr(_X, "DOCUMENT_TYPES", ()))
DOCUMENT_TYPES |= _EXTRA_DOCS
SEMANTIC_DOCUMENT_TYPES |= _EXTRA_DOCS
SEMANTIC_TYPES |= _EXTRA_DOCS
LEGACY_NAMES = {"claude_code"} | set(getattr(_X, "LEGACY_NAMES", ()))


def local_cells(cells):
    """Rootless external publishers are deliberately not local compilers.

    Only the two named historical Claude rows receive legacy enrollment; a
    static clone of a session database must not accidentally start ingesting.
    Listing is a discovery presentation setting, not source-update authority.
    """
    return [c for c in cells if c.get("active", 1) and (
        (c.get("lifecycle") == "watch" and (
            (c.get("cell_type") in DOCUMENT_TYPES | SESSION_TYPES
             and (c.get("watch_path") or c.get("corpus_path")))
            or c.get("refresh_module") or c.get("refresh_script")
        )) or c.get("name") in LEGACY_NAMES
    )]


def _connection(cell):
    db = sqlite3.connect(cell["path"], timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA synchronous=NORMAL")  # WAL is already on; fewer fsyncs per commit
    return db


def _module(cell, *, semantic=False):
    """Module owns its signature short circuit; no parent-process probe imports."""
    from flex.refresh import _is_safe_refresh_module
    module = cell.get("refresh_module")
    if not module:
        raise RuntimeError(f"No provider adapter for {cell['name']}")
    safe, reason = _is_safe_refresh_module(module)
    if not safe:
        raise RuntimeError(reason)
    from flex.registry import load_plugins
    load_plugins()
    from flex.modules.specs import discover_install_modules
    discover_install_modules()
    mod = importlib.import_module(module)
    fn = mod.refresh
    kwargs = {"graph": False, "dry_run": False}
    if "semantic" in inspect.signature(fn).parameters:
        kwargs["semantic"] = semantic
    stats = fn(cell["path"], **kwargs)
    if stats is None:
        raise RuntimeError("Provider returned no completion")
    if stats.get("skipped") in {"source missing", "stat failed"}:
        raise RuntimeError(stats["skipped"])
    return stats


def _reconcile(cell, operation):
    name, kind = cell["name"], cell.get("cell_type")
    root = cell.get("watch_path") or cell.get("corpus_path")
    if root and not Path(root).expanduser().exists():
        raise FileNotFoundError(f"Declared source missing: {root}")
    if _X is not None:
        stats = _X.reconcile(cell, operation)
        if stats is not None:
            return stats
    if kind in {"claude_code", "claude-code"}:
        from flex.modules.claude_code.compile.worker import scan_sessions
        with _connection(cell) as db:
            stats = scan_sessions(db, {}, {})
        if stats.get("failed") or stats.get("backoff_pending"):
            raise RuntimeError(f"Claude reconciliation incomplete: {stats}")
        return stats
    if kind == "codex":
        from flex.modules.codex.compile.worker import scan_codex_cells
        return scan_codex_cells(
            deadline=time.time() + (2 if operation == "tick" else 20),
            embed=False, discover=operation != "tick", cell_names={name},
        )
    if kind == "filesystem":
        from flex.modules.fs.compile.worker import scan_filesystem_cells
        return scan_filesystem_cells(cell_names={name})
    if kind == "code":
        from flex.modules.fs.compile.index_code import scan_code_cells
        return scan_code_cells({}, cell_names={name})
    if kind in {"docpac", "markdown", "obsidian"}:
        from flex.modules.markdown.compile.profile import profile_name_for_cell
        profile = profile_name_for_cell(cell)
        if profile in {"markdown", "obsidian"}:
            from flex.modules.markdown.compile.worker import scan_markdown_cells
            return scan_markdown_cells(cell_names={name})
        from flex.modules.docpac.compile.worker import scan_docpac_cells
        return scan_docpac_cells(None, {}, cell_names={name})
    if cell.get("refresh_script"):
        from flex.refresh import _refresh_cell
        stats = _refresh_cell(name, quiet=True)
        if stats is None:
            raise RuntimeError("Provider script failed")
        return stats
    return _module(cell)


def _events(cell, paths):
    kind = cell.get("cell_type")
    if kind in {"claude_code", "claude-code"} and cell["name"] != "claude_chat":
        from flex.modules.claude_code.compile.worker import sync_session_paths
        with _connection(cell) as db:
            stats = sync_session_paths(db, paths, {}, {})
        if stats.get("failed") or stats.get("backoff_pending"):
            raise RuntimeError(f"Claude events incomplete: {stats}")
        return stats
    if kind not in DOCUMENT_TYPES | {"codex", "goose", "pi"}:
        # Derived modules understand complete input signatures, not source paths.
        return _reconcile(cell, "reconcile")
    from flex.watch import Invalidation
    invalidations = [
        Invalidation(cell["name"], Path(p), time.monotonic(), "modified")
        for p in paths
    ]
    if kind == "filesystem":
        # The public Filesystem compiler must not depend on optional
        # engine glue being installed merely to consume an exact path event.
        from flex.modules.fs.compile.worker import drain_filesystem_invalidations
        stats = drain_filesystem_invalidations(invalidations)
    else:
        try:
            from flex.modules.engines import drain_corpus_paths
        except ImportError:
            return _reconcile(cell, "reconcile")
        stats = drain_corpus_paths(invalidations)
    if stats.get("failed") or stats.get("deferred"):
        raise RuntimeError("Provider event batch incomplete")
    return {k: v for k, v in stats.items() if k not in {"deferred", "failed_events"}}


def _embeddable(db, cell):
    """True when the cell has NULL-able chunk vectors this job may fill.

    Session cells predate vector tags and are stamped on first write. A document
    cell without a tag has no declared vector space, so it is left alone.
    """
    columns = {r[1] for r in db.execute("PRAGMA table_info(_raw_chunks)")}
    if not {"embedding", "content", "timestamp"} <= columns:
        return False
    if cell.get("cell_type") in SESSION_TYPES:
        return True
    from flex.core import get_meta
    return bool(get_meta(db, "vec:model"))


def _semantic(cell):
    from flex.admission import try_heavy_lease
    with try_heavy_lease(detail=f"lifecycle semantic:{cell['name']}") as lease:
        if not lease.acquired:
            return {"deferred": True}
        from flex.modules.claude_code.compile.worker import _batch_embed_chunks
        from flex.core import get_meta, set_meta
        with _connection(cell) as db:
            if not _embeddable(db, cell):
                return {"skipped": "no embeddable chunks"}
            if get_meta(db, "embed") in {"false", "0"}:
                return {"skipped": "embed disabled"}
            try:
                # Must stay in step with the selection query in
                # _batch_embed_chunks or the planner will not use it.
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_raw_chunks_null_embedding "
                    "ON _raw_chunks(timestamp) "
                    "WHERE embedding IS NULL AND content IS NOT NULL"
                )
                # Document cells were created without it; newest-row and lag
                # queries otherwise scan the whole table.
                db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_raw_chunks_timestamp "
                    "ON _raw_chunks(timestamp)"
                )
                db.commit()
            except sqlite3.OperationalError as exc:
                print(f"[lifecycle-job] {cell['name']}: null-embedding index skipped: {exc}",
                      file=sys.stderr)
            embedded = _batch_embed_chunks(
                db, batch_size=64, quiet=True, deadline=time.time() + SEMANTIC_BUDGET_S,
            )
            db.commit()
            # Debt left after real progress: the scheduler should come back soon.
            more = embedded > 0 and db.execute(
                "SELECT 1 FROM _raw_chunks WHERE embedding IS NULL "
                "AND content IS NOT NULL LIMIT 1"
            ).fetchone() is not None
            # Graph/fingerprint/source-pooling work is admitted here, not on
            # structural capture, and waits until the backlog is drained.
            if not more and cell.get("cell_type") in SESSION_TYPES:
                last = float(get_meta(db, "last_enrichment_ts") or 0)
                if time.time() - last >= ENRICHMENT_INTERVAL_S:
                    from flex.modules.claude_code import run_enrichment
                    # Stamp the attempt first: a run killed by the child timeout
                    # must wait its interval, not retake the semantic turn forever.
                    set_meta(db, "last_enrichment_ts", str(time.time()))
                    db.commit()
                    run_enrichment(db, cell_type=cell.get("cell_type"))
                    db.commit()
            return {"embedded": embedded, "more": more}


def execute_job(name, operation="reconcile", paths=(), *, admitted=False):
    from flex.admission import try_cell_lease
    from flex.registry import (
        get_cell_metadata, mark_refresh_started, mark_refresh_failed,
        mark_refresh_committed,
    )
    cell = get_cell_metadata(name)
    if not cell or not cell.get("active", 1):
        return {"skipped": "inactive or missing"}
    if not Path(cell["path"]).is_file():
        mark_refresh_failed(name, f"Database missing: {cell['path']}", pending=1)
        raise FileNotFoundError(cell["path"])
    from contextlib import nullcontext
    from types import SimpleNamespace
    admission = nullcontext(SimpleNamespace(acquired=True)) if admitted else try_cell_lease(cell["path"])
    with admission as lease:
        if not lease.acquired:
            return {"deferred": True}
        if operation == "semantic":
            # Semantic availability is not structural freshness.
            return _semantic(cell)
        if operation == "remote":
            from flex.refresh import refresh_cell
            stats = refresh_cell(name, quiet=True, _cell_admitted=True)
            if stats is None:
                raise RuntimeError("Remote refresh failed")
            return stats
        if operation == "tick":
            # The provider already commits changed append generations. A warm
            # no-op stat check must not manufacture a Registry generation or
            # erase full-source debt on every two-second heartbeat.
            try:
                stats = _reconcile(cell, operation)
            except Exception as exc:
                mark_refresh_failed(name, str(exc), pending=1)
                raise
            pending = max(int(stats.get("reconciliation_deferred") or 0),
                          int(stats.get("deadline_hit") or 0),
                          int(cell.get("refresh_pending") or 0) if cell.get("reconciliation_required") else 0)
            if pending:
                mark_refresh_started(name, pending=pending, reconciliation_required=True)
            return {**stats, "pending": pending}
        mark_refresh_started(name, pending=1,
                             reconciliation_required=True if operation == "reconcile" else None)
        try:
            stats = _events(cell, paths) if operation == "events" else _reconcile(cell, operation)
            current = get_cell_metadata(name)
            if str(current.get("refresh_status") or "").startswith("error"):
                raise RuntimeError(current.get("refresh_error") or current["refresh_status"])
            pending = max(int(stats.get("pending") or 0),
                          int(stats.get("reconciliation_deferred") or 0),
                          int(stats.get("deadline_hit") or 0))
            if cell.get("cell_type") in DOCUMENT_TYPES:
                # These adapters own scoped completion receipts. Never turn a
                # partial legacy scanner into a clean full reconciliation.
                pending = max(pending, int(current.get("refresh_pending") or 0))
                if operation == "reconcile" and current.get("reconciliation_required"):
                    pending = max(1, pending)
            if operation != "reconcile" and cell.get("reconciliation_required"):
                pending = max(1, pending)
            if pending:
                mark_refresh_started(name, pending=pending, reconciliation_required=True)
            else:
                mark_refresh_committed(
                    name, source_signature=stats.get("source_signature") or current.get("source_signature"),
                    source_high_water=stats.get("source_high_water") or current.get("source_high_water"),
                )
            _after_job(name, stats)
            return {**stats, "pending": pending}
        except Exception as exc:
            mark_refresh_failed(name, str(exc), pending=1)
            raise


def _after_job(name, stats):
    """Optional post-job extension; it must never fail the job."""
    if _X is not None:
        _X.after_job(name, stats)


def main():
    os.environ["FLEX_EMBED_SERVICE_ONLY"] = "1"
    payload = json.load(sys.stdin)
    try:
        result = execute_job(payload["cell"], payload["operation"], payload.get("paths", ()))
    except Exception as exc:
        print(f"[lifecycle-job] {payload['cell']}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, default=str))
    if result.get("deferred"):
        return 75
    return SEMANTIC_MORE_EXIT if result.get("more") else 0


if __name__ == "__main__":
    raise SystemExit(main())
