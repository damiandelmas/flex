"""Single refresh owner for `cell_type=filesystem` cells."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

from flex.modules.fs.compile.index import delete_source, index_file
from flex.modules.fs.compile.schema import ensure_schema
from flex.modules.fs.compile.walker import entry_for_path, walk_files


class FilesystemRefreshError(RuntimeError):
    pass


_process_cache: dict[str, dict[str, str]] = {}


def _bool_meta(conn, key: str, default: bool = False) -> bool:
    row = conn.execute("SELECT value FROM _meta WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    return str(row[0]).strip().lower() in {"1", "true", "yes", "on"}


def _json_meta(conn, key: str, default):
    row = conn.execute("SELECT value FROM _meta WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        value = json.loads(row[0])
    except (TypeError, ValueError):
        return default
    return value


def _signature(entry) -> str:
    return f"{entry.size_bytes}:{entry.mtime_ns}"


def _empty_stats() -> dict[str, int]:
    return {"indexed": 0, "empty": 0, "unchanged": 0, "skipped": 0, "deleted": 0}


def reconcile_cell(conn: sqlite3.Connection, root: Path, *, embed_fn=None,
                   process_cache: dict[str, str] | None = None,
                   obsidian: bool | None = None, exclude=(),
                   file_kinds: tuple[str, ...] | None = None) -> dict[str, int]:
    """Converge one cell with disk; durable source state outranks process caches."""
    ensure_schema(conn)
    conn.commit()
    root = Path(root).expanduser().resolve()
    cache = process_cache if process_cache is not None else {}
    use_obsidian = _bool_meta(conn, "obsidian") if obsidian is None else obsidian
    entries = [
        entry for entry in walk_files(root, exclude=tuple(exclude))
        if file_kinds is None or entry.file_kind in file_kinds
    ]
    seen = {entry.source_id for entry in entries}
    durable = {
        source_id: (int(size_bytes), int(mtime_ns))
        for source_id, size_bytes, mtime_ns in conn.execute(
            "SELECT source_id,size_bytes,mtime_ns FROM _filesystem_source_state"
        ).fetchall()
    }
    stats = _empty_stats()
    failures = []

    for entry in entries:
        sig = _signature(entry)
        # A missing process cache entry means process restart: hash through the
        # writer even when size/mtime equals durable state, so offline edits are
        # never blessed as a new in-memory baseline.
        if (cache.get(entry.source_id) == sig
                and durable.get(entry.source_id) == (entry.size_bytes, entry.mtime_ns)):
            stats["skipped"] += 1
            continue
        try:
            outcome = index_file(
                conn, entry, embed_fn=embed_fn, obsidian=use_obsidian,
            )
            stats[outcome.status] += 1
            cache[entry.source_id] = sig
        except Exception as exc:
            failures.append(f"{entry.rel_path}: {exc}")

    known = conn.execute(
        "SELECT source_id,source_path FROM _filesystem_source_state"
    ).fetchall()
    for source_id, source_path in known:
        if source_id in seen:
            continue
        candidate = Path(source_path)
        if candidate.exists():
            # A path that still exists but vanished from discovery may have
            # become unsupported, or it may merely be unreadable. Only the
            # former is a successful removal; read failures retain last-good.
            try:
                with candidate.open("rb") as handle:
                    handle.read(1)
            except OSError as exc:
                failures.append(f"{source_id}: {exc}")
                continue
        try:
            if delete_source(conn, source_id, obsidian=use_obsidian):
                stats["deleted"] += 1
            cache.pop(source_id, None)
        except Exception as exc:
            failures.append(f"{source_id}: {exc}")

    if failures:
        raise FilesystemRefreshError("; ".join(failures))
    return stats


def _relative_source(root: Path, path: Path) -> str | None:
    root = root.resolve()
    try:
        candidate = path.expanduser().resolve()
        rel = candidate.relative_to(root).as_posix()
    except (OSError, ValueError):
        return None
    return unicodedata.normalize("NFD", rel)


def drain_paths(conn: sqlite3.Connection, root: Path, paths, *, embed_fn=None,
                obsidian: bool | None = None, exclude=(),
                file_kinds: tuple[str, ...] | None = None) -> dict[str, int]:
    """Apply event-selected paths through the same validating atomic writer."""
    ensure_schema(conn)
    conn.commit()
    root = Path(root).expanduser().resolve()
    use_obsidian = _bool_meta(conn, "obsidian") if obsidian is None else obsidian
    stats = {"indexed": 0, "empty": 0, "unchanged": 0, "skipped": 0, "deleted": 0}
    failures = []
    for raw_path in paths:
        path = Path(raw_path)
        source_id = _relative_source(root, path)
        if source_id is None:
            stats["skipped"] += 1
            continue
        entry = entry_for_path(root, path, exclude=tuple(exclude))
        if entry is not None and file_kinds is not None and entry.file_kind not in file_kinds:
            entry = None
        if entry is None:
            # A delete or supported->unsupported transition can arrive without
            # a discoverable file. Only delete if this exact source was known.
            if not path.exists() and delete_source(conn, source_id, obsidian=use_obsidian):
                stats["deleted"] += 1
                continue
            stats["skipped"] += 1
            continue
        try:
            outcome = index_file(
                conn, entry, embed_fn=embed_fn, obsidian=use_obsidian,
            )
            stats[outcome.status] += 1
        except Exception as exc:
            failures.append(f"{entry.rel_path}: {exc}")
    if failures:
        raise FilesystemRefreshError("; ".join(failures))
    return stats


def _state_signature(conn: sqlite3.Connection) -> tuple[str, str | None]:
    digest = hashlib.sha256()
    high_water = 0
    for source_id, content_hash, mtime_ns in conn.execute(
        "SELECT source_id,content_hash,mtime_ns FROM _filesystem_source_state "
        "ORDER BY source_id"
    ).fetchall():
        digest.update(source_id.encode())
        digest.update(content_hash.encode())
        high_water = max(high_water, int(mtime_ns or 0))
    return f"sha256:{digest.hexdigest()}", str(high_water) if high_water else None


def scan_filesystem_cells() -> dict[str, int]:
    """Reconcile every active watched filesystem cell and own freshness receipts."""
    from flex.registry import (
        list_cells, mark_refresh_committed, mark_refresh_failed, mark_refresh_started,
    )

    total = _empty_stats()
    cells = [
        cell for cell in list_cells()
        if cell.get("cell_type") == "filesystem"
        and cell.get("lifecycle") == "watch"
        and cell.get("active", 1)
    ]
    for cell in cells:
        root_value = cell.get("watch_path") or cell.get("corpus_path")
        if not root_value:
            continue
        name = cell["name"]
        mark_refresh_started(name)
        conn = sqlite3.connect(cell["path"], timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            exclude = _json_meta(conn, "exclude", [])
            kinds_value = _json_meta(conn, "file_kinds", None)
            file_kinds = tuple(kinds_value) if isinstance(kinds_value, list) else None
            cache = _process_cache.setdefault(name, {})
            stats = reconcile_cell(
                conn, Path(root_value), process_cache=cache, exclude=exclude,
                file_kinds=file_kinds,
            )
            signature, high_water = _state_signature(conn)
            mark_refresh_committed(
                name, source_signature=signature, source_high_water=high_water,
            )
            for key in total:
                total[key] += stats[key]
        except Exception as exc:
            mark_refresh_failed(name, str(exc))
        finally:
            conn.close()
    return total


def drain_filesystem_invalidations(invalidations) -> dict[str, int]:
    """Apply watcher invalidations for public filesystem cells only."""
    from flex.registry import (
        list_cells, mark_refresh_committed, mark_refresh_failed, mark_refresh_started,
    )

    grouped = defaultdict(list)
    for invalidation in invalidations:
        grouped[invalidation.cell_name].append(Path(invalidation.source_path))
    cells = {
        cell["name"]: cell for cell in list_cells()
        if cell.get("cell_type") == "filesystem"
        and cell.get("lifecycle") == "watch"
        and cell.get("active", 1)
    }
    total = {"indexed": 0, "skipped": 0, "failed": 0}
    for name, paths in grouped.items():
        cell = cells.get(name)
        if not cell:
            total["skipped"] += len(paths)
            continue
        root_value = cell.get("watch_path") or cell.get("corpus_path")
        if not root_value:
            total["skipped"] += len(paths)
            continue
        mark_refresh_started(name, pending=len(paths))
        conn = sqlite3.connect(cell["path"], timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            exclude = _json_meta(conn, "exclude", [])
            kinds_value = _json_meta(conn, "file_kinds", None)
            file_kinds = tuple(kinds_value) if isinstance(kinds_value, list) else None
            stats = drain_paths(
                conn, Path(root_value), paths, embed_fn=None,
                exclude=exclude, file_kinds=file_kinds,
            )
            changed = stats["indexed"] + stats["empty"] + stats["deleted"]
            total["indexed"] += changed
            total["skipped"] += stats["skipped"] + stats["unchanged"]
            signature, high_water = _state_signature(conn)
            mark_refresh_committed(
                name, source_signature=signature, source_high_water=high_water,
            )
        except Exception as exc:
            conn.rollback()
            total["failed"] += len(paths)
            mark_refresh_failed(name, str(exc), pending=len(paths))
        finally:
            conn.close()
    return total


def daemon_loop(interval: float = 2, *, invalidation_queue=None, watcher=None,
                reconcile_interval: float | None = None) -> None:
    """Run filesystem refresh when no coding-session worker owns the daemon."""
    cadence = reconcile_interval or float(
        os.environ.get("FLEX_CORPUS_RECONCILE_INTERVAL_S", "45")
    )
    last_reconcile = 0.0
    while True:
        if invalidation_queue is not None:
            try:
                ready = invalidation_queue.drain_ready(time.monotonic())
                if ready:
                    drain_filesystem_invalidations(ready)
            except Exception as exc:
                print(f"[filesystem-worker] event drain: {exc}", file=sys.stderr)
        due = (
            time.monotonic() - last_reconcile >= cadence
            or (invalidation_queue is not None
                and invalidation_queue.reconciliation_required())
            or (watcher is not None and not watcher.healthy)
        )
        if due:
            try:
                scan_filesystem_cells()
                last_reconcile = time.monotonic()
                if invalidation_queue is not None:
                    invalidation_queue.clear_reconciliation_required()
            except Exception as exc:
                print(f"[filesystem-worker] reconcile: {exc}", file=sys.stderr)
        time.sleep(interval)
