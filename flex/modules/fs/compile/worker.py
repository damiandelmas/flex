"""Single refresh owner for `cell_type=filesystem` cells."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

from flex.modules.fs.compile.extract import ExtractedChunk, ExtractionResult, extract_file
from flex.modules.fs.compile.index import (
    SidecarCallback, _compute_chunk_embeddings, _pool_source_embedding,
    _resolve_embedder, apply_result, delete_source, index_file, refresh_wikilinks,
)
from flex.modules.fs.compile.schema import (
    DOCUMENT_PROFILE_VERSION, DocumentProfileState, ensure_schema,
    ensure_document_identity_contract, ensure_document_vector_contract,
    validate_document_profile,
)
from flex.modules.fs.compile.walker import entries_for_paths, entry_for_path, walk_files


class FilesystemRefreshError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExplicitCandidateResult:
    stats: dict[str, int]
    profile: DocumentProfileState


@dataclass
class _PendingCandidateDocument:
    result: ExtractionResult
    expected_generation: int
    chunk_embeddings: list[bytes | None]
    scheduled_complete: bool = False


DEFAULT_CANDIDATE_EMBED_CHUNKS = 512


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


def _latch_adapter(conn: sqlite3.Connection):
    """Load an optional provider-owned repository-latch adapter.

    The public filesystem compiler has no knowledge of a workspace-specific
    manifest grammar. A private provider can opt in by setting
    ``FLEX_FILESYSTEM_LATCH_ADAPTER`` to an importable module that implements
    the small adapter methods used below.
    """
    module_name = os.environ.get("FLEX_FILESYSTEM_LATCH_ADAPTER")
    if not module_name:
        return None
    try:
        import importlib
        adapter = importlib.import_module(module_name)
    except (ImportError, ValueError):
        return None
    return adapter if adapter.repository_latch_enabled(conn) else None


def _record_latch_publications(stats: dict[str, int], publications) -> None:
    """Fold provider-sidecar work into existing aggregate worker counters."""
    for publication in publications:
        if publication.changed:
            stats["indexed"] += 1
        else:
            stats["unchanged"] += 1


def reconcile_cell(conn: sqlite3.Connection, root: Path, *, embed_fn=None,
                   process_cache: dict[str, str] | None = None,
                   obsidian: bool | None = None, exclude=(),
                   file_kinds: tuple[str, ...] | None = None,
                   sidecar_callback: SidecarCallback | None = None,
                   defer_wikilinks: bool = False) -> dict[str, int]:
    """Converge one cell with disk; durable source state outranks process caches."""
    ensure_schema(conn)
    conn.commit()
    root = Path(root).expanduser().resolve()
    if (root / ".publishing").exists():
        return _empty_stats()
    cache = process_cache if process_cache is not None else {}
    use_obsidian = _bool_meta(conn, "obsidian") if obsidian is None else obsidian
    entries = [
        entry for entry in walk_files(root, exclude=tuple(exclude))
        if file_kinds is None or entry.file_kind in file_kinds
    ]
    latch_adapter = _latch_adapter(conn)
    if latch_adapter is not None:
        entries = [
            entry for entry in entries
            if not latch_adapter.is_repository_manifest_path(root, entry.path)
        ]
    seen = {entry.source_id for entry in entries}
    durable = {
        source_id: (
            int(size_bytes), int(mtime_ns), int(profile_version),
            int(accepted_generation),
        )
        for (source_id, size_bytes, mtime_ns, profile_version,
             accepted_generation) in conn.execute(
            "SELECT source_id,size_bytes,mtime_ns,profile_version,accepted_generation "
            "FROM _filesystem_source_state"
        ).fetchall()
    }
    stats = _empty_stats()
    failures = []
    batch_wikilinks = defer_wikilinks and sidecar_callback is None
    wikilinks_dirty = False

    for entry in entries:
        sig = _signature(entry)
        # A missing process cache entry means process restart: hash through the
        # writer even when size/mtime equals durable state, so offline edits are
        # never blessed as a new in-memory baseline.
        if (sidecar_callback is None
                and cache.get(entry.source_id) == sig
                and durable.get(entry.source_id, ())[:3] == (
                    entry.size_bytes, entry.mtime_ns, DOCUMENT_PROFILE_VERSION,
                )):
            stats["skipped"] += 1
            continue
        try:
            outcome = index_file(
                conn, entry, embed_fn=embed_fn, obsidian=use_obsidian,
                sidecar_callback=sidecar_callback,
                _defer_wikilinks=batch_wikilinks,
            )
            stats[outcome.status] += 1
            if (batch_wikilinks and entry.file_kind == "markdown"
                    and outcome.status != "unchanged"):
                wikilinks_dirty = True
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
            if delete_source(
                conn, source_id, obsidian=use_obsidian,
                sidecar_callback=sidecar_callback,
                expected_generation=durable[source_id][3],
                _defer_wikilinks=batch_wikilinks,
            ):
                stats["deleted"] += 1
                if batch_wikilinks:
                    wikilinks_dirty = True
            cache.pop(source_id, None)
        except Exception as exc:
            failures.append(f"{source_id}: {exc}")

    if wikilinks_dirty:
        try:
            refresh_wikilinks(conn)
        except Exception as exc:
            failures.append(f"wikilinks: {exc}")
    if latch_adapter is not None:
        try:
            latch_result = latch_adapter.reconcile_repository_latches(conn, root)
            _record_latch_publications(stats, latch_result.publications)
        except Exception as exc:
            failures.append(f"repository latches: {exc}")
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
                file_kinds: tuple[str, ...] | None = None,
                sidecar_callback: SidecarCallback | None = None) -> dict[str, int]:
    """Apply event-selected paths through the same validating atomic writer."""
    ensure_schema(conn)
    conn.commit()
    root = Path(root).expanduser().resolve()
    marker = root / ".publishing"
    paths = list(paths)
    if marker.exists():
        stats = _empty_stats()
        stats["skipped"] = len(paths)
        return stats
    if any(Path(path).name == marker.name for path in paths):
        # Marker removal is the commit event for a generated source snapshot.
        # Reconcile the complete tree through the canonical per-source writer
        # rather than trusting the watcher event subset.
        return reconcile_cell(
            conn, root, embed_fn=embed_fn, obsidian=obsidian,
            exclude=exclude, file_kinds=file_kinds,
            sidecar_callback=sidecar_callback,
        )
    latch_paths = []
    latch_adapter = _latch_adapter(conn)
    if latch_adapter is not None:
        latch_paths = [
            path for path in paths
            if latch_adapter.is_repository_manifest_path(root, path)
        ]
        paths = [path for path in paths if path not in latch_paths]
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
            if not path.exists() and delete_source(
                conn, source_id, obsidian=use_obsidian,
                sidecar_callback=sidecar_callback,
            ):
                stats["deleted"] += 1
                continue
            stats["skipped"] += 1
            continue
        try:
            outcome = index_file(
                conn, entry, embed_fn=embed_fn, obsidian=use_obsidian,
                sidecar_callback=sidecar_callback,
            )
            stats[outcome.status] += 1
        except Exception as exc:
            failures.append(f"{entry.rel_path}: {exc}")
    if latch_paths:
        for path in latch_paths:
            try:
                publication = latch_adapter.reconcile_repository_manifest(conn, root, path)
                _record_latch_publications(stats, (publication,))
            except Exception as exc:
                failures.append(f"{path}: {exc}")
    if failures:
        raise FilesystemRefreshError("; ".join(failures))
    return stats


def build_explicit_candidate(
    conn: sqlite3.Connection,
    root: Path,
    paths,
    *,
    embed_fn=None,
    embed_enabled: bool | None = None,
    obsidian: bool | None = None,
    exclude=(),
    file_kinds: tuple[str, ...] | None = ("markdown",),
    sidecar_callback: SidecarCallback | None = None,
    embed_batch_chunks: int = DEFAULT_CANDIDATE_EMBED_CHUNKS,
    expected_model: str = "nomic-v1.5-fp32",
    expected_model_fingerprint: str = "nomic-embed-text-v1.5-fp32",
    expected_storage_dim: int = 768,
    expected_serve_dim: int = 256,
) -> ExplicitCandidateResult:
    """Populate and validate one private candidate from only declared paths.

    ``paths`` is the complete authoritative source set.  It is validated in
    full before the first write; no directory walk, publication marker, or
    process-cache shortcut participates.  Changed documents share one
    length-sorted, bounded chunk-embedding window, then each complete document
    publishes through the ordinary atomic per-file transaction.  The window is
    an internal memory/throughput bound, never a publication cap or semantic
    debt queue: every window is exhausted before validation.  Unselected rows
    already in the private candidate are removed through the same transactional
    sidecar seam.  Corpus-level wikilink resolution runs once after those
    source-local facts publish and before validation; the candidate is never
    visible in the intermediate state.  If any source fails, the caller may
    resume or discard the still-inactive partial candidate, but cannot activate
    it.
    """
    ensure_schema(conn)
    conn.commit()
    root = Path(root).expanduser().resolve()
    try:
        entries = entries_for_paths(root, tuple(paths), exclude=tuple(exclude))
    except ValueError as exc:
        raise FilesystemRefreshError(str(exc)) from exc
    if file_kinds is not None:
        invalid = [entry.rel_path for entry in entries if entry.file_kind not in file_kinds]
        if invalid:
            raise FilesystemRefreshError(
                "explicit source kind is not admitted: " + ", ".join(invalid)
            )
    if (not isinstance(embed_batch_chunks, int)
            or isinstance(embed_batch_chunks, bool)
            or embed_batch_chunks < 1):
        raise FilesystemRefreshError("embed_batch_chunks must be a positive integer")

    enabled = _bool_meta(conn, "embed", True) if embed_enabled is None else embed_enabled
    ensure_document_identity_contract(conn)
    if enabled:
        from flex.compile.embed import ensure_initial_vector_contract
        ensure_initial_vector_contract(conn)
        ensure_document_vector_contract(conn)
    conn.commit()

    stats = _empty_stats()
    selected = {entry.source_id for entry in entries}
    use_obsidian = _bool_meta(conn, "obsidian") if obsidian is None else obsidian
    if not enabled:
        for entry in entries:
            try:
                outcome = index_file(
                    conn,
                    entry,
                    embed_fn=embed_fn,
                    embed_enabled=False,
                    obsidian=use_obsidian,
                    sidecar_callback=sidecar_callback,
                    _defer_wikilinks=True,
                )
            except Exception as exc:
                raise FilesystemRefreshError(f"{entry.rel_path}: {exc}") from exc
            stats[outcome.status] += 1
    else:
        candidate_embedder = embed_fn or _resolve_embedder(conn)
        pending_documents: deque[_PendingCandidateDocument] = deque()
        chunk_window: list[
            tuple[_PendingCandidateDocument, int, ExtractedChunk]
        ] = []

        def publish_ready_documents() -> None:
            while (pending_documents
                   and pending_documents[0].scheduled_complete
                   and all(
                       blob is not None
                       for blob in pending_documents[0].chunk_embeddings
                   )):
                pending = pending_documents.popleft()
                chunk_embeddings = [
                    blob for blob in pending.chunk_embeddings if blob is not None
                ]
                try:
                    outcome = apply_result(
                        conn,
                        pending.result,
                        chunk_embeddings=chunk_embeddings,
                        source_embedding=_pool_source_embedding(chunk_embeddings),
                        obsidian=use_obsidian,
                        sidecar_callback=sidecar_callback,
                        expected_generation=pending.expected_generation,
                        _defer_wikilinks=True,
                        _schema_ready=True,
                    )
                except Exception as exc:
                    raise FilesystemRefreshError(
                        f"{pending.result.source_path}: {exc}"
                    ) from exc
                stats[outcome.status] += 1

        def flush_chunk_window() -> None:
            if not chunk_window:
                return
            ordered = sorted(
                chunk_window,
                key=lambda item: (
                    len(item[2].content), item[0].result.source_id, item[1],
                ),
            )
            try:
                blobs = _compute_chunk_embeddings(
                    tuple(item[2] for item in ordered), candidate_embedder,
                )
            except Exception as exc:
                sources = sorted({item[0].result.source_id for item in ordered})
                raise FilesystemRefreshError(
                    "candidate embedding batch failed for " + ", ".join(sources)
                ) from exc
            for (pending, position, _chunk), blob in zip(ordered, blobs):
                pending.chunk_embeddings[position] = blob
            chunk_window.clear()
            publish_ready_documents()

        for entry in entries:
            result = extract_file(entry)
            if result.status == "failed":
                raise FilesystemRefreshError(
                    f"{entry.rel_path}: {result.error or 'extraction failed'}"
                )
            previous = conn.execute(
                "SELECT content_hash,file_kind,source_state,profile_version,"
                "accepted_generation FROM _filesystem_source_state WHERE source_id=?",
                (result.source_id,),
            ).fetchone()
            if previous and tuple(previous[:4]) == (
                result.content_hash, result.file_kind, result.status,
                DOCUMENT_PROFILE_VERSION,
            ):
                try:
                    outcome = index_file(
                        conn,
                        entry,
                        embed_fn=candidate_embedder,
                        embed_enabled=True,
                        obsidian=use_obsidian,
                        sidecar_callback=sidecar_callback,
                        _defer_wikilinks=True,
                    )
                except Exception as exc:
                    raise FilesystemRefreshError(f"{entry.rel_path}: {exc}") from exc
                stats[outcome.status] += 1
                continue

            expected_generation = int(previous[4]) if previous else 0
            if result.status == "empty":
                try:
                    outcome = apply_result(
                        conn,
                        result,
                        obsidian=use_obsidian,
                        sidecar_callback=sidecar_callback,
                        expected_generation=expected_generation,
                        _defer_wikilinks=True,
                        _schema_ready=True,
                    )
                except Exception as exc:
                    raise FilesystemRefreshError(f"{entry.rel_path}: {exc}") from exc
                stats[outcome.status] += 1
                continue

            pending = _PendingCandidateDocument(
                result=result,
                expected_generation=expected_generation,
                chunk_embeddings=[None] * len(result.chunks),
            )
            pending_documents.append(pending)
            for position, chunk in enumerate(result.chunks):
                chunk_window.append((pending, position, chunk))
                if len(chunk_window) >= embed_batch_chunks:
                    flush_chunk_window()
            pending.scheduled_complete = True
            publish_ready_documents()

        flush_chunk_window()
        publish_ready_documents()
        if pending_documents:
            raise FilesystemRefreshError(
                "candidate embedding loop ended with incomplete documents"
            )

    for (source_id,) in conn.execute(
        "SELECT source_id FROM _filesystem_source_state ORDER BY source_id"
    ).fetchall():
        if source_id in selected:
            continue
        try:
            if delete_source(
                conn,
                source_id,
                obsidian=_bool_meta(conn, "obsidian") if obsidian is None else obsidian,
                sidecar_callback=sidecar_callback,
                _defer_wikilinks=True,
            ):
                stats["deleted"] += 1
        except Exception as exc:
            raise FilesystemRefreshError(f"{source_id}: {exc}") from exc

    live = {row[0] for row in conn.execute(
        "SELECT source_id FROM _filesystem_source_state"
    )}
    if live != selected:
        missing = sorted(selected - live)
        unexpected = sorted(live - selected)
        raise FilesystemRefreshError(
            f"explicit candidate coverage mismatch: missing={missing}, "
            f"unexpected={unexpected}"
        )

    try:
        refresh_wikilinks(conn)
    except Exception as exc:
        raise FilesystemRefreshError(f"wikilinks: {exc}") from exc

    profile = validate_document_profile(
        conn,
        require_embeddings=enabled,
        expected_model=expected_model if enabled else None,
        expected_model_fingerprint=(
            expected_model_fingerprint if enabled else None
        ),
        expected_storage_dim=expected_storage_dim if enabled else None,
        expected_serve_dim=expected_serve_dim if enabled else None,
        require_identity=True,
    )
    return ExplicitCandidateResult(stats=stats, profile=profile)


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
    latch_adapter = _latch_adapter(conn)
    if latch_adapter is not None:
        latch = latch_adapter.validate_repository_latches(conn)
        digest.update(b"provider-repository-latch\0")
        digest.update(latch.latch_signature.encode())
    return f"sha256:{digest.hexdigest()}", str(high_water) if high_water else None


def _is_structural_profile(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM _meta WHERE key='profile'").fetchone()
    return bool(row and row[0] == "filesystem-structural")


def scan_filesystem_cells(cell_names=None) -> dict[str, int]:
    """Reconcile selected watched filesystem cells and own freshness receipts.

    ``cell_names`` is an exact optional scope used by reconciliation-debt
    scheduling. Broad roots run first so an authority such as a complete
    workspace view cannot sit behind one of its narrower consumers.
    """
    from flex.registry import (
        list_cells, mark_refresh_committed, mark_refresh_failed, mark_refresh_started,
    )

    total = _empty_stats()
    selected = set(cell_names) if cell_names is not None else None
    cells = [
        cell for cell in list_cells()
        if cell.get("cell_type") == "filesystem"
        and cell.get("lifecycle") == "watch"
        and cell.get("active", 1)
        and (selected is None or cell.get("name") in selected)
    ]
    cells.sort(key=lambda cell: (
        len(Path(cell.get("watch_path") or cell.get("corpus_path") or "/").parts),
        cell.get("name") or "",
    ))
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
            if _is_structural_profile(conn):
                from flex.modules.fs.compile.structural import reconcile_structural_cell
                stats = reconcile_structural_cell(conn, exclude=exclude)
            else:
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


def drain_reconciliation_debt(invalidation_queue) -> None:
    """Settle exact Filesystem debt and acknowledge only a clean receipt."""
    from flex.registry import list_cells

    before = {cell['name']: cell for cell in list_cells()}
    debt_cells = [
        name for name in invalidation_queue.reconciliation_debt_cells()
        if (before.get(name) or {}).get('cell_type') == 'filesystem'
    ]
    if not debt_cells:
        return
    debt_generations = {
        name: invalidation_queue.reconciliation_debt_generation(name)
        for name in debt_cells
    }
    print(
        f"[filesystem-worker] debt: reconciling {', '.join(sorted(debt_cells))}",
        file=sys.stderr,
    )
    scan_filesystem_cells(cell_names=debt_cells)

    after = {cell['name']: cell for cell in list_cells()}
    for name, debt_generation in debt_generations.items():
        previous = before.get(name)
        current = after.get(name)
        if debt_generation is None or previous is None or current is None:
            continue
        if int(current.get('refresh_generation') or 0) <= int(
            previous.get('refresh_generation') or 0
        ):
            continue
        if int(current.get('refresh_pending') or 0) != 0:
            continue
        if bool(current.get('reconciliation_required')):
            continue
        invalidation_queue.clear_reconciliation_required(
            name, through_generation=debt_generation,
        )
        print(
            f"[filesystem-worker] debt committed: {name} "
            f"generation={current.get('refresh_generation')}",
            file=sys.stderr,
        )


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
            if _is_structural_profile(conn):
                # A structural cell has a union of roots.  The event payload is
                # intentionally not trusted as its complete deletion authority;
                # reconcile its declared recipe through the canonical writer.
                from flex.modules.fs.compile.structural import reconcile_structural_cell
                stats = reconcile_structural_cell(conn, exclude=exclude)
            else:
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
                    from flex.registry import list_cells
                    code_names = {
                        c['name'] for c in list_cells()
                        if c.get('cell_type') == 'code' and c.get('active', 1)
                    }
                    code_events = [i for i in ready if i.cell_name in code_names]
                    fs_events = [i for i in ready if i.cell_name not in code_names]
                    if fs_events:
                        drain_filesystem_invalidations(fs_events)
                    if code_events:
                        try:
                            from flex.modules.engines import drain_corpus_paths
                        except ImportError:
                            # The filesystem worker can run without the optional
                            # aggregate integration. Reconciliation below
                            # remains the correctness floor for those cells.
                            invalidation_queue.mark_reconciliation_required(
                                {item.cell_name for item in code_events}
                            )
                        else:
                            event_stats = drain_corpus_paths(code_events)
                            for invalidation in event_stats.get('deferred') or ():
                                invalidation_queue.put(invalidation)
            except Exception as exc:
                print(f"[filesystem-worker] event drain: {exc}", file=sys.stderr)
        due = (
            time.monotonic() - last_reconcile >= cadence
            or (invalidation_queue is not None
                and invalidation_queue.reconciliation_required())
            or (watcher is not None and not watcher.healthy)
        )
        if due:
            reconciled = True
            try:
                scan_filesystem_cells()
            except Exception as exc:
                reconciled = False
                print(f"[filesystem-worker] filesystem reconcile: {exc}", file=sys.stderr)
            try:
                from flex.modules.fs.compile.index_code import scan_code_cells
                scan_code_cells({})
            except Exception as exc:
                reconciled = False
                print(f"[filesystem-worker] code reconcile: {exc}", file=sys.stderr)
            if reconciled:
                last_reconcile = time.monotonic()
                if invalidation_queue is not None:
                    invalidation_queue.clear_reconciliation_required()
        time.sleep(interval)
