"""Per-file refresh for watched, non-code Instant cells.

The full compiler remains the first-build/recipe-change path. Ordinary file
edits route here: rebuild one source inside the existing cell, preserve its
absolute-path identity and Instant query surface, and reconcile deletions.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

from flex.modules.instant.install import (
    _INSTANT_DDL,
    _build_nodes,
    _build_profiled,
    _iter_indexed_paths,
    _resolve_profile,
)


def _recipe(conn: sqlite3.Connection) -> tuple[list[Path], str, bool]:
    rows = dict(conn.execute(
        "SELECT key,value FROM _meta WHERE key IN ('selections','chunking')"
    ).fetchall())
    try:
        selections = [Path(p) for p in json.loads(rows.get('selections', '[]'))]
    except (TypeError, ValueError):
        selections = []
    try:
        chunking = json.loads(rows.get('chunking', '{}'))
    except (TypeError, ValueError):
        chunking = {}
    return selections, chunking.get('split_mode', 'flat'), bool(chunking.get('code', False))


def _delete_source(conn: sqlite3.Connection, source_id: str) -> int:
    ids = [r[0] for r in conn.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id=?", (source_id,)
    ).fetchall()]
    if ids:
        ph = ','.join('?' * len(ids))
        conn.execute(f"DELETE FROM _types_instant WHERE chunk_id IN ({ph})", ids)
        conn.execute(f"DELETE FROM _edges_tree WHERE id IN ({ph})", ids)
        conn.execute(f"DELETE FROM _edges_call WHERE caller_id IN ({ph})", ids)
        conn.execute(f"DELETE FROM _raw_chunks WHERE id IN ({ph})", ids)
        conn.execute("DELETE FROM _edges_source WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _edges_import WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _edges_fs_identity WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _raw_sources WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _instant_source_state WHERE source_id=?", (source_id,))
    return len(ids)


def _mint_identity(conn: sqlite3.Connection, source_id: str) -> None:
    try:
        from flex.modules.soma.lib.identity.file_identity import get_instance
        uuid = get_instance().assign_batch([source_id]).get(source_id)
        if uuid:
            conn.execute(
                "INSERT OR REPLACE INTO _edges_fs_identity (source_id,file_uuid) VALUES (?,?)",
                (source_id, uuid),
            )
    except Exception:
        pass


def _record_state(conn, source_id: str, digest: str, stat) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _instant_source_state "
        "(source_id,content_hash,size_bytes,mtime_ns) VALUES (?,?,?,?)",
        (source_id, digest, stat.st_size, stat.st_mtime_ns),
    )


def index_file(conn: sqlite3.Connection, file_path: str, *, split_mode: str) -> bool:
    """Refresh one file in an existing Instant cell. Returns False if unchanged."""
    from flex.sdk import ingest, link, source

    path = Path(file_path).resolve()
    try:
        raw = path.read_bytes()
        stat = path.stat()
    except OSError:
        return False
    digest = hashlib.sha256(raw).hexdigest()
    previous = conn.execute(
        "SELECT content_hash FROM _instant_source_state WHERE source_id=?", (str(path),)
    ).fetchone()
    if previous and previous[0] == digest:
        return False

    text = raw.decode('utf-8', 'ignore')
    if not text.strip():
        _record_state(conn, str(path), digest, stat)
        conn.commit()
        return False
    if split_mode in ('nest', 'config'):
        profile = _resolve_profile(
            str(path), {}, default_split='nest' if split_mode == 'nest' else 'flat'
        )
        nodes = _build_profiled(str(path), text, profile)
    else:
        nodes = _build_nodes(str(path), text, 'flat')
    if not nodes:
        _record_state(conn, str(path), digest, stat)
        conn.commit()
        return False

    source_id = str(path)
    _delete_source(conn, source_id)
    source(conn, source_id, path.name)
    for node in nodes:
        node['content_hash'] = hashlib.sha256(
            (node.get('content') or '').encode('utf-8')
        ).hexdigest()
        node.setdefault('section_type', None)
    ingest(conn, source_id, nodes, types='_types_instant')
    for node in nodes:
        if node.get('container_id'):
            link(conn, node['id'], node['container_id'],
                 relation='subsection', depth=node.get('depth', 1))
    _mint_identity(conn, source_id)
    _record_state(conn, source_id, digest, stat)
    conn.commit()
    return True


def scan_instant_cells(size_cache: dict) -> dict:
    """Drive migrated Instant cells (`refresh_module=NULL`) per file."""
    from flex.registry import list_cells, update_refresh_status

    stats = {'indexed': 0, 'skipped': 0, 'deleted': 0}
    cells = [c for c in list_cells()
             if c.get('cell_type') == 'instant'
             and c.get('lifecycle') == 'watch'
             and not c.get('refresh_module')
             and c.get('active', 1)]
    for cell in cells:
        conn = sqlite3.connect(cell['path'], timeout=30)
        conn.executescript(_INSTANT_DDL)
        selections, split_mode, code = _recipe(conn)
        if code or not selections:
            conn.close()
            continue
        seen = set()
        changed = 0
        candidates = []
        for root in selections:
            if not root.is_dir():
                continue
            for path in _iter_indexed_paths(root):
                resolved = str(path.resolve())
                seen.add(resolved)
                key = f"instant:{cell['name']}:{resolved}"
                try:
                    stat = path.stat()
                    signature = f"{stat.st_size}:{stat.st_mtime_ns}"
                except OSError:
                    continue
                if size_cache.get(key) == signature:
                    continue
                # Cold daemon cache: hydrate from the durable per-source cursor
                # without reading file bodies. This prevents every restart from
                # hashing an entire large Instant corpus once.
                if key not in size_cache:
                    stored = conn.execute(
                        "SELECT size_bytes,mtime_ns FROM _instant_source_state "
                        "WHERE source_id=?", (resolved,),
                    ).fetchone()
                    if stored and stored == (stat.st_size, stat.st_mtime_ns):
                        size_cache[key] = signature
                        continue
                candidates.append((resolved, (resolved, key, signature)))

        from flex.watch import fair_batch
        limit = max(1, int(os.environ.get('FLEX_DRAIN_FILES_PER_CELL', '200')))
        batch = fair_batch(conn, 'instant', candidates, limit)
        if batch:
            conn.commit()
        for _, (resolved, key, signature) in batch:
            if index_file(conn, resolved, split_mode=split_mode):
                changed += 1
                stats['indexed'] += 1
            else:
                stats['skipped'] += 1
            size_cache[key] = signature

        known = [r[0] for r in conn.execute(
            "SELECT source_id FROM _instant_source_state"
        ).fetchall()]
        deleted = 0
        if seen:
            for source_id in known:
                if source_id not in seen and not Path(source_id).exists():
                    _delete_source(conn, source_id)
                    deleted += 1
        if changed or deleted:
            conn.commit()
            update_refresh_status(cell['name'], 'ok')
        stats['deleted'] += deleted
        conn.close()
    return stats
