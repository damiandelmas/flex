"""One-shot: bridge a cell's SOMA file uuids to sd-store uuids, by path.

    python -m flex.modules.identity.bridge <cell.db>

Reads the cell's `_edges_fs_identity` (SOMA uuids), walks the cell's selections
through sd-store, and writes `_identity_bridge(soma_uuid, sd_uuid, path)` so
old joins keep resolving. The cell's own `_edges_fs_identity` is not touched.
"""
import os
import sqlite3
import sys
from pathlib import Path

from .sdstore import SdStore, selections

DDL = ("CREATE TABLE IF NOT EXISTS _identity_bridge "
       "(soma_uuid TEXT PRIMARY KEY, sd_uuid TEXT, path TEXT)")


def build_bridge(conn: sqlite3.Connection, store: SdStore | None = None) -> tuple[int, int]:
    """Returns (soma rows, bridged rows). Joins on absolute path under each selection."""
    meta = dict(conn.execute("SELECT key, value FROM _meta").fetchall())
    roots = selections(conn)
    store = store or SdStore(meta.get("cell_id", "default"), roots)
    walked = {}
    for r in roots:
        store.bind(r)
        walked.update(store.cache.get(r) or store.walk(r))
    conn.execute(DDL)
    rows = conn.execute("SELECT source_id, file_uuid FROM _edges_fs_identity WHERE file_uuid IS NOT NULL").fetchall()
    out = []
    for sid, soma in rows:
        for r in roots:
            p = sid if os.path.isabs(sid) else f"{r}/{sid}"
            if p in walked:
                out.append((soma, walked[p], p))
                break
        else:
            out.append((soma, None, None))
    conn.executemany("INSERT OR REPLACE INTO _identity_bridge VALUES (?,?,?)", out)
    conn.commit()
    return len(rows), sum(1 for o in out if o[1])


if __name__ == "__main__":
    c = sqlite3.connect(sys.argv[1], timeout=120)  # live cells: wait out the worker's writes
    n, b = build_bridge(c)
    print(f"{b}/{n} bridged")
