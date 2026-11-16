"""Emit `contains` (dir -> child) and `links_to` (symlink -> target) into the
existing `_edges_relationships` carrier (flex/relationships.py) from a walk.
The carrier has no witness column, so the evidence lives in the typed sidecar
`_types_relationship_witness(edge_id, witness)`."""
import hashlib
import json
import os
import sqlite3

from flex.relationships import install_relationships

from .sdstore import SdStore

_WITNESS_DDL = "CREATE TABLE IF NOT EXISTS _types_relationship_witness (edge_id TEXT PRIMARY KEY, witness TEXT)"


def edge_id(*parts: str) -> str:
    """sha256 of (source_cell_id, source_object_id, relation, target_cell_id, target_object_id)."""
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


def write_edges(conn: sqlite3.Connection, store: SdStore, cell_id: str, root: str) -> int:
    install_relationships(conn)
    conn.execute(_WITNESS_DDL)
    uuids, kinds = store.cache[root], store.kinds[root]
    rows = []
    ordinals: dict[str, int] = {}
    for path in sorted(uuids):
        parent = os.path.dirname(path)
        rel = os.path.relpath(path, root)
        if parent in uuids and kinds.get(parent) == "directory":
            n = ordinals[parent] = ordinals.get(parent, -1) + 1
            rows.append(("contains", uuids[parent], uuids[path], n, {"path": rel}))
        if kinds[path] == "symlink":
            raw = os.readlink(path)
            target = os.path.normpath(os.path.join(parent, raw))
            if target in uuids:
                rows.append(("links_to", uuids[path], uuids[target], None,
                             {"path": rel, "link_target": raw}))
    conn.execute("DELETE FROM _types_relationship_witness WHERE edge_id IN (SELECT edge_id FROM "
                 "_edges_relationships WHERE source_cell_id=? AND relation IN ('contains','links_to'))", (cell_id,))
    conn.execute("DELETE FROM _edges_relationships WHERE source_cell_id=? AND relation IN ('contains','links_to')", (cell_id,))
    edges = [(edge_id(cell_id, s, rel, cell_id, t), cell_id, s, rel, cell_id, t, o, w) for rel, s, t, o, w in rows]
    conn.executemany("INSERT OR REPLACE INTO _edges_relationships VALUES (?,?,?,?,?,?,?)", [e[:7] for e in edges])
    conn.executemany("INSERT OR REPLACE INTO _types_relationship_witness VALUES (?,?)",
                     [(e[0], json.dumps(e[7], sort_keys=True)) for e in edges])
    conn.commit()
    return len(edges)
