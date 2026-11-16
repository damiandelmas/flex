"""Query-local graph projection over Ledger's existing Index and selected sources."""

from __future__ import annotations

import hashlib
import json
import sqlite3

from flex import registry
from flex.relationships import relationship_id

NODES = "_ledger_graph_nodes"
EDGES = "_ledger_graph_edges"
VERTICES = "_ledger_graph_vertices"
EDGE_TABLE = "_ledger_graph_edge_table"


def clear_graph(db: sqlite3.Connection) -> None:
    for name in (EDGES, VERTICES, EDGE_TABLE, NODES):
        row = db.execute("SELECT type FROM sqlite_temp_master WHERE name=? AND type IN ('table','view')", (name,)).fetchone()
        if row:
            db.execute(f'DROP {row[0]} temp."{name}"')


def _rows(db, sql, params=()):
    cursor = db.execute(sql, params)
    return [dict(zip((c[0] for c in cursor.description), row)) for row in cursor]


def _text(value):
    # Model-facing previews are not hydration; exact bodies stay with their owner.
    value = "" if value is None else value
    if not isinstance(value, str):
        raise ValueError("Ledger graph properties require stored text, not binary content")
    return {"text": value[:1200], "text_chars": len(value),
            "text_sha256": hashlib.sha256(value.encode()).hexdigest(),
            "representation": "complete" if len(value) <= 1200 else "preview"}


def install_graph(db: sqlite3.Connection) -> None:
    """Rebuild a connection-local graph; never write a source or maintained cell."""
    ledger = registry.get_cell_metadata("ledger")
    if not ledger or not ledger.get("id"):
        raise ValueError("Ledger graph requires the registered Ledger cell identity")
    ledger_id = ledger["id"]
    nodes, edges = {}, {}

    def node(address, label, props):
        nodes[address] = (address, label, json.dumps(props, ensure_ascii=False))
        return address

    def edge(source, relation, target, ordinal=None):
        occurrence = json.dumps([source, relation, target, ordinal], separators=(",", ":"))
        eid = relationship_id("ledger-graph-v1", occurrence)
        props = {"canonical_edge_id": eid, "canonical_source_id": source,
                 "canonical_target_id": target, "canonical_relation": relation,
                 "ordinal": ordinal, "inverse": False}
        inverse = relationship_id("ledger-graph-v1", "inverse:" + eid)
        edges[eid] = (eid, source, target, relation, ordinal, json.dumps(props), inverse)

    cards = _rows(db, "SELECT * FROM _ledger_index_cards")
    metadata = {r[0]: r[1] for r in db.execute("SELECT annotation_id,metadata FROM annotations")}
    for card in cards:
        role = card["captured_role"]
        props = {k: v for k, v in card.items() if k != "summary"}
        props.update(_text(card["summary"]))
        if card["annotation_id"]:
            props.update(cell_id=ledger_id, object_id=card["annotation_id"])
            props["object_type"] = role
            if role == "annotation" and metadata.get(card["annotation_id"]):
                props["metadata"] = json.loads(metadata[card["annotation_id"]])
        label = {"annotation": "Annotation", "annotation-revision": "Annotation revision",
                 "session": "Session", "self": "Self selection"}.get(role, role)
        node(card["address"], label, props)

    sources = {}
    for row in _rows(db, "SELECT * FROM _flex_self_content"):
        sources[(row["target_cell_id"], row["target_object_id"])] = row
    pi_info, parents, sessions = {}, {}, set()
    for cell in _rows(db, "SELECT DISTINCT target_cell_id,target_cell_name,cell_alias FROM _flex_self_objects"):
        alias = '"' + cell["cell_alias"].replace('"', '""') + '"'
        if cell["target_cell_name"] in {"pi", "codex", "claude_code"}:
            has_sources = db.execute(f"SELECT 1 FROM {alias}.sqlite_master WHERE name='sources' AND type IN ('table','view')").fetchone()
            if has_sources:
                for (sid,) in db.execute(f"""SELECT source_id FROM {alias}.sources
                    WHERE source_id IN (SELECT target_chunk_id FROM _ledger_index_cards
                        WHERE captured_role='annotation' AND target_cell_id=?
                          AND target_end_chunk_id IS NULL)""", (cell["target_cell_id"],)):
                    sessions.add((cell["target_cell_id"], sid))
        if cell["target_cell_name"] != "pi":
            continue
        info = _rows(db, f"""
            SELECT e.chunk_id,e.source_id,t.entry_uuid,m.position
            FROM {alias}._edges_source e
            JOIN {alias}._types_message t ON t.chunk_id=e.chunk_id
            JOIN {alias}.messages m ON m.id=e.chunk_id AND m.session_id=e.source_id
            WHERE e.source_type='pi' AND e.chunk_id IN (
                SELECT target_object_id FROM _flex_self_content WHERE target_cell_id=?)
        """, (cell["target_cell_id"],))
        canonical = {r["chunk_id"] for r in info}
        sources = {k: v for k, v in sources.items()
                   if k[0] != cell["target_cell_id"] or k[1] in canonical}
        for r in info:
            pi_info[(cell["target_cell_id"], r["chunk_id"])] = r
        for sid in sorted({r["source_id"] for r in info}):
            parents[(cell["target_cell_id"], sid)] = {
                r[0]: (r[1], r[2]) for r in db.execute(
                    f"SELECT native_entry_id,parent_native_id,file_position FROM {alias}._types_pi_entry WHERE source_id=?", (sid,))}

    def source_node(cell_id, object_id):
        if (cell_id, object_id) in sessions:
            address = "session://" + cell_id + "/" + object_id
            props = json.loads(nodes[address][2]) if address in nodes else {}
            props.update(cell_id=cell_id, object_id=object_id, session_id=object_id,
                         object_type="session", resolution="resolved")
            return node(address, "Session", props)
        address = "flex://object/" + cell_id + "/" + object_id
        if address not in nodes:
            row = sources.get((cell_id, object_id))
            props = {"cell_id": cell_id, "object_id": object_id,
                     "resolution": "resolved" if row and row["content"] is not None else "unresolved"}
            if row:
                info = pi_info.get((cell_id, object_id), {})
                props.update(session_id=info.get("source_id", row["parent_object_id"]),
                             source_order=info.get("position", row["source_order"]),
                             native_type=row["native_type"], **_text(row["content"]))
            node(address, "Source" if props["resolution"] == "resolved" else "Unresolved source", props)
        return address

    def range_members(card):
        cell_id = card["target_cell_id"]
        start = pi_info.get((cell_id, card["target_chunk_id"]))
        end = pi_info.get((cell_id, card["target_end_chunk_id"]))
        if not start or not end:
            return [], {"resolution": "unresolved-or-unsupported-range"}
        if start["source_id"] != end["source_id"] or start["position"] >= end["position"]:
            return [], {"resolution": "invalid-range"}
        tree = parents[(cell_id, start["source_id"])]
        current, visited = end["entry_uuid"], set()
        while current is not None:
            if current in visited or current not in tree:
                return [], {"resolution": "invalid-ancestry"}
            visited.add(current)
            if current == start["entry_uuid"]:
                break
            current = tree[current][0]
        if current != start["entry_uuid"]:
            return [], {"resolution": "start-not-ancestor"}
        members = sorted((r for (cid, _), r in pi_info.items()
                          if cid == cell_id and r["source_id"] == start["source_id"]
                          and start["position"] <= r["position"] <= end["position"]),
                         key=lambda r: (r["position"], r["chunk_id"]))
        if any(r["entry_uuid"] not in visited for r in members):
            return [], {"resolution": "off-path-members"}
        return members, {"resolution": "resolved", "session_id": start["source_id"],
                         "projection_leaf_entry_id": end["entry_uuid"],
                         "cutoff_file_position": tree[end["entry_uuid"]][1],
                         "member_count": len(members)}

    by_address = {c["address"]: c for c in cards}
    for r in _rows(db, "SELECT * FROM _ledger_index_relations"):
        target = r["object_address"]
        if r["predicate"] == "annotates":
            card = by_address.get(r["subject_address"])
            if card is None:
                raise ValueError(f"Missing Ledger relationship source card: {r['subject_address']}")
            if card["target_end_chunk_id"]:
                target = "ledger://selection/" + card["annotation_id"]
                members, status = range_members(card)
                node(target, "Selection", {"derived": True, "owner_cell_id": ledger_id,
                     "annotation_id": card["annotation_id"],
                     "source_cell_id": card["target_cell_id"],
                     "start_object_id": card["target_chunk_id"],
                     "end_object_id": card["target_end_chunk_id"], **status})
                for ordinal, member in enumerate(members):
                    edge(target, "selects", source_node(card["target_cell_id"], member["chunk_id"]), ordinal)
            else:
                target = source_node(card["target_cell_id"], card["target_chunk_id"])
        edge(r["subject_address"], r["predicate"], target, r["position"])

    clear_graph(db)
    db.execute(f"CREATE TEMP TABLE {NODES}(node_id TEXT PRIMARY KEY NOT NULL, label TEXT NOT NULL, properties_json TEXT NOT NULL)")
    db.executemany(f"INSERT INTO {NODES} VALUES(?,?,?)", nodes.values())
    db.execute(f"CREATE TEMP VIEW {VERTICES} AS SELECT node_id AS vertex_id,label,properties_json FROM {NODES}")
    db.execute(f"""CREATE TEMP TABLE {EDGE_TABLE}(edge_id TEXT PRIMARY KEY NOT NULL,
        source_vertex_id TEXT NOT NULL REFERENCES {NODES}(node_id),
        destination_vertex_id TEXT NOT NULL REFERENCES {NODES}(node_id),
        relation TEXT NOT NULL,ordinal INTEGER,properties_json TEXT NOT NULL,
        inverse_edge_id TEXT NOT NULL UNIQUE)""")
    for row in edges.values():
        _, source, target, *_ = row
        if source not in nodes or target not in nodes:
            raise ValueError(f"Unresolved Ledger graph endpoint: {source} -> {target}")
    db.executemany(f"INSERT INTO {EDGE_TABLE} VALUES(?,?,?,?,?,?,?)", edges.values())
    db.execute(f"CREATE INDEX temp.ledger_graph_outgoing ON {EDGE_TABLE}(source_vertex_id,edge_id)")
    db.execute(f"CREATE INDEX temp.ledger_graph_incoming ON {EDGE_TABLE}(destination_vertex_id,inverse_edge_id)")
    # Graph consumers ask for outgoing arcs. Reverse traversal is a view over the same edge,
    # not another stored assertion; both arms use their endpoint index.
    db.execute(f"""CREATE TEMP VIEW {EDGES} AS
        SELECT e.edge_id,s.node_id source_id,t.node_id target_id,e.relation,
               s.label source_label,t.label target_label,e.properties_json,
               t.properties_json target_properties_json
        FROM {EDGE_TABLE} e JOIN {NODES} s ON s.node_id=e.source_vertex_id
        JOIN {NODES} t ON t.node_id=e.destination_vertex_id
        UNION ALL
        SELECT e.inverse_edge_id,t.node_id,s.node_id,'inverse:'||e.relation,
               t.label,s.label,json_set(e.properties_json,'$.inverse',json('true')),
               s.properties_json
        FROM {EDGE_TABLE} e JOIN {NODES} s ON s.node_id=e.source_vertex_id
        JOIN {NODES} t ON t.node_id=e.destination_vertex_id""")
