"""Compile provider-authored SQL projections into durable Flex objects."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from flex import registry
from flex.compile.embed import embed_new
from flex.core import validate_tree_projection
from flex.envelope import (
    install_cell_core,
    install_retrieval,
    install_view_catalog,
    metadata_relation,
    validate_envelope,
    validate_retrieval,
)
from flex.meta import attach_registered_cells
from flex.relationships import (
    install_relationships,
    relationship_id,
    validate_relationships,
)


COMPILER_VERSION = "flex-projection@3"
_IDENTITY_NAMESPACE = uuid.UUID("db429cf2-05a8-5a73-bd22-71d2760dc2e5")
_ALIAS_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class ProjectionError(RuntimeError):
    """A candidate did not satisfy the durable projection contract."""


ReferenceResolver = Callable[[sqlite3.Connection, Mapping[str, object]], bool | str]
ProviderValidator = Callable[[sqlite3.Connection], None]
EmbeddingRunner = Callable[[sqlite3.Connection], int]


@dataclass(frozen=True)
class ProjectionSpec:
    projection_id: str
    objects_sql: str
    source_cell_id: str | None = None
    attachments: tuple[tuple[str, str], ...] = ()
    references_sql: str | None = None
    tree_sql: str | None = None
    description: str = ""
    source_signature: str = ""
    view_directories: tuple[Path, ...] = ()
    preset_directories: tuple[Path, ...] = ()
    provider_validate: ProviderValidator | None = None
    reference_resolver: ReferenceResolver | None = None


@dataclass(frozen=True)
class ProjectionReceipt:
    projection_id: str
    output_path: str
    source_signature: str
    compiler_version: str
    object_count: int
    reference_count: int
    unresolved_reference_count: int
    tree_edge_count: int
    fts_count: int
    embedded_count: int
    embedding_model: str | None
    embedding_storage_dimension: int | None
    embedding_serve_dimension: int | None
    semantic_ready: bool
    embedding_debt: int
    embedding_error: str | None
    content_signature: str
    completed_at: str

    def as_dict(self) -> dict:
        return asdict(self)


_PROJECTION_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _types_projection (
  chunk_id       TEXT PRIMARY KEY,
  projection_id  TEXT NOT NULL,
  object_key     TEXT NOT NULL,
  object_kind    TEXT NOT NULL,
  source_order   INTEGER,
  timestamp      INTEGER,
  UNIQUE (projection_id, object_key)
);
CREATE INDEX IF NOT EXISTS idx_projection_membership
ON _types_projection(projection_id, source_order, object_key);
"""

_RELATIONSHIP_PROJECTION_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _types_projection_reference (
  edge_id             TEXT PRIMARY KEY REFERENCES _edges_relationships(edge_id),
  target_object_type  TEXT NOT NULL,
  evidence_basis      TEXT,
  resolution_state    TEXT NOT NULL,
  target_locator      TEXT
);

CREATE TABLE IF NOT EXISTS _types_projection_tree (
  edge_id  TEXT PRIMARY KEY REFERENCES _edges_relationships(edge_id),
  depth    INTEGER NOT NULL CHECK (depth >= 0)
);
"""


def derived_object_id(projection_id: str, object_key: str) -> str:
    return str(uuid.uuid5(_IDENTITY_NAMESPACE, f"{projection_id}\0{object_key}"))


def _validate_spec(spec: ProjectionSpec) -> None:
    if not spec.projection_id.strip():
        raise ProjectionError("projection_id must not be blank")
    if not spec.objects_sql.strip().upper().startswith(("SELECT", "WITH")):
        raise ProjectionError("objects_sql must be ordinary SELECT or WITH SQL")
    if (spec.references_sql is not None or spec.tree_sql is not None) and not (
        spec.source_cell_id and spec.source_cell_id.strip()
    ):
        raise ProjectionError(
            "source_cell_id is required for addressable relationship projections"
        )
    for name, alias in spec.attachments:
        if not name.strip():
            raise ProjectionError("attachment cell name must not be blank")
        if _ALIAS_RE.fullmatch(alias) is None or alias.casefold() in {"main", "temp"}:
            raise ProjectionError(f"invalid attachment alias: {alias!r}")
    aliases = [alias.casefold() for _, alias in spec.attachments]
    if len(aliases) != len(set(aliases)):
        raise ProjectionError("attachment aliases must be unique")


def _refuse_registered_path(candidate: Path) -> None:
    resolved = candidate.resolve()
    for cell in registry.list_cells():
        try:
            registered = Path(str(cell["path"])).resolve()
        except (KeyError, OSError, ValueError):
            continue
        if registered == resolved:
            raise ProjectionError(
                f"candidate path is already registered as live cell {cell.get('name')!r}"
            )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _attach_sources(db: sqlite3.Connection, spec: ProjectionSpec) -> None:
    if not spec.attachments:
        return
    prelude = " ".join(
        f"ATTACH {_sql_literal(name)} AS {alias};" for name, alias in spec.attachments
    ) + " SELECT 1"
    _remaining, error = attach_registered_cells(
        db,
        prelude,
        explicit_cells=[name for name, _alias in spec.attachments],
        available_cells=[name for name, _alias in spec.attachments],
    )
    if error:
        raise ProjectionError(error)


def _detach_sources(db: sqlite3.Connection, spec: ProjectionSpec) -> None:
    """Release read-only inputs before mutating the unpublished candidate.

    SQLite resolves an unqualified DROP VIEW/TRIGGER across attached schemas.
    Envelope installers intentionally operate on ``main``; detaching inputs
    after their rows and reference states have been materialized prevents an
    identically named object in a read-only source from intercepting a later
    provider or envelope migration.
    """
    for _name, alias in reversed(spec.attachments):
        try:
            db.execute(f'DETACH DATABASE "{alias}"')
        except sqlite3.DatabaseError as exc:
            raise ProjectionError(
                f"could not release projection source {alias!r}: {exc}"
            ) from exc


def _rows(db: sqlite3.Connection, sql: str | None) -> list[dict]:
    if not sql:
        return []
    try:
        from flex.mcp_core import search_authorizer

        db.set_authorizer(search_authorizer)
        cursor = db.execute(sql)
        columns = [str(item[0]) for item in cursor.description or ()]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except sqlite3.DatabaseError as exc:
        raise ProjectionError(f"projection SQL failed: {exc}") from exc
    finally:
        db.set_authorizer(None)


def _text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ProjectionError(f"{field} must not be blank")
    return result


def _optional_int(value: object, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ProjectionError(f"{field} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ProjectionError(f"{field} must be an integer") from exc


def _normalize_objects(rows: Sequence[dict], projection_id: str) -> list[dict]:
    required = {"object_key", "content", "object_kind"}
    objects: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        missing = required - row.keys()
        if missing:
            raise ProjectionError(
                "objects_sql is missing column(s): " + ", ".join(sorted(missing))
            )
        key = _text(row["object_key"], "object_key")
        if key in seen:
            raise ProjectionError(f"duplicate object_key: {key}")
        seen.add(key)
        content = str(row["content"] if row["content"] is not None else "")
        if not content.strip():
            raise ProjectionError(f"content must not be blank: {key}")
        objects.append({
            "object_key": key,
            "chunk_id": derived_object_id(projection_id, key),
            "content": content,
            "object_kind": _text(row["object_kind"], "object_kind"),
            "timestamp": _optional_int(row.get("timestamp"), "timestamp"),
            "source_order": _optional_int(row.get("source_order"), "source_order"),
        })
    return objects


def _normalize_references(
    db: sqlite3.Connection,
    rows: Sequence[dict],
    objects: Mapping[str, dict],
    resolver: ReferenceResolver | None,
    *,
    projection_id: str,
    source_cell_id: str,
) -> list[dict]:
    required = {
        "object_key", "target_cell_id", "target_object_id",
        "target_object_type", "relation",
    }
    references: list[dict] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in rows:
        missing = required - row.keys()
        if missing:
            raise ProjectionError(
                "references_sql is missing column(s): " + ", ".join(sorted(missing))
            )
        key = _text(row["object_key"], "reference object_key")
        if key not in objects:
            raise ProjectionError(f"reference names unknown object_key: {key}")
        normalized = {
            "object_key": key,
            "source_chunk_id": objects[key]["chunk_id"],
            "source_cell_id": source_cell_id,
            "target_cell_id": _text(row["target_cell_id"], "target_cell_id"),
            "target_object_id": _text(row["target_object_id"], "target_object_id"),
            "target_object_type": _text(
                row["target_object_type"], "target_object_type"
            ),
            "relation": _text(row["relation"], "relation"),
            "source_order": _optional_int(row.get("source_order"), "source_order"),
            "evidence_basis": (
                str(row["evidence_basis"]) if row.get("evidence_basis") is not None
                else None
            ),
            "target_locator": (
                str(row["target_locator"]) if row.get("target_locator") is not None
                else None
            ),
        }
        identity = (
            key, normalized["target_cell_id"], normalized["target_object_id"],
            normalized["target_object_type"], normalized["relation"],
        )
        if identity in seen:
            raise ProjectionError(f"duplicate exact reference: {identity}")
        seen.add(identity)
        state: bool | str = False
        if resolver is not None:
            try:
                state = resolver(db, normalized)
            except Exception as exc:
                raise ProjectionError(f"reference resolution failed: {exc}") from exc
        if state is True:
            state = "resolved"
        elif state is False or state is None:
            state = "unresolved"
        state = str(state)
        if state not in {"resolved", "unresolved"}:
            raise ProjectionError(f"invalid reference resolution state: {state}")
        normalized["resolution_state"] = state
        normalized["edge_id"] = relationship_id(
            projection_id,
            "reference\0" + "\0".join(identity),
        )
        references.append(normalized)
    return references


def _normalize_tree(
    rows: Sequence[dict],
    objects: Mapping[str, dict],
    *,
    projection_id: str,
    source_cell_id: str,
) -> list[dict]:
    required = {"child_key", "parent_key", "relation", "position"}
    tree: list[dict] = []
    children: set[str] = set()
    sibling_positions: set[tuple[str, int]] = set()
    parents: dict[str, str] = {}
    for row in rows:
        missing = required - row.keys()
        if missing:
            raise ProjectionError(
                "tree_sql is missing column(s): " + ", ".join(sorted(missing))
            )
        child = _text(row["child_key"], "child_key")
        parent = _text(row["parent_key"], "parent_key")
        if child not in objects or parent not in objects:
            raise ProjectionError(f"tree edge has unknown object: {child} -> {parent}")
        if child == parent:
            raise ProjectionError(f"tree object cannot parent itself: {child}")
        if child in children:
            raise ProjectionError(f"tree object has multiple parents: {child}")
        position = _optional_int(row["position"], "position")
        if position is None or position < 0:
            raise ProjectionError("tree position must be a non-negative integer")
        if (parent, position) in sibling_positions:
            raise ProjectionError(f"duplicate sibling position: {parent}:{position}")
        children.add(child)
        sibling_positions.add((parent, position))
        parents[child] = parent
        tree.append({
            "child_key": child,
            "parent_key": parent,
            "child_id": objects[child]["chunk_id"],
            "parent_id": objects[parent]["chunk_id"],
            "relation": _text(row["relation"], "tree relation"),
            "position": position,
            "source_cell_id": source_cell_id,
            "edge_id": relationship_id(projection_id, f"tree\0{child}"),
        })

    for start in parents:
        seen: set[str] = set()
        current = start
        while current in parents:
            if current in seen:
                raise ProjectionError(f"tree contains a cycle at: {current}")
            seen.add(current)
            current = parents[current]
    depths: dict[str, int] = {}
    for item in tree:
        depth = 1
        current = item["parent_key"]
        seen = {item["child_key"]}
        while current in parents:
            if current in seen:
                raise ProjectionError(f"tree contains a cycle at: {current}")
            seen.add(current)
            depth += 1
            current = parents[current]
        depths[item["child_key"]] = depth
        item["depth"] = depth
    return tree


def _content_signature(objects: Sequence[dict], references: Sequence[dict], tree: Sequence[dict]) -> str:
    payload = {
        "objects": [
            [item["object_key"], item["content"], item["object_kind"],
             item["timestamp"], item["source_order"]]
            for item in objects
        ],
        "references": [
            [item["object_key"], item["target_cell_id"], item["target_object_id"],
             item["target_object_type"], item["relation"], item["source_order"],
             item["evidence_basis"], item["resolution_state"], item["target_locator"]]
            for item in references
        ],
        "tree": [
            [item["child_key"], item["parent_key"], item["relation"], item["position"]]
            for item in tree
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _embedding_contract_is_reusable(db: sqlite3.Connection) -> bool:
    """Return whether stored vectors still have an authoritative space contract.

    Exact content equality is not enough to reuse a vector.  The cell must also
    retain the model and native storage dimension that make the bytes
    interpretable.  If that metadata was lost, rebuilding the projection leaves
    vectors NULL so the ordinary embedding runner can establish the contract
    again instead of silently blessing an unknown vector space.
    """
    relation = metadata_relation(db)
    if relation is None:
        return False
    metadata = dict(db.execute(
        f"SELECT key,value FROM {relation} WHERE key IN "
        "('embedding_model','embedding_dim','vec:model')"
    ))
    if not (metadata.get("embedding_model") or metadata.get("vec:model")):
        return False
    try:
        storage_dimension = int(metadata.get("embedding_dim") or "")
    except ValueError:
        return False
    if storage_dimension <= 0:
        return False
    expected_bytes = storage_dimension * 4
    return db.execute(
        "SELECT NOT EXISTS("
        "SELECT 1 FROM _raw_chunks WHERE embedding IS NOT NULL "
        "AND length(embedding) != ?)",
        (expected_bytes,),
    ).fetchone()[0] == 1


def _object_type(db: sqlite3.Connection, name: str) -> str | None:
    row = db.execute(
        "SELECT type FROM sqlite_master WHERE name=? AND type IN ('table','view')",
        (name,),
    ).fetchone()
    return str(row[0]) if row else None


def _migrate_legacy_edge_tables(
    db: sqlite3.Connection,
    *,
    source_cell_id: str,
) -> None:
    """Move compiler-owned legacy edge rows into the relationship profile.

    This runs only on an unpublished candidate.  Provider fidelity that does
    not fit the mechanical carrier is preserved in typed sidecars before the
    physical legacy tables are replaced by read-only compatibility views.
    """
    if _object_type(db, "_edges_tree") == "table":
        rows = db.execute(
            "SELECT id,parent_id,relation,COALESCE(depth,0),position "
            "FROM _edges_tree WHERE parent_id IS NOT NULL"
        ).fetchall()
        for child, parent, relation, depth, position in rows:
            edge_id = relationship_id(
                "legacy-projection-tree",
                "\0".join(map(str, (source_cell_id, child, parent, relation, position))),
            )
            db.execute(
                "INSERT OR IGNORE INTO _edges_relationships "
                "(edge_id,source_cell_id,source_object_id,relation,"
                "target_cell_id,target_object_id,ordinal) VALUES (?,?,?,?,?,?,?)",
                (edge_id, source_cell_id, child, relation,
                 source_cell_id, parent, position),
            )
            db.execute(
                "INSERT OR REPLACE INTO _types_projection_tree(edge_id,depth) "
                "VALUES (?,?)",
                (edge_id, int(depth or 0)),
            )
        db.execute("DROP TABLE _edges_tree")

    if _object_type(db, "_edges_reference") == "table":
        rows = db.execute(
            "SELECT source_chunk_id,target_cell_id,target_object_id,"
            "target_object_type,relation,source_order,evidence_basis,"
            "resolution_state,target_locator FROM _edges_reference"
        ).fetchall()
        for row in rows:
            (source_object_id, target_cell_id, target_object_id,
             target_object_type, relation, source_order, evidence_basis,
             resolution_state, target_locator) = row
            edge_id = relationship_id(
                "legacy-projection-reference",
                "\0".join(map(str, (
                    source_cell_id, source_object_id, target_cell_id,
                    target_object_id, target_object_type, relation,
                ))),
            )
            db.execute(
                "INSERT OR IGNORE INTO _edges_relationships "
                "(edge_id,source_cell_id,source_object_id,relation,"
                "target_cell_id,target_object_id,ordinal) VALUES (?,?,?,?,?,?,?)",
                (edge_id, source_cell_id, source_object_id, relation,
                 target_cell_id, target_object_id, source_order),
            )
            db.execute(
                "INSERT OR REPLACE INTO _types_projection_reference "
                "(edge_id,target_object_type,evidence_basis,resolution_state,"
                "target_locator) VALUES (?,?,?,?,?)",
                (edge_id, target_object_type, evidence_basis,
                 resolution_state, target_locator),
            )
        db.execute("DROP TABLE _edges_reference")


def _install_edge_compatibility_views(db: sqlite3.Connection) -> None:
    """Expose the v2 tree/reference contracts as read-only projections."""
    for name in ("_edges_tree", "_edges_reference"):
        if _object_type(db, name) == "view":
            db.execute(f'DROP VIEW "{name}"')
        elif _object_type(db, name) == "table":
            raise ProjectionError(f"legacy physical {name} was not migrated")
    db.executescript("""
        CREATE VIEW _edges_tree AS
        SELECT r.source_object_id AS id,
               r.target_object_id AS parent_id,
               NULL AS branch_at,
               r.relation AS relation,
               t.depth AS depth,
               r.ordinal AS position
        FROM _edges_relationships r
        JOIN _types_projection_tree t ON t.edge_id=r.edge_id;

        CREATE VIEW _edges_reference AS
        SELECT r.source_object_id AS source_chunk_id,
               r.target_cell_id AS target_cell_id,
               r.target_object_id AS target_object_id,
               t.target_object_type AS target_object_type,
               r.relation AS relation,
               r.ordinal AS source_order,
               t.evidence_basis AS evidence_basis,
               t.resolution_state AS resolution_state,
               t.target_locator AS target_locator
        FROM _edges_relationships r
        JOIN _types_projection_reference t ON t.edge_id=r.edge_id;
    """)


def _replace_projection(
    db: sqlite3.Connection,
    spec: ProjectionSpec,
    objects: Sequence[dict],
    references: Sequence[dict],
    tree: Sequence[dict],
) -> None:
    reusable_embeddings = {}
    if _embedding_contract_is_reusable(db):
        reusable_embeddings = {
            str(row[0]): (str(row[1]), row[2])
            for row in db.execute(
                "SELECT p.object_key,c.content,c.embedding "
                "FROM _types_projection p JOIN _raw_chunks c ON c.id=p.chunk_id "
                "WHERE p.projection_id=? AND c.embedding IS NOT NULL",
                (spec.projection_id,),
            )
        }
    prior = [
        row[0] for row in db.execute(
            "SELECT chunk_id FROM _types_projection WHERE projection_id=?",
            (spec.projection_id,),
        )
    ]
    if prior:
        placeholders = ",".join("?" for _ in prior)
        edge_ids = []
        if _object_type(db, "_edges_relationships") == "table":
            edge_ids = [str(row[0]) for row in db.execute(
                f"SELECT edge_id FROM _edges_relationships "
                f"WHERE source_cell_id=? AND source_object_id IN ({placeholders})",
                [spec.source_cell_id, *prior],
            )]
        if edge_ids:
            edge_placeholders = ",".join("?" for _ in edge_ids)
            db.execute(
                f"DELETE FROM _types_projection_reference "
                f"WHERE edge_id IN ({edge_placeholders})", edge_ids,
            )
            db.execute(
                f"DELETE FROM _types_projection_tree "
                f"WHERE edge_id IN ({edge_placeholders})", edge_ids,
            )
            db.execute(
                f"DELETE FROM _edges_relationships "
                f"WHERE edge_id IN ({edge_placeholders})", edge_ids,
            )
        db.execute(
            "DELETE FROM _types_projection WHERE projection_id=?",
            (spec.projection_id,),
        )
        db.execute(f"DELETE FROM _raw_chunks WHERE id IN ({placeholders})", prior)
    chunk_rows = []
    for item in objects:
        prior_content, prior_embedding = reusable_embeddings.get(
            item["object_key"], (None, None)
        )
        chunk_rows.append((
            item["chunk_id"], item["content"],
            prior_embedding if prior_content == item["content"] else None,
            item["timestamp"],
        ))
    db.executemany(
        "INSERT INTO _raw_chunks(id,content,embedding,timestamp) VALUES (?,?,?,?)",
        chunk_rows,
    )
    db.executemany(
        "INSERT INTO _types_projection"
        "(chunk_id,projection_id,object_key,object_kind,source_order,timestamp) "
        "VALUES (?,?,?,?,?,?)",
        ((o["chunk_id"], spec.projection_id, o["object_key"], o["object_kind"],
          o["source_order"], o["timestamp"]) for o in objects),
    )
    if references:
        db.executemany(
            "INSERT INTO _edges_relationships"
            "(edge_id,source_cell_id,source_object_id,relation,target_cell_id,"
            "target_object_id,ordinal) VALUES (?,?,?,?,?,?,?)",
            ((r["edge_id"], r["source_cell_id"], r["source_chunk_id"],
              r["relation"], r["target_cell_id"], r["target_object_id"],
              r["source_order"]) for r in references),
        )
        db.executemany(
            "INSERT INTO _types_projection_reference"
            "(edge_id,target_object_type,evidence_basis,resolution_state,target_locator) "
            "VALUES (?,?,?,?,?)",
            ((r["edge_id"], r["target_object_type"], r["evidence_basis"],
              r["resolution_state"], r["target_locator"]) for r in references),
        )
    if tree:
        db.executemany(
            "INSERT INTO _edges_relationships"
            "(edge_id,source_cell_id,source_object_id,relation,target_cell_id,"
            "target_object_id,ordinal) VALUES (?,?,?,?,?,?,?)",
            ((t["edge_id"], t["source_cell_id"], t["child_id"], t["relation"],
              t["source_cell_id"], t["parent_id"], t["position"]) for t in tree),
        )
        db.executemany(
            "INSERT INTO _types_projection_tree(edge_id,depth) VALUES (?,?)",
            ((t["edge_id"], t["depth"]) for t in tree),
        )


def _prevalidate(
    db: sqlite3.Connection,
    objects: Sequence[dict],
    references: Sequence[dict],
    tree: Sequence[dict],
) -> None:
    validate_retrieval(db)
    if references or tree:
        validate_relationships(db)
    if len({item["chunk_id"] for item in objects}) != len(objects):
        raise ProjectionError("derived object identities are not unique")
    unresolved = sum(r["resolution_state"] == "unresolved" for r in references)
    if references:
        stored_unresolved = db.execute(
            "SELECT count(*) FROM _edges_reference "
            "WHERE resolution_state='unresolved'"
        ).fetchone()[0]
        if stored_unresolved < unresolved:
            raise ProjectionError("reference resolution state was not preserved")
    if references and db.execute(
        "SELECT count(*) FROM _types_projection_reference"
    ).fetchone()[0] < len(references):
        raise ProjectionError("reference fidelity sidecar is incomplete")
    if tree and db.execute(
        "SELECT count(*) FROM _types_projection_tree"
    ).fetchone()[0] < len(tree):
        raise ProjectionError("tree compatibility sidecar is incomplete")
    if tree:
        validate_tree_projection(
            db,
            known_nodes={row[0] for row in db.execute("SELECT id FROM _raw_chunks")},
            relations={item["relation"] for item in tree},
            validate_depth=True,
            validate_order=True,
        )


def _install_presets(db: sqlite3.Connection, directories: Iterable[Path]) -> None:
    """Compile projection-owned query programs into the candidate database."""
    from flex.retrieve.presets import install_presets

    for directory in directories:
        path = Path(directory)
        if not path.is_dir():
            raise ProjectionError(f"preset directory does not exist: {path}")
        install_presets(db, path, commit=False, record_operation=False)


def _install_views(db: sqlite3.Connection, directories: Iterable[Path]) -> None:
    from flex.views import install_views

    for directory in directories:
        path = Path(directory)
        if not path.is_dir():
            raise ProjectionError(f"view directory does not exist: {path}")
        install_views(
            db,
            path,
            prepare_provider_state=False,
            record_operation=False,
        )


def _install_projection_view(db: sqlite3.Connection) -> None:
    """Expose retrievable objects without discovering provider sidecars."""
    db.execute("DROP VIEW IF EXISTS chunks")
    db.execute("""
        CREATE VIEW chunks AS
        SELECT c.id,c.content,c.timestamp,c.created_at,
               p.projection_id,p.object_key,p.object_kind,p.source_order
        FROM _raw_chunks c
        LEFT JOIN _types_projection p ON p.chunk_id=c.id
    """)


def compile_projection(
    candidate_path: Path,
    spec: ProjectionSpec,
    *,
    embedding_runner: EmbeddingRunner | None = None,
) -> ProjectionReceipt:
    """Publish one structural projection; vectors may converge independently.

    ``embedding_runner`` is an optional eager optimization. Its failure is
    recorded as semantic debt and never invalidates committed objects, metadata,
    references, tree edges, views, presets, or FTS.
    """
    _validate_spec(spec)
    candidate = Path(candidate_path).expanduser().resolve()
    _refuse_registered_path(candidate)
    candidate.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(candidate)
    db.row_factory = sqlite3.Row
    try:
        _attach_sources(db, spec)
        object_rows = _rows(db, spec.objects_sql)
        objects = _normalize_objects(object_rows, spec.projection_id)
        by_key = {item["object_key"]: item for item in objects}
        references = _normalize_references(
            db, _rows(db, spec.references_sql), by_key, spec.reference_resolver,
            projection_id=spec.projection_id,
            source_cell_id=str(spec.source_cell_id or ""),
        )
        tree = _normalize_tree(
            _rows(db, spec.tree_sql), by_key,
            projection_id=spec.projection_id,
            source_cell_id=str(spec.source_cell_id or ""),
        )
        signature = _content_signature(objects, references, tree)
        _detach_sources(db, spec)

        # Do not heal or extend the candidate until the complete provider
        # result has normalized.  Invalid SQL/identity/reference/tree input
        # therefore leaves an existing candidate byte-for-byte untouched.
        install_cell_core(db)
        install_retrieval(db)
        if spec.view_directories:
            install_view_catalog(db)
        db.executescript(_PROJECTION_SCHEMA)
        uses_relationships = spec.references_sql is not None or spec.tree_sql is not None
        if uses_relationships:
            install_relationships(db)
            db.executescript(_RELATIONSHIP_PROJECTION_SCHEMA)
            _migrate_legacy_edge_tables(
                db, source_cell_id=str(spec.source_cell_id or "")
            )
            _install_edge_compatibility_views(db)

        try:
            db.execute("BEGIN IMMEDIATE")
            _replace_projection(db, spec, objects, references, tree)
            _prevalidate(db, objects, references, tree)
            db.commit()
        except Exception:
            db.rollback()
            raise

        _install_projection_view(db)
        _install_views(db, spec.view_directories)
        _install_presets(db, spec.preset_directories)
        if spec.description:
            db.execute(
                "INSERT INTO _metadata(key,value) VALUES('description',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (spec.description,),
            )
        db.executemany(
            "INSERT INTO _metadata(key,value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (
                ("meta:projection_id", spec.projection_id),
                ("meta:projection_version", COMPILER_VERSION),
                ("meta:source_signature", spec.source_signature),
                ("meta:content_signature", signature),
            ),
        )
        db.commit()
        state = validate_envelope(db)
        membership = db.execute(
            "SELECT count(*) FROM _types_projection WHERE projection_id=?",
            (spec.projection_id,),
        ).fetchone()[0]
        if membership != len(objects):
            raise ProjectionError(
                f"projection membership mismatch: {membership} != {len(objects)}"
            )
        if db.execute("SELECT count(*) FROM chunks").fetchone()[0] != state.chunks:
            raise ProjectionError("generated chunks view does not preserve cardinality")
        if spec.provider_validate is not None:
            try:
                spec.provider_validate(db)
            except Exception as exc:
                raise ProjectionError(f"provider validation failed: {exc}") from exc

        embedding_error = None
        if embedding_runner is not None and state.embedding_debt:
            try:
                embedding_runner(db)
            except Exception as exc:
                embedding_error = str(exc)
            state = validate_envelope(db)
        semantic_status = "ready" if state.semantic_ready else "pending"
        db.execute(
            "INSERT INTO _metadata(key,value) VALUES('meta:semantic_status',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (semantic_status,),
        )
        if embedding_error is None:
            db.execute("DELETE FROM _metadata WHERE key='meta:semantic_error'")
        else:
            db.execute(
                "INSERT INTO _metadata(key,value) VALUES('meta:semantic_error',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (embedding_error,),
            )
        db.commit()
        return ProjectionReceipt(
            projection_id=spec.projection_id,
            output_path=str(candidate),
            source_signature=spec.source_signature,
            compiler_version=COMPILER_VERSION,
            object_count=len(objects),
            reference_count=len(references),
            unresolved_reference_count=sum(
                item["resolution_state"] == "unresolved" for item in references
            ),
            tree_edge_count=len(tree),
            fts_count=state.fts_rows,
            embedded_count=state.embedded_chunks,
            embedding_model=state.embedding_model,
            embedding_storage_dimension=state.embedding_storage_dimension,
            embedding_serve_dimension=state.embedding_serve_dimension,
            semantic_ready=state.semantic_ready,
            embedding_debt=state.embedding_debt,
            embedding_error=embedding_error,
            content_signature=signature,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
    except ProjectionError:
        raise
    except Exception as exc:
        raise ProjectionError(str(exc)) from exc
    finally:
        db.close()


def converge_projection_embeddings(
    candidate_path: Path,
    *,
    embedding_runner: EmbeddingRunner = embed_new,
):
    """Converge NULL-vector debt without rebuilding structural projection rows."""
    candidate = Path(candidate_path).expanduser().resolve()
    db = sqlite3.connect(candidate)
    try:
        try:
            embedding_runner(db)
            state = validate_envelope(db, require_embeddings=True)
        except Exception as exc:
            db.execute(
                "INSERT INTO _metadata(key,value) VALUES('meta:semantic_status','pending') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
            )
            db.execute(
                "INSERT INTO _metadata(key,value) VALUES('meta:semantic_error',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(exc),),
            )
            db.commit()
            raise ProjectionError(f"projection embedding failed: {exc}") from exc
        db.execute(
            "INSERT INTO _metadata(key,value) VALUES('meta:semantic_status','ready') "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
        )
        db.execute("DELETE FROM _metadata WHERE key='meta:semantic_error'")
        db.commit()
        return state
    finally:
        db.close()
