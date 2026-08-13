"""Optional mechanical relationship profile for addressable Flex objects.

The carrier deliberately records only occurrence identity, complete endpoint
addresses, an open provider-declared relation token, and optional order.  It
does not replace provider-native relation tables or assign semantics to their
tokens.
"""

from __future__ import annotations

import sqlite3
import uuid


_RELATIONSHIP_NAMESPACE = uuid.UUID("10a35856-57e3-5446-9d20-853b28ad4f6c")

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _edges_relationships (
    edge_id          TEXT PRIMARY KEY CHECK (length(trim(edge_id)) > 0),
    source_cell_id   TEXT NOT NULL CHECK (length(trim(source_cell_id)) > 0),
    source_object_id TEXT NOT NULL CHECK (length(trim(source_object_id)) > 0),
    relation         TEXT NOT NULL CHECK (length(trim(relation)) > 0),
    target_cell_id   TEXT NOT NULL CHECK (length(trim(target_cell_id)) > 0),
    target_object_id TEXT NOT NULL CHECK (length(trim(target_object_id)) > 0),
    ordinal          INTEGER CHECK (ordinal IS NULL OR ordinal >= 0)
);
CREATE INDEX IF NOT EXISTS idx_relationships_source
ON _edges_relationships(source_cell_id, source_object_id, relation, ordinal);
CREATE INDEX IF NOT EXISTS idx_relationships_target
ON _edges_relationships(target_cell_id, target_object_id, relation);
"""


def relationship_id(profile: str, occurrence_key: str) -> str:
    """Return a deterministic identity for one provider occurrence."""
    if not profile.strip() or not occurrence_key.strip():
        raise ValueError("relationship identity inputs must not be blank")
    return str(uuid.uuid5(_RELATIONSHIP_NAMESPACE, f"{profile}\0{occurrence_key}"))


def install_relationships(db: sqlite3.Connection) -> None:
    """Install the optional relationship interoperability profile."""
    row = db.execute(
        "SELECT type FROM sqlite_master WHERE name='_edges_relationships'"
    ).fetchone()
    if row is not None and str(row[0]) != "table":
        raise ValueError("_edges_relationships must be a physical table")
    db.executescript(_SCHEMA)


def validate_relationships(db: sqlite3.Connection) -> None:
    """Validate the lossless mechanical carrier without interpreting tokens."""
    row = db.execute(
        "SELECT type FROM sqlite_master WHERE name='_edges_relationships'"
    ).fetchone()
    if row is None or str(row[0]) != "table":
        raise ValueError("Flex relationship profile is missing")
    expected = [
        "edge_id", "source_cell_id", "source_object_id", "relation",
        "target_cell_id", "target_object_id", "ordinal",
    ]
    actual = [str(row[1]) for row in db.execute(
        "PRAGMA table_info(_edges_relationships)"
    )]
    if actual != expected:
        raise ValueError(
            "Flex relationship carrier has incompatible columns: "
            + ", ".join(actual)
        )
    invalid = db.execute(
        "SELECT count(*) FROM _edges_relationships "
        "WHERE edge_id='' OR source_cell_id='' OR source_object_id='' "
        "OR relation='' OR target_cell_id='' OR target_object_id='' "
        "OR ordinal < 0"
    ).fetchone()[0]
    if invalid:
        raise ValueError(f"Flex relationship carrier has {invalid} invalid rows")
