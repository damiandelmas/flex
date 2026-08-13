"""Composable storage profiles for Flex cells.

A Flex cell is a registered SQLite database with a small self-describing core.
Knowledge cells may add the conventional retrieval profile; source provenance,
view catalogs, operational receipts, and tree navigation are independent
extensions.  Providers decide which profiles their data needs and retain
authority over domain tables and relations.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


_CORE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _metadata (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS _presets (
    name        TEXT PRIMARY KEY,
    description TEXT,
    params      TEXT DEFAULT '',
    sql         TEXT,
    source      TEXT
);
"""

_LEGACY_CORE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS _presets (
    name        TEXT PRIMARY KEY,
    description TEXT,
    params      TEXT DEFAULT '',
    sql         TEXT,
    source      TEXT
);
"""

_RETRIEVAL_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _raw_chunks (
    id          TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    embedding   BLOB,
    timestamp   INTEGER,
    created_at  INTEGER DEFAULT (strftime('%s','now'))
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);
"""

_SOURCE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _raw_sources (
    source_id   TEXT PRIMARY KEY,
    title       TEXT,
    embedding   BLOB,
    timestamp   INTEGER,
    created_at  INTEGER DEFAULT (strftime('%s','now'))
);

-- This profile models one build/source membership per retrievable object.
-- It is not the universal Flex relationship model.
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id    TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    PRIMARY KEY (chunk_id)
);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);
"""

_VIEW_CATALOG_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _views (
    name        TEXT PRIMARY KEY,
    sql         TEXT NOT NULL,
    description TEXT,
    created_at  INTEGER
);
"""

_OPERATIONS_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _ops (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     INTEGER DEFAULT (strftime('%s','now')),
    operation     TEXT,
    target        TEXT,
    sql           TEXT,
    params        TEXT,
    rows_affected INTEGER,
    source        TEXT
);
"""

_TREE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _edges_tree (
    id          TEXT NOT NULL,
    parent_id   TEXT,
    branch_at   TEXT,
    relation    TEXT NOT NULL,
    depth       INTEGER DEFAULT 0,
    position    INTEGER,
    PRIMARY KEY (id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_tree_parent ON _edges_tree(parent_id);
CREATE INDEX IF NOT EXISTS idx_tree_relation ON _edges_tree(relation);
"""

_FTS_TRIGGERS = """\
DROP TRIGGER IF EXISTS main.raw_chunks_ai;
DROP TRIGGER IF EXISTS main.raw_chunks_ad;
DROP TRIGGER IF EXISTS main.raw_chunks_au;

CREATE TRIGGER main.raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
END;
CREATE TRIGGER main.raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content)
    VALUES('delete', OLD.rowid, OLD.content);
END;
CREATE TRIGGER main.raw_chunks_au AFTER UPDATE OF content ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content)
    VALUES('delete', OLD.rowid, OLD.content);
    INSERT INTO chunks_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
END;
"""

_META_COMPATIBILITY = """\
CREATE VIEW _meta AS SELECT key,value FROM _metadata;

CREATE TRIGGER _meta_insert INSTEAD OF INSERT ON _meta BEGIN
    INSERT INTO _metadata(key,value) VALUES(NEW.key,NEW.value)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value;
END;
CREATE TRIGGER _meta_update INSTEAD OF UPDATE ON _meta BEGIN
    DELETE FROM _metadata WHERE key=OLD.key AND NEW.key<>OLD.key;
    INSERT INTO _metadata(key,value) VALUES(NEW.key,NEW.value)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value;
END;
CREATE TRIGGER _meta_delete INSTEAD OF DELETE ON _meta BEGIN
    DELETE FROM _metadata WHERE key=OLD.key;
END;
"""


@dataclass(frozen=True)
class EnvelopeState:
    chunks: int
    sources: int
    fts_rows: int
    embedded_chunks: int
    embedding_model: str | None
    embedding_storage_dimension: int | None
    embedding_serve_dimension: int | None

    @property
    def semantic_ready(self) -> bool:
        return self.chunks == self.embedded_chunks and (
            self.chunks == 0 or self.embedding_model is not None
        )

    @property
    def embedding_debt(self) -> int:
        return self.chunks - self.embedded_chunks

    @property
    def structural_ready(self) -> bool:
        return self.fts_rows == self.chunks


def _object_type(db: sqlite3.Connection, name: str) -> str | None:
    row = db.execute(
        "SELECT type FROM sqlite_master WHERE name=? AND type IN ('table','view')",
        (name,),
    ).fetchone()
    return str(row[0]) if row else None


def _has_metadata_shape(db: sqlite3.Connection, name: str) -> bool:
    columns = [
        (str(row[1]), str(row[2]).upper())
        for row in db.execute(f'PRAGMA table_info("{name}")')
    ]
    return columns == [("key", "TEXT"), ("value", "TEXT")]


def _has_readable_metadata_shape(db: sqlite3.Connection, name: str) -> bool:
    """Probe the read contract without schema PRAGMAs or mutation.

    Query materializers intentionally run under a narrower authorizer than
    final user SQL.  Ordinary metadata discovery is engine plumbing inside
    that boundary, so it must use the same harmless relational read available
    to the materializer rather than request schema introspection privileges.
    Explicit writable migration still uses :func:`_has_metadata_shape` for
    strict column/type validation.
    """
    try:
        db.execute(f'SELECT key,value FROM "{name}" LIMIT 0').fetchall()
    except sqlite3.DatabaseError:
        return False
    return True


def metadata_relation(db: sqlite3.Connection) -> str | None:
    """Return the canonical readable metadata relation without migrating."""
    canonical = _object_type(db, "_metadata")
    if canonical is not None:
        if not _has_readable_metadata_shape(db, "_metadata"):
            return None
        return "_metadata"
    if (_object_type(db, "_meta") is not None
            and _has_readable_metadata_shape(db, "_meta")):
        return "_meta"
    return None


def _table_rows(db: sqlite3.Connection, name: str) -> list[tuple]:
    return db.execute(f'SELECT key,value FROM "{name}" ORDER BY key').fetchall()


def _metadata_tables_equivalent(db: sqlite3.Connection) -> bool:
    columns = {}
    for name in ("_metadata", "_meta"):
        columns[name] = [
            (str(row[1]), str(row[2]).upper())
            for row in db.execute(f'PRAGMA table_info("{name}")')
        ]
    expected = [("key", "TEXT"), ("value", "TEXT")]
    return (
        columns["_metadata"] == expected
        and columns["_meta"] == expected
        and _table_rows(db, "_metadata") == _table_rows(db, "_meta")
    )


def _install_meta_compatibility(db: sqlite3.Connection) -> None:
    for trigger in ("_meta_insert", "_meta_update", "_meta_delete"):
        db.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
    if _object_type(db, "_meta") == "view":
        db.execute("DROP VIEW _meta")
    db.executescript(_META_COMPATIBILITY)


def ensure_metadata_surface(db: sqlite3.Connection) -> None:
    """Make ``_metadata`` authoritative and expose legacy ``_meta`` reads.

    Migration is explicit and writable. Read-only discovery should call
    :func:`metadata_relation` instead. Conflicting dual physical tables are
    refused rather than merged heuristically.
    """
    canonical = _object_type(db, "_metadata")
    legacy = _object_type(db, "_meta")
    if canonical is not None and not _has_metadata_shape(db, "_metadata"):
        raise ValueError("_metadata exists but is not key/value cell metadata")
    if legacy is not None and not _has_metadata_shape(db, "_meta"):
        raise ValueError("_meta exists but is not legacy key/value cell metadata")
    if canonical == "table" and legacy == "view":
        triggers = {
            str(row[0]) for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND name IN ('_meta_insert','_meta_update','_meta_delete')"
            )
        }
        if triggers == {"_meta_insert", "_meta_update", "_meta_delete"}:
            return
    if canonical == "view":
        raise ValueError("_metadata must be a physical table")
    if legacy == "table" and canonical == "table":
        if not _metadata_tables_equivalent(db):
            raise ValueError("conflicting physical _meta and _metadata tables")
        db.execute("DROP TABLE _meta")
        legacy = None
    elif legacy == "table" and canonical is None:
        db.execute("ALTER TABLE _meta RENAME TO _metadata")
        canonical = "table"
        legacy = None
    elif legacy == "view" and canonical is None:
        raise ValueError("_meta compatibility view has no _metadata authority")

    if canonical is None:
        db.execute(
            "CREATE TABLE _metadata(key TEXT PRIMARY KEY,value TEXT)"
        )
    _install_meta_compatibility(db)


def install_cell_core(db: sqlite3.Connection) -> None:
    """Install the minimal self-describing Flex cell core."""
    ensure_metadata_surface(db)
    db.executescript(_CORE_SCHEMA)


def install_retrieval(db: sqlite3.Connection) -> None:
    """Install lossless text, FTS, and nullable shared-space vectors."""
    had_fts = _object_type(db, "chunks_fts") == "table"
    db.executescript(_RETRIEVAL_SCHEMA)
    if not had_fts and db.execute("SELECT 1 FROM _raw_chunks LIMIT 1").fetchone():
        db.execute("INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')")
    db.executescript(_FTS_TRIGGERS)


def install_source_provenance(db: sqlite3.Connection) -> None:
    """Install the optional SDK source-membership profile."""
    db.executescript(_SOURCE_SCHEMA)


def install_view_catalog(db: sqlite3.Connection) -> None:
    """Install the optional catalog for provider-authored SQL views."""
    db.executescript(_VIEW_CATALOG_SCHEMA)


def install_operations(db: sqlite3.Connection) -> None:
    """Install optional local build and maintenance receipts."""
    db.executescript(_OPERATIONS_SCHEMA)


def ensure_tree(db: sqlite3.Connection) -> None:
    """Install the optional cell-local hierarchical navigation projection."""
    db.executescript(_TREE_SCHEMA)
    columns = {row[1] for row in db.execute("PRAGMA table_info(_edges_tree)")}
    if "position" not in columns:
        db.execute("ALTER TABLE _edges_tree ADD COLUMN position INTEGER")


def install_envelope(
    db: sqlite3.Connection,
    *,
    tree: bool = True,
    generate_views: bool = False,
) -> None:
    """Install the legacy SDK bundle from explicit composable profiles.

    New providers should request the core and only the extensions they use.
    This compatibility entry point preserves the existing SDK source/tree
    contract while sharing the same underlying installers.
    """
    # Existing SDK providers still issue native UPSERT statements against a
    # physical _meta table. Keep that provider lifecycle intact until each is
    # migrated deliberately; new projection/profile code uses install_cell_core.
    relation = metadata_relation(db)
    if relation is None:
        if _object_type(db, "_metadata") is not None or _object_type(db, "_meta") is not None:
            raise ValueError("existing metadata name has a non-key/value provider shape")
        db.executescript(_LEGACY_CORE_SCHEMA)
    else:
        db.execute("""CREATE TABLE IF NOT EXISTS _presets (
            name TEXT PRIMARY KEY,description TEXT,params TEXT DEFAULT '',
            sql TEXT,source TEXT
        )""")
    install_retrieval(db)
    install_source_provenance(db)
    install_view_catalog(db)
    install_operations(db)
    if tree:
        ensure_tree(db)
    if generate_views:
        regenerate_envelope_views(db)
    db.commit()


def regenerate_envelope_views(db: sqlite3.Connection) -> None:
    """Install neutral retrieval views without discovering provider schema."""
    db.execute("DROP VIEW IF EXISTS chunks")
    db.execute(
        "CREATE VIEW chunks AS SELECT id,content,timestamp,created_at "
        "FROM _raw_chunks"
    )
    if _object_type(db, "_raw_sources") == "table":
        db.execute("DROP VIEW IF EXISTS sources")
        db.execute(
            "CREATE VIEW sources AS "
            "SELECT s.source_id,s.title,s.timestamp,s.created_at,"
            "count(e.chunk_id) chunk_count FROM _raw_sources s "
            "LEFT JOIN _edges_source e ON e.source_id=s.source_id "
            "GROUP BY s.source_id"
        )
    db.commit()


def inspect_envelope(db: sqlite3.Connection) -> EnvelopeState:
    """Inspect the installed retrieval profile without assuming extensions."""
    relation = metadata_relation(db)
    metadata = {}
    if relation is not None:
        metadata = dict(db.execute(
            f"SELECT key,value FROM {relation} WHERE key IN "
            "('embedding_model','embedding_dim','vec:model','vec:serve_dim')"
        ))
    model = metadata.get("embedding_model") or metadata.get("vec:model")
    storage = metadata.get("embedding_dim")
    serve = metadata.get("vec:serve_dim")
    sources = 0
    if _object_type(db, "_raw_sources") == "table":
        sources = db.execute("SELECT count(*) FROM _raw_sources").fetchone()[0]
    return EnvelopeState(
        chunks=db.execute("SELECT count(*) FROM _raw_chunks").fetchone()[0],
        sources=sources,
        fts_rows=db.execute("SELECT count(*) FROM chunks_fts").fetchone()[0],
        embedded_chunks=db.execute(
            "SELECT count(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
        ).fetchone()[0],
        embedding_model=model,
        embedding_storage_dimension=int(storage) if storage else None,
        embedding_serve_dimension=int(serve) if serve else None,
    )


def validate_cell_core(db: sqlite3.Connection) -> None:
    required = {"_metadata", "_meta", "_presets"}
    present = {
        row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise ValueError("Flex cell core is missing: " + ", ".join(missing))
    if _object_type(db, "_metadata") != "table" or _object_type(db, "_meta") != "view":
        raise ValueError("Flex cell metadata authority is not canonical")


def validate_retrieval(
    db: sqlite3.Connection,
    *,
    require_embeddings: bool = False,
) -> EnvelopeState:
    """Validate exact/lexical retrieval and optionally semantic convergence."""
    validate_cell_core(db)
    required = {"_raw_chunks", "chunks_fts"}
    present = {
        row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
        )
    }
    missing = sorted(required - present)
    if missing:
        raise ValueError("Flex retrieval profile is missing: " + ", ".join(missing))

    state = inspect_envelope(db)
    if state.fts_rows != state.chunks:
        raise ValueError(
            f"Flex retrieval FTS coverage mismatch: {state.fts_rows} != {state.chunks}"
        )
    if require_embeddings and not state.semantic_ready:
        raise ValueError(
            "Flex semantic retrieval is incomplete: "
            f"{state.embedded_chunks}/{state.chunks} embedded"
        )
    if require_embeddings and state.chunks:
        if state.embedding_storage_dimension is None:
            raise ValueError("Flex retrieval has no embedding storage dimension")
        bad_dimensions = db.execute(
            "SELECT count(*) FROM _raw_chunks WHERE embedding IS NOT NULL "
            "AND length(embedding) != ?",
            (state.embedding_storage_dimension * 4,),
        ).fetchone()[0]
        if bad_dimensions:
            raise ValueError(
                f"Flex retrieval has {bad_dimensions} incompatible embeddings"
            )
    return state


def validate_source_provenance(db: sqlite3.Connection) -> None:
    """Validate the optional one-source-per-object SDK membership profile."""
    if _object_type(db, "_raw_sources") != "table" or _object_type(
        db, "_edges_source"
    ) != "table":
        raise ValueError("Flex source provenance profile is incomplete")
    orphaned = db.execute(
        "SELECT count(*) FROM _raw_chunks c "
        "LEFT JOIN _edges_source e ON e.chunk_id=c.id "
        "WHERE e.chunk_id IS NULL"
    ).fetchone()[0]
    if orphaned:
        raise ValueError(f"Flex source provenance has {orphaned} unowned objects")


def validate_envelope(
    db: sqlite3.Connection,
    *,
    require_embeddings: bool = False,
) -> EnvelopeState:
    """Validate the installed core/retrieval profiles and present extensions.

    Kept as the compatibility validator for existing callers. Source
    provenance is checked only when that profile is installed; tree and domain
    relationships remain provider-owned validations.
    """
    state = validate_retrieval(db, require_embeddings=require_embeddings)
    source_objects = (
        _object_type(db, "_raw_sources"), _object_type(db, "_edges_source")
    )
    if any(value == "table" for value in source_objects):
        validate_source_provenance(db)
    return state
