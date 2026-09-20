"""Canonical additive schema and validator for mixed filesystem cells."""

from __future__ import annotations

import json
from dataclasses import dataclass


DOCUMENT_PROFILE_VERSION = 2
DOCUMENT_IDENTITY_KEY = "fs:identity"
DOCUMENT_IDENTITY_REQUIRED = "required"

DOCUMENT_VECTOR_CONTRACT = {
    "vec:model": "nomic-v1.5-fp32",
    "embedding_model": "nomic-embed-text-v1.5-fp32",
    "embedding_dim": "768",
    "vec:serve_dim": "256",
    "vec:dtype": "float32",
    "vec:normalization": "l2",
    "vec:score": "cosine",
}

FILESYSTEM_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS _types_filesystem (
    chunk_id         TEXT PRIMARY KEY,
    file_kind        TEXT NOT NULL,
    chunk_kind       TEXT NOT NULL,
    section_title    TEXT,
    section_type     TEXT,
    position         INTEGER NOT NULL,
    depth            INTEGER NOT NULL DEFAULT 0,
    container_id     TEXT,
    content_hash     TEXT NOT NULL,
    language         TEXT,
    extraction_state TEXT NOT NULL DEFAULT 'ok'
);
CREATE INDEX IF NOT EXISTS idx_filesystem_kind ON _types_filesystem(file_kind);
CREATE INDEX IF NOT EXISTS idx_filesystem_title ON _types_filesystem(section_title);

CREATE TABLE IF NOT EXISTS _filesystem_source_state (
    source_id        TEXT PRIMARY KEY,
    source_path      TEXT NOT NULL,
    file_kind        TEXT NOT NULL,
    content_hash     TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    mtime_ns         INTEGER NOT NULL,
    source_state     TEXT NOT NULL,
    extraction_state TEXT NOT NULL DEFAULT 'ok',
    accepted_generation INTEGER NOT NULL DEFAULT 1,
    profile_version  INTEGER NOT NULL DEFAULT 2
);

CREATE TABLE IF NOT EXISTS _types_markdown_source (
    source_id     TEXT PRIMARY KEY,
    folder        TEXT,
    tags          TEXT,
    aliases       TEXT,
    note_created  TEXT,
    file_modified TEXT
);

CREATE TABLE IF NOT EXISTS _fields_inline (
    chunk_id    TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    field_key   TEXT NOT NULL,
    field_value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fields_key ON _fields_inline(field_key);
CREATE INDEX IF NOT EXISTS idx_fields_source ON _fields_inline(source_id);

CREATE TABLE IF NOT EXISTS _fields_frontmatter (
    source_id   TEXT NOT NULL,
    field_key   TEXT NOT NULL,
    field_value TEXT NOT NULL,
    position    INTEGER NOT NULL,
    PRIMARY KEY (source_id, field_key, position)
);
CREATE INDEX IF NOT EXISTS idx_frontmatter_key ON _fields_frontmatter(field_key);

CREATE TABLE IF NOT EXISTS _edges_wikilink (
    chunk_id TEXT NOT NULL,
    from_path TEXT NOT NULL,
    to_path TEXT NOT NULL,
    PRIMARY KEY (from_path, to_path, chunk_id)
);
CREATE INDEX IF NOT EXISTS idx_wikilink_to ON _edges_wikilink(to_path);

CREATE TABLE IF NOT EXISTS _edges_wikilink_unresolved (
    from_path TEXT NOT NULL,
    raw_target TEXT NOT NULL,
    PRIMARY KEY (from_path, raw_target)
);

CREATE TABLE IF NOT EXISTS _edges_wikilink_raw (
    source_id TEXT NOT NULL,
    raw_target TEXT NOT NULL,
    PRIMARY KEY (source_id, raw_target)
);

CREATE TABLE IF NOT EXISTS _edges_call (
    caller_id TEXT NOT NULL,
    callee_name TEXT NOT NULL,
    PRIMARY KEY (caller_id, callee_name)
);
CREATE INDEX IF NOT EXISTS idx_filesystem_call_name ON _edges_call(callee_name);

CREATE TABLE IF NOT EXISTS _edges_import (
    source_id TEXT NOT NULL,
    module TEXT NOT NULL,
    name TEXT,
    UNIQUE (source_id, module, name)
);
CREATE INDEX IF NOT EXISTS idx_filesystem_import_module ON _edges_import(module);

CREATE TABLE IF NOT EXISTS _symbols (
    name TEXT NOT NULL,
    def_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    kind TEXT,
    PRIMARY KEY (name, def_id)
);
CREATE INDEX IF NOT EXISTS idx_filesystem_symbols_name ON _symbols(name);
CREATE INDEX IF NOT EXISTS idx_filesystem_symbols_file ON _symbols(file_id);

CREATE TABLE IF NOT EXISTS _edges_fs_identity (
    source_id TEXT PRIMARY KEY,
    file_uuid TEXT
);
"""


SOURCES_VIEW_SQL = """CREATE VIEW sources AS
SELECT
    src.source_id,
    src.title,
    src.timestamp,
    src.created_at,
    COUNT(edge.chunk_id) AS chunk_count,
    ident.file_uuid,
    state.source_path,
    state.file_kind,
    state.content_hash,
    state.size_bytes,
    state.mtime_ns,
    state.source_state,
    state.extraction_state,
    state.accepted_generation,
    state.profile_version,
    markdown.folder,
    markdown.tags,
    markdown.aliases,
    markdown.note_created,
    markdown.file_modified
FROM _raw_sources AS src
LEFT JOIN _edges_source AS edge ON edge.source_id = src.source_id
LEFT JOIN _filesystem_source_state AS state ON state.source_id = src.source_id
LEFT JOIN _edges_fs_identity AS ident ON ident.source_id = src.source_id
LEFT JOIN _types_markdown_source AS markdown ON markdown.source_id = src.source_id
GROUP BY src.source_id"""


CHUNKS_VIEW_SQL = """CREATE VIEW chunks AS
SELECT
    raw.id,
    raw.content,
    raw.timestamp,
    raw.created_at,
    edge.source_id,
    src.title,
    ident.file_uuid,
    typed.file_kind,
    typed.chunk_kind,
    typed.section_title,
    typed.section_type,
    typed.position,
    typed.depth,
    typed.container_id,
    typed.content_hash,
    typed.language,
    typed.extraction_state,
    state.source_path,
    state.accepted_generation,
    state.profile_version
FROM _raw_chunks AS raw
JOIN _edges_source AS edge ON edge.chunk_id = raw.id
JOIN _raw_sources AS src ON src.source_id = edge.source_id
JOIN _types_filesystem AS typed ON typed.chunk_id = raw.id
LEFT JOIN _filesystem_source_state AS state ON state.source_id = edge.source_id
LEFT JOIN _edges_fs_identity AS ident ON ident.source_id = edge.source_id"""


class DocumentProfileError(RuntimeError):
    pass


@dataclass(frozen=True)
class DocumentProfileState:
    sources: int
    chunks: int
    fts_rows: int
    embedded_sources: int
    embedded_chunks: int
    vector_bytes: int | None


def _install_recovery_views(conn) -> None:
    """Install cardinality-one recovery views and protect them from discovery.

    Generic view discovery joins every apparent sidecar.  Multivalued
    frontmatter and wikilink relations would consequently multiply documents,
    and coding-agent visibility tables can inappropriately filter filesystem
    cells.  These curated views join only one-to-one profile relations.
    """
    conn.execute("DROP VIEW IF EXISTS sources")
    conn.execute("DROP VIEW IF EXISTS chunks")
    conn.execute(SOURCES_VIEW_SQL)
    conn.execute(CHUNKS_VIEW_SQL)
    conn.execute(
        "INSERT OR REPLACE INTO _views(name,sql,description,created_at) "
        "VALUES('sources',?,'Cardinality-one filesystem source recovery',strftime('%s','now'))",
        (SOURCES_VIEW_SQL,),
    )
    conn.execute(
        "INSERT OR REPLACE INTO _views(name,sql,description,created_at) "
        "VALUES('chunks',?,'Cardinality-one filesystem chunk recovery',strftime('%s','now'))",
        (CHUNKS_VIEW_SQL,),
    )


def _ensure_state_columns(conn) -> None:
    columns = {row[1] for row in conn.execute(
        "PRAGMA table_info(_filesystem_source_state)"
    )}
    if "accepted_generation" not in columns:
        conn.execute(
            "ALTER TABLE _filesystem_source_state "
            "ADD COLUMN accepted_generation INTEGER NOT NULL DEFAULT 1"
        )
    if "profile_version" not in columns:
        conn.execute(
            "ALTER TABLE _filesystem_source_state "
            "ADD COLUMN profile_version INTEGER NOT NULL DEFAULT 1"
        )


def ensure_schema(conn) -> None:
    conn.executescript(FILESYSTEM_SCHEMA_DDL)
    _ensure_state_columns(conn)
    conn.execute("""CREATE TABLE IF NOT EXISTS _views (
        name TEXT PRIMARY KEY,
        sql TEXT NOT NULL,
        description TEXT,
        created_at INTEGER
    )""")
    current = dict(conn.execute(
        "SELECT name,sql FROM _views WHERE name IN ('sources','chunks')"
    ))
    if (current.get("sources") != SOURCES_VIEW_SQL
            or current.get("chunks") != CHUNKS_VIEW_SQL):
        _install_recovery_views(conn)


def ensure_document_vector_contract(conn) -> None:
    """Fill missing metadata for the one accepted document-vector space.

    Existing conflicting values are deliberately retained so the activation
    validator can reject a mislabeled or incompatible candidate instead of
    silently rewriting its declared semantics.
    """
    from flex.envelope import metadata_relation

    relation = metadata_relation(conn)
    if relation is None:
        raise DocumentProfileError("document cell has no metadata authority")
    conn.executemany(
        f"INSERT OR IGNORE INTO {relation}(key,value) VALUES(?,?)",
        DOCUMENT_VECTOR_CONTRACT.items(),
    )


def ensure_document_identity_contract(conn) -> None:
    """Declare strict filesystem identity for a shared-profile candidate."""
    from flex.envelope import metadata_relation

    relation = metadata_relation(conn)
    if relation is None:
        raise DocumentProfileError("document cell has no metadata authority")
    conn.execute(
        f"INSERT OR IGNORE INTO {relation}(key,value) VALUES(?,?)",
        (DOCUMENT_IDENTITY_KEY, DOCUMENT_IDENTITY_REQUIRED),
    )


def document_identity_required(conn) -> bool:
    """Return whether per-source publication must fail without a Soma UUID."""
    from flex.envelope import metadata_relation

    relation = metadata_relation(conn)
    if relation is None:
        return False
    row = conn.execute(
        f"SELECT value FROM {relation} WHERE key=?", (DOCUMENT_IDENTITY_KEY,),
    ).fetchone()
    return bool(row and row[0] == DOCUMENT_IDENTITY_REQUIRED)


def validate_document_profile(
    conn, *, require_embeddings: bool = True,
    expected_model: str | None = None,
    expected_model_fingerprint: str | None = None,
    expected_storage_dim: int | None = None,
    expected_serve_dim: int | None = None,
    expected_dtype: str | None = "float32",
    expected_normalization: str | None = "l2",
    expected_score: str | None = "cosine",
    require_identity: bool | None = None,
) -> DocumentProfileState:
    """Fail closed unless one candidate is complete and internally exact."""
    ensure_schema(conn)
    conn.commit()

    quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
    if quick_check != "ok":
        raise DocumentProfileError(f"PRAGMA quick_check failed: {quick_check}")

    sources = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    fts_rows = conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
    if chunks != fts_rows:
        raise DocumentProfileError(
            f"FTS coverage mismatch: chunks={chunks}, fts={fts_rows}"
        )
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _raw_chunks raw "
        "WHERE NOT EXISTS (SELECT 1 FROM chunks_fts fts WHERE fts.rowid=raw.rowid))"
    ).fetchone()[0]:
        raise DocumentProfileError("FTS membership does not match live chunks")

    for relation, key in (("_edges_source", "chunk_id"),
                          ("_types_filesystem", "chunk_id")):
        rows = conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
        if rows != chunks:
            raise DocumentProfileError(
                f"{relation} coverage mismatch: rows={rows}, chunks={chunks}"
            )
        if conn.execute(
            f"SELECT EXISTS(SELECT 1 FROM {relation} rel "
            f"LEFT JOIN _raw_chunks raw ON raw.id=rel.{key} WHERE raw.id IS NULL)"
        ).fetchone()[0]:
            raise DocumentProfileError(f"{relation} contains orphaned rows")
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _edges_source edge "
        "LEFT JOIN _raw_sources raw ON raw.source_id=edge.source_id "
        "WHERE raw.source_id IS NULL)"
    ).fetchone()[0]:
        raise DocumentProfileError("source containment points outside live sources")

    if conn.execute(
        "SELECT EXISTS("
        "SELECT source_id FROM _raw_sources "
        "EXCEPT SELECT source_id FROM _filesystem_source_state WHERE source_state='indexed'"
        ") OR EXISTS("
        "SELECT source_id FROM _filesystem_source_state WHERE source_state='indexed' "
        "EXCEPT SELECT source_id FROM _raw_sources)"
    ).fetchone()[0]:
        raise DocumentProfileError("source rows do not match indexed source state")
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _filesystem_source_state "
        "WHERE profile_version<>? OR accepted_generation<1)",
        (DOCUMENT_PROFILE_VERSION,),
    ).fetchone()[0]:
        raise DocumentProfileError("source publication state is stale or invalid")
    identity_required = (
        document_identity_required(conn) if require_identity is None
        else require_identity
    )
    if identity_required and not document_identity_required(conn):
        raise DocumentProfileError(
            f"document identity contract must be {DOCUMENT_IDENTITY_KEY}="
            f"{DOCUMENT_IDENTITY_REQUIRED}"
        )
    if identity_required and conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _filesystem_source_state state "
        "LEFT JOIN _edges_fs_identity ident USING(source_id) "
        "WHERE ident.file_uuid IS NULL OR ident.file_uuid='')"
    ).fetchone()[0]:
        raise DocumentProfileError("filesystem identity coverage is incomplete")
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _edges_fs_identity ident "
        "LEFT JOIN _filesystem_source_state state USING(source_id) "
        "WHERE state.source_id IS NULL)"
    ).fetchone()[0]:
        raise DocumentProfileError("filesystem identity contains orphaned rows")

    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _types_markdown_source markdown "
        "LEFT JOIN _raw_sources raw USING(source_id) WHERE raw.source_id IS NULL)"
    ).fetchone()[0]:
        raise DocumentProfileError("Markdown source metadata contains orphaned rows")
    if conn.execute(
        "SELECT EXISTS("
        "SELECT source_id FROM _filesystem_source_state "
        "WHERE source_state='indexed' AND file_kind='markdown' "
        "EXCEPT SELECT source_id FROM _types_markdown_source"
        ") OR EXISTS("
        "SELECT source_id FROM _types_markdown_source "
        "EXCEPT SELECT source_id FROM _filesystem_source_state "
        "WHERE source_state='indexed' AND file_kind='markdown'"
        ")"
    ).fetchone()[0]:
        raise DocumentProfileError("Markdown source metadata coverage is incomplete")
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _fields_frontmatter field "
        "LEFT JOIN _raw_sources raw USING(source_id) WHERE raw.source_id IS NULL)"
    ).fetchone()[0]:
        raise DocumentProfileError("frontmatter contains orphaned rows")
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _fields_frontmatter field "
        "JOIN _filesystem_source_state state USING(source_id) "
        "WHERE state.file_kind<>'markdown')"
    ).fetchone()[0]:
        raise DocumentProfileError("frontmatter is attached to a non-Markdown source")
    for source_id, field_key, field_value, position in conn.execute(
        "SELECT source_id,field_key,field_value,position FROM _fields_frontmatter"
    ):
        try:
            json.loads(field_value)
        except (TypeError, ValueError) as exc:
            raise DocumentProfileError(
                f"invalid frontmatter JSON: {source_id}:{field_key}"
            ) from exc
        if int(position) < -2:
            raise DocumentProfileError(
                f"invalid frontmatter position: {source_id}:{field_key}:{position}"
            )
        if int(position) == -2 and field_value != "[]":
            raise DocumentProfileError(
                f"invalid empty-list sentinel: {source_id}:{field_key}"
            )
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _fields_frontmatter "
        "GROUP BY source_id,field_key "
        "HAVING MIN(position)<0 AND MAX(position)>=0)"
    ).fetchone()[0]:
        raise DocumentProfileError("frontmatter mixes scalar and list positions")
    if conn.execute(
        "SELECT EXISTS(SELECT 1 FROM _fields_frontmatter "
        "WHERE position>=0 GROUP BY source_id,field_key "
        "HAVING MIN(position)<>0 OR MAX(position)<>COUNT(*)-1)"
    ).fetchone()[0]:
        raise DocumentProfileError("frontmatter list positions are not contiguous")

    from flex.sdk import _make_chunk_id
    for chunk_id, source_id, position, content in conn.execute(
        "SELECT raw.id,edge.source_id,typed.position,raw.content "
        "FROM _raw_chunks raw JOIN _edges_source edge ON edge.chunk_id=raw.id "
        "JOIN _types_filesystem typed ON typed.chunk_id=raw.id"
    ):
        if chunk_id != _make_chunk_id(source_id, position, content):
            raise DocumentProfileError(f"nondeterministic chunk identity: {chunk_id}")

    embedded_chunks = conn.execute(
        "SELECT COUNT(*) FROM _raw_chunks WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    embedded_sources = conn.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE embedding IS NOT NULL"
    ).fetchone()[0]
    if require_embeddings and embedded_chunks != chunks:
        raise DocumentProfileError(
            f"chunk embedding coverage mismatch: {embedded_chunks}/{chunks}"
        )
    if require_embeddings and embedded_sources != sources:
        raise DocumentProfileError(
            f"source embedding coverage mismatch: {embedded_sources}/{sources}"
        )

    chunk_widths = {row[0] for row in conn.execute(
        "SELECT DISTINCT length(embedding) FROM _raw_chunks "
        "WHERE embedding IS NOT NULL"
    )}
    source_widths = {row[0] for row in conn.execute(
        "SELECT DISTINCT length(embedding) FROM _raw_sources "
        "WHERE embedding IS NOT NULL"
    )}
    if len(chunk_widths) > 1 or len(source_widths) > 1 or (
        chunk_widths and source_widths and chunk_widths != source_widths
    ):
        raise DocumentProfileError(
            f"mixed vector widths: chunks={sorted(chunk_widths)}, "
            f"sources={sorted(source_widths)}"
        )
    vector_bytes = next(iter(chunk_widths or source_widths), None)
    if vector_bytes is not None and vector_bytes % 4:
        raise DocumentProfileError(f"invalid fp32 vector width: {vector_bytes} bytes")

    metadata = dict(conn.execute(
        "SELECT key,value FROM _meta WHERE key IN "
        "('vec:model','embedding_model','embedding_dim','vec:serve_dim',"
        "'vec:dtype','vec:normalization','vec:score')"
    ))
    model = metadata.get("vec:model")
    model_fingerprint = metadata.get("embedding_model")
    storage_dim = int(metadata["embedding_dim"]) if metadata.get("embedding_dim") else None
    serve_dim = int(metadata["vec:serve_dim"]) if metadata.get("vec:serve_dim") else None
    dtype = metadata.get("vec:dtype")
    normalization = metadata.get("vec:normalization")
    score = metadata.get("vec:score")
    if expected_model is not None and model != expected_model:
        raise DocumentProfileError(
            f"vector model mismatch: expected {expected_model}, found {model}"
        )
    if (expected_model_fingerprint is not None
            and model_fingerprint != expected_model_fingerprint):
        raise DocumentProfileError(
            "embedding model fingerprint mismatch: expected "
            f"{expected_model_fingerprint}, found {model_fingerprint}"
        )
    if expected_storage_dim is not None and storage_dim != expected_storage_dim:
        raise DocumentProfileError(
            f"storage dimension mismatch: expected {expected_storage_dim}, found {storage_dim}"
        )
    if expected_serve_dim is not None and serve_dim != expected_serve_dim:
        raise DocumentProfileError(
            f"serve dimension mismatch: expected {expected_serve_dim}, found {serve_dim}"
        )
    if require_embeddings and expected_dtype is not None and dtype != expected_dtype:
        raise DocumentProfileError(
            f"vector dtype mismatch: expected {expected_dtype}, found {dtype}"
        )
    if (require_embeddings and expected_normalization is not None
            and normalization != expected_normalization):
        raise DocumentProfileError(
            "vector normalization mismatch: expected "
            f"{expected_normalization}, found {normalization}"
        )
    if require_embeddings and expected_score is not None and score != expected_score:
        raise DocumentProfileError(
            f"vector score mismatch: expected {expected_score}, found {score}"
        )
    declared_dim = expected_storage_dim or storage_dim
    if vector_bytes is not None and declared_dim is not None and vector_bytes != declared_dim * 4:
        raise DocumentProfileError(
            f"vector blob width {vector_bytes} does not match declared {declared_dim}d fp32"
        )
    if require_embeddings and chunks and (
        not model or not model_fingerprint or storage_dim is None or serve_dim is None
        or not dtype or not normalization or not score
    ):
        raise DocumentProfileError("embedded candidate lacks an explicit vector contract")

    if conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0] != sources:
        raise DocumentProfileError("sources view multiplies or drops native sources")
    if conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] != chunks:
        raise DocumentProfileError("chunks view multiplies or drops native chunks")
    if conn.execute(
        "SELECT COUNT(*)<>COUNT(DISTINCT source_id) FROM sources"
    ).fetchone()[0]:
        raise DocumentProfileError("sources view identity is not unique")
    if conn.execute(
        "SELECT COUNT(*)<>COUNT(DISTINCT id) FROM chunks"
    ).fetchone()[0]:
        raise DocumentProfileError("chunks view identity is not unique")

    return DocumentProfileState(
        sources=sources,
        chunks=chunks,
        fts_rows=fts_rows,
        embedded_sources=embedded_sources,
        embedded_chunks=embedded_chunks,
        vector_bytes=vector_bytes,
    )
