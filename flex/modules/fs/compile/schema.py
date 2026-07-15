"""Canonical additive schema for mixed filesystem cells."""

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
    extraction_state TEXT NOT NULL DEFAULT 'ok'
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


def ensure_schema(conn) -> None:
    conn.executescript(FILESYSTEM_SCHEMA_DDL)
