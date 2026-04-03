-- SOMA Identity Module — Edge Tables
-- All tables are 1:N (NO PK on chunk_id) — NOT in auto-generated views.

-- File identity: stable UUID that survives renames and moves
CREATE TABLE IF NOT EXISTS _edges_file_identity (
    chunk_id TEXT NOT NULL,
    file_uuid TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_efi_chunk ON _edges_file_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_efi_uuid ON _edges_file_identity(file_uuid);

-- Repo identity: git root commit hash (stable across repo moves)
CREATE TABLE IF NOT EXISTS _edges_repo_identity (
    chunk_id TEXT NOT NULL,
    repo_root TEXT NOT NULL,
    is_tracked INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_eri_chunk ON _edges_repo_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eri_root ON _edges_repo_identity(repo_root);

-- Content identity: SHA-256 of file content + git blob hashes
CREATE TABLE IF NOT EXISTS _edges_content_identity (
    chunk_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    blob_hash TEXT,
    old_blob_hash TEXT
);
CREATE INDEX IF NOT EXISTS idx_eci_chunk ON _edges_content_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eci_hash ON _edges_content_identity(content_hash);
CREATE INDEX IF NOT EXISTS idx_eci_blob ON _edges_content_identity(blob_hash);

-- URL identity: stable UUID for web resources
CREATE TABLE IF NOT EXISTS _edges_url_identity (
    chunk_id TEXT NOT NULL,
    url_uuid TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eui_chunk ON _edges_url_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eui_uuid ON _edges_url_identity(url_uuid);
