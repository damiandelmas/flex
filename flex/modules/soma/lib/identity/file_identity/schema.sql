-- File Identity Schema
-- Location: ~/.soma/file-identity.db
--
-- Provides stable UUIDs for files that survive moves, renames, and repo relocations.
-- Multiple signals (xattr, content_hash, git-registry) enable resolution when paths change.

-- Core identity table
CREATE TABLE IF NOT EXISTS files (
    uuid TEXT PRIMARY KEY,           -- stable UUID (also stored in xattr)
    path TEXT NOT NULL,              -- current known absolute path
    content_hash TEXT,               -- SHA256 of content at last verification
    size INTEGER,                    -- file size at last verification
    repo_root_commit TEXT,           -- git-registry stable repo ID (optional)
    repo_relative_path TEXT,         -- path within repo (optional)
    xattr_verified TIMESTAMP,        -- when xattr was last confirmed present
    last_seen TIMESTAMP,             -- when file was last verified to exist
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_files_path ON files(path);
CREATE INDEX IF NOT EXISTS idx_files_hash ON files(content_hash);
CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_root_commit);

-- Path history (track moves)
CREATE TABLE IF NOT EXISTS path_history (
    id INTEGER PRIMARY KEY,
    file_uuid TEXT NOT NULL REFERENCES files(uuid) ON DELETE CASCADE,
    path TEXT NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_history_uuid ON path_history(file_uuid);
CREATE INDEX IF NOT EXISTS idx_history_path ON path_history(path);

-- Resolution attempts (for debugging/audit)
CREATE TABLE IF NOT EXISTS resolution_log (
    id INTEGER PRIMARY KEY,
    file_uuid TEXT,
    old_path TEXT,
    new_path TEXT,
    method TEXT,                     -- 'xattr', 'content_hash', 'git_registry', 'manual'
    resolved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_resolution_uuid ON resolution_log(file_uuid);
