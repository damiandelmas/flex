-- Content Identity Schema
-- Tracks stored content with metadata

CREATE TABLE IF NOT EXISTS content (
    content_hash TEXT PRIMARY KEY,      -- SHA-256 of content
    size INTEGER NOT NULL,              -- Size in bytes
    mime_type TEXT,                     -- Optional MIME type
    first_seen TEXT NOT NULL,           -- ISO timestamp
    last_accessed TEXT,                 -- ISO timestamp
    ref_count INTEGER DEFAULT 1,        -- How many things reference this
    compressed INTEGER DEFAULT 1        -- 1 if gzip compressed
);

CREATE TABLE IF NOT EXISTS refs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash TEXT NOT NULL,
    ref_type TEXT NOT NULL,             -- 'episode', 'file', 'chunk', etc.
    ref_id TEXT NOT NULL,               -- The referencing entity's ID
    created_at TEXT NOT NULL,
    FOREIGN KEY (content_hash) REFERENCES content(content_hash),
    UNIQUE(content_hash, ref_type, ref_id)
);

CREATE INDEX IF NOT EXISTS idx_content_size ON content(size);
CREATE INDEX IF NOT EXISTS idx_refs_hash ON refs(content_hash);
CREATE INDEX IF NOT EXISTS idx_refs_type ON refs(ref_type, ref_id);
