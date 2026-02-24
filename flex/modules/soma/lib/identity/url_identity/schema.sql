-- URL Identity Schema
-- Location: ~/.soma/url-identity.db
--
-- Stable identifiers for URLs with fetch history and drift detection.
-- Content stored in shared content-store (deduplicated, compressed).

-- Core URL table
CREATE TABLE IF NOT EXISTS urls (
    url_id TEXT PRIMARY KEY,              -- UUID for this URL
    canonical_url TEXT NOT NULL UNIQUE,   -- Normalized URL
    original_url TEXT,                    -- Pre-normalization (debugging)
    scheme TEXT NOT NULL,                 -- 'https', 'http', 'search'
    domain TEXT,                          -- Extracted domain
    first_seen TEXT NOT NULL,             -- ISO timestamp
    last_fetched TEXT,                    -- ISO timestamp
    fetch_count INTEGER DEFAULT 0,        -- Total fetches
    drift_detected INTEGER DEFAULT 0,     -- 1 if content changed
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_urls_canonical ON urls(canonical_url);
CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
CREATE INDEX IF NOT EXISTS idx_urls_scheme ON urls(scheme);

-- Fetch history (every access)
CREATE TABLE IF NOT EXISTS fetches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id TEXT NOT NULL,                 -- FK to urls
    content_hash TEXT,                    -- SHA-256 in content-store
    status_code INTEGER,                  -- HTTP status
    response_size INTEGER,                -- Bytes
    fetched_at TEXT NOT NULL,             -- ISO timestamp
    session_id TEXT,                      -- Thread session
    episode_id INTEGER,                   -- Thread episode
    prompt TEXT,                          -- WebFetch prompt (truncated)
    FOREIGN KEY (url_id) REFERENCES urls(url_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fetches_url ON fetches(url_id);
CREATE INDEX IF NOT EXISTS idx_fetches_content ON fetches(content_hash);
CREATE INDEX IF NOT EXISTS idx_fetches_time ON fetches(fetched_at);
CREATE INDEX IF NOT EXISTS idx_fetches_session ON fetches(session_id);

-- Redirect tracking
CREATE TABLE IF NOT EXISTS redirects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url_id TEXT NOT NULL,          -- URL that redirects
    target_url_id TEXT NOT NULL,          -- Destination URL
    status_code INTEGER,                  -- 301, 302, 307, 308
    first_seen TEXT NOT NULL,
    last_seen TEXT,
    FOREIGN KEY (source_url_id) REFERENCES urls(url_id) ON DELETE CASCADE,
    FOREIGN KEY (target_url_id) REFERENCES urls(url_id) ON DELETE CASCADE,
    UNIQUE(source_url_id, target_url_id)
);

CREATE INDEX IF NOT EXISTS idx_redirects_source ON redirects(source_url_id);
CREATE INDEX IF NOT EXISTS idx_redirects_target ON redirects(target_url_id);
