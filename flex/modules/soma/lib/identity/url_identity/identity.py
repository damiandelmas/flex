#!/usr/bin/env python3
"""
URL Identity System

Stable identifiers for URLs with fetch history and drift detection.
Integrates with content-store for deduplicated content storage.

Usage:
    from soma.identity import url_identity

    ui = URLIdentity()
    url_id = ui.assign("https://example.com/page?utm_source=x")
    ui.record_fetch(url_id, content, status=200, session_id="abc")
    info = ui.get(url_id)
    ui.has_drifted(url_id)
"""

import hashlib
import re
import sqlite3
import uuid as uuid_lib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Union
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote, unquote

# Import content_identity for content storage
try:
    from ..content_identity import ContentIdentity
    HAS_CONTENT_IDENTITY = True
except ImportError:
    HAS_CONTENT_IDENTITY = False
    ContentIdentity = None

DB_PATH = Path.home() / ".soma" / "url-identity.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Tracking parameters to strip during normalization
TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'ref', 'source', 'campaign', 'mc_cid', 'mc_eid',
    '_ga', '_gl', 'msclkid', 'zanpid', 'dclid', 'yclid', 'wickedid',
    'twclid', 'li_fat_id', 'igshid', 'epik', 'si'
}


@dataclass
class URLInfo:
    """Information about a tracked URL."""
    url_id: str
    canonical_url: str
    original_url: Optional[str]
    scheme: str
    domain: Optional[str]
    first_seen: str
    last_fetched: Optional[str]
    fetch_count: int
    drift_detected: bool


@dataclass
class FetchInfo:
    """Information about a single fetch."""
    id: int
    url_id: str
    content_hash: Optional[str]
    status_code: Optional[int]
    response_size: Optional[int]
    fetched_at: str
    session_id: Optional[str]
    episode_id: Optional[int]
    prompt: Optional[str]


class URLIdentity:
    """
    URL identity system with fetch tracking and drift detection.

    Resolution: canonical_url is the primary key.
    Content: stored in shared content-store, referenced by hash.
    Drift: detected when content_hash changes between fetches.
    """

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DB_PATH
        self.db = self._get_db()
        self.content = ContentIdentity() if HAS_CONTENT_IDENTITY else None

    def _get_db(self) -> sqlite3.Connection:
        """Get database connection, init if needed."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        db = sqlite3.connect(str(self.db_path))
        db.row_factory = sqlite3.Row

        # Init schema if fresh
        if not db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='urls'"
        ).fetchone():
            with open(SCHEMA_PATH) as f:
                db.executescript(f.read())
            db.commit()

        return db

    # ─────────────────────────────────────────────────────────────────────────
    # Normalization
    # ─────────────────────────────────────────────────────────────────────────

    def normalize(self, url: str) -> str:
        """
        Normalize URL for consistent identity.

        - Lowercase domain
        - Sort query params alphabetically
        - Remove tracking params
        - Strip trailing slashes from path
        - Remove default ports
        - Decode unnecessarily encoded chars
        """
        if not url:
            return url

        # Handle search:// pseudo-URLs
        if url.startswith('search://'):
            return url

        try:
            parsed = urlparse(url)
        except Exception:
            return url

        # Lowercase domain
        netloc = parsed.netloc.lower()

        # Remove default ports
        if ':80' in netloc and parsed.scheme == 'http':
            netloc = netloc.replace(':80', '')
        if ':443' in netloc and parsed.scheme == 'https':
            netloc = netloc.replace(':443', '')

        # Parse and filter query params
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        filtered_params = {
            k: v for k, v in query_params.items()
            if k.lower() not in TRACKING_PARAMS
        }

        # Sort params and rebuild query string
        sorted_params = sorted(filtered_params.items())
        query_string = urlencode(sorted_params, doseq=True) if sorted_params else ''

        # Normalize path (strip trailing slash, decode unnecessary encoding)
        path = parsed.path.rstrip('/') if parsed.path != '/' else '/'
        try:
            path = unquote(path)
            path = quote(path, safe='/-_.~')
        except Exception:
            pass

        # Rebuild URL
        normalized = urlunparse((
            parsed.scheme.lower(),
            netloc,
            path,
            '',  # params (rarely used)
            query_string,
            parsed.fragment  # preserve fragment
        ))

        return normalized

    def normalize_search_query(self, query: str) -> str:
        """
        Normalize a WebSearch query to pseudo-URL.

        Returns: search://web?q=<normalized_query>
        """
        if not query:
            return ''

        # Normalize whitespace
        normalized = ' '.join(query.split())
        # Lowercase for dedup
        normalized = normalized.lower()
        # URL encode
        encoded = quote(normalized, safe='')

        return f"search://web?q={encoded}"

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        if url.startswith('search://'):
            return None
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return None

    def _extract_scheme(self, url: str) -> str:
        """Extract scheme from URL."""
        if url.startswith('search://'):
            return 'search'
        try:
            parsed = urlparse(url)
            return parsed.scheme.lower() or 'https'
        except Exception:
            return 'https'

    # ─────────────────────────────────────────────────────────────────────────
    # Core API
    # ─────────────────────────────────────────────────────────────────────────

    def assign(self, url: str, is_search: bool = False) -> str:
        """
        Register URL and return stable url_id.

        If URL already registered, returns existing url_id.
        If new, normalizes URL and creates new entry.

        Args:
            url: Raw URL or search query
            is_search: If True, treat as WebSearch query

        Returns:
            url_id (UUID string)
        """
        if is_search:
            canonical = self.normalize_search_query(url)
            original = url
        else:
            canonical = self.normalize(url)
            original = url if url != canonical else None

        if not canonical:
            raise ValueError("Empty URL")

        # Check if already exists
        row = self.db.execute(
            "SELECT url_id FROM urls WHERE canonical_url = ?",
            (canonical,)
        ).fetchone()

        if row:
            return row["url_id"]

        # Create new entry
        url_id = str(uuid_lib.uuid4())
        scheme = self._extract_scheme(canonical)
        domain = self._extract_domain(canonical)
        now = datetime.now().isoformat()

        self.db.execute("""
            INSERT INTO urls (url_id, canonical_url, original_url, scheme, domain, first_seen)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (url_id, canonical, original, scheme, domain, now))
        self.db.commit()

        return url_id

    def resolve(self, url: str, is_search: bool = False) -> Optional[str]:
        """
        Get url_id for a URL without creating.

        Returns None if URL not tracked.
        """
        if is_search:
            canonical = self.normalize_search_query(url)
        else:
            canonical = self.normalize(url)

        if not canonical:
            return None

        row = self.db.execute(
            "SELECT url_id FROM urls WHERE canonical_url = ?",
            (canonical,)
        ).fetchone()

        return row["url_id"] if row else None

    def get(self, url_id: str) -> Optional[URLInfo]:
        """Get full info for a URL by its id."""
        row = self.db.execute("""
            SELECT url_id, canonical_url, original_url, scheme, domain,
                   first_seen, last_fetched, fetch_count, drift_detected
            FROM urls WHERE url_id = ?
        """, (url_id,)).fetchone()

        if not row:
            return None

        return URLInfo(
            url_id=row["url_id"],
            canonical_url=row["canonical_url"],
            original_url=row["original_url"],
            scheme=row["scheme"],
            domain=row["domain"],
            first_seen=row["first_seen"],
            last_fetched=row["last_fetched"],
            fetch_count=row["fetch_count"],
            drift_detected=bool(row["drift_detected"])
        )

    def locate(self, url_id: str) -> Optional[str]:
        """
        Get canonical URL for a url_id.

        (Named 'locate' for consistency with file_identity pattern)
        """
        row = self.db.execute(
            "SELECT canonical_url FROM urls WHERE url_id = ?",
            (url_id,)
        ).fetchone()

        return row["canonical_url"] if row else None

    def exists(self, url_id: str) -> bool:
        """Check if url_id exists."""
        row = self.db.execute(
            "SELECT 1 FROM urls WHERE url_id = ?",
            (url_id,)
        ).fetchone()
        return row is not None

    # ─────────────────────────────────────────────────────────────────────────
    # Fetch Tracking
    # ─────────────────────────────────────────────────────────────────────────

    def record_fetch(
        self,
        url_id: str,
        content: Optional[Union[bytes, str]] = None,
        status_code: Optional[int] = None,
        session_id: Optional[str] = None,
        episode_id: Optional[int] = None,
        prompt: Optional[str] = None
    ) -> Optional[str]:
        """
        Record a fetch event.

        Stores content in content-store and links to this URL.
        Updates drift_detected if content changed.

        Returns:
            content_hash if content was stored, None otherwise
        """
        if not self.exists(url_id):
            raise ValueError(f"Unknown url_id: {url_id}")

        content_hash = None
        response_size = None

        # Store content if provided
        if content and self.content:
            if isinstance(content, str):
                content_bytes = content.encode('utf-8')
            else:
                content_bytes = content

            response_size = len(content_bytes)
            content_hash = self.content.store(content_bytes, mime_type='text/html')

        now = datetime.now().isoformat()
        truncated_prompt = prompt[:500] if prompt else None

        # Insert fetch record
        self.db.execute("""
            INSERT INTO fetches (url_id, content_hash, status_code, response_size,
                                 fetched_at, session_id, episode_id, prompt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (url_id, content_hash, status_code, response_size,
              now, session_id, episode_id, truncated_prompt))

        # Update URL stats
        self.db.execute("""
            UPDATE urls SET
                last_fetched = ?,
                fetch_count = fetch_count + 1
            WHERE url_id = ?
        """, (now, url_id))

        # Check for drift
        if content_hash:
            self._check_and_update_drift(url_id, content_hash)

        self.db.commit()
        return content_hash

    def _check_and_update_drift(self, url_id: str, new_hash: str):
        """Check if content changed and update drift flag."""
        # Get first fetch's content hash
        first_fetch = self.db.execute("""
            SELECT content_hash FROM fetches
            WHERE url_id = ? AND content_hash IS NOT NULL
            ORDER BY fetched_at ASC
            LIMIT 1
        """, (url_id,)).fetchone()

        if first_fetch and first_fetch["content_hash"] != new_hash:
            self.db.execute(
                "UPDATE urls SET drift_detected = 1 WHERE url_id = ?",
                (url_id,)
            )

    def get_fetches(self, url_id: str, limit: int = 100) -> List[FetchInfo]:
        """Get fetch history for a URL, most recent first."""
        rows = self.db.execute("""
            SELECT id, url_id, content_hash, status_code, response_size,
                   fetched_at, session_id, episode_id, prompt
            FROM fetches
            WHERE url_id = ?
            ORDER BY fetched_at DESC
            LIMIT ?
        """, (url_id, limit)).fetchall()

        return [FetchInfo(
            id=r["id"],
            url_id=r["url_id"],
            content_hash=r["content_hash"],
            status_code=r["status_code"],
            response_size=r["response_size"],
            fetched_at=r["fetched_at"],
            session_id=r["session_id"],
            episode_id=r["episode_id"],
            prompt=r["prompt"]
        ) for r in rows]

    def get_latest_fetch(self, url_id: str) -> Optional[FetchInfo]:
        """Get most recent fetch for a URL."""
        fetches = self.get_fetches(url_id, limit=1)
        return fetches[0] if fetches else None

    def get_content(self, url_id: str, at_time: str = None) -> Optional[bytes]:
        """
        Get content for a URL.

        Args:
            url_id: The URL's identifier
            at_time: ISO timestamp - get content as of this time (default: latest)

        Returns:
            Content bytes or None
        """
        if not self.content:
            return None

        if at_time:
            row = self.db.execute("""
                SELECT content_hash FROM fetches
                WHERE url_id = ? AND content_hash IS NOT NULL
                  AND fetched_at <= ?
                ORDER BY fetched_at DESC
                LIMIT 1
            """, (url_id, at_time)).fetchone()
        else:
            row = self.db.execute("""
                SELECT content_hash FROM fetches
                WHERE url_id = ? AND content_hash IS NOT NULL
                ORDER BY fetched_at DESC
                LIMIT 1
            """, (url_id,)).fetchone()

        if not row or not row["content_hash"]:
            return None

        return self.content.retrieve(row["content_hash"])

    # ─────────────────────────────────────────────────────────────────────────
    # Drift Detection
    # ─────────────────────────────────────────────────────────────────────────

    def has_drifted(self, url_id: str) -> bool:
        """Check if URL content has changed since first fetch."""
        row = self.db.execute(
            "SELECT drift_detected FROM urls WHERE url_id = ?",
            (url_id,)
        ).fetchone()

        return bool(row["drift_detected"]) if row else False

    def get_drift_history(self, url_id: str) -> List[dict]:
        """
        Get content change history.

        Returns list of {from_hash, to_hash, changed_at} dicts.
        """
        rows = self.db.execute("""
            SELECT content_hash, fetched_at FROM fetches
            WHERE url_id = ? AND content_hash IS NOT NULL
            ORDER BY fetched_at ASC
        """, (url_id,)).fetchall()

        if len(rows) < 2:
            return []

        changes = []
        prev_hash = rows[0]["content_hash"]

        for row in rows[1:]:
            if row["content_hash"] != prev_hash:
                changes.append({
                    "from_hash": prev_hash,
                    "to_hash": row["content_hash"],
                    "changed_at": row["fetched_at"]
                })
                prev_hash = row["content_hash"]

        return changes

    # ─────────────────────────────────────────────────────────────────────────
    # Redirect Tracking
    # ─────────────────────────────────────────────────────────────────────────

    def record_redirect(
        self,
        source_url: str,
        target_url: str,
        status_code: int = 301
    ) -> tuple:
        """
        Record a redirect from source to target.

        Both URLs are assigned url_ids if not already tracked.

        Returns: (source_url_id, target_url_id)
        """
        source_id = self.assign(source_url)
        target_id = self.assign(target_url)
        now = datetime.now().isoformat()

        # Check if redirect already exists
        existing = self.db.execute("""
            SELECT id FROM redirects
            WHERE source_url_id = ? AND target_url_id = ?
        """, (source_id, target_id)).fetchone()

        if existing:
            self.db.execute("""
                UPDATE redirects SET last_seen = ?
                WHERE source_url_id = ? AND target_url_id = ?
            """, (now, source_id, target_id))
        else:
            self.db.execute("""
                INSERT INTO redirects (source_url_id, target_url_id, status_code, first_seen)
                VALUES (?, ?, ?, ?)
            """, (source_id, target_id, status_code, now))

        self.db.commit()
        return (source_id, target_id)

    def get_redirect_chain(self, url_id: str) -> List[str]:
        """Get full redirect chain starting from url_id."""
        chain = [url_id]
        current = url_id

        for _ in range(10):  # Max 10 redirects
            row = self.db.execute("""
                SELECT target_url_id FROM redirects
                WHERE source_url_id = ?
            """, (current,)).fetchone()

            if not row:
                break

            current = row["target_url_id"]
            if current in chain:  # Avoid loops
                break
            chain.append(current)

        return chain

    def get_final_url(self, url_id: str) -> str:
        """Follow redirects to get final url_id."""
        chain = self.get_redirect_chain(url_id)
        return chain[-1]

    # ─────────────────────────────────────────────────────────────────────────
    # Bulk Operations
    # ─────────────────────────────────────────────────────────────────────────

    def list_by_domain(self, domain: str, limit: int = 100) -> List[URLInfo]:
        """List all URLs for a domain."""
        rows = self.db.execute("""
            SELECT url_id, canonical_url, original_url, scheme, domain,
                   first_seen, last_fetched, fetch_count, drift_detected
            FROM urls
            WHERE domain = ?
            ORDER BY last_fetched DESC
            LIMIT ?
        """, (domain.lower(), limit)).fetchall()

        return [URLInfo(
            url_id=r["url_id"],
            canonical_url=r["canonical_url"],
            original_url=r["original_url"],
            scheme=r["scheme"],
            domain=r["domain"],
            first_seen=r["first_seen"],
            last_fetched=r["last_fetched"],
            fetch_count=r["fetch_count"],
            drift_detected=bool(r["drift_detected"])
        ) for r in rows]

    def list_recent(self, days: int = 7, limit: int = 100) -> List[URLInfo]:
        """List recently fetched URLs."""
        rows = self.db.execute("""
            SELECT url_id, canonical_url, original_url, scheme, domain,
                   first_seen, last_fetched, fetch_count, drift_detected
            FROM urls
            WHERE last_fetched IS NOT NULL
            ORDER BY last_fetched DESC
            LIMIT ?
        """, (limit,)).fetchall()

        return [URLInfo(
            url_id=r["url_id"],
            canonical_url=r["canonical_url"],
            original_url=r["original_url"],
            scheme=r["scheme"],
            domain=r["domain"],
            first_seen=r["first_seen"],
            last_fetched=r["last_fetched"],
            fetch_count=r["fetch_count"],
            drift_detected=bool(r["drift_detected"])
        ) for r in rows]

    def list_drifted(self, limit: int = 100) -> List[URLInfo]:
        """List URLs with detected content drift."""
        rows = self.db.execute("""
            SELECT url_id, canonical_url, original_url, scheme, domain,
                   first_seen, last_fetched, fetch_count, drift_detected
            FROM urls
            WHERE drift_detected = 1
            ORDER BY last_fetched DESC
            LIMIT ?
        """, (limit,)).fetchall()

        return [URLInfo(
            url_id=r["url_id"],
            canonical_url=r["canonical_url"],
            original_url=r["original_url"],
            scheme=r["scheme"],
            domain=r["domain"],
            first_seen=r["first_seen"],
            last_fetched=r["last_fetched"],
            fetch_count=r["fetch_count"],
            drift_detected=bool(r["drift_detected"])
        ) for r in rows]

    def stats(self) -> dict:
        """Get statistics."""
        url_count = self.db.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
        fetch_count = self.db.execute("SELECT COUNT(*) FROM fetches").fetchone()[0]
        domain_count = self.db.execute(
            "SELECT COUNT(DISTINCT domain) FROM urls WHERE domain IS NOT NULL"
        ).fetchone()[0]
        drifted_count = self.db.execute(
            "SELECT COUNT(*) FROM urls WHERE drift_detected = 1"
        ).fetchone()[0]

        # Content size (approximate from fetches)
        size_row = self.db.execute(
            "SELECT SUM(response_size) FROM fetches WHERE response_size IS NOT NULL"
        ).fetchone()
        content_size = size_row[0] or 0

        return {
            "url_count": url_count,
            "fetch_count": fetch_count,
            "domains": domain_count,
            "drifted_count": drifted_count,
            "content_size_mb": round(content_size / (1024 * 1024), 2)
        }


# Singleton
_instance: Optional[URLIdentity] = None


def get_instance() -> URLIdentity:
    """Get singleton URLIdentity instance."""
    global _instance
    if _instance is None:
        _instance = URLIdentity()
    return _instance
