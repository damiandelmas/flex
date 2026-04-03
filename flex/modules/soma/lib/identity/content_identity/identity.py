#!/usr/bin/env python3
"""
Content Identity System

Content-addressed storage for the SOMA ecosystem.
Stores content by SHA-256 hash, deduplicated and compressed.

Usage:
    from content_identity import ContentIdentity

    ci = ContentIdentity()
    content_hash = ci.store("file content here")  # store content
    content = ci.retrieve(content_hash)            # get it back
    ci.exists(content_hash)                        # check existence
"""

import gzip
import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

STORE_PATH = Path.home() / ".soma" / "content-store"
DB_PATH = STORE_PATH / "index.db"
OBJECTS_PATH = STORE_PATH / "objects"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


@dataclass
class ContentInfo:
    """Information about stored content."""
    content_hash: str
    size: int
    mime_type: Optional[str]
    first_seen: str
    last_accessed: Optional[str]
    ref_count: int
    compressed: bool
    exists: bool  # Whether object file exists


class ContentIdentity:
    """
    Content-addressed storage.

    Store content once, reference many times.
    Content is gzip-compressed and stored by SHA-256 hash.

    Storage layout:
        ~/.soma/content-store/
        ├── index.db           # metadata
        └── objects/
            └── {hash[:2]}/
                └── {hash[2:]}.gz
    """

    def __init__(self, store_path: Path = None):
        self.store_path = store_path or STORE_PATH
        self.objects_path = self.store_path / "objects"
        self.db_path = self.store_path / "index.db"
        self.db = self._get_db()

    def _get_db(self) -> sqlite3.Connection:
        """Get database connection, init if needed."""
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.objects_path.mkdir(parents=True, exist_ok=True)

        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row

        # Enable WAL mode for crash safety and better concurrency
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")  # Safe with WAL, faster than FULL

        # Init schema if fresh
        if not db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='content'").fetchone():
            with open(SCHEMA_PATH) as f:
                db.executescript(f.read())
            db.commit()

        return db

    def close(self):
        """Close database connection cleanly."""
        if self.db:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.db.close()
            self.db = None

    def _object_path(self, content_hash: str) -> Path:
        """Get filesystem path for content object."""
        return self.objects_path / content_hash[:2] / f"{content_hash[2:]}.gz"

    def _compute_hash(self, content: Union[str, bytes]) -> str:
        """Compute SHA-256 hash of content."""
        if isinstance(content, str):
            content = content.encode('utf-8')
        return hashlib.sha256(content).hexdigest()

    # ─────────────────────────────────────────────────────────────────────────
    # Core API
    # ─────────────────────────────────────────────────────────────────────────

    def store(self, content: Union[str, bytes], mime_type: str = None) -> str:
        """
        Store content, return hash.

        Idempotent: storing same content returns same hash.
        Content is gzip-compressed before storage.

        Args:
            content: String or bytes to store
            mime_type: Optional MIME type hint

        Returns:
            SHA-256 hash of content
        """
        if isinstance(content, str):
            content_bytes = content.encode('utf-8')
        else:
            content_bytes = content

        content_hash = self._compute_hash(content_bytes)
        object_path = self._object_path(content_hash)

        # Check if already stored
        row = self.db.execute(
            "SELECT content_hash FROM content WHERE content_hash = ?",
            (content_hash,)
        ).fetchone()

        if row:
            # Already exists, update ref_count
            self.db.execute(
                "UPDATE content SET ref_count = ref_count + 1, last_accessed = ? WHERE content_hash = ?",
                (datetime.now().isoformat(), content_hash)
            )
            self.db.commit()
            return content_hash

        # Store compressed content
        object_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(object_path, 'wb') as f:
            f.write(content_bytes)

        # Record in DB
        now = datetime.now().isoformat()
        self.db.execute("""
            INSERT INTO content (content_hash, size, mime_type, first_seen, last_accessed, ref_count, compressed)
            VALUES (?, ?, ?, ?, ?, 1, 1)
        """, (content_hash, len(content_bytes), mime_type, now, now))
        self.db.commit()

        return content_hash

    def retrieve(self, content_hash: str) -> Optional[bytes]:
        """
        Get content by hash.

        Returns:
            Content as bytes, or None if not found
        """
        object_path = self._object_path(content_hash)

        if not object_path.exists():
            return None

        try:
            with gzip.open(object_path, 'rb') as f:
                content = f.read()

            # Update last_accessed
            self.db.execute(
                "UPDATE content SET last_accessed = ? WHERE content_hash = ?",
                (datetime.now().isoformat(), content_hash)
            )
            self.db.commit()

            return content
        except Exception:
            return None

    def retrieve_text(self, content_hash: str, encoding: str = 'utf-8') -> Optional[str]:
        """
        Get content as text.

        Returns:
            Content as string, or None if not found
        """
        content = self.retrieve(content_hash)
        if content is None:
            return None
        return content.decode(encoding)

    def exists(self, content_hash: str) -> bool:
        """Check if content is stored."""
        return self._object_path(content_hash).exists()

    def get(self, content_hash: str) -> Optional[ContentInfo]:
        """Get full info for stored content."""
        row = self.db.execute("""
            SELECT content_hash, size, mime_type, first_seen, last_accessed, ref_count, compressed
            FROM content WHERE content_hash = ?
        """, (content_hash,)).fetchone()

        if not row:
            return None

        return ContentInfo(
            content_hash=row["content_hash"],
            size=row["size"],
            mime_type=row["mime_type"],
            first_seen=row["first_seen"],
            last_accessed=row["last_accessed"],
            ref_count=row["ref_count"],
            compressed=bool(row["compressed"]),
            exists=self._object_path(content_hash).exists()
        )

    def delete(self, content_hash: str, force: bool = False) -> bool:
        """
        Delete content.

        By default, only deletes if ref_count <= 1.
        Use force=True to delete regardless.

        Returns:
            True if deleted, False if not found or still referenced
        """
        row = self.db.execute(
            "SELECT ref_count FROM content WHERE content_hash = ?",
            (content_hash,)
        ).fetchone()

        if not row:
            return False

        if row["ref_count"] > 1 and not force:
            # Decrement ref_count instead
            self.db.execute(
                "UPDATE content SET ref_count = ref_count - 1 WHERE content_hash = ?",
                (content_hash,)
            )
            self.db.commit()
            return False

        # Delete object file
        object_path = self._object_path(content_hash)
        if object_path.exists():
            object_path.unlink()

        # Delete from DB
        self.db.execute("DELETE FROM refs WHERE content_hash = ?", (content_hash,))
        self.db.execute("DELETE FROM content WHERE content_hash = ?", (content_hash,))
        self.db.commit()

        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Reference Tracking
    # ─────────────────────────────────────────────────────────────────────────

    def add_ref(self, content_hash: str, ref_type: str, ref_id: str) -> bool:
        """
        Add a reference to content.

        Use this to track what uses this content (episodes, files, chunks).

        Args:
            content_hash: The content being referenced
            ref_type: Type of referrer ('episode', 'file', 'chunk', etc.)
            ref_id: ID of the referrer

        Returns:
            True if added, False if content doesn't exist
        """
        if not self.exists(content_hash):
            return False

        try:
            self.db.execute("""
                INSERT OR IGNORE INTO refs (content_hash, ref_type, ref_id, created_at)
                VALUES (?, ?, ?, ?)
            """, (content_hash, ref_type, ref_id, datetime.now().isoformat()))
            self.db.commit()
            return True
        except Exception:
            return False

    def get_refs(self, content_hash: str) -> list[tuple[str, str]]:
        """
        Get all references to content.

        Returns:
            List of (ref_type, ref_id) tuples
        """
        rows = self.db.execute(
            "SELECT ref_type, ref_id FROM refs WHERE content_hash = ?",
            (content_hash,)
        ).fetchall()

        return [(r["ref_type"], r["ref_id"]) for r in rows]

    def find_by_ref(self, ref_type: str, ref_id: str) -> Optional[str]:
        """
        Find content hash by reference.

        Returns:
            content_hash or None
        """
        row = self.db.execute(
            "SELECT content_hash FROM refs WHERE ref_type = ? AND ref_id = ?",
            (ref_type, ref_id)
        ).fetchone()

        return row["content_hash"] if row else None

    # ─────────────────────────────────────────────────────────────────────────
    # Bulk Operations
    # ─────────────────────────────────────────────────────────────────────────

    def store_batch(self, items: list[Union[str, bytes]]) -> list[str]:
        """
        Store multiple content items.

        Returns:
            List of content hashes in same order as input
        """
        return [self.store(item) for item in items]

    def retrieve_batch(self, hashes: list[str]) -> dict[str, Optional[bytes]]:
        """
        Retrieve multiple content items.

        Returns:
            Dict mapping hash → content (or None if not found)
        """
        return {h: self.retrieve(h) for h in hashes}

    def stats(self) -> dict:
        """
        Get storage statistics.

        Returns:
            Dict with counts and sizes
        """
        row = self.db.execute("""
            SELECT
                COUNT(*) as count,
                SUM(size) as total_size,
                AVG(size) as avg_size,
                SUM(ref_count) as total_refs
            FROM content
        """).fetchone()

        # Get disk usage (compressed)
        disk_size = sum(
            f.stat().st_size
            for f in self.objects_path.rglob("*.gz")
            if f.is_file()
        )

        return {
            "count": row["count"] or 0,
            "total_size": row["total_size"] or 0,
            "avg_size": int(row["avg_size"] or 0),
            "total_refs": row["total_refs"] or 0,
            "disk_size": disk_size,
            "compression_ratio": round(disk_size / row["total_size"], 2) if row["total_size"] else 0
        }

    def verify_integrity(self) -> dict:
        """
        Verify all stored content.

        Checks that DB records match object files.

        Returns:
            Dict with ok, missing, orphaned counts
        """
        result = {"ok": 0, "missing": 0, "orphaned": 0}

        # Check DB records have object files
        rows = self.db.execute("SELECT content_hash FROM content").fetchall()
        for row in rows:
            if self._object_path(row["content_hash"]).exists():
                result["ok"] += 1
            else:
                result["missing"] += 1

        # Check for orphaned object files
        db_hashes = {r["content_hash"] for r in rows}
        for obj_file in self.objects_path.rglob("*.gz"):
            # Reconstruct hash from path
            hash_suffix = obj_file.stem
            hash_prefix = obj_file.parent.name
            content_hash = hash_prefix + hash_suffix

            if content_hash not in db_hashes:
                result["orphaned"] += 1

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Convenience Methods
    # ─────────────────────────────────────────────────────────────────────────

    def store_file(self, file_path: str, mime_type: str = None) -> str:
        """
        Store file content.

        Reads file and stores its content.

        Returns:
            content_hash
        """
        content = Path(file_path).read_bytes()
        return self.store(content, mime_type=mime_type)

    def hash_content(self, content: Union[str, bytes]) -> str:
        """
        Compute hash without storing.

        Use this to check if content exists before storing.
        """
        return self._compute_hash(content)


# Convenience singleton
_instance: Optional[ContentIdentity] = None


def get_instance() -> ContentIdentity:
    """Get singleton ContentIdentity instance."""
    global _instance
    if _instance is None:
        _instance = ContentIdentity()
    return _instance
