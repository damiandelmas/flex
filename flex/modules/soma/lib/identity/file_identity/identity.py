#!/usr/bin/env python3
"""
File Identity System

Provides stable UUIDs for files that survive moves, renames, and repo relocations.
Uses multiple signals: xattr, content_hash, git-registry, path.

Usage:
    from identity import FileIdentity

    fi = FileIdentity()
    uuid = fi.assign("/path/to/file")    # get or create UUID
    path = fi.locate(uuid)               # find current path
    fi.heal()                            # repair moved files
"""

import hashlib
import os
import sqlite3
import subprocess
import sys
import time
import uuid as uuid_lib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# Import repo_identity from soma.identity
try:
    from ..repo_identity import RepoIdentity
    HAS_REPO_IDENTITY = True
except ImportError:
    HAS_REPO_IDENTITY = False

XATTR_NAME = "user.soma.file_uuid"
DB_PATH = Path.home() / ".soma" / "file-identity.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


@dataclass
class FileInfo:
    """Information about a tracked file."""
    uuid: str
    path: str
    content_hash: Optional[str]
    size: Optional[int]
    repo_root_commit: Optional[str]
    repo_relative_path: Optional[str]
    exists: bool
    xattr_present: bool


class FileIdentity:
    """
    Stable file identity system.

    Resolution priority:
    1. xattr (user.soma.file_uuid) - survives same-FS moves
    2. git-registry (root_commit + relative) - survives repo moves
    3. content_hash - survives if content unchanged
    4. path - fallback
    """

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DB_PATH
        self.db = self._get_db()
        self.repo_id = RepoIdentity() if HAS_REPO_IDENTITY else None

    def _get_db(self) -> sqlite3.Connection:
        """Get database connection, init if needed."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        db = sqlite3.connect(self.db_path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")

        # Init schema if fresh
        if not db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'").fetchone():
            with open(SCHEMA_PATH) as f:
                db.executescript(f.read())
            db.commit()

        return db

    # ─────────────────────────────────────────────────────────────────────────
    # Core API
    # ─────────────────────────────────────────────────────────────────────────

    def assign(self, path: str) -> str:
        """
        Get or create stable UUID for a file.

        1. Check if file already has UUID (DB or xattr)
        2. If not, generate new UUID
        3. Store in DB and set xattr

        Returns UUID string.
        """
        path = str(Path(path).resolve())

        # Check DB first
        row = self.db.execute("SELECT uuid FROM files WHERE path = ?", (path,)).fetchone()
        if row:
            # Read hit: refresh the filesystem xattr best-effort but take NO DB
            # write lock. This path runs in tight assign_batch loops; a DB write
            # here serializes the shared identity DB against the capture worker
            # and loses the lock race — the root cause of file_uuid=0 on batch
            # builds. record=False keeps it a pure read on the DB.
            self._set_xattr(path, row["uuid"], record=False)
            return row["uuid"]

        # Check xattr (file might have moved, DB has old path)
        existing_uuid = self._get_xattr(path)
        if existing_uuid:
            # Update DB with new path. A False return means another uuid already
            # claims this path (collision) so the DB row was left untouched — but
            # the file's own xattr is authoritative, so returning existing_uuid
            # is still correct; we just don't assume the DB path was rewritten.
            self._update_path(existing_uuid, path)
            return existing_uuid

        # Generate new UUID
        file_uuid = str(uuid_lib.uuid4())

        # Gather file info
        content_hash = self._content_hash(path)
        size = self._file_size(path)
        repo_root, repo_relative = self._git_info(path)

        # Store in DB
        now = datetime.now().isoformat()
        self.db.execute("""
            INSERT INTO files (uuid, path, content_hash, size, repo_root_commit, repo_relative_path, last_seen, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (file_uuid, path, content_hash, size, repo_root, repo_relative, now, now))

        # Record initial path
        self.db.execute(
            "INSERT INTO path_history (file_uuid, path) VALUES (?, ?)",
            (file_uuid, path)
        )

        self.db.commit()

        # Set xattr (fs only; the INSERT above already recorded the row — a
        # post-commit DB write here would leave an uncommitted lock open)
        self._set_xattr(path, file_uuid, record=False)

        return file_uuid

    def resolve(self, path: str) -> Optional[str]:
        """
        Get UUID for a path without creating one.
        Returns None if file not tracked.
        """
        path = str(Path(path).resolve())

        # Check DB
        row = self.db.execute("SELECT uuid FROM files WHERE path = ?", (path,)).fetchone()
        if row:
            return row["uuid"]

        # Check xattr
        return self._get_xattr(path)

    def locate(self, file_uuid: str) -> Optional[str]:
        """
        Find current path for a UUID.

        1. Check DB path - if exists, return it
        2. If not exists, try to find via xattr scan, git-registry, or content hash

        Returns path string or None if not found.
        """
        row = self.db.execute(
            "SELECT path, content_hash, repo_root_commit, repo_relative_path FROM files WHERE uuid = ?",
            (file_uuid,)
        ).fetchone()

        if not row:
            return None

        path = Path(row["path"])

        # Check if path still valid
        if path.exists():
            # Verify xattr matches
            xattr_uuid = self._get_xattr(str(path))
            if xattr_uuid == file_uuid:
                return str(path)

        # Path invalid - try to resolve
        new_path = self._find_moved_file(
            file_uuid,
            row["path"],
            row["content_hash"],
            row["repo_root_commit"],
            row["repo_relative_path"]
        )

        if new_path:
            # Honor the collision-skip return: if new_path is already claimed by
            # another uuid, _update_path is a no-op and returning new_path here
            # would silently misattribute another file's path to this uuid.
            if self._update_path(file_uuid, new_path, method="auto"):
                return new_path
            return None

        return None

    def get(self, file_uuid: str) -> Optional[FileInfo]:
        """Get full info for a tracked file."""
        row = self.db.execute("""
            SELECT uuid, path, content_hash, size, repo_root_commit, repo_relative_path, xattr_verified
            FROM files WHERE uuid = ?
        """, (file_uuid,)).fetchone()

        if not row:
            return None

        path = Path(row["path"])
        exists = path.exists()
        xattr_present = self._get_xattr(str(path)) == file_uuid if exists else False

        return FileInfo(
            uuid=row["uuid"],
            path=row["path"],
            content_hash=row["content_hash"],
            size=row["size"],
            repo_root_commit=row["repo_root_commit"],
            repo_relative_path=row["repo_relative_path"],
            exists=exists,
            xattr_present=xattr_present
        )

    def history(self, file_uuid: str) -> list[tuple[str, str]]:
        """Get path history for a file. Returns [(path, detected_at), ...]"""
        rows = self.db.execute("""
            SELECT path, detected_at FROM path_history
            WHERE file_uuid = ? ORDER BY detected_at
        """, (file_uuid,)).fetchall()

        return [(r["path"], r["detected_at"]) for r in rows]

    # ─────────────────────────────────────────────────────────────────────────
    # Bulk Operations
    # ─────────────────────────────────────────────────────────────────────────

    def heal(self, verbose: bool = False) -> dict:
        """
        Scan all tracked files and repair broken paths.

        Returns dict with counts: {ok, moved, missing}
        """
        files = self.db.execute("""
            SELECT uuid, path, content_hash, repo_root_commit, repo_relative_path
            FROM files
        """).fetchall()

        stats = {"ok": 0, "moved": 0, "missing": 0, "conflict": 0, "error": 0}

        for f in files:
            # Per-file isolation: one bad row (a path conflict, an unreadable
            # file, a transient DB error) must never abort the whole heal.
            try:
                path = Path(f["path"])

                if path.exists():
                    # Verify/update
                    current_hash = self._content_hash(str(path))
                    if current_hash != f["content_hash"]:
                        self.db.execute(
                            "UPDATE files SET content_hash = ?, last_seen = ? WHERE uuid = ?",
                            (current_hash, datetime.now().isoformat(), f["uuid"])
                        )

                    # Ensure xattr
                    self._set_xattr(str(path), f["uuid"])
                    stats["ok"] += 1

                    # Commit per file so a later file's rollback can't discard
                    # this file's already-applied UPDATE — the whole loop is one
                    # transaction, so an uncommitted good row would otherwise be
                    # lost when a subsequent bad row rolls back.
                    self.db.commit()

                    if verbose:
                        print(f"  OK: {path}")
                    continue

                # Try to find moved file
                new_path = self._find_moved_file(
                    f["uuid"],
                    f["path"],
                    f["content_hash"],
                    f["repo_root_commit"],
                    f["repo_relative_path"]
                )

                if new_path:
                    if self._update_path(f["uuid"], new_path, method="heal"):
                        stats["moved"] += 1
                        if verbose:
                            print(f"  MOVED: {f['path']} -> {new_path}")
                    else:
                        stats["conflict"] += 1
                        if verbose:
                            print(f"  CONFLICT: {f['path']} -> {new_path} (path held by another uuid)")
                else:
                    stats["missing"] += 1
                    if verbose:
                        print(f"  MISSING: {f['path']}")

                # Commit this file's work before moving on (missing/conflict are
                # no-ops or already committed by _update_path; harmless here).
                self.db.commit()
            except Exception as e:
                # Rollback only discards THIS file's uncommitted partial work —
                # prior files were committed above, so they survive.
                try:
                    self.db.rollback()
                except Exception:
                    pass
                stats["error"] += 1
                if verbose:
                    print(f"  ERROR: {f['path']}: {e}")

        self.db.commit()
        return stats

    def scan_directory(self, directory: str, pattern: str = "**/*") -> int:
        """
        Scan directory and assign UUIDs to all matching files.
        Returns count of files processed.
        """
        directory = Path(directory).resolve()
        count = 0

        for path in directory.glob(pattern):
            if path.is_file():
                self.assign(str(path))
                count += 1

        return count

    def list_all(self, include_missing: bool = False) -> list[FileInfo]:
        """List all tracked files."""
        rows = self.db.execute("""
            SELECT uuid, path, content_hash, size, repo_root_commit, repo_relative_path, xattr_verified
            FROM files ORDER BY path
        """).fetchall()

        results = []
        for row in rows:
            path = Path(row["path"])
            exists = path.exists()

            if not exists and not include_missing:
                continue

            xattr_present = self._get_xattr(str(path)) == row["uuid"] if exists else False

            results.append(FileInfo(
                uuid=row["uuid"],
                path=row["path"],
                content_hash=row["content_hash"],
                size=row["size"],
                repo_root_commit=row["repo_root_commit"],
                repo_relative_path=row["repo_relative_path"],
                exists=exists,
                xattr_present=xattr_present
            ))

        return results

    def orphans(self) -> list[FileInfo]:
        """List files that exist but have broken paths in DB."""
        return [f for f in self.list_all(include_missing=True) if not f.exists]

    # ─────────────────────────────────────────────────────────────────────────
    # Batch Operations (for service integration)
    # ─────────────────────────────────────────────────────────────────────────

    def assign_batch(self, paths: list[str]) -> dict[str, str]:
        """
        Assign UUIDs to many files through a single writer.

        DB hits and xattr hits resolve read-only. Genuinely new files are
        inserted in ONE transaction (retried on a locked DB) instead of one
        commit per file, so a batch build never churns the shared identity DB's
        write lock against the capture worker — the file_uuid=0-under-contention
        failure mode.

        Returns dict mapping path → uuid (paths that error are omitted).
        """
        results: dict[str, str] = {}
        pending = []  # (uuid, path, content_hash, size, repo_root, repo_relative, now)

        for raw in paths:
            try:
                path = str(Path(raw).resolve())
                if path in results:
                    # already handled this batch (dup input path) — skip so we
                    # don't re-generate a UUID and orphan a path_history row
                    continue
                row = self.db.execute(
                    "SELECT uuid FROM files WHERE path = ?", (path,)
                ).fetchone()
                if row:
                    results[path] = row["uuid"]
                    self._set_xattr(path, row["uuid"], record=False)
                    continue
                existing = self._get_xattr(path)
                if existing:
                    # moved/copied file carrying an xattr — rare in bulk indexing;
                    # delegate to the single-file path which records the new location
                    results[path] = self.assign(path)
                    continue
                file_uuid = str(uuid_lib.uuid4())
                repo_root, repo_relative = self._git_info(path)
                now = datetime.now().isoformat()
                pending.append((
                    file_uuid, path, self._content_hash(path), self._file_size(path),
                    repo_root, repo_relative, now, now,  # last_seen, created_at
                ))
                results[path] = file_uuid
            except Exception:
                # keep per-path resilience: a locked DB or transient error on
                # one path must not discard results already accumulated for
                # the rest of the batch (matches resolve_batch/locate_batch)
                continue

        if pending:
            self._insert_files_batch(pending)
            for rec in pending:
                self._set_xattr(rec[1], rec[0], record=False)  # fs only, no lock

        return results

    def _insert_files_batch(self, rows, max_retries: int = 5) -> None:
        """Insert new file rows in ONE transaction, retrying on a locked DB.

        rows: (uuid, path, content_hash, size, repo_root, repo_relative,
        last_seen, created_at).
        busy_timeout already waits on the lock; this retry is the backstop for a
        contended shared DB so a batch build survives the capture worker.
        """
        for attempt in range(max_retries):
            try:
                self.db.executemany(
                    "INSERT OR IGNORE INTO files "
                    "(uuid, path, content_hash, size, repo_root_commit, "
                    "repo_relative_path, last_seen, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
                self.db.executemany(
                    "INSERT INTO path_history (file_uuid, path) VALUES (?, ?)",
                    [(r[0], r[1]) for r in rows],
                )
                self.db.commit()
                return
            except sqlite3.OperationalError as e:
                try:
                    self.db.rollback()
                except Exception:
                    pass
                if "locked" in str(e).lower() and attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise

    def resolve_batch(self, paths: list[str]) -> dict[str, Optional[str]]:
        """
        Resolve UUIDs for multiple paths (read-only, no creation).

        Returns dict mapping path → uuid (or None if not tracked).
        """
        results = {}

        for path in paths:
            try:
                path = str(Path(path).resolve())
                results[path] = self.resolve(path)
            except Exception:
                results[path] = None

        return results

    def locate_batch(self, uuids: list[str]) -> dict[str, Optional[str]]:
        """
        Locate current paths for multiple UUIDs.

        Returns dict mapping uuid → current_path (or None if not found).
        """
        results = {}

        for uuid in uuids:
            try:
                results[uuid] = self.locate(uuid)
            except Exception:
                results[uuid] = None

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Service Integration Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def verify(self, file_uuid: str) -> dict:
        """
        Verify consistency between DB, xattr, and filesystem.

        Returns dict with status:
            {ok: bool, db: bool, xattr: bool, exists: bool, path: str, issues: []}
        """
        result = {
            "ok": False,
            "db": False,
            "xattr": False,
            "exists": False,
            "path": None,
            "issues": []
        }

        # Check DB
        row = self.db.execute(
            "SELECT path, content_hash FROM files WHERE uuid = ?", (file_uuid,)
        ).fetchone()

        if not row:
            result["issues"].append("not_in_db")
            return result

        result["db"] = True
        result["path"] = row["path"]
        path = Path(row["path"])

        # Check exists
        if not path.exists():
            result["issues"].append("file_missing")
            return result

        result["exists"] = True

        # Check xattr
        xattr_uuid = self._get_xattr(str(path))
        if xattr_uuid != file_uuid:
            result["issues"].append(f"xattr_mismatch: {xattr_uuid}")
        else:
            result["xattr"] = True

        # Check content hash
        current_hash = self._content_hash(str(path))
        if current_hash != row["content_hash"]:
            result["issues"].append("content_changed")

        result["ok"] = len(result["issues"]) == 0
        return result

    def resolve_or_locate(self, path: str, file_uuid: str = None) -> Optional[str]:
        """
        Smart resolution: try path first, fall back to UUID lookup.

        Use this in services for lazy resolution:
            current = fi.resolve_or_locate(stored_path, stored_uuid)
        """
        path = str(Path(path).resolve()) if path else None

        # Fast path: file exists at stored location
        if path and Path(path).exists():
            return path

        # Slow path: locate by UUID
        if file_uuid:
            return self.locate(file_uuid)

        return None

    @staticmethod
    def migration_sql(table: str = "files", column: str = "file_uuid") -> str:
        """
        Generate SQL to add file_uuid column to a table.

        Usage:
            sql = FileIdentity.migration_sql("chunks", "file_uuid")
            db.execute(sql)
        """
        return f"""
-- Add file_uuid column for stable file identity
ALTER TABLE {table} ADD COLUMN {column} TEXT;
CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column});
"""

    @staticmethod
    def integration_example() -> str:
        """Return example code for integrating file-identity into a service."""
        return '''
# Service Integration Example
# ===========================

from soma.file_identity import FileIdentity

fi = FileIdentity()

# On write/create operations:
def on_file_write(path: str) -> str:
    """Call when creating/modifying a file reference."""
    file_uuid = fi.assign(path)  # get-or-create UUID, sets xattr
    return file_uuid

# On read/query operations:
def get_current_path(stored_path: str, file_uuid: str) -> str:
    """Resolve stored path, handling moves."""
    return fi.resolve_or_locate(stored_path, file_uuid)

# Batch operations (for bulk indexing):
def index_directory(dir_path: str) -> dict:
    """Assign UUIDs to all files in directory."""
    paths = [str(p) for p in Path(dir_path).rglob("*") if p.is_file()]
    return fi.assign_batch(paths)

# Periodic maintenance:
def heal_file_refs():
    """Repair broken paths in your service."""
    stats = fi.heal(verbose=True)
    print(f"Healed: {stats}")
'''

    # ─────────────────────────────────────────────────────────────────────────
    # Internal Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _get_xattr(self, path: str) -> Optional[str]:
        """Read UUID from file's extended attributes."""
        getxattr = getattr(os, "getxattr", None)
        if getxattr is None:
            return None
        try:
            return getxattr(path, XATTR_NAME).decode()
        except (OSError, FileNotFoundError):
            return None

    def _set_xattr(self, path: str, file_uuid: str, record: bool = True) -> bool:
        """Set UUID in file's extended attributes.

        record=True (default) also stamps files.xattr_verified in the DB — use on
        write paths (new assign, path update, heal). record=False sets only the
        filesystem xattr and takes NO DB write lock — use on hot read paths
        (assign_batch DB-hits) so lookups never contend for the shared DB.
        """
        setxattr = getattr(os, "setxattr", None)
        if setxattr is None:
            return False
        try:
            setxattr(path, XATTR_NAME, file_uuid.encode())
            if record:
                self.db.execute(
                    "UPDATE files SET xattr_verified = ? WHERE uuid = ?",
                    (datetime.now().isoformat(), file_uuid)
                )
            return True
        except (OSError, FileNotFoundError):
            return False

    def _content_hash(self, path: str) -> Optional[str]:
        """SHA256 of file content."""
        try:
            return hashlib.sha256(Path(path).read_bytes()).hexdigest()
        except Exception:
            return None

    def _file_size(self, path: str) -> Optional[int]:
        """Get file size."""
        try:
            return Path(path).stat().st_size
        except Exception:
            return None

    def _git_info(self, path: str) -> tuple[Optional[str], Optional[str]]:
        """Get git-registry info for file."""
        if not self.repo_id:
            return None, None

        try:
            result = self.repo_id.resolve_file(path)
            if result:
                relative, repo = result
                return repo.root_commit, relative
        except Exception:
            pass

        return None, None

    def _update_path(self, file_uuid: str, new_path: str, method: str = "auto") -> bool:
        """Update file's path in DB and record in history.

        Returns True if updated, False if skipped because another uuid already
        claims new_path. Never raises on the UNIQUE(path) index — a batch heal
        must survive a collision, not abort on the first one.
        """
        now = datetime.now().isoformat()

        # Get old path for logging
        old_row = self.db.execute("SELECT path FROM files WHERE uuid = ?", (file_uuid,)).fetchone()
        old_path = old_row["path"] if old_row else None

        # Guard the UNIQUE(path) index. An absolute path belongs to one file, so
        # if another uuid already holds new_path the rows are a duplicate-identity
        # conflict (moved file whose xattr was lost, re-minted at the new path).
        # Do NOT clobber the incumbent and do NOT raise: log and skip.
        holder = self.db.execute(
            "SELECT uuid FROM files WHERE path = ? AND uuid <> ?",
            (new_path, file_uuid),
        ).fetchone()
        if holder:
            self.db.execute(
                "INSERT INTO resolution_log (file_uuid, old_path, new_path, method) "
                "VALUES (?, ?, ?, ?)",
                (file_uuid, old_path, new_path, f"{method}-conflict:{holder['uuid']}"),
            )
            self.db.commit()
            return False

        # Update main record
        content_hash = self._content_hash(new_path)
        size = self._file_size(new_path)
        repo_root, repo_relative = self._git_info(new_path)

        self.db.execute("""
            UPDATE files SET
                path = ?, content_hash = ?, size = ?,
                repo_root_commit = ?, repo_relative_path = ?,
                last_seen = ?
            WHERE uuid = ?
        """, (new_path, content_hash, size, repo_root, repo_relative, now, file_uuid))

        # Record in history
        self.db.execute(
            "INSERT INTO path_history (file_uuid, path) VALUES (?, ?)",
            (file_uuid, new_path)
        )

        # Log resolution
        self.db.execute(
            "INSERT INTO resolution_log (file_uuid, old_path, new_path, method) VALUES (?, ?, ?, ?)",
            (file_uuid, old_path, new_path, method)
        )

        self.db.commit()

        # Set xattr on new path (fs only; the UPDATE above is already committed)
        self._set_xattr(new_path, file_uuid, record=False)
        return True

    def _find_moved_file(self, file_uuid: str, old_path: str, content_hash: str,
                         repo_root: str, repo_relative: str) -> Optional[str]:
        """
        Try to find a moved file using multiple signals.

        Priority:
        1. xattr scan (in likely directories)
        2. git-registry (repo moved)
        3. content hash (same content, different location)
        """
        # Strategy 1: Check if repo moved (git-registry)
        if repo_root and repo_relative and self.repo_id:
            try:
                repo = self.repo_id.get_by_root_commit(repo_root)
                if repo and repo.path:
                    candidate = Path(repo.path) / repo_relative
                    if candidate.exists():
                        # Verify it's the same file via xattr or content
                        xattr_uuid = self._get_xattr(str(candidate))
                        if xattr_uuid == file_uuid:
                            return str(candidate)

                        candidate_hash = self._content_hash(str(candidate))
                        if candidate_hash == content_hash:
                            return str(candidate)
            except Exception:
                pass

        # Strategy 2: Scan parent directory for xattr match
        old_parent = Path(old_path).parent
        if old_parent.exists():
            for candidate in old_parent.iterdir():
                if candidate.is_file():
                    if self._get_xattr(str(candidate)) == file_uuid:
                        return str(candidate)

        # Strategy 3: Content hash search (expensive, limited scope)
        # Only check files with same size in nearby directories
        if content_hash:
            old_size = self.db.execute(
                "SELECT size FROM files WHERE uuid = ?", (file_uuid,)
            ).fetchone()

            if old_size and old_size["size"]:
                target_size = old_size["size"]

                # Check siblings and parent
                search_dirs = [old_parent]
                if old_parent.parent.exists():
                    search_dirs.append(old_parent.parent)

                for search_dir in search_dirs:
                    if not search_dir.exists():
                        continue

                    for candidate in search_dir.rglob("*"):
                        if not candidate.is_file():
                            continue

                        try:
                            if candidate.stat().st_size == target_size:
                                if self._content_hash(str(candidate)) == content_hash:
                                    return str(candidate)
                        except Exception:
                            continue

        return None


# Convenience singleton
_instance: Optional[FileIdentity] = None

def get_instance() -> FileIdentity:
    """Get singleton FileIdentity instance."""
    global _instance
    if _instance is None:
        _instance = FileIdentity()
    return _instance
