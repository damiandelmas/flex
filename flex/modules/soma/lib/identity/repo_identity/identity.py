"""
Repo Identity - Git repository tracker with stable identity.

Track git repos by root_commit (stable UUID), path, and remote URL.
Survives moves, renames, and clones.
"""

import json
import os
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class Repo:
    id: int
    path: str
    root_commit: Optional[str]       # stable UUID (first commit)
    remote_url: Optional[str]
    github_id: Optional[int]         # immutable GitHub ID
    name: str
    alias: Optional[str]             # user-assigned name
    last_seen: str

    @property
    def exists(self) -> bool:
        return Path(self.path).exists()

    @property
    def display_name(self) -> str:
        """Alias > name > path basename."""
        return self.alias or self.name or Path(self.path).name


class RepoIdentity:
    """Stable identity for git repositories via root_commit."""

    DEFAULT_DB = Path.home() / ".soma" / "repo-identity.db"
    OLD_DB = Path.home() / ".soma" / "git-registry.db"  # Migration source
    LEGACY_DB = Path.home() / ".home" / "git-registry.db"  # Old location

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else self.DEFAULT_DB

        # Migrate from old locations if needed
        if not self.db_path.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            if self.OLD_DB.exists():
                shutil.copy2(self.OLD_DB, self.db_path)
            elif self.LEGACY_DB.exists():
                shutil.copy2(self.LEGACY_DB, self.db_path)

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # Crash safety
        return conn

    def _init_schema(self):
        with self._get_conn() as conn:
            # Check if table exists
            table_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='repos'"
            ).fetchone()

            if not table_exists:
                # Fresh install
                conn.execute("""
                    CREATE TABLE repos (
                        id INTEGER PRIMARY KEY,
                        path TEXT NOT NULL UNIQUE,
                        root_commit TEXT,
                        remote_url TEXT,
                        github_id INTEGER,
                        name TEXT,
                        alias TEXT,
                        last_seen TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                # Migrate: add new columns if missing
                cols = {r[1] for r in conn.execute("PRAGMA table_info(repos)").fetchall()}
                if "root_commit" not in cols:
                    conn.execute("ALTER TABLE repos ADD COLUMN root_commit TEXT")
                if "github_id" not in cols:
                    conn.execute("ALTER TABLE repos ADD COLUMN github_id INTEGER")
                if "alias" not in cols:
                    conn.execute("ALTER TABLE repos ADD COLUMN alias TEXT")

            # Create indexes (safe to run always)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_repos_root ON repos(root_commit)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_repos_remote ON repos(remote_url)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_repos_github ON repos(github_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_repos_name ON repos(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_repos_alias ON repos(alias)")

    def _row_to_repo(self, row: sqlite3.Row) -> Repo:
        return Repo(
            id=row["id"],
            path=row["path"],
            root_commit=row["root_commit"] if "root_commit" in row.keys() else None,
            remote_url=row["remote_url"],
            github_id=row["github_id"] if "github_id" in row.keys() else None,
            name=row["name"],
            alias=row["alias"] if "alias" in row.keys() else None,
            last_seen=row["last_seen"]
        )

    @staticmethod
    def _get_root_commit(path: Path) -> Optional[str]:
        """Get first/root commit - stable UUID for the repo."""
        try:
            result = subprocess.run(
                ["git", "-C", str(path), "rev-list", "--max-parents=0", "HEAD"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                # Take first line (in case of multiple roots from merges)
                return result.stdout.strip().split('\n')[0]
        except Exception:
            pass
        return None

    @staticmethod
    def _get_remote_url(path: Path) -> Optional[str]:
        """Get origin remote URL."""
        try:
            result = subprocess.run(
                ["git", "-C", str(path), "remote", "get-url", "origin"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None

    @staticmethod
    def _derive_name(path: Path, remote_url: Optional[str]) -> str:
        """Derive display name from remote or path."""
        if remote_url:
            # git@github.com:user/repo.git → user/repo
            # https://github.com/user/repo.git → user/repo
            url = remote_url.replace(".git", "")
            if "github.com" in url:
                parts = url.split("github.com")[-1]
                parts = parts.lstrip(":/")
                return parts
            # Fallback: last segment
            return url.split("/")[-1]
        else:
            # Local: parent/name
            parts = path.parts
            if len(parts) >= 2:
                return f"{parts[-2]}/{parts[-1]}"
            return path.name

    @staticmethod
    def _derive_alias(path: Path) -> Optional[str]:
        """Auto-derive alias for common patterns like /main/, /master/, /src/."""
        folder_name = path.name.lower()
        if folder_name in ("main", "master", "src", "repo", "code"):
            # Use parent folder as alias
            return path.parent.name
        return None

    def register(self, path: str | Path) -> Optional[Repo]:
        """Register a git repository."""
        path = Path(path).expanduser().resolve()
        if not (path / ".git").exists():
            return None

        root_commit = self._get_root_commit(path)
        remote_url = self._get_remote_url(path)
        name = self._derive_name(path, remote_url)
        auto_alias = self._derive_alias(path)
        now = datetime.now().isoformat()

        with self._get_conn() as conn:
            # Check if exists by root_commit at a DIFFERENT path (repo moved)
            if root_commit:
                old_entry = conn.execute(
                    "SELECT * FROM repos WHERE root_commit = ? AND path != ?",
                    (root_commit, str(path))
                ).fetchone()
                if old_entry:
                    # Repo moved - delete any stale entry at new path, update old entry
                    conn.execute("DELETE FROM repos WHERE path = ?", (str(path),))
                    preserve_alias = old_entry["alias"] if old_entry["alias"] else auto_alias
                    conn.execute(
                        "UPDATE repos SET path = ?, alias = ?, last_seen = ? WHERE id = ?",
                        (str(path), preserve_alias, now, old_entry["id"])
                    )
                    row = conn.execute("SELECT * FROM repos WHERE id = ?", (old_entry["id"],)).fetchone()
                    return self._row_to_repo(row)

            # Check if exists
            existing = conn.execute(
                "SELECT id, alias FROM repos WHERE path = ?", (str(path),)
            ).fetchone()

            if existing:
                # Update existing, preserve alias if already set
                preserve_alias = existing["alias"] if existing["alias"] else auto_alias
                conn.execute(
                    """UPDATE repos SET
                       root_commit = ?, remote_url = ?, name = ?, alias = ?, last_seen = ?
                       WHERE id = ?""",
                    (root_commit, remote_url, name, preserve_alias, now, existing["id"])
                )
            else:
                # Insert new
                conn.execute(
                    """INSERT INTO repos (path, root_commit, remote_url, name, alias, last_seen)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (str(path), root_commit, remote_url, name, auto_alias, now)
                )

            row = conn.execute("SELECT * FROM repos WHERE path = ?", (str(path),)).fetchone()

        return self._row_to_repo(row) if row else None

    def set_alias(self, path_or_name: str, alias: str) -> Optional[Repo]:
        """Set a user-defined alias for a repo."""
        with self._get_conn() as conn:
            # Find by path, name, or existing alias
            row = conn.execute(
                "SELECT * FROM repos WHERE path = ? OR name = ? OR alias = ?",
                (path_or_name, path_or_name, path_or_name)
            ).fetchone()
            if not row:
                return None

            conn.execute(
                "UPDATE repos SET alias = ? WHERE id = ?",
                (alias, row["id"])
            )
            row = conn.execute("SELECT * FROM repos WHERE id = ?", (row["id"],)).fetchone()
            return self._row_to_repo(row)

    def scan(self, directory: str | Path, depth: int = 3, verbose: bool = False) -> list[Repo]:
        """Scan directory for git repos and register them."""
        directory = Path(directory).expanduser().resolve()
        found = []

        for root, dirs, _ in os.walk(directory):
            rel_depth = str(root).count(os.sep) - str(directory).count(os.sep)
            if rel_depth >= depth:
                dirs.clear()
                continue

            if ".git" in dirs:
                repo = self.register(root)
                if repo:
                    found.append(repo)
                    if verbose:
                        print(f"  {repo.display_name}: {repo.path}")
                dirs.remove(".git")

        return found

    def find(self, query: str) -> list[Repo]:
        """Find repos by alias, name, remote, or path (fuzzy)."""
        pattern = f"%{query}%"
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT * FROM repos
                   WHERE alias LIKE ? OR name LIKE ? OR remote_url LIKE ? OR path LIKE ?
                   ORDER BY COALESCE(alias, name)""",
                (pattern, pattern, pattern, pattern)
            ).fetchall()
        return [self._row_to_repo(r) for r in rows]

    def get_by_root_commit(self, root_commit: str) -> Optional[Repo]:
        """Get repo by root commit (stable UUID)."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM repos WHERE root_commit = ?", (root_commit,)
            ).fetchone()
        return self._row_to_repo(row) if row else None

    def get_by_remote(self, remote_url: str) -> Optional[Repo]:
        """Get repo by exact remote URL."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM repos WHERE remote_url = ?", (remote_url,)
            ).fetchone()
        return self._row_to_repo(row) if row else None

    def get_by_github_id(self, github_id: int) -> Optional[Repo]:
        """Get repo by GitHub ID (survives renames)."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM repos WHERE github_id = ?", (github_id,)
            ).fetchone()
        return self._row_to_repo(row) if row else None

    def get_by_path(self, path: str | Path) -> Optional[Repo]:
        """Get repo by exact path."""
        path = str(Path(path).expanduser().resolve())
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM repos WHERE path = ?", (path,)
            ).fetchone()
        return self._row_to_repo(row) if row else None

    def get_by_name(self, name: str) -> Optional[Repo]:
        """Get repo by name or alias."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM repos WHERE alias = ? OR name = ?", (name, name)
            ).fetchone()
        return self._row_to_repo(row) if row else None

    def all(self) -> list[Repo]:
        """Get all registered repos."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM repos ORDER BY COALESCE(alias, name)").fetchall()
        return [self._row_to_repo(r) for r in rows]

    def resolve_file(self, abs_path: str | Path) -> Optional[tuple[str, Repo]]:
        """Given an absolute file path, find its repo and return (relative_path, repo)."""
        abs_path = Path(abs_path).expanduser().resolve()
        abs_str = str(abs_path)

        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM repos ORDER BY length(path) DESC"
            ).fetchall()

        for row in rows:
            repo_path = row["path"]
            if abs_str.startswith(repo_path + os.sep) or abs_str == repo_path:
                repo = self._row_to_repo(row)
                try:
                    relative = os.path.relpath(abs_str, repo_path)
                    return (relative, repo)
                except ValueError:
                    continue

        return None

    def remove(self, path: str | Path) -> bool:
        """Remove a repo from registry."""
        path = str(Path(path).expanduser().resolve())
        with self._get_conn() as conn:
            cursor = conn.execute("DELETE FROM repos WHERE path = ?", (path,))
            return cursor.rowcount > 0

    def prune(self) -> list[str]:
        """Remove repos that no longer exist."""
        removed = []
        for repo in self.all():
            if not repo.exists:
                self.remove(repo.path)
                removed.append(repo.path)
        return removed

    def heal(self, scan_dirs: Optional[list[str]] = None, verbose: bool = False) -> dict:
        """Find moved repos by matching root_commit in filesystem."""
        if scan_dirs is None:
            scan_dirs = [str(Path.home() / "projects")]

        results = {"found": [], "still_missing": [], "already_ok": []}

        # Get all repos with root_commit that don't exist at current path
        stale = [r for r in self.all() if not r.exists and r.root_commit]
        ok = [r for r in self.all() if r.exists]
        results["already_ok"] = [r.path for r in ok]

        if not stale:
            return results

        # Build map of root_commit -> stale repo
        stale_map = {r.root_commit: r for r in stale}

        # Scan for .git directories
        for scan_dir in scan_dirs:
            scan_path = Path(scan_dir).expanduser().resolve()
            for root, dirs, _ in os.walk(scan_path):
                if ".git" in dirs:
                    repo_path = Path(root)
                    root_commit = self._get_root_commit(repo_path)

                    if root_commit and root_commit in stale_map:
                        old_repo = stale_map[root_commit]
                        # Found it! Update path
                        with self._get_conn() as conn:
                            conn.execute(
                                "UPDATE repos SET path = ?, last_seen = ? WHERE id = ?",
                                (str(repo_path), datetime.now().isoformat(), old_repo.id)
                            )
                        if verbose:
                            print(f"  Found: {old_repo.path} -> {repo_path}")
                        results["found"].append((old_repo.path, str(repo_path)))
                        del stale_map[root_commit]

                    dirs.remove(".git")

        results["still_missing"] = [r.path for r in stale_map.values()]
        return results

    def sync_github(self, verbose: bool = False) -> dict:
        """Sync GitHub metadata (ID, current name) for repos with remotes."""
        results = {"synced": [], "failed": [], "skipped": []}

        for repo in self.all():
            if not repo.remote_url or "github.com" not in repo.remote_url:
                results["skipped"].append(repo.path)
                continue

            # Extract owner/repo from URL
            url = repo.remote_url.replace(".git", "")
            if "github.com:" in url:
                # git@github.com:user/repo
                owner_repo = url.split("github.com:")[-1]
            elif "github.com/" in url:
                # https://github.com/user/repo
                owner_repo = url.split("github.com/")[-1]
            else:
                results["skipped"].append(repo.path)
                continue

            try:
                result = subprocess.run(
                    ["gh", "api", f"repos/{owner_repo}", "--jq", "{id, full_name}"],
                    capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    with self._get_conn() as conn:
                        conn.execute(
                            "UPDATE repos SET github_id = ?, name = ? WHERE id = ?",
                            (data["id"], data["full_name"], repo.id)
                        )
                    if verbose:
                        print(f"  Synced: {data['full_name']} (ID: {data['id']})")
                    results["synced"].append(repo.path)
                else:
                    results["failed"].append(repo.path)
            except Exception as e:
                if verbose:
                    print(f"  Failed: {repo.path} - {e}")
                results["failed"].append(repo.path)

        return results

    def get_repo_stats(self, repo: Repo) -> Optional[dict]:
        """Get activity stats for a repo (computed on-demand)."""
        if not repo.exists:
            return None

        stats = {}
        path = repo.path

        try:
            # Last commit date
            result = subprocess.run(
                ["git", "-C", path, "log", "-1", "--format=%aI"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                stats["last_commit"] = result.stdout.strip()

            # First commit date (repo creation)
            result = subprocess.run(
                ["git", "-C", path, "log", "--reverse", "--format=%aI", "-1"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                stats["created"] = result.stdout.strip()

            # Commit count
            result = subprocess.run(
                ["git", "-C", path, "rev-list", "--count", "HEAD"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                stats["commit_count"] = int(result.stdout.strip())

            # Recent commits (last 7 days)
            result = subprocess.run(
                ["git", "-C", path, "rev-list", "--count", "--since=7 days ago", "HEAD"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                stats["commits_7d"] = int(result.stdout.strip())

            # Current branch
            result = subprocess.run(
                ["git", "-C", path, "branch", "--show-current"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                stats["branch"] = result.stdout.strip()

            # Dirty state
            result = subprocess.run(
                ["git", "-C", path, "status", "--porcelain"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                stats["dirty"] = bool(result.stdout.strip())

        except Exception:
            pass

        return stats

    def recently_active(self, limit: int = 10) -> list[tuple[Repo, dict]]:
        """Get repos sorted by most recent commit."""
        results = []

        for repo in self.all():
            if not repo.exists:
                continue
            stats = self.get_repo_stats(repo)
            if stats and "last_commit" in stats:
                results.append((repo, stats))

        # Sort by last_commit descending
        results.sort(key=lambda x: x[1].get("last_commit", ""), reverse=True)
        return results[:limit]

    def recently_created(self, limit: int = 10) -> list[tuple[Repo, dict]]:
        """Get repos sorted by creation date (first commit)."""
        results = []

        for repo in self.all():
            if not repo.exists:
                continue
            stats = self.get_repo_stats(repo)
            if stats and "created" in stats:
                results.append((repo, stats))

        # Sort by created descending
        results.sort(key=lambda x: x[1].get("created", ""), reverse=True)
        return results[:limit]

    def most_active(self, limit: int = 10) -> list[tuple[Repo, dict]]:
        """Get repos with most commits in last 7 days."""
        results = []

        for repo in self.all():
            if not repo.exists:
                continue
            stats = self.get_repo_stats(repo)
            if stats and stats.get("commits_7d", 0) > 0:
                results.append((repo, stats))

        # Sort by commits_7d descending
        results.sort(key=lambda x: x[1].get("commits_7d", 0), reverse=True)
        return results[:limit]

    def backfill_root_commits(self, verbose: bool = False) -> int:
        """Add root_commit to repos that don't have it."""
        count = 0
        for repo in self.all():
            if repo.root_commit:
                continue
            if not repo.exists:
                continue

            root_commit = self._get_root_commit(Path(repo.path))
            if root_commit:
                with self._get_conn() as conn:
                    conn.execute(
                        "UPDATE repos SET root_commit = ? WHERE id = ?",
                        (root_commit, repo.id)
                    )
                count += 1
                if verbose:
                    print(f"  {repo.display_name}: {root_commit[:12]}")

        return count
