#!/usr/bin/env python3
"""
Eternity - Backup, versioning, and cloud sync

Provides:
- SQLite database backups with WAL checkpoint
- Git version tracking for granular history
- Cloud sync via rsync to detected sync folders
- Automatic cloud provider detection

Usage:
    from soma.eternity import Eternity

    e = Eternity()
    e.backup()              # snapshot all databases
    e.commit("message")     # commit changes to git
    e.sync()                # rsync to cloud folder
    e.status()              # show backup health
"""

import os
import sqlite3
import subprocess
import shutil
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict
import json

SOMA_DIR = Path.home() / ".soma"
BACKUP_DIR = SOMA_DIR / "backups"
CONFIG_FILE = SOMA_DIR / "eternity.json"


class CloudProvider(Enum):
    """Detected cloud sync providers."""
    ONEDRIVE = "onedrive"
    DROPBOX = "dropbox"
    GOOGLE_DRIVE = "google_drive"
    ICLOUD = "icloud"
    CUSTOM = "custom"
    NONE = "none"


@dataclass
class CloudFolder:
    """Detected cloud sync folder."""
    provider: CloudProvider
    path: Path
    writable: bool


@dataclass
class BackupResult:
    """Result of a backup operation."""
    name: str
    source: Path
    destination: Path
    size_bytes: int
    success: bool
    error: Optional[str] = None


@dataclass
class SyncResult:
    """Result of a sync operation."""
    provider: CloudProvider
    destination: Path
    files_synced: int
    bytes_transferred: int
    success: bool
    error: Optional[str] = None


class Eternity:
    """
    Eternal backup system.

    Databases survive crashes. Versions survive time.
    Cloud sync survives disasters.
    """

    FLEX_HOME = Path.home() / ".flex"

    # Default databases to backup
    DEFAULT_DATABASES = {
        "file-identity": SOMA_DIR / "file-identity.db",
        "repo-identity": SOMA_DIR / "repo-identity.db",
        "url-identity":  SOMA_DIR / "url-identity.db",
    }

    @classmethod
    def flex_databases(cls) -> Dict[str, Path]:
        """Discover flex cells from registry + registry.db itself."""
        dbs = {"flex-registry": cls.FLEX_HOME / "registry.db"}
        registry = cls.FLEX_HOME / "registry.db"
        if not registry.exists():
            return dbs
        try:
            conn = sqlite3.connect(str(registry))
            rows = conn.execute("SELECT name, path FROM cells").fetchall()
            conn.close()
            for name, path in rows:
                p = Path(path)
                if p.exists():
                    dbs[f"flex-{name}"] = p
        except Exception:
            pass
        return dbs

    # Cloud folder detection patterns
    CLOUD_PATTERNS = [
        # Windows (via WSL)
        (CloudProvider.ONEDRIVE, Path("/mnt/c/Users") / "*" / "OneDrive"),
        (CloudProvider.DROPBOX, Path("/mnt/c/Users") / "*" / "Dropbox"),
        (CloudProvider.GOOGLE_DRIVE, Path("/mnt/c/Users") / "*" / "Google Drive"),
        # macOS
        (CloudProvider.ICLOUD, Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"),
        (CloudProvider.DROPBOX, Path.home() / "Dropbox"),
        (CloudProvider.GOOGLE_DRIVE, Path.home() / "Google Drive"),
        (CloudProvider.ONEDRIVE, Path.home() / "OneDrive"),
        # Linux
        (CloudProvider.DROPBOX, Path.home() / ".local/share/dropbox"),
        (CloudProvider.GOOGLE_DRIVE, Path.home() / ".local/share/google-drive"),
    ]

    def __init__(self, backup_dir: Path = None, config_file: Path = None):
        self.backup_dir = backup_dir or BACKUP_DIR
        self.config_file = config_file or CONFIG_FILE
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load configuration from file."""
        if self.config_file.exists():
            try:
                return json.loads(self.config_file.read_text())
            except:
                pass
        return {
            "databases": {},
            "cloud_path": None,
            "cloud_provider": None,
            "retention": {"daily": 30, "weekly": 12, "monthly": 6},
            "auto_sync": True,
        }

    def _save_config(self):
        """Save configuration to file."""
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        self.config_file.write_text(json.dumps(self.config, indent=2, default=str))

    # ─────────────────────────────────────────────────────────────────────────
    # Cloud Detection
    # ─────────────────────────────────────────────────────────────────────────

    def detect_cloud_folders(self) -> List[CloudFolder]:
        """Detect available cloud sync folders on this system."""
        found = []

        for provider, pattern in self.CLOUD_PATTERNS:
            # Handle glob patterns (for /mnt/c/Users/*/...)
            if "*" in str(pattern):
                parent = Path(str(pattern).split("*")[0])
                if parent.exists():
                    for user_dir in parent.iterdir():
                        if user_dir.is_dir():
                            suffix = str(pattern).split("*")[1]
                            candidate = user_dir / suffix.lstrip("/")
                            if candidate.exists() and candidate.is_dir():
                                writable = os.access(candidate, os.W_OK)
                                found.append(CloudFolder(provider, candidate, writable))
            else:
                if pattern.exists() and pattern.is_dir():
                    writable = os.access(pattern, os.W_OK)
                    found.append(CloudFolder(provider, pattern, writable))

        return found

    def get_cloud_folder(self) -> Optional[CloudFolder]:
        """Get configured or auto-detected cloud folder."""
        # Check config first
        if self.config.get("cloud_path"):
            path = Path(self.config["cloud_path"])
            if path.exists():
                provider = CloudProvider(self.config.get("cloud_provider", "custom"))
                return CloudFolder(provider, path, os.access(path, os.W_OK))

        # Auto-detect
        folders = self.detect_cloud_folders()
        if folders:
            # Prefer writable folders
            writable = [f for f in folders if f.writable]
            return writable[0] if writable else folders[0]

        return None

    def set_cloud_folder(self, path: str, provider: CloudProvider = CloudProvider.CUSTOM):
        """Manually set cloud sync folder."""
        self.config["cloud_path"] = str(path)
        self.config["cloud_provider"] = provider.value
        self._save_config()

    # ─────────────────────────────────────────────────────────────────────────
    # Database Backup
    # ─────────────────────────────────────────────────────────────────────────

    def backup_database(self, name: str, source: Path, timestamp: str = None) -> BackupResult:
        """Backup a single SQLite database with WAL checkpoint."""
        if not source.exists():
            return BackupResult(name, source, Path(), 0, False, "Source not found")

        timestamp = timestamp or datetime.now().strftime("%Y%m%d")
        dest = self.backup_dir / f"{name}.daily.{timestamp}.db"

        if dest.exists():
            return BackupResult(name, source, dest, dest.stat().st_size, True, "Already exists")

        try:
            # Connect and WAL checkpoint
            src_conn = sqlite3.connect(str(source))
            src_conn.execute("PRAGMA wal_checkpoint(RESTART)")

            # Backup using Python's backup API (atomic)
            dst_conn = sqlite3.connect(str(dest))
            src_conn.backup(dst_conn)
            src_conn.close()
            dst_conn.close()

            # Verify integrity
            verify = sqlite3.connect(str(dest))
            result = verify.execute("PRAGMA integrity_check").fetchone()[0]
            verify.close()

            if result != "ok":
                dest.unlink(missing_ok=True)
                return BackupResult(name, source, dest, 0, False, f"Integrity check failed: {result}")

            return BackupResult(name, source, dest, dest.stat().st_size, True)

        except Exception as e:
            dest.unlink(missing_ok=True)
            return BackupResult(name, source, dest, 0, False, str(e))

    def backup(self, databases: Dict[str, Path] = None) -> List[BackupResult]:
        """Backup all configured databases (soma identity + flex cells)."""
        if databases is None:
            databases = {**self.DEFAULT_DATABASES, **self.flex_databases()}
        timestamp = datetime.now().strftime("%Y%m%d")

        results = []
        for name, path in databases.items():
            result = self.backup_database(name, path, timestamp)
            results.append(result)

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Git Version Tracking
    # ─────────────────────────────────────────────────────────────────────────

    def init_git(self) -> bool:
        """Initialize git repo in backup directory."""
        git_dir = self.backup_dir / ".git"
        if git_dir.exists():
            return True

        try:
            subprocess.run(
                ["git", "init"],
                cwd=self.backup_dir,
                capture_output=True,
                check=True
            )
            subprocess.run(
                ["git", "config", "user.email", "eternity@soma.local"],
                cwd=self.backup_dir,
                capture_output=True
            )
            subprocess.run(
                ["git", "config", "user.name", "Eternity"],
                cwd=self.backup_dir,
                capture_output=True
            )
            return True
        except Exception:
            return False

    def commit(self, message: str, stats: Dict = None) -> bool:
        """Commit backup changes to git."""
        if not (self.backup_dir / ".git").exists():
            self.init_git()

        try:
            # Stage all changes
            subprocess.run(
                ["git", "add", "-A"],
                cwd=self.backup_dir,
                capture_output=True
            )

            # Check for changes
            result = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=self.backup_dir,
                capture_output=True,
                text=True
            )

            if not result.stdout.strip():
                return False  # No changes

            # Build commit message
            full_msg = message
            if stats:
                full_msg += "\n\n"
                for db, info in stats.items():
                    full_msg += f"{db}: {info}\n"

            # Commit
            subprocess.run(
                ["git", "commit", "-m", full_msg],
                cwd=self.backup_dir,
                capture_output=True
            )

            return True

        except Exception:
            return False

    def git_log(self, limit: int = 10) -> List[Dict]:
        """Get recent git commits."""
        try:
            result = subprocess.run(
                ["git", "log", f"-{limit}", "--format=%H|%s|%ai"],
                cwd=self.backup_dir,
                capture_output=True,
                text=True
            )

            commits = []
            for line in result.stdout.strip().split("\n"):
                if "|" in line:
                    sha, msg, date = line.split("|", 2)
                    commits.append({"sha": sha[:8], "message": msg, "date": date})

            return commits
        except:
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Cloud Sync
    # ─────────────────────────────────────────────────────────────────────────

    def sync(self, cloud_folder: CloudFolder = None) -> SyncResult:
        """Sync backups to cloud folder using rsync."""
        cloud = cloud_folder or self.get_cloud_folder()

        if not cloud:
            return SyncResult(
                CloudProvider.NONE, Path(), 0, 0, False,
                "No cloud folder detected. Run 'eternity detect' or set manually."
            )

        if not cloud.writable:
            return SyncResult(
                cloud.provider, cloud.path, 0, 0, False,
                f"Cloud folder not writable: {cloud.path}"
            )

        dest = cloud.path / "SomaBackups"
        dest.mkdir(exist_ok=True)

        try:
            result = subprocess.run(
                [
                    "rsync", "-av", "--delete",
                    "--stats",
                    str(self.backup_dir) + "/",
                    str(dest) + "/"
                ],
                capture_output=True,
                text=True
            )

            # Parse rsync stats
            files_synced = 0
            bytes_transferred = 0
            for line in result.stdout.split("\n"):
                if "Number of regular files transferred:" in line:
                    files_synced = int(line.split(":")[1].strip().replace(",", ""))
                elif "Total transferred file size:" in line:
                    # Parse bytes (handles "1,234,567 bytes")
                    size_str = line.split(":")[1].strip().split()[0].replace(",", "")
                    bytes_transferred = int(size_str)

            return SyncResult(
                cloud.provider, dest, files_synced, bytes_transferred, True
            )

        except FileNotFoundError:
            return SyncResult(
                cloud.provider, dest, 0, 0, False,
                "rsync not found. Install with: apt install rsync"
            )
        except Exception as e:
            return SyncResult(cloud.provider, dest, 0, 0, False, str(e))

    # ─────────────────────────────────────────────────────────────────────────
    # Maintenance
    # ─────────────────────────────────────────────────────────────────────────

    def prune(self, pattern: str = "*.daily.*.db", keep: int = None) -> int:
        """Remove old backups, keeping most recent N."""
        keep = keep or self.config["retention"]["daily"]

        files = sorted(
            self.backup_dir.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )

        removed = 0
        for f in files[keep:]:
            f.unlink()
            removed += 1

        return removed

    def prune_all(self) -> Dict[str, int]:
        """Prune all backup types according to retention policy."""
        retention = self.config["retention"]

        return {
            "daily": self.prune("*.daily.*.db", retention["daily"]),
            "weekly": self.prune("*.weekly.*.db", retention["weekly"]),
            "monthly": self.prune("*.monthly.*.db", retention["monthly"]),
            "tarballs": self.prune("*.daily.*.tar.gz", retention["daily"]),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Status & Info
    # ─────────────────────────────────────────────────────────────────────────

    def status(self) -> Dict:
        """Get comprehensive backup status."""
        cloud = self.get_cloud_folder()

        # Count backups
        daily = list(self.backup_dir.glob("*.daily.*.db"))
        weekly = list(self.backup_dir.glob("*.weekly.*.db"))
        monthly = list(self.backup_dir.glob("*.monthly.*.db"))

        # Get last backup time
        all_backups = daily + weekly + monthly
        last_backup = max((f.stat().st_mtime for f in all_backups), default=0)
        last_backup_str = datetime.fromtimestamp(last_backup).isoformat() if last_backup else "never"

        # Total size
        total_size = sum(f.stat().st_size for f in self.backup_dir.iterdir() if f.is_file())

        # Git status
        git_enabled = (self.backup_dir / ".git").exists()
        commits = self.git_log(1) if git_enabled else []

        return {
            "backup_dir": str(self.backup_dir),
            "backups": {
                "daily": len(daily),
                "weekly": len(weekly),
                "monthly": len(monthly),
            },
            "last_backup": last_backup_str,
            "total_size_mb": round(total_size / (1024 * 1024), 1),
            "git": {
                "enabled": git_enabled,
                "last_commit": commits[0] if commits else None,
            },
            "cloud": {
                "provider": cloud.provider.value if cloud else None,
                "path": str(cloud.path) if cloud else None,
                "writable": cloud.writable if cloud else False,
            },
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Full Pipeline
    # ─────────────────────────────────────────────────────────────────────────

    def run(self, message: str = None, sync: bool = None) -> Dict:
        """
        Run full backup pipeline:
        1. Backup all databases
        2. Commit to git
        3. Sync to cloud (if enabled)
        4. Prune old backups
        """
        sync = sync if sync is not None else self.config.get("auto_sync", True)
        message = message or f"Backup {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        results = {
            "backups": [],
            "commit": False,
            "sync": None,
            "pruned": {},
        }

        # Backup
        backup_results = self.backup()
        results["backups"] = [
            {"name": r.name, "size_mb": round(r.size_bytes / (1024*1024), 1), "success": r.success}
            for r in backup_results
        ]

        # Commit
        stats = {r.name: f"{round(r.size_bytes/(1024*1024), 1)}MB" for r in backup_results if r.success}
        results["commit"] = self.commit(message, stats)

        # Sync
        if sync:
            sync_result = self.sync()
            results["sync"] = {
                "provider": sync_result.provider.value,
                "success": sync_result.success,
                "files": sync_result.files_synced,
                "error": sync_result.error,
            }

        # Prune
        results["pruned"] = self.prune_all()

        return results


# Convenience singleton
_instance: Optional[Eternity] = None

def get_instance() -> Eternity:
    """Get singleton Eternity instance."""
    global _instance
    if _instance is None:
        _instance = Eternity()
    return _instance
