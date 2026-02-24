#!/usr/bin/env python3
"""
Eternity CLI - Backup, version, and sync soma databases.

Usage:
    eternity backup              # Snapshot all databases
    eternity commit "message"    # Commit changes to git
    eternity sync                # Sync to cloud folder
    eternity run                 # Full pipeline (backup + commit + sync)
    eternity status              # Show backup health
    eternity detect              # Detect cloud folders
    eternity set-cloud PATH      # Set cloud folder manually
    eternity log                 # Show git history
    eternity prune               # Remove old backups
"""

import argparse
import json
import sys
from pathlib import Path

from .eternity import Eternity, CloudProvider


def cmd_backup(args, e: Eternity):
    """Backup all databases."""
    results = e.backup()

    for r in results:
        status = "OK" if r.success else "FAIL"
        size = f"{r.size_bytes / (1024*1024):.1f}MB" if r.success else r.error
        print(f"  {status}: {r.name} ({size})")

    success = sum(1 for r in results if r.success)
    print(f"\nBacked up {success}/{len(results)} databases")


def cmd_commit(args, e: Eternity):
    """Commit changes to git."""
    message = args.message or f"Manual backup"

    if e.commit(message):
        print(f"Committed: {message}")
    else:
        print("No changes to commit")


def cmd_sync(args, e: Eternity):
    """Sync to cloud folder."""
    result = e.sync()

    if result.success:
        print(f"Synced to {result.provider.value}: {result.destination}")
        print(f"  Files: {result.files_synced}")
        print(f"  Bytes: {result.bytes_transferred:,}")
    else:
        print(f"Sync failed: {result.error}")
        sys.exit(1)


def cmd_run(args, e: Eternity):
    """Run full backup pipeline."""
    message = args.message or None
    no_sync = getattr(args, 'no_sync', False)

    print("Running backup pipeline...")
    results = e.run(message=message, sync=not no_sync)

    # Backups
    print("\nBackups:")
    for b in results["backups"]:
        status = "OK" if b["success"] else "FAIL"
        print(f"  {status}: {b['name']} ({b['size_mb']}MB)")

    # Commit
    print(f"\nGit: {'committed' if results['commit'] else 'no changes'}")

    # Sync
    if results["sync"]:
        s = results["sync"]
        if s["success"]:
            print(f"Cloud: synced to {s['provider']} ({s['files']} files)")
        else:
            print(f"Cloud: {s['error']}")

    # Pruned
    if any(results["pruned"].values()):
        pruned = results["pruned"]
        print(f"\nPruned: {sum(pruned.values())} old backups")


def cmd_status(args, e: Eternity):
    """Show backup status."""
    status = e.status()

    print(f"Backup Directory: {status['backup_dir']}")
    print(f"Total Size: {status['total_size_mb']}MB")
    print(f"Last Backup: {status['last_backup']}")

    print(f"\nBackups:")
    for k, v in status["backups"].items():
        print(f"  {k}: {v}")

    print(f"\nGit:")
    git = status["git"]
    print(f"  enabled: {git['enabled']}")
    if git["last_commit"]:
        print(f"  last: {git['last_commit']['message']} ({git['last_commit']['date'][:10]})")

    print(f"\nCloud:")
    cloud = status["cloud"]
    if cloud["provider"]:
        print(f"  provider: {cloud['provider']}")
        print(f"  path: {cloud['path']}")
        print(f"  writable: {cloud['writable']}")
    else:
        print("  not configured (run 'eternity detect')")


def cmd_detect(args, e: Eternity):
    """Detect cloud sync folders."""
    folders = e.detect_cloud_folders()

    if not folders:
        print("No cloud sync folders detected.")
        print("\nSupported:")
        print("  - OneDrive (Windows/WSL)")
        print("  - Dropbox")
        print("  - Google Drive")
        print("  - iCloud (macOS)")
        print("\nUse 'eternity set-cloud PATH' to set manually.")
        return

    print("Detected cloud folders:\n")
    for i, f in enumerate(folders):
        status = "writable" if f.writable else "read-only"
        print(f"  [{i+1}] {f.provider.value}: {f.path} ({status})")

    if args.select:
        # Auto-select first writable
        writable = [f for f in folders if f.writable]
        if writable:
            e.set_cloud_folder(str(writable[0].path), writable[0].provider)
            print(f"\nSelected: {writable[0].provider.value}")
    else:
        print("\nRun 'eternity detect --select' to auto-configure")
        print("Or 'eternity set-cloud PATH' to set manually")


def cmd_set_cloud(args, e: Eternity):
    """Set cloud folder manually."""
    path = Path(args.path).resolve()

    if not path.exists():
        print(f"Path does not exist: {path}")
        sys.exit(1)

    if not path.is_dir():
        print(f"Path is not a directory: {path}")
        sys.exit(1)

    e.set_cloud_folder(str(path))
    print(f"Cloud folder set: {path}")


def cmd_log(args, e: Eternity):
    """Show git commit history."""
    commits = e.git_log(args.limit)

    if not commits:
        print("No commits yet. Run 'eternity run' to create first backup.")
        return

    for c in commits:
        print(f"{c['sha']} {c['date'][:10]} {c['message']}")


def cmd_prune(args, e: Eternity):
    """Remove old backups."""
    if args.dry_run:
        print("Dry run - would prune:")
        # Just show what would be pruned
        status = e.status()
        retention = e.config["retention"]
        for k, v in status["backups"].items():
            keep = retention.get(k, 30)
            excess = max(0, v - keep)
            if excess:
                print(f"  {k}: {excess} backups")
        return

    pruned = e.prune_all()

    total = sum(pruned.values())
    if total:
        print(f"Pruned {total} old backups:")
        for k, v in pruned.items():
            if v:
                print(f"  {k}: {v}")
    else:
        print("Nothing to prune")


def main():
    parser = argparse.ArgumentParser(
        description="Eternity - Backup, version, and sync soma databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  eternity run                    # Full backup pipeline
  eternity status                 # Check backup health
  eternity detect --select        # Auto-detect and configure cloud
  eternity log                    # Show backup history
"""
    )

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # backup
    p = subparsers.add_parser("backup", help="Snapshot all databases")

    # commit
    p = subparsers.add_parser("commit", help="Commit changes to git")
    p.add_argument("message", nargs="?", help="Commit message")

    # sync
    p = subparsers.add_parser("sync", help="Sync to cloud folder")

    # run
    p = subparsers.add_parser("run", help="Full pipeline (backup + commit + sync)")
    p.add_argument("-m", "--message", help="Commit message")
    p.add_argument("--no-sync", action="store_true", help="Skip cloud sync")

    # status
    p = subparsers.add_parser("status", help="Show backup health")

    # detect
    p = subparsers.add_parser("detect", help="Detect cloud folders")
    p.add_argument("--select", action="store_true", help="Auto-select first writable folder")

    # set-cloud
    p = subparsers.add_parser("set-cloud", help="Set cloud folder manually")
    p.add_argument("path", help="Path to cloud sync folder")

    # log
    p = subparsers.add_parser("log", help="Show git history")
    p.add_argument("-n", "--limit", type=int, default=10, help="Number of commits")

    # prune
    p = subparsers.add_parser("prune", help="Remove old backups")
    p.add_argument("--dry-run", action="store_true", help="Show what would be pruned")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    e = Eternity()

    commands = {
        "backup": cmd_backup,
        "commit": cmd_commit,
        "sync": cmd_sync,
        "run": cmd_run,
        "status": cmd_status,
        "detect": cmd_detect,
        "set-cloud": cmd_set_cloud,
        "log": cmd_log,
        "prune": cmd_prune,
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        cmd_func(args, e)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
