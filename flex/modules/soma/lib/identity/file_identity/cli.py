#!/usr/bin/env python3
"""
File Identity CLI

Usage:
    fid assign <path>              # get or create UUID for file
    fid resolve <path>             # get UUID without creating
    fid locate <uuid>              # find current path for UUID
    fid info <uuid>                # full info for file
    fid history <uuid>             # path history
    fid heal [-v]                  # repair moved files
    fid scan <dir> [--pattern PAT] # bulk assign UUIDs
    fid list [--all]               # list tracked files
    fid orphans                    # list missing files
"""

import argparse
import sys
from pathlib import Path

from .identity import FileIdentity


def cmd_assign(args):
    fi = FileIdentity()
    uuid = fi.assign(args.path)
    print(uuid)


def cmd_resolve(args):
    fi = FileIdentity()
    uuid = fi.resolve(args.path)
    if uuid:
        print(uuid)
    else:
        print(f"File not tracked: {args.path}", file=sys.stderr)
        sys.exit(1)


def cmd_locate(args):
    fi = FileIdentity()
    path = fi.locate(args.uuid)
    if path:
        print(path)
    else:
        print(f"UUID not found or file missing: {args.uuid}", file=sys.stderr)
        sys.exit(1)


def cmd_info(args):
    fi = FileIdentity()
    info = fi.get(args.uuid)
    if not info:
        print(f"UUID not found: {args.uuid}", file=sys.stderr)
        sys.exit(1)

    status = "exists" if info.exists else "MISSING"
    xattr = "xattr:yes" if info.xattr_present else "xattr:no"

    print(f"UUID:   {info.uuid}")
    print(f"Path:   {info.path}")
    print(f"Status: {status} ({xattr})")
    print(f"Hash:   {info.content_hash or 'N/A'}")
    print(f"Size:   {info.size or 'N/A'}")

    if info.repo_root_commit:
        print(f"Repo:   {info.repo_root_commit[:12]}... / {info.repo_relative_path}")


def cmd_history(args):
    fi = FileIdentity()
    history = fi.history(args.uuid)

    if not history:
        print(f"No history for UUID: {args.uuid}", file=sys.stderr)
        sys.exit(1)

    print(f"Path history for {args.uuid[:8]}...\n")
    for path, detected_at in history:
        ts = detected_at[:16] if detected_at else "?"
        print(f"  {ts}  {path}")


def cmd_heal(args):
    fi = FileIdentity()
    print("Healing file identities...\n")
    stats = fi.heal(verbose=args.verbose)
    print(f"\nHealed: {stats['ok']} ok, {stats['moved']} moved, {stats['missing']} missing")


def cmd_scan(args):
    fi = FileIdentity()
    pattern = args.pattern or "**/*"
    print(f"Scanning {args.directory} with pattern '{pattern}'...")
    count = fi.scan_directory(args.directory, pattern)
    print(f"Assigned UUIDs to {count} files")


def cmd_list(args):
    fi = FileIdentity()
    files = fi.list_all(include_missing=args.all)

    if not files:
        print("No tracked files")
        return

    for f in files:
        status = "  " if f.exists else "? "
        xattr = "*" if f.xattr_present else " "
        print(f"{status}{xattr} {f.uuid[:8]}  {f.path}")

    print(f"\n{len(files)} file(s)")


def cmd_orphans(args):
    fi = FileIdentity()
    orphans = fi.orphans()

    if not orphans:
        print("No orphaned files (all paths valid)")
        return

    print("Missing files:\n")
    for f in orphans:
        print(f"  {f.uuid[:8]}  {f.path}")

    print(f"\n{len(orphans)} orphan(s)")
    print("\nRun 'fid heal' to attempt recovery")


def main():
    parser = argparse.ArgumentParser(description="File Identity System")
    subs = parser.add_subparsers(dest="cmd", required=True)

    # assign
    p = subs.add_parser("assign", help="Get or create UUID for file")
    p.add_argument("path")
    p.set_defaults(func=cmd_assign)

    # resolve
    p = subs.add_parser("resolve", help="Get UUID without creating")
    p.add_argument("path")
    p.set_defaults(func=cmd_resolve)

    # locate
    p = subs.add_parser("locate", help="Find current path for UUID")
    p.add_argument("uuid")
    p.set_defaults(func=cmd_locate)

    # info
    p = subs.add_parser("info", help="Full info for file")
    p.add_argument("uuid")
    p.set_defaults(func=cmd_info)

    # history
    p = subs.add_parser("history", help="Path history for file")
    p.add_argument("uuid")
    p.set_defaults(func=cmd_history)

    # heal
    p = subs.add_parser("heal", help="Repair moved files")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_heal)

    # scan
    p = subs.add_parser("scan", help="Bulk assign UUIDs to directory")
    p.add_argument("directory")
    p.add_argument("--pattern", help="Glob pattern (default: **/*)")
    p.set_defaults(func=cmd_scan)

    # list
    p = subs.add_parser("list", help="List tracked files")
    p.add_argument("--all", action="store_true", help="Include missing files")
    p.set_defaults(func=cmd_list)

    # orphans
    p = subs.add_parser("orphans", help="List files with broken paths")
    p.set_defaults(func=cmd_orphans)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
