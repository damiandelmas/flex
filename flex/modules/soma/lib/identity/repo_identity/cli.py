#!/usr/bin/env python3
"""Repo Identity CLI - Track repos with stable identity."""

import argparse
import sys
from pathlib import Path

from .identity import RepoIdentity


def cmd_list(args):
    reg = RepoIdentity()
    repos = reg.all()
    if not repos:
        print("No repos registered. Use: git-registry scan <dir>")
        return

    for r in repos:
        exists = "  " if r.exists else "? "
        name = r.display_name
        root = f" [{r.root_commit[:8]}]" if r.root_commit else " [no-root]"
        gh = f" (gh:{r.github_id})" if r.github_id else ""
        print(f"{exists}{name}: {r.path}{root}{gh}")


def cmd_scan(args):
    reg = RepoIdentity()
    found = reg.scan(args.dir, depth=args.depth, verbose=args.verbose)
    print(f"Found {len(found)} repos in {args.dir}")


def cmd_find(args):
    reg = RepoIdentity()
    repos = reg.find(args.query)
    if not repos:
        print(f"No repos matching '{args.query}'")
        return

    for r in repos:
        exists = "  " if r.exists else "? "
        print(f"{exists}{r.display_name}: {r.path}")


def cmd_add(args):
    reg = RepoIdentity()
    repo = reg.register(args.path)
    if repo:
        root = repo.root_commit[:12] if repo.root_commit else "none"
        print(f"Registered: {repo.display_name}")
        print(f"  Path: {repo.path}")
        print(f"  Root: {root}")
        if repo.remote_url:
            print(f"  Remote: {repo.remote_url}")
    else:
        print(f"Not a git repo: {args.path}")


def cmd_resolve(args):
    reg = RepoIdentity()
    result = reg.resolve_file(args.path)
    if result:
        relative, repo = result
        print(f"Repo: {repo.display_name}")
        print(f"  Path: {repo.path}")
        print(f"  Relative: {relative}")
        if repo.root_commit:
            print(f"  Root: {repo.root_commit[:12]}")
        if repo.remote_url:
            print(f"  Remote: {repo.remote_url}")
    else:
        print(f"Not in any registered repo: {args.path}")


def cmd_alias(args):
    reg = RepoIdentity()
    repo = reg.set_alias(args.repo, args.alias)
    if repo:
        print(f"Set alias '{args.alias}' for {repo.path}")
    else:
        print(f"Repo not found: {args.repo}")


def cmd_heal(args):
    reg = RepoIdentity()
    results = reg.heal(verbose=args.verbose)

    print(f"\nHeal results:")
    print(f"  OK: {len(results['already_ok'])}")
    print(f"  Found (moved): {len(results['found'])}")
    print(f"  Still missing: {len(results['still_missing'])}")

    if args.verbose and results['found']:
        print("\nMoved repos:")
        for old, new in results['found']:
            print(f"  {old} -> {new}")


def cmd_sync_github(args):
    reg = RepoIdentity()
    print("Syncing GitHub metadata...")
    results = reg.sync_github(verbose=args.verbose)

    print(f"\nSync results:")
    print(f"  Synced: {len(results['synced'])}")
    print(f"  Failed: {len(results['failed'])}")
    print(f"  Skipped (no GitHub remote): {len(results['skipped'])}")


def cmd_backfill(args):
    reg = RepoIdentity()
    print("Backfilling root commits...")
    count = reg.backfill_root_commits(verbose=args.verbose)
    print(f"Added root commits to {count} repos")


def cmd_prune(args):
    reg = RepoIdentity()
    removed = reg.prune()
    if removed:
        print(f"Removed {len(removed)} stale repos:")
        for p in removed:
            print(f"  {p}")
    else:
        print("No stale repos to remove")


def cmd_active(args):
    """List most recently active repos."""
    reg = RepoIdentity()
    results = reg.recently_active(limit=args.limit)

    if not results:
        print("No active repos found")
        return

    print(f"Most recently active ({len(results)}):\n")
    for repo, stats in results:
        last = stats.get("last_commit", "")[:10]
        commits_7d = stats.get("commits_7d", 0)
        dirty = " *" if stats.get("dirty") else ""
        branch = stats.get("branch", "")
        print(f"  {repo.display_name}{dirty}")
        print(f"    Last: {last}  |  7d: {commits_7d} commits  |  {branch}")


def cmd_created(args):
    """List most recently created repos."""
    reg = RepoIdentity()
    results = reg.recently_created(limit=args.limit)

    if not results:
        print("No repos found")
        return

    print(f"Most recently created ({len(results)}):\n")
    for repo, stats in results:
        created = stats.get("created", "")[:10]
        commits = stats.get("commit_count", 0)
        print(f"  {repo.display_name}")
        print(f"    Created: {created}  |  {commits} commits")


def cmd_hot(args):
    """List repos with most activity in last 7 days."""
    reg = RepoIdentity()
    results = reg.most_active(limit=args.limit)

    if not results:
        print("No active repos in last 7 days")
        return

    print(f"Most active last 7 days ({len(results)}):\n")
    for repo, stats in results:
        commits_7d = stats.get("commits_7d", 0)
        dirty = " *" if stats.get("dirty") else ""
        branch = stats.get("branch", "")
        print(f"  {repo.display_name}{dirty}: {commits_7d} commits  ({branch})")


def cmd_stats(args):
    """Show detailed stats for a repo."""
    reg = RepoIdentity()
    repo = reg.get_by_name(args.repo) or reg.get_by_path(args.repo)
    if not repo:
        repos = reg.find(args.repo)
        if repos:
            repo = repos[0]

    if not repo:
        print(f"Repo not found: {args.repo}")
        return

    stats = reg.get_repo_stats(repo)
    if not stats:
        print(f"Could not get stats for {repo.path}")
        return

    print(f"Stats for {repo.display_name}:")
    print(f"  Path: {repo.path}")
    print(f"  Branch: {stats.get('branch', 'N/A')}")
    print(f"  Dirty: {'yes' if stats.get('dirty') else 'no'}")
    print(f"  Created: {stats.get('created', 'N/A')}")
    print(f"  Last commit: {stats.get('last_commit', 'N/A')}")
    print(f"  Total commits: {stats.get('commit_count', 'N/A')}")
    print(f"  Commits (7d): {stats.get('commits_7d', 'N/A')}")


def cmd_show(args):
    reg = RepoIdentity()
    repo = reg.get_by_name(args.repo) or reg.get_by_path(args.repo)
    if not repo:
        # Try fuzzy
        repos = reg.find(args.repo)
        if repos:
            repo = repos[0]

    if not repo:
        print(f"Repo not found: {args.repo}")
        return

    print(f"Name: {repo.display_name}")
    print(f"Path: {repo.path}")
    print(f"Exists: {'yes' if repo.exists else 'NO'}")
    if repo.alias:
        print(f"Alias: {repo.alias}")
    if repo.root_commit:
        print(f"Root commit: {repo.root_commit}")
    if repo.remote_url:
        print(f"Remote: {repo.remote_url}")
    if repo.github_id:
        print(f"GitHub ID: {repo.github_id}")
    print(f"Last seen: {repo.last_seen}")


def main():
    parser = argparse.ArgumentParser(description="Git repository registry")
    subs = parser.add_subparsers(dest="cmd")

    # list (default)
    p = subs.add_parser("list", help="List registered repos")
    p.set_defaults(func=cmd_list)

    # scan
    p = subs.add_parser("scan", help="Scan for repos")
    p.add_argument("dir", help="Directory to scan")
    p.add_argument("--depth", type=int, default=3)
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_scan)

    # find
    p = subs.add_parser("find", help="Find repo by name/remote/path")
    p.add_argument("query")
    p.set_defaults(func=cmd_find)

    # add
    p = subs.add_parser("add", help="Register a repo")
    p.add_argument("path")
    p.set_defaults(func=cmd_add)

    # show
    p = subs.add_parser("show", help="Show repo details")
    p.add_argument("repo", help="Repo name, alias, or path")
    p.set_defaults(func=cmd_show)

    # resolve
    p = subs.add_parser("resolve", help="Resolve file to repo + relative path")
    p.add_argument("path")
    p.set_defaults(func=cmd_resolve)

    # alias
    p = subs.add_parser("alias", help="Set alias for a repo")
    p.add_argument("repo", help="Repo name or path")
    p.add_argument("alias", help="Alias to set")
    p.set_defaults(func=cmd_alias)

    # heal
    p = subs.add_parser("heal", help="Find moved repos by root commit")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_heal)

    # sync-github
    p = subs.add_parser("sync-github", help="Sync GitHub IDs and names")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_sync_github)

    # backfill
    p = subs.add_parser("backfill", help="Backfill root commits for existing repos")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_backfill)

    # prune
    p = subs.add_parser("prune", help="Remove stale repos")
    p.set_defaults(func=cmd_prune)

    # active
    p = subs.add_parser("active", help="List recently active repos")
    p.add_argument("-n", "--limit", type=int, default=10)
    p.set_defaults(func=cmd_active)

    # created
    p = subs.add_parser("created", help="List recently created repos")
    p.add_argument("-n", "--limit", type=int, default=10)
    p.set_defaults(func=cmd_created)

    # hot
    p = subs.add_parser("hot", help="List most active repos (7 days)")
    p.add_argument("-n", "--limit", type=int, default=10)
    p.set_defaults(func=cmd_hot)

    # stats
    p = subs.add_parser("stats", help="Show detailed stats for a repo")
    p.add_argument("repo", help="Repo name, alias, or path")
    p.set_defaults(func=cmd_stats)

    args = parser.parse_args()

    if not args.cmd:
        cmd_list(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
