"""
R2 upload — push cells to Cloudflare R2 for public distribution.

Publisher-side tool. Most users never touch this.
boto3 is an optional dependency under [publish] extra.

Usage:
    python -m flex.distribute.push skills-test
    python -m flex.distribute.push --all
    python -m flex.distribute.push --all --dry-run
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.distribute.manifest import (
    MANIFEST_URL,
    CellEntry,
    _sha256_file,
    fetch_manifest,
    ManifestError,
)
from flex.registry import resolve_cell, list_cells


BUCKET = "flex-cells"
BASE_URL = "https://cells.flex.dev"


def _get_s3_client():
    """Create boto3 S3 client for R2. Raises ImportError or EnvironmentError."""
    try:
        import boto3
    except ImportError:
        raise ImportError(
            "boto3 is required for publishing. Install with: "
            "pip install getflex[publish]"
        )

    endpoint = os.environ.get("FLEX_R2_ENDPOINT")
    access_key = os.environ.get("FLEX_R2_ACCESS_KEY")
    secret_key = os.environ.get("FLEX_R2_SECRET_KEY")

    if not all([endpoint, access_key, secret_key]):
        missing = []
        if not endpoint:
            missing.append("FLEX_R2_ENDPOINT")
        if not access_key:
            missing.append("FLEX_R2_ACCESS_KEY")
        if not secret_key:
            missing.append("FLEX_R2_SECRET_KEY")
        raise EnvironmentError(
            f"Missing R2 credentials: {', '.join(missing)}. "
            "Set these environment variables to push cells."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def push_cell(
    cell_name: str,
    bucket: str = BUCKET,
) -> dict:
    """Upload a cell to R2.

    1. Resolve cell_name → local .db path via registry
    2. SHA-256 the .db file
    3. Upload .db to s3://{bucket}/{cell_name}.db
    4. Return {"url": ..., "checksum": ..., "size": ...}

    Raises ImportError if boto3 not installed.
    Raises ValueError if cell not found in registry.
    Raises EnvironmentError if R2 credentials not set.
    """
    cell_path = resolve_cell(cell_name)
    if not cell_path:
        raise ValueError(f"Cell '{cell_name}' not found in registry")

    s3 = _get_s3_client()
    checksum = _sha256_file(cell_path)
    size = cell_path.stat().st_size
    key = f"{cell_name}.db"

    print(f"[push] Uploading {cell_name} ({size / 1048576:.1f}MB)...",
          file=sys.stderr)

    s3.upload_file(
        str(cell_path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/x-sqlite3",
            "CacheControl": "public, max-age=3600",
        },
    )

    url = f"{BASE_URL}/{key}"
    print(f"[push] {cell_name} → {url}", file=sys.stderr)

    return {
        "url": url,
        "checksum": checksum,
        "size": size,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def push_manifest(
    cells: dict[str, dict],
    bucket: str = BUCKET,
) -> None:
    """Upload manifest.json to R2.

    cells: {name: {url, checksum, size, updated_at, description, ...}}
    """
    s3 = _get_s3_client()

    manifest = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": BASE_URL,
        "cells": cells,
    }

    body = json.dumps(manifest, indent=2).encode()

    s3.put_object(
        Bucket=bucket,
        Key="manifest.json",
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=30",
    )

    print(f"[push] manifest.json updated ({len(cells)} cells)", file=sys.stderr)


def _build_cell_manifest_entry(cell_name: str, push_result: dict) -> dict:
    """Build a manifest entry for a cell, combining push result with registry metadata."""
    cells = list_cells()
    cell_info = next((c for c in cells if c["name"] == cell_name), {})

    # Get chunk/source counts from the cell itself
    chunk_count = None
    source_count = None
    cell_path = resolve_cell(cell_name)
    if cell_path:
        try:
            import sqlite3
            with sqlite3.connect(str(cell_path), timeout=5) as db:
                row = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()
                chunk_count = row[0] if row else None
                row = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()
                source_count = row[0] if row else None
        except Exception:
            pass

    return {
        "url": push_result["url"],
        "checksum": push_result["checksum"],
        "size": push_result["size"],
        "updated_at": push_result["updated_at"],
        "description": cell_info.get("description", ""),
        "cell_type": cell_info.get("cell_type", ""),
        "freshness": "live",
        "chunk_count": chunk_count,
        "source_count": source_count,
    }


def push_all(
    cell_names: list[str] | None = None,
    bucket: str = BUCKET,
    dry_run: bool = False,
) -> None:
    """Push multiple cells, then update manifest.

    If cell_names is None, reads from ~/.flex/config.json publish.cells.
    After all cells are uploaded, rebuilds and pushes manifest.json.
    """
    if cell_names is None:
        config_path = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")) / "config.json"
        if config_path.exists():
            config = json.loads(config_path.read_text())
            cell_names = config.get("publish", {}).get("cells", [])
        else:
            cell_names = []

    if not cell_names:
        print("[push] No cells configured for publishing.", file=sys.stderr)
        print("[push] Set publish.cells in ~/.flex/config.json or pass cell names.",
              file=sys.stderr)
        return

    if dry_run:
        print(f"[push] Dry run — would push: {', '.join(cell_names)}", file=sys.stderr)
        for name in cell_names:
            cell_path = resolve_cell(name)
            if cell_path:
                size = cell_path.stat().st_size
                print(f"  {name:20s} {size / 1048576:.1f}MB", file=sys.stderr)
            else:
                print(f"  {name:20s} NOT FOUND", file=sys.stderr)
        return

    # Fetch current remote manifest to preserve entries we're not pushing
    try:
        remote = fetch_manifest()
        current_manifest = {}
        for name, entry in remote.items():
            current_manifest[name] = {
                "url": entry.url,
                "checksum": entry.checksum,
                "size": entry.size,
                "updated_at": entry.updated_at,
                "description": entry.description,
                "cell_type": entry.cell_type,
                "freshness": entry.freshness,
                "chunk_count": entry.chunk_count,
                "source_count": entry.source_count,
            }
    except ManifestError:
        current_manifest = {}

    # Push each cell
    for name in cell_names:
        try:
            result = push_cell(name, bucket=bucket)
            current_manifest[name] = _build_cell_manifest_entry(name, result)
        except Exception as e:
            print(f"[push] FAILED {name}: {e}", file=sys.stderr)

    # Push updated manifest
    if current_manifest:
        push_manifest(current_manifest, bucket=bucket)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Push cells to R2")
    parser.add_argument("cells", nargs="*", help="Cell names to push")
    parser.add_argument("--all", action="store_true",
                        help="Push all configured cells")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be uploaded")
    parser.add_argument("--bucket", default=BUCKET,
                        help=f"R2 bucket name (default: {BUCKET})")
    args = parser.parse_args()

    if args.all:
        push_all(bucket=args.bucket, dry_run=args.dry_run)
    elif args.cells:
        if args.dry_run:
            push_all(cell_names=args.cells, bucket=args.bucket, dry_run=True)
        else:
            push_all(cell_names=args.cells, bucket=args.bucket)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
