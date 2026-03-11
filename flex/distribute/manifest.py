"""
Manifest protocol — fetch, parse, diff, download public cells.

The manifest is the single source of truth for what public cells exist,
where to download them, and whether the local copy is stale.
Hosted at cells.flex.dev/manifest.json.
"""

import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


MANIFEST_URL = "https://cells.flex.dev/manifest.json"


class ManifestError(Exception):
    """Network or parse error fetching manifest."""
    pass


class ChecksumError(Exception):
    """Downloaded file doesn't match expected checksum."""
    pass


@dataclass
class CellEntry:
    name: str
    url: str
    checksum: str        # "sha256:..."
    size: int            # bytes
    updated_at: str      # ISO 8601
    description: str
    cell_type: str
    freshness: str       # "live" or "snapshot"
    chunk_count: Optional[int] = None
    source_count: Optional[int] = None


def fetch_manifest(url: str = MANIFEST_URL) -> dict[str, CellEntry]:
    """Fetch and parse manifest.json. Returns {name: CellEntry}.

    Uses urllib (stdlib) — no requests dependency.
    Timeout: 10s connect, 30s read.
    Raises ManifestError on network failure or parse error.
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "getflex"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except Exception as e:
        raise ManifestError(f"Failed to fetch manifest: {e}") from e

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ManifestError(f"Invalid manifest JSON: {e}") from e

    version = data.get("version", 0)
    if version > 1:
        print(
            f"Warning: Manifest version {version} not fully supported. "
            "Upgrade flex: pip install --upgrade getflex",
            file=sys.stderr,
        )

    cells = {}
    for name, info in data.get("cells", {}).items():
        try:
            cells[name] = CellEntry(
                name=name,
                url=info["url"],
                checksum=info["checksum"],
                size=info["size"],
                updated_at=info["updated_at"],
                description=info["description"],
                cell_type=info["cell_type"],
                freshness=info.get("freshness", "snapshot"),
                chunk_count=info.get("chunk_count"),
                source_count=info.get("source_count"),
            )
        except KeyError as e:
            print(f"Warning: Skipping cell '{name}' — missing field {e}",
                  file=sys.stderr)

    return cells


def diff_manifest(
    remote: dict[str, CellEntry],
    local_cells: list[dict],
) -> dict[str, str]:
    """Compare remote manifest against local registry.

    Returns {cell_name: status} where status is one of:
    - "new"      — not installed locally
    - "stale"    — installed but checksum differs
    - "current"  — installed and checksums match
    - "orphan"   — installed locally with source_url but not in manifest
    """
    local_by_name = {c["name"]: c for c in local_cells}
    result = {}

    # Check remote cells against local
    for name, entry in remote.items():
        local = local_by_name.get(name)
        if not local or not local.get("source_url"):
            result[name] = "new"
        elif local.get("checksum") != entry.checksum:
            result[name] = "stale"
        else:
            result[name] = "current"

    # Check for orphans — installed remote cells not in manifest
    for name, cell in local_by_name.items():
        if cell.get("source_url") and name not in remote:
            result[name] = "orphan"

    return result


def download_cell(entry: CellEntry, dest_dir: Path) -> Path:
    """Download a cell from its URL to dest_dir/{name}.db.

    Streams to a temp file, verifies checksum, renames to final path.
    Returns path to downloaded file.
    Raises ChecksumError if verification fails (deletes partial file).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    final_path = dest_dir / f"{entry.name}.db"

    # Stream to temp file in same directory (for atomic rename)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".db.tmp", prefix=f"{entry.name}_", dir=str(dest_dir)
    )
    tmp = Path(tmp_path)

    try:
        req = urllib.request.Request(entry.url, headers={"User-Agent": "getflex"})
        sha = hashlib.sha256()
        downloaded = 0

        with urllib.request.urlopen(req, timeout=300) as resp:
            with os.fdopen(fd, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
                    sha.update(chunk)
                    downloaded += len(chunk)

        # Verify checksum
        expected = entry.checksum
        if expected.startswith("sha256:"):
            expected = expected[7:]
        actual = sha.hexdigest()

        if actual != expected:
            tmp.unlink(missing_ok=True)
            raise ChecksumError(
                f"Checksum mismatch for {entry.name}: "
                f"expected {expected[:16]}..., got {actual[:16]}..."
            )

        # Atomic rename
        tmp.rename(final_path)
        return final_path

    except ChecksumError:
        raise
    except Exception as e:
        tmp.unlink(missing_ok=True)
        raise ManifestError(f"Download failed for {entry.name}: {e}") from e


def _sha256_file(path: Path) -> str:
    """Compute sha256 of a file. Returns 'sha256:{hex}'."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            sha.update(chunk)
    return f"sha256:{sha.hexdigest()}"
