"""
Manifest protocol — fetch, parse, diff, download public cells.

The manifest is the single source of truth for what public cells exist,
where to download them, and whether the local copy is stale.
Hosted at a public URL.
"""

import base64
import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# Base URL is env-overridable so a caller can point at an access-controlled
# registry instead of the public one. FLEX_R2_BASE_URL, when set, also
# governs the URL push.py embeds in manifest entries — keep the two in sync.
DEFAULT_BASE_URL = "https://hub.getflex.dev"
BASE_URL = os.environ.get("FLEX_R2_BASE_URL", DEFAULT_BASE_URL)
MANIFEST_URL = f"{BASE_URL}/manifest.json"
MANIFEST_SIG_URL = f"{BASE_URL}/manifest.json.sig"


class ManifestError(Exception):
    """Network or parse error fetching manifest."""
    pass


class ManifestSignatureError(ManifestError):
    """Manifest signature verification failed."""
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


# Baked trust root: base64 of raw 32-byte ed25519 PUBLIC key(s). A list so a
# next key can be published alongside the current one and rotation needs no
# flag-day. Populated at the owner-gated deploy step — keypair generation and
# the first signed publish happen together, out of band; NO key is generated in
# code. Empty on purpose: ships checksum-only. With no trusted key baked,
# signature verification is SKIPPED — checksum-only + one warning (see
# _verify_manifest_signature). Signing activates when a trusted public key is
# baked in — owner-gated, not yet done — at which point the same code enforces
# fail-closed with no change. The matching private key is owner-held and never
# ships in the wheel.
_TRUSTED_PUBLIC_KEYS: list[str] = []


def _load_trusted_public_keys(keys: Optional[list[str]] = None) -> list:
    """Decode the baked base64 ed25519 public keys into verifier objects.

    `keys` overrides the baked constant (tests pass a throwaway test key). A
    malformed baked key raises — a broken trust root must never degrade to a
    silent pass.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    src = _TRUSTED_PUBLIC_KEYS if keys is None else keys
    verifiers = []
    for b64 in src:
        try:
            verifiers.append(
                Ed25519PublicKey.from_public_bytes(base64.b64decode(b64, validate=True))
            )
        except Exception as e:
            raise ManifestSignatureError(
                f"Malformed trusted public key: {e}. Aborting."
            ) from e
    return verifiers


def _verify_manifest_signature(
    raw: bytes,
    sig_url: str = MANIFEST_SIG_URL,
    sig_bytes: Optional[bytes] = None,
    trusted_keys: Optional[list[str]] = None,
) -> None:
    """Verify the detached ed25519 signature of the manifest — FAIL CLOSED.

    `raw` is the exact manifest.json bytes as fetched. The detached signature is
    base64(ed25519_sign(private_key, raw)), published as manifest.json.sig, and
    is verified against the baked public key(s) in _TRUSTED_PUBLIC_KEYS (the
    trust root shipped in the wheel). The matching private key is owner-held and
    never ships, so a valid signature can only come from the publisher —
    asymmetric, unforgeable by a client (unlike the retired symmetric HMAC).

    Posture (ships checksum-only): if NO trusted key is baked, signature
    verification is SKIPPED with one stderr warning and the per-cell SHA-256
    checksum (download_cell) is the integrity guarantee. When a trusted key IS
    configured, a signature is fully required — a missing, malformed, or bad
    sig, or one matching no trusted key, each raise ManifestSignatureError and
    abort (no cell bytes written). Baking the public key — owner-gated, not yet
    done — activates enforcement with no code change. Verification depends on no
    local secret.

    The signature bytes are fetched over anonymous HTTP from sig_url by default.
    Callers on the authenticated path pass the already-fetched signature via
    sig_bytes so the same invariant holds without an out-of-band HTTP GET.
    """
    from cryptography.exceptions import InvalidSignature

    verifiers = _load_trusted_public_keys(trusted_keys)
    if not verifiers:
        # Ships checksum-only. With no baked trusted key we SKIP signature
        # verification and rely on the per-cell SHA-256 checksum (enforced in
        # download_cell) as the integrity guarantee, warning once. Baking the
        # public-hub key — owner-gated, not yet done — makes this same path
        # enforce fail-closed with no code change. A signature is still fully
        # required whenever a key IS configured (the checks below).
        print(
            "Warning: manifest not signature-verified: no trusted key configured; "
            "integrity via checksum only.",
            file=sys.stderr,
        )
        return

    if sig_bytes is None:
        try:
            req = urllib.request.Request(sig_url, headers={"User-Agent": "getflex"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                sig_bytes = resp.read()
        except Exception as e:
            raise ManifestSignatureError(
                f"Failed to fetch manifest signature: {e}. "
                "Cannot verify manifest integrity — aborting."
            ) from e

    try:
        signature = base64.b64decode(sig_bytes.strip(), validate=True)
    except Exception as e:
        raise ManifestSignatureError(
            f"Malformed manifest signature: {e}. Aborting."
        ) from e

    for verifier in verifiers:
        try:
            verifier.verify(signature, raw)
            return
        except InvalidSignature:
            continue

    raise ManifestSignatureError(
        "Manifest signature mismatch — possible tampering. Aborting."
    )


def _r2_auth_configured() -> bool:
    """True when credentials for an access-controlled registry are set."""
    return bool(
        os.environ.get("FLEX_R2_ENDPOINT")
        and os.environ.get("FLEX_R2_ACCESS_KEY")
        and os.environ.get("FLEX_R2_SECRET_KEY")
    )


def _r2_get_object(key: str) -> bytes:
    """Authenticated GET of one object via boto3, for an access-controlled
    (non-public) registry. Only called when _r2_auth_configured() is True.

    FLEX_R2_BUCKET has no default here — the anonymous public path never
    needs a bucket name (it just uses BASE_URL), so this path must not
    invent one either; it must be explicitly configured.

    Raises ManifestError if boto3 is absent, FLEX_R2_BUCKET is unset, or
    the request fails.
    """
    try:
        import boto3
    except ImportError as e:
        raise ManifestError(
            "Authenticated registry access requires boto3. Install with: "
            "pip install boto3"
        ) from e

    bucket = os.environ.get("FLEX_R2_BUCKET")
    if not bucket:
        raise ManifestError(
            "FLEX_R2_BUCKET must be set for authenticated registry access."
        )
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["FLEX_R2_ENDPOINT"],
        aws_access_key_id=os.environ["FLEX_R2_ACCESS_KEY"],
        aws_secret_access_key=os.environ["FLEX_R2_SECRET_KEY"],
    )
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    except Exception as e:
        raise ManifestError(f"Authenticated fetch failed for '{key}': {e}") from e


def fetch_manifest(url: str = MANIFEST_URL) -> dict[str, CellEntry]:
    """Fetch and parse manifest.json. Returns {name: CellEntry}.

    Default path: anonymous HTTP GET via urllib (stdlib, no dependency), with
    mandatory ed25519 signature verification against the baked public key.
    Timeout: 10s connect, 30s read.

    When FLEX_R2_ENDPOINT/FLEX_R2_ACCESS_KEY/FLEX_R2_SECRET_KEY are all set,
    fetches via an authenticated boto3 S3 get_object instead — for an
    access-controlled registry the anonymous public URL can't reach. The same
    invariant holds on this path: the detached signature is fetched over the
    same authenticated channel and MUST verify.

    Raises ManifestError on network/auth failure or parse error, and
    ManifestSignatureError (a ManifestError subclass) if the signature is
    absent, malformed, or does not verify.
    """
    if _r2_auth_configured():
        raw = _r2_get_object("manifest.json")
        # A signature is ALWAYS required. Fetch the detached sig over the same
        # authenticated channel and verify against the baked public key.
        try:
            sig_bytes = _r2_get_object("manifest.json.sig")
        except ManifestError as e:
            raise ManifestSignatureError(
                f"Failed to fetch manifest signature: {e}. "
                "Cannot verify manifest integrity — aborting."
            ) from e
        _verify_manifest_signature(raw, sig_bytes=sig_bytes)
    else:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "getflex"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
        except Exception as e:
            raise ManifestError(f"Failed to fetch manifest: {e}") from e

        _verify_manifest_signature(raw)

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

    Default path: streams entry.url to a temp file over anonymous HTTP.
    When FLEX_R2_ENDPOINT/FLEX_R2_ACCESS_KEY/FLEX_R2_SECRET_KEY are all set,
    fetches the cell's bytes via an authenticated boto3 S3 get_object
    instead (key = "{entry.name}.db"), for an access-controlled registry.

    Verifies checksum, renames to final path atomically either way.
    Returns path to downloaded file.
    Raises ChecksumError if verification fails (deletes partial file).
    Raises ManifestError if entry.name contains path traversal.
    """
    # Validate entry.name — no path traversal
    safe_name = Path(entry.name).name  # strip any directory components
    if safe_name != entry.name or '..' in entry.name or '/' in entry.name:
        raise ManifestError(
            f"Invalid cell name '{entry.name}' — path traversal not allowed"
        )

    dest_dir.mkdir(parents=True, exist_ok=True)
    final_path = dest_dir / f"{safe_name}.db"

    # Stream to temp file in same directory (for atomic rename)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".db.tmp", prefix=f"{entry.name}_", dir=str(dest_dir)
    )
    tmp = Path(tmp_path)

    try:
        sha = hashlib.sha256()

        if _r2_auth_configured():
            raw = _r2_get_object(f"{safe_name}.db")
            with os.fdopen(fd, "wb") as f:
                f.write(raw)
            sha.update(raw)
        else:
            req = urllib.request.Request(entry.url, headers={"User-Agent": "getflex"})
            with urllib.request.urlopen(req, timeout=300) as resp:
                with os.fdopen(fd, "wb") as f:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        sha.update(chunk)

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

        # Atomic rename + restrictive permissions
        tmp.rename(final_path)
        os.chmod(final_path, 0o600)
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
