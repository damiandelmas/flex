"""
Download the public fp32 ONNX embedding model from its authoritative release.

Called by `flex init`. Model stored at ~/.flex/models/ to persist across
pip upgrades. Uses urllib only — no extra dependencies.
"""
import hashlib
import os
import sys
import urllib.request
from pathlib import Path

FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
MODEL_DIR = FLEX_HOME / "models"

# Nomic's optimized ONNX export, pinned to an exact upstream revision.
# The SHA pins are the embedding-space contract: changing either
# artifact requires an explicit model migration, never a transparent download.
MODEL_REVISION = "ac6fcd72429d86ff25c17895e47a9bfcfc50c1b2"
BASE_URL = (
    "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/"
    f"{MODEL_REVISION}"
)
MODEL_SUBDIR = "nomic-v1.5-fp32"
FILES = [
    ("model.onnx", "onnx/model.onnx",
     "147d5aa88c2101237358e17796cf3a227cead1ec304ec34b465bb08e9d952965"),
    ("tokenizer.json", "tokenizer.json",
     "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66"),
]


def model_dir() -> Path:
    """Return model directory, creating if needed."""
    dest = MODEL_DIR / MODEL_SUBDIR
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def _files_valid(directory: Path) -> bool:
    """Check all model files exist AND have correct checksums."""
    for name, _remote_path, expected_hash in FILES:
        p = directory / name
        if not p.exists():
            return False
        if _sha256(p) != expected_hash:
            return False
    return True


def model_ready() -> bool:
    """Check whether the pinned fp32 model and tokenizer are installed."""
    return _files_valid(MODEL_DIR / MODEL_SUBDIR)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _progress_hook(block_num, block_size, total_size):
    downloaded = block_num * block_size
    if total_size > 0:
        pct = min(100, downloaded * 100 // total_size)
        mb = downloaded / (1 << 20)
        total_mb = total_size / (1 << 20)
        sys.stdout.write(f"\r  downloading: {mb:.1f}/{total_mb:.1f} MB ({pct}%)")
        sys.stdout.flush()


def download_model(force: bool = False) -> Path:
    """
    Install the pinned fp32 model and tokenizer.

    Args:
        force: Re-copy/download even if files exist.

    Returns:
        Path to model directory.

    Raises:
        RuntimeError: If download fails or checksum mismatch.
    """
    dest = model_dir()

    for name, remote_path, expected_hash in FILES:
        target = dest / name
        if target.exists() and not force:
            if _sha256(target) == expected_hash:
                continue
            # Corrupt or truncated — re-download
            target.unlink(missing_ok=True)

        url = f"{BASE_URL}/{remote_path}?download=true"
        partial = target.with_suffix(target.suffix + ".part")
        print(f"  {name}")
        try:
            partial.unlink(missing_ok=True)
            urllib.request.urlretrieve(url, partial, reporthook=_progress_hook)
            print()  # newline after progress
        except Exception as e:
            partial.unlink(missing_ok=True)
            raise RuntimeError(
                f"Failed to download {name} from {url}: {e}\n"
                f"You can download manually and place in {dest}/"
            ) from e

        # Verify checksum
        actual = _sha256(partial)
        if actual != expected_hash:
            partial.unlink(missing_ok=True)
            raise RuntimeError(
                f"Checksum mismatch for {name}.\n"
                f"  expected: {expected_hash}\n"
                f"  got:      {actual}\n"
                f"Re-run 'flex init' to retry."
            )
        partial.replace(target)

    return dest
