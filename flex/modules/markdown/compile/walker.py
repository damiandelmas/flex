"""Directory walker for markdown vaults."""

import unicodedata
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterator

DEFAULT_DIR_EXCLUDE = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    ".obsidian", ".trash", ".cache", ".DS_Store", "Templates",
}

DEFAULT_FILE_EXCLUDE = [
    "*.conflict*",
    "*.sync-conflict-*",
]


@dataclass
class VaultEntry:
    path: Path          # absolute path
    rel_path: str       # relative to root, NFD-normalized
    folder: str         # parent directory relative to root
    stem: str           # filename without extension
    mtime: float        # last modified timestamp
    size: int           # file size in bytes


def _load_vault_config(vault_root: Path) -> dict:
    """Load .flexrc from vault root if it exists."""
    rc_path = vault_root / '.flexrc'
    if not rc_path.exists():
        return {}
    try:
        import yaml
        return yaml.safe_load(rc_path.read_text()) or {}
    except Exception:
        return {}


def should_exclude(rel_path: str, dir_excludes: set, file_excludes: list) -> bool:
    """Check if a path should be excluded from indexing."""
    parts = rel_path.split('/')

    # Directory check: any path component matches
    for part in parts[:-1]:
        if part in dir_excludes:
            return True

    # File pattern check: fnmatch on full path and basename
    basename = parts[-1] if parts else rel_path
    for pattern in file_excludes:
        if fnmatch(rel_path, pattern) or fnmatch(basename, pattern):
            return True

    return False


def walk_vault(
    root: Path,
    exclude: list[str] | None = None,
) -> Iterator[VaultEntry]:
    """Yield VaultEntry for each .md file under root.

    All rel_path values are NFD-normalized for cross-platform consistency
    (macOS uses NFD, Linux uses NFC).
    """
    root = root.resolve()
    config = _load_vault_config(root)

    dir_excludes = set(DEFAULT_DIR_EXCLUDE)
    file_excludes = list(DEFAULT_FILE_EXCLUDE)

    # Merge .flexrc excludes
    for pattern in config.get('exclude', []):
        if pattern.endswith('/'):
            dir_excludes.add(pattern.rstrip('/'))
        else:
            file_excludes.append(pattern)

    # Merge CLI excludes
    if exclude:
        for pattern in exclude:
            if pattern.endswith('/'):
                dir_excludes.add(pattern.rstrip('/'))
            else:
                file_excludes.append(pattern)

    for f in sorted(root.rglob('*.md')):
        if not f.is_file():
            continue

        rel = unicodedata.normalize('NFD', str(f.relative_to(root)))

        if should_exclude(rel, dir_excludes, file_excludes):
            continue

        stat = f.stat()
        folder = str(f.parent.relative_to(root)) if f.parent != root else ''

        yield VaultEntry(
            path=f,
            rel_path=rel,
            folder=folder,
            stem=f.stem,
            mtime=stat.st_mtime,
            size=stat.st_size,
        )
