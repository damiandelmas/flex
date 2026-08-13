"""Directory walker for markdown vaults."""

import os
import time
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
    deadline: float | None = None,
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

    def walk_dir(directory: Path) -> Iterator[VaultEntry]:
        """Depth-first lexical walk without materializing the whole vault."""
        if deadline is not None and time.time() >= deadline:
            return
        try:
            # pathlib compares path components, so lexical child ordering plus
            # depth-first recursion retains ``sorted(root.rglob(...))`` order
            # while holding only one directory's entries in memory.
            children = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError:
            return
        for child in children:
            if deadline is not None and time.time() >= deadline:
                return
            path = Path(child.path)
            try:
                if child.is_dir(follow_symlinks=False):
                    relative = str(path.relative_to(root))
                    if not should_exclude(relative + '/_', dir_excludes, file_excludes):
                        yield from walk_dir(path)
                    continue
            except OSError:
                continue
            # `root` is already resolved — hand it through instead of letting
            # every file re-realpath the same directory.
            entry = entry_for_path(
                root, path, dir_excludes=dir_excludes, file_excludes=file_excludes,
                root_resolved=root,
            )
            if entry is not None:
                yield entry

    yield from walk_dir(root)


def entry_for_path(root: Path, path: Path, *, dir_excludes=None,
                   file_excludes=None, root_resolved: Path | None = None) -> VaultEntry | None:
    """Validate and describe one vault path without walking the whole vault."""
    # resolve() is a realpath: it walks EVERY component of the path. Called per
    # file over a whole vault this dominates the scan — and on a slow mount
    # (/mnt/c under WSL) it stalled the daemon's serial tick for minutes, starving
    # capture and every corpus cell behind it.
    #
    # Two costs, both avoidable without weakening the check:
    #  - `root` is invariant, so resolving it per file was pure waste (the caller
    #    now hands in the resolved root once per vault).
    #  - the walk yields paths already under `root`, so the ONLY way out of the
    #    vault is a symlink. lstat (is_symlink) is one syscall; realpath is one
    #    per component. Resolve only when there is actually a link to follow —
    #    the escape check below is unchanged for the paths that can escape.
    root = root_resolved if root_resolved is not None else root.resolve()
    if path.is_symlink():
        path = path.resolve()
    try:
        rel_path = path.relative_to(root)
    except ValueError:
        return None
    if path.suffix.lower() != '.md' or not path.is_file():
        return None
    if dir_excludes is None or file_excludes is None:
        config = _load_vault_config(root)
        dir_excludes = set(DEFAULT_DIR_EXCLUDE)
        file_excludes = list(DEFAULT_FILE_EXCLUDE)
        for pattern in config.get('exclude', []):
            if pattern.endswith('/'):
                dir_excludes.add(pattern.rstrip('/'))
            else:
                file_excludes.append(pattern)
    rel = unicodedata.normalize('NFD', str(rel_path))
    if should_exclude(rel, set(dir_excludes), list(file_excludes)):
        return None
    try:
        stat = path.stat()
    except OSError:
        return None
    folder = str(path.parent.relative_to(root)) if path.parent != root else ''
    return VaultEntry(path, rel, folder, path.stem, stat.st_mtime, stat.st_size)
