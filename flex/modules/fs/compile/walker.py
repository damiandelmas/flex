"""Deterministic, pruned discovery for mixed filesystem cells."""

from __future__ import annotations

import fnmatch
import os
import unicodedata
from dataclasses import dataclass
from pathlib import Path


MARKDOWN_EXTENSIONS = {".md", ".markdown"}
CODE_EXTENSIONS = {".py", ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs"}
TEXT_EXTENSIONS = {
    ".txt", ".rst", ".mdx", ".json", ".jsonl", ".yaml", ".yml", ".toml",
    ".ini", ".cfg", ".conf", ".csv", ".tsv", ".xml", ".html", ".htm",
    ".css", ".scss", ".sql", ".sh", ".bash", ".zsh", ".fish", ".env",
    ".properties", ".graphql", ".gql", ".proto", ".dockerfile",
}

DEFAULT_PRUNE_DIRS = {
    ".git", ".hg", ".svn", ".obsidian", ".trash", ".cache", ".idea",
    ".vscode", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".tox",
    ".next", ".nuxt", ".turbo", ".svelte-kit", "__pycache__", "node_modules",
    "venv", ".venv", "env", "site-packages", "build", "dist", "out", "target",
    "coverage", "htmlcov",
}
CONFIG_FILENAMES = {".flexchunk.json", ".flexpresets.json", ".flexrc"}
DEFAULT_MAX_FILE_BYTES = 1_000_000


@dataclass(frozen=True)
class FileEntry:
    root: Path
    path: Path
    rel_path: str
    file_kind: str
    size_bytes: int
    mtime_ns: int

    @property
    def source_id(self) -> str:
        return self.rel_path


def _kind_for(path: Path, size: int, max_file_bytes: int) -> str | None:
    if path.name in CONFIG_FILENAMES or size > max_file_bytes:
        return None
    suffix = path.suffix.lower()
    if suffix in MARKDOWN_EXTENSIONS:
        return "markdown"
    if suffix in CODE_EXTENSIONS:
        return "code"
    if suffix in TEXT_EXTENSIONS or path.name.lower() in {"dockerfile", "makefile"}:
        return "text"
    try:
        sample = path.read_bytes()[:8192]
    except OSError:
        return None
    if b"\x00" in sample:
        return None
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return "text" if sample or size == 0 else None


def _excluded(rel: str, patterns: tuple[str, ...]) -> bool:
    name = rel.rsplit("/", 1)[-1]
    return any(fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern)
               for pattern in patterns)


def entry_for_path(root: Path, path: Path, *, exclude: tuple[str, ...] = (),
                   max_file_bytes: int = DEFAULT_MAX_FILE_BYTES) -> FileEntry | None:
    """Validate one path against the same containment and type policy as the walk."""
    root = Path(root).expanduser().resolve()
    candidate = Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
        stat = resolved.stat()
    except (OSError, ValueError):
        return None
    if not resolved.is_file() or candidate.is_symlink():
        return None
    rel = unicodedata.normalize("NFD", resolved.relative_to(root).as_posix())
    if any(part in DEFAULT_PRUNE_DIRS for part in Path(rel).parts[:-1]):
        return None
    if _excluded(rel, tuple(exclude)):
        return None
    kind = _kind_for(resolved, stat.st_size, max_file_bytes)
    if kind is None:
        return None
    return FileEntry(root, resolved, rel, kind, stat.st_size, stat.st_mtime_ns)


def walk_files(root: Path, *, exclude: tuple[str, ...] = (),
               max_file_bytes: int = DEFAULT_MAX_FILE_BYTES):
    """Yield supported files in stable root-relative order without following links."""
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"filesystem root is not a readable directory: {root}")
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(dirpath)
        dirnames[:] = sorted(
            name for name in dirnames
            if name not in DEFAULT_PRUNE_DIRS and not (base / name).is_symlink()
        )
        for name in sorted(filenames):
            entry = entry_for_path(
                root, base / name, exclude=tuple(exclude), max_file_bytes=max_file_bytes,
            )
            if entry is not None:
                yield entry
