"""
Doc-pac folder structure parser.

Walks a directory, maps folders to (path, temporal, doc_type) entries.
Any directory containing doc-pac indicator folders (changes/, current/, intended/)
is a boundary — temporal resolution resets at each boundary.
Returns flat list of indexable entries.

Temporal dimensions: past (fact), present (current truth),
future (speculation), exogenous (external knowledge).

The temporal field carries semantic time (past/present/future/exogenous).
The file_date field carries calendar time (YYMMDD or YYMMDD-HHMM).
These are different dimensions and never conflated.

Facets are NOT auto-detected. Facets are domain concepts (subsystems like
'supabase', 'appsscript') that emerge from human conversation during
pipeline creation. The init script assigns facets, not docpac.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class DocPacEntry:
    path: str                   # absolute file path
    temporal: Optional[str]     # past | present | future | exogenous | None
    doc_type: Optional[str]     # changelog | architecture | design | plan | etc
    title: str                  # human-readable title from filename
    file_date: Optional[str] = None  # YYMMDD or YYMMDD-HHMM from filename
    skip: bool = False          # True for buffer/, _raw/, _qmem/, cache/


# Folder path → (temporal, doc_type) mapping
# More specific paths listed first — longest match wins
FOLDER_MAP = {
    'changes/code':     ('past',      'changelog'),
    'changes/testing':  ('past',      'testing'),
    'changes/workflow': ('past',      'workflow'),
    'changes/states':   ('past',      'states'),
    'changes/tracking': ('past',      'tracking'),
    'changes/audits':   ('past',      'audit'),
    'changes/review':   ('past',      'review'),
    'changes/session':  ('past',      'session'),
    'current/ast':      ('present',   'ast'),
    'current':          ('present',   'architecture'),
    'intended/design':    ('future',  'design'),
    'intended/proximate': ('future',  'vision'),
    'intended/ultimate':  ('future',  'vision'),
    'intended':         ('future',    'vision'),      # fallback
    'knowledge':        ('exogenous', 'knowledge'),
    'philosophy':       ('exogenous', 'philosophy'),
    'onboard':          ('present',   'onboard'),
    'lexicon':          ('present',   'lexicon'),
    'reference':        ('present',   'reference'),
    'specs':            ('future',    'spec'),
    'slots':            ('future',    'slot'),
    'shapes':           ('future',    'shape'),
    'plans':            ('future',    'plan'),
}

SKIP_FOLDERS = {'buffer', '_raw', '_qmem', 'cache', '__pycache__', '.git'}

# Doc-pac indicator folders — if a dir contains these, it's a nested doc-pac
DOCPAC_INDICATORS = {'changes', 'current', 'intended'}

# Temporal pattern: YYMMDD or YYMMDD-HHMM
TEMPORAL_RE = re.compile(r'^(\d{6})(?:-(\d{4}))?')

# Pre-sorted for specificity (longest key first)
_SORTED_KEYS = sorted(FOLDER_MAP.keys(), key=lambda k: -len(k))


def parse_docpac(root, pattern: str = '**/*.md') -> list[DocPacEntry]:
    """
    Walk a doc-pac directory, return flat list of indexable entries.

    Any directory containing indicator folders (changes/, current/, intended/)
    is a doc-pac boundary. Temporal resolution happens relative to the
    innermost boundary, not the top-level root. Frame resets at boundaries.

    Facets are NOT assigned here. Facets are domain concepts (subsystems)
    that emerge from human-AI conversation during pipeline creation.
    The init script assigns facets to chunks, not docpac.

    Args:
        root: Root directory of the doc-pac
        pattern: Glob pattern for files

    Returns:
        List of DocPacEntry
    """
    root = Path(root)

    if not root.exists():
        return []

    entries = []

    for filepath in sorted(root.rglob(pattern)):
        if filepath.is_dir():
            continue

        # Skip files in skip folders
        if _in_skip_folder(filepath, root):
            entries.append(DocPacEntry(
                path=str(filepath),
                temporal=None,
                doc_type='skip',
                title=_extract_title(filepath.name),
                skip=True,
            ))
            continue

        # Find innermost doc-pac boundary
        boundary = _find_boundary(filepath, root)

        # Infer temporal + doc_type relative to boundary
        temporal, doc_type = _infer_from_path(filepath, boundary)

        # Calendar date from filename (separate from semantic temporal)
        file_date = _extract_file_date(filepath.name)

        entries.append(DocPacEntry(
            path=str(filepath),
            temporal=temporal,
            doc_type=doc_type,
            title=_extract_title(filepath.name),
            file_date=file_date,
        ))

    return entries


def parse_docpac_file(filepath, root) -> DocPacEntry:
    """
    Classify a single file without walking the corpus.

    Same logic as parse_docpac but O(depth) instead of O(N).
    Used by the live index worker for incremental updates.
    """
    filepath = Path(filepath)
    root = Path(root)

    if _in_skip_folder(filepath, root):
        return DocPacEntry(
            path=str(filepath), temporal=None, doc_type='skip',
            title=_extract_title(filepath.name), skip=True)

    boundary = _find_boundary(filepath, root)
    temporal, doc_type = _infer_from_path(filepath, boundary)
    file_date = _extract_file_date(filepath.name)

    return DocPacEntry(
        path=str(filepath), temporal=temporal, doc_type=doc_type,
        title=_extract_title(filepath.name), file_date=file_date)


def _in_skip_folder(filepath: Path, root: Path) -> bool:
    """Check if file is under a skip folder."""
    try:
        relative = filepath.relative_to(root)
    except ValueError:
        return False
    return any(part in SKIP_FOLDERS for part in relative.parts)


def _find_boundary(filepath: Path, root: Path) -> Path:
    """
    Walk from file toward root, find the innermost doc-pac boundary.

    A boundary is any directory that contains doc-pac indicator folders
    (changes/, current/, intended/). The innermost one wins.

    Returns:
        boundary_path
    """
    # Walk parent directories from file toward root
    # Start from file's parent, stop before root
    current = filepath.parent
    innermost = root

    # Collect all ancestors between file and root (exclusive of root)
    ancestors = []
    while current != root and current != current.parent:
        ancestors.append(current)
        current = current.parent

    # Check innermost first (closest to file)
    for ancestor in ancestors:
        if _is_docpac(ancestor):
            innermost = ancestor
            break  # innermost wins

    return innermost


def _is_docpac(directory: Path) -> bool:
    """Check if a directory looks like a doc-pac (has indicator folders)."""
    try:
        children = {p.name for p in directory.iterdir() if p.is_dir()}
    except PermissionError:
        return False
    return bool(children & DOCPAC_INDICATORS)


def _infer_from_path(filepath: Path, boundary: Path) -> tuple[Optional[str], Optional[str]]:
    """
    Infer (temporal, doc_type) from folder path relative to boundary.

    Uses deepest-match rule: the match closest to the file wins.
    This respects recursive doc-pac nesting — a slots/ folder inside
    a plans/ folder resolves as 'slot', not 'plan'.

    Resolution is relative to the boundary, not the top-level root.
    """
    try:
        relative = filepath.relative_to(boundary)
    except ValueError:
        return None, None

    # Build the folder path (exclude filename)
    folder_parts = relative.parts[:-1]
    if not folder_parts:
        return None, None

    folder_path = '/'.join(folder_parts).lower()
    path_parts = folder_path.split('/')

    # Find all matches, keep the deepest (rightmost / closest to file)
    best_match = None
    best_position = -1

    for key in _SORTED_KEYS:
        key_parts = key.split('/')
        for i in range(len(path_parts) - len(key_parts) + 1):
            if path_parts[i:i + len(key_parts)] == key_parts:
                match_end = i + len(key_parts)
                if match_end > best_position or (match_end == best_position and len(key_parts) > len(best_match.split('/'))):
                    best_match = key
                    best_position = match_end
                break  # only need first occurrence per key

    if best_match:
        return FOLDER_MAP[best_match]

    return None, None


def _extract_file_date(filename: str) -> Optional[str]:
    """Extract YYMMDD or YYMMDD-HHMM from filename. Calendar time, not semantic temporal."""
    match = TEMPORAL_RE.match(filename)
    if match:
        date = match.group(1)
        time = match.group(2)
        return f"{date}-{time}" if time is not None else date
    return None


def _extract_title(filename: str) -> str:
    """Extract human-readable title from filename."""
    name = Path(filename).stem

    # Strip temporal prefix: YYMMDD-HHMM_ or YYMMDD_
    name = re.sub(r'^\d{6}(?:-\d{4})?_?', '', name)

    # Convert hyphens/underscores to spaces
    name = name.replace('-', ' ').replace('_', ' ')

    return name.strip() or filename
