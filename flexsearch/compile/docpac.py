"""
Doc-pac folder structure parser.

Walks a directory, maps folders to (path, temporal, doc_type, facet) entries.
Detects recursive plans (intended/proximate/{name}/ IS a doc-pac) and recurses.
Returns flat list of indexable entries.

Replaces every hardcoded CANONICAL_PATHS in cell init scripts.

Temporal dimensions: past (fact), present (current truth),
future (speculation), exogenous (external knowledge).
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class DocPacEntry:
    path: str           # absolute file path
    temporal: str       # past | present | future | exogenous | None
    doc_type: str       # changelog | architecture | design | plan | etc
    facet: Optional[str]  # subsystem or plan name (None = cross-cutting)
    title: str          # human-readable title from filename
    skip: bool = False  # True for buffer/, _raw/, _qmem/, cache/


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
    'changes/design':   ('future',    'design'),     # NOTE: future, not past
    'changes/session':  ('past',      'session'),
    'current':          ('present',   'architecture'),
    'intended/proximate': ('future',  'plan'),
    'intended/ultimate':  ('future',  'vision'),
    'intended':         ('future',    'plan'),        # fallback
    'knowledge':        ('exogenous', 'knowledge'),
    'philosophy':       ('exogenous', 'philosophy'),
    'onboard':          ('present',   'onboard'),
    'lexicon':          ('present',   'lexicon'),
    'reference':        ('present',   'reference'),
    'specs':            ('future',    'spec'),
    'slots':            ('future',    'slot'),
    'plans':            ('future',    'plan'),
}

SKIP_FOLDERS = {'buffer', '_raw', '_qmem', 'cache', '__pycache__', '.git'}

# Doc-pac indicator folders — if a dir contains these, it's a nested doc-pac
DOCPAC_INDICATORS = {'changes', 'current', 'intended'}

# Temporal pattern: YYMMDD or YYMMDD-HHMM
TEMPORAL_RE = re.compile(r'^(\d{6})(?:-(\d{4}))?')


def parse_docpac(root, facet: str = None,
                 pattern: str = '**/*.md') -> list[DocPacEntry]:
    """
    Walk a doc-pac directory, return flat list of indexable entries.

    Recursion rule: If intended/proximate/{name}/ contains doc-pac
    indicator folders (changes/, current/, intended/), recurse with
    facet={name}.

    Args:
        root: Root directory of the doc-pac
        facet: Facet name. Defaults to root dir name.
        pattern: Glob pattern for files

    Returns:
        List of DocPacEntry
    """
    root = Path(root)
    facet = facet or root.name

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
                facet=facet,
                title=_extract_title(filepath.name),
                skip=True,
            ))
            continue

        # Detect nested doc-pac facet
        entry_facet = _detect_nested_facet(filepath, root, facet)

        # Infer temporal + doc_type from folder path
        temporal, doc_type = _infer_from_path(filepath, root)

        entries.append(DocPacEntry(
            path=str(filepath),
            temporal=temporal,
            doc_type=doc_type,
            facet=entry_facet,
            title=_extract_title(filepath.name),
        ))

    return entries


def _in_skip_folder(filepath: Path, root: Path) -> bool:
    """Check if file is under a skip folder."""
    try:
        relative = filepath.relative_to(root)
    except ValueError:
        return False
    return any(part in SKIP_FOLDERS for part in relative.parts)


def _detect_nested_facet(filepath: Path, root: Path,
                         default_facet: str) -> str:
    """
    Detect if file is inside a nested doc-pac under intended/proximate/{name}/.

    If {name}/ contains doc-pac indicator folders, use {name} as facet.
    """
    try:
        relative = filepath.relative_to(root)
    except ValueError:
        return default_facet

    parts = relative.parts
    # Look for intended/proximate/{name}/... pattern
    for i in range(len(parts) - 1):
        if parts[i] == 'proximate' and i > 0 and parts[i - 1] == 'intended':
            if i + 1 < len(parts):
                candidate_name = parts[i + 1]
                candidate_dir = root / '/'.join(parts[:i + 2])
                if candidate_dir.is_dir() and _is_docpac(candidate_dir):
                    return candidate_name

    return default_facet


def _is_docpac(directory: Path) -> bool:
    """Check if a directory looks like a doc-pac (has indicator folders)."""
    children = {p.name for p in directory.iterdir() if p.is_dir()}
    return bool(children & DOCPAC_INDICATORS)


def _infer_from_path(filepath: Path, root: Path) -> tuple[Optional[str], Optional[str]]:
    """
    Infer (temporal, doc_type) from folder path.

    Uses specificity rule: longest matching path wins.
    """
    try:
        relative = filepath.relative_to(root)
    except ValueError:
        return None, None

    # Build the folder path (exclude filename)
    folder_parts = relative.parts[:-1]
    if not folder_parts:
        return None, None

    # Try progressively shorter folder paths (longest match wins)
    # Sort FOLDER_MAP keys by length descending for specificity
    sorted_keys = sorted(FOLDER_MAP.keys(), key=lambda k: -len(k))

    folder_path = '/'.join(folder_parts).lower()

    for key in sorted_keys:
        if key in folder_path:
            return FOLDER_MAP[key]

    return None, None


def _extract_temporal(filename: str) -> Optional[str]:
    """Extract YYMMDD or YYMMDD-HHMM from filename."""
    match = TEMPORAL_RE.match(filename)
    if match:
        date = match.group(1)
        time = match.group(2)
        return f"{date}-{time}" if time else date
    return None


def _extract_title(filename: str) -> str:
    """Extract human-readable title from filename."""
    name = Path(filename).stem

    # Strip temporal prefix: YYMMDD-HHMM_ or YYMMDD_
    name = re.sub(r'^\d{6}(?:-\d{4})?_?', '', name)

    # Convert hyphens/underscores to spaces
    name = name.replace('-', ' ').replace('_', ' ')

    return name.strip() or filename
