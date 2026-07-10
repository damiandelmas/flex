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
    temporal: Optional[str]     # past|present|future|exogenous (derived from category)
    doc_type: Optional[str]     # compat: "category.subtype" (e.g. change.code)
    title: str                  # human-readable title from filename
    file_date: Optional[str] = None  # YYMMDD or YYMMDD-HHMM from filename
    skip: bool = False          # True for buffer/, _raw/, cache/
    category: Optional[str] = None   # change|current|intended|external (coordinate axis)
    subtype: Optional[str] = None    # code|design|architecture|… (coordinate axis)


# Category → temporal. Code-level derivation ONLY (keeps the grammar JSON to the
# category × subtype coordinate; temporal never appears there). category carries
# the temporal sense; this fills the legacy `temporal` column for back-compat.
CATEGORY_TEMPORAL = {
    'change':   'past',
    'current':  'present',
    'intended': 'future',
    'external': 'exogenous',
}

# Folder path → (category, subtype) COORDINATE.
#   temporal = CATEGORY_TEMPORAL[category]      (derived, not stored here)
#   doc_type = "category.subtype"               (compat string)
# subtype is open vocabulary; category is the fixed temporal-bearing set.
# `level` is the reserved next axis. Plan support folders (slots/specs/shapes)
# are handled separately (subordinate to a plan packet, not top-level lanes).
FOLDER_MAP = {
    'changes/code':             ('change',   'code'),
    'changes/design':           ('change',   'design'),
    'changes/analysis':         ('change',   'analysis'),
    'changes/features':         ('change',   'features'),
    'changes/testing':          ('change',   'testing'),
    'changes/workflow':         ('change',   'workflow'),
    # Universal lanes (states/tracks recur per scope). 'tracks' is canonical
    # (the context-changes-tracks skill writes changes/tracks/), distinct from
    # the system-wide 'traces' concept.
    'changes/states':           ('change',   'states'),
    'changes/tracks':           ('change',   'tracks'),
    # creative is an OBJECT (artifact), parity with code — NOT the design stage.
    'changes/creative':         ('change',   'creative'),
    # Activities that ARE analysis collapse to it.
    'changes/audits':           ('change',   'analysis'),
    'changes/review':           ('change',   'analysis'),
    # Legacy spellings → canonical subtype.
    'changes/tracking':         ('change',   'tracks'),
    'changes/session':          ('change',   'tracks'),
    'changes':                  ('change',   'changelog'),
    'current/architecture/ast': ('current',  'ast'),
    'current/ast':              ('current',  'ast'),   # pre-0.50 folder convention
    'current/architecture':     ('current',  'architecture'),
    'current':                  ('current',  'architecture'),
    'intended/architecture':    ('intended', 'architecture'),
    'intended/design':          ('intended', 'design'),
    'intended/proximate':       ('intended', 'proximate'),
    'intended/ultimate':        ('intended', 'ultimate'),
    'intended':                 ('intended', 'design'),
    'plans':                    ('intended', 'plan'),
    'issues':                   ('change',   'issue'),
    'logs':                     ('change',   'log'),
    'archive':                  ('change',   'archive'),
    'work':                     ('change',   'work'),
    # external = exogenous "guiding lights" + outside knowledge.
    'vision':                   ('external', 'vision'),
    'philosophy':               ('external', 'philosophy'),
    'onboard':                  ('external', 'onboard'),
    'knowledge':                ('external', 'knowledge'),
    'lexicon':                  ('current',  'lexicon'),
    'reference':                ('current',  'reference'),
}

# Plan support folders are REFERENCES inside a plan packet, not peers in any
# category. category=None keeps them OUT of category/temporal sweeps (a search
# for 'intended' must return plans, not their slot/spec fragments); they surface
# only through their parent plan — the parent edge is the reserved tree axis.
# doc_type stays the flat subtype ('slot'/'spec'/'shape').
PLAN_SUBFOLDER_MAP = {
    'slots':  (None, 'slot'),
    'specs':  (None, 'spec'),
    'shapes': (None, 'shape'),
}


def coordinate_to_legacy(category, subtype):
    """(category, subtype) → (temporal, doc_type) compat. temporal derives from
    category; doc_type is the compound 'category.subtype' string."""
    if category is None and subtype is None:
        return None, None
    temporal = CATEGORY_TEMPORAL.get(category)
    doc_type = f"{category}.{subtype}" if (category and subtype) else (category or subtype)
    return temporal, doc_type

SKIP_FOLDERS = {'buffer', '_raw', 'cache', '__pycache__', '.git'}

# Doc-pac indicator folders — if a dir contains these, it's a nested doc-pac
DOCPAC_INDICATORS = {'changes', 'current', 'intended'}

# Temporal pattern: YYMMDD or YYMMDD-HHMM
TEMPORAL_RE = re.compile(r'^(\d{6})(?:-(\d{4}))?')

# Pre-sorted for specificity (longest key first)
_SORTED_KEYS = sorted(FOLDER_MAP.keys(), key=lambda k: -len(k))


def _resolve_config(config):
    """Resolve the active grammar maps. config=None → module constants (==
    docpac-v1), so the default path is byte-identical. A config dict (from
    context_config.load_context_config) overrides per-node."""
    if config:
        folder_map = config['folder_map']
        return (folder_map, config['plan_subfolder_map'],
                config['skip_folders'], config['indicators'],
                sorted(folder_map.keys(), key=lambda k: -len(k)))
    return (FOLDER_MAP, PLAN_SUBFOLDER_MAP, SKIP_FOLDERS, DOCPAC_INDICATORS, _SORTED_KEYS)


def parse_docpac(root, pattern: str = '**/*.md', config: dict = None) -> list[DocPacEntry]:
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

    folder_map, plan_map, skip, indicators, sorted_keys = _resolve_config(config)
    entries = []

    for filepath in sorted(root.rglob(pattern)):
        if filepath.is_dir():
            continue

        # Skip files in skip folders
        if _in_skip_folder(filepath, root, skip):
            entries.append(DocPacEntry(
                path=str(filepath),
                temporal=None,
                doc_type='skip',
                title=_extract_title(filepath.name),
                skip=True,
            ))
            continue

        # Find innermost doc-pac boundary
        boundary = _find_boundary(filepath, root, indicators)

        # Infer the (category, subtype) coordinate relative to boundary;
        # derive temporal + compat doc_type from it.
        category, subtype = _infer_from_path(filepath, boundary, folder_map, plan_map, sorted_keys)
        temporal, doc_type = coordinate_to_legacy(category, subtype)

        # Calendar date from filename (separate from semantic temporal)
        file_date = _extract_file_date(filepath.name)

        entries.append(DocPacEntry(
            path=str(filepath),
            temporal=temporal,
            doc_type=doc_type,
            title=_extract_title(filepath.name),
            file_date=file_date,
            category=category,
            subtype=subtype,
        ))

    return entries


def parse_docpac_file(filepath, root, config: dict = None) -> DocPacEntry:
    """
    Classify a single file without walking the corpus.

    Same logic as parse_docpac but O(depth) instead of O(N).
    Used by the live index worker for incremental updates.
    """
    filepath = Path(filepath)
    root = Path(root)
    folder_map, plan_map, skip, indicators, sorted_keys = _resolve_config(config)

    if _in_skip_folder(filepath, root, skip):
        return DocPacEntry(
            path=str(filepath), temporal=None, doc_type='skip',
            title=_extract_title(filepath.name), skip=True)

    boundary = _find_boundary(filepath, root, indicators)
    category, subtype = _infer_from_path(filepath, boundary, folder_map, plan_map, sorted_keys)
    temporal, doc_type = coordinate_to_legacy(category, subtype)
    file_date = _extract_file_date(filepath.name)

    return DocPacEntry(
        path=str(filepath), temporal=temporal, doc_type=doc_type,
        title=_extract_title(filepath.name), file_date=file_date,
        category=category, subtype=subtype)


def _in_skip_folder(filepath: Path, root: Path, skip=SKIP_FOLDERS) -> bool:
    """Check if file is under a skip folder."""
    try:
        relative = filepath.relative_to(root)
    except ValueError:
        return False
    return any(part in skip for part in relative.parts)


def _find_boundary(filepath: Path, root: Path, indicators=DOCPAC_INDICATORS) -> Path:
    """
    Walk from file toward root, find the innermost doc-pac boundary.

    A boundary is any directory that contains doc-pac indicator folders
    (changes/, current/, intended/). The innermost one wins.

    Returns:
        boundary_path
    """
    try:
        relative = filepath.relative_to(root)
    except ValueError:
        relative = None
    if relative is not None:
        parts = relative.parts
        if parts and parts[0] == 'archive':
            return root
        if len(parts) >= 2 and parts[0] == 'current' and parts[1] == 'old':
            return root

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
        if _is_docpac(ancestor, indicators):
            innermost = ancestor
            break  # innermost wins

    return innermost


def _is_docpac(directory: Path, indicators=DOCPAC_INDICATORS) -> bool:
    """Check if a directory looks like a doc-pac (has indicator folders)."""
    try:
        children = {p.name for p in directory.iterdir() if p.is_dir()}
    except PermissionError:
        return False
    return bool(children & set(indicators))


def _infer_from_path(filepath: Path, boundary: Path,
                     folder_map=FOLDER_MAP, plan_map=PLAN_SUBFOLDER_MAP,
                     sorted_keys=_SORTED_KEYS) -> tuple[Optional[str], Optional[str]]:
    """
    Infer (temporal, doc_type) from folder path relative to boundary.

    Uses hard history guards first, then plan-subfolder support, then the
    deepest-match rule: the match closest to the file wins.

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

    history = _infer_history_guard(path_parts)
    if history is not None:
        return history

    subordinate = _infer_plan_subfolder(path_parts, plan_map)
    if subordinate is not None:
        return subordinate

    # Find all matches, keep the deepest (rightmost / closest to file)
    best_match = None
    best_position = -1

    for key in sorted_keys:
        key_parts = key.split('/')
        for i in range(len(path_parts) - len(key_parts) + 1):
            if path_parts[i:i + len(key_parts)] == key_parts:
                match_end = i + len(key_parts)
                if match_end > best_position or (match_end == best_position and len(key_parts) > len(best_match.split('/'))):
                    best_match = key
                    best_position = match_end
                break  # only need first occurrence per key

    if best_match:
        return folder_map[best_match]

    return None, None


def _infer_history_guard(path_parts: list[str]) -> Optional[tuple[str, str]]:
    """Archive lanes stay archive (category=change) even if they contain
    current/intended folders. Returns a (category, subtype) coordinate."""
    if not path_parts:
        return None
    if path_parts[0] == 'archive':
        return 'change', 'archive'
    if len(path_parts) >= 2 and path_parts[0] == 'current' and path_parts[1] == 'old':
        return 'change', 'archive'
    return None


def _infer_plan_subfolder(path_parts: list[str],
                          plan_map=PLAN_SUBFOLDER_MAP) -> Optional[tuple[str, str]]:
    """Classify plan support folders only when they are under plans/.

    `slots/`, `specs/`, and `shapes/` are not top-level context lanes. They are
    subordinate reference folders inside plan packets, so `plans/x/slots/y.md`
    can resolve as a slot while `slots/y.md` remains unmapped.
    """
    try:
        plans_index = path_parts.index('plans')
    except ValueError:
        return None

    best = None
    best_index = -1
    for i in range(plans_index + 1, len(path_parts)):
        part = path_parts[i]
        if part in plan_map and i > best_index:
            best = plan_map[part]
            best_index = i
    return best


def git_creation_date(file_path: str) -> Optional[str]:
    """First-commit date of a file (YYMMDD-HHMM), discovered from the file's own
    directory so an ancestor repo (e.g. ~/notes) is found. None if untracked
    or git is unavailable."""
    import os
    import subprocess
    from datetime import datetime
    try:
        r = subprocess.run(
            ['git', 'log', '--follow', '--format=%at', '--diff-filter=A', '--', file_path],
            capture_output=True, text=True, timeout=5,
            cwd=os.path.dirname(file_path) or '.')
        if r.returncode == 0 and r.stdout.strip():
            ts = int(r.stdout.strip().split('\n')[-1])  # earliest = last line
            return datetime.fromtimestamp(ts).strftime('%y%m%d-%H%M')
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError, OSError):
        pass
    return None


def mtime_date(file_path: str) -> Optional[str]:
    """Filesystem mtime as YYMMDD-HHMM (coarse: reflects last write/sync)."""
    import os
    from datetime import datetime
    try:
        return datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%y%m%d-%H%M')
    except OSError:
        return None


def resolve_file_date(filename_date: Optional[str], file_path: str,
                      existing: Optional[str] = None) -> Optional[str]:
    """Best available date — fill-only and stable:
        filename date (authoritative; changelogs) > existing stored date (no
        drift on edit) > git first-commit (if tracked) > mtime (coarse fallback).
    Passing `existing` (the date already on the row) prevents a content edit from
    drifting a previously-resolved date forward to the new mtime."""
    return filename_date or existing or git_creation_date(file_path) or mtime_date(file_path)


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
