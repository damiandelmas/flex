"""
Unified markdown-family compile entry — point it at a path, get a cell.

    python -m flex.modules.markdown.compile.auto <path> [name] [--flavor F]

Auto-detects what <path> is and dispatches to the right backend:

    <path> with .obsidian/                 → obsidian   (wikilinks, dataview)
    <path> with docpac indicators / context.json
                                           → docpac     (category × subtype coordinate)
    <path> otherwise                       → markdown   (plain)

`--flavor {markdown,obsidian,docpac}` overrides detection.
`name` defaults from the path (`<dir>` or `<parent>-context` for a `.context`).
"""
import argparse
import subprocess
import sys
from pathlib import Path

from flex.modules.markdown.compile.init import compile_vault

# A directory carrying any of these is a docpac corpus (temporal/posture lanes).
DOCPAC_INDICATORS = {'changes', 'current', 'intended'}


def _derive_name(p: Path) -> str:
    return f"{p.parent.name}-context" if p.name == '.context' else p.name


def detect(path) -> str:
    """Resolve a path to one of: obsidian | docpac | markdown."""
    p = Path(path)
    if (p / '.obsidian').is_dir():
        return 'obsidian'
    if (p / 'context.json').exists():
        return 'docpac'
    try:
        subdirs = {c.name for c in p.iterdir() if c.is_dir()}
    except OSError:
        subdirs = set()
    if subdirs & DOCPAC_INDICATORS:
        return 'docpac'
    return 'markdown'


def compile_path(path, name=None, flavor=None) -> dict:
    """Compile any markdown-family path into a cell. Returns {flavor, name, …}."""
    p = Path(path).resolve()
    name = name or _derive_name(p)
    flavor = flavor or detect(p)

    if flavor == 'docpac':
        # docpac's own pipeline owns the coordinate + sidecar + file_date backfill.
        subprocess.run(
            [sys.executable, '-m', 'flex.modules.docpac.compile.init', str(p)],
            check=True)
        return {'flavor': 'docpac', 'name': name, 'path': str(p)}

    # markdown / obsidian — compile_vault keys behaviour off cell_type.
    compile_vault(p, name, cell_type=flavor)
    return {'flavor': flavor, 'name': name, 'path': str(p)}


def main():
    ap = argparse.ArgumentParser(description="Compile a markdown-family path into a cell")
    ap.add_argument('path')
    ap.add_argument('name', nargs='?')
    ap.add_argument('--flavor', choices=['markdown', 'obsidian', 'docpac'])
    ap.add_argument('--detect-only', action='store_true', help="print detected flavor, don't compile")
    a = ap.parse_args()
    if a.detect_only:
        print(detect(a.path)); return
    print(compile_path(a.path, a.name, a.flavor))


if __name__ == '__main__':
    main()
