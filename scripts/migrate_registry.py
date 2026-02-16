#!/usr/bin/env python3
"""Seed cell registry with existing cells on disk.

One-shot: scans ~/.qmem/cells/projects/, registers each cell.
Auto-detects cell_type and description from each cell's schema and _meta.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flexsearch.registry import CELLS_ROOT, register_cell


def main():
    if not CELLS_ROOT.exists():
        print(f"CELLS_ROOT not found: {CELLS_ROOT}")
        sys.exit(1)

    cells = sorted(
        d for d in CELLS_ROOT.iterdir()
        if d.is_dir() and (d / "main.db").exists()
    )

    print(f"Found {len(cells)} cells at {CELLS_ROOT}\n")
    for cell_dir in cells:
        name = cell_dir.name
        db_path = cell_dir / "main.db"
        # register_cell auto-detects cell_type and description
        register_cell(name, db_path)
        print(f"  {name}")

    print(f"\nDone. Registry at ~/.flexsearch/registry.db")


if __name__ == '__main__':
    main()
