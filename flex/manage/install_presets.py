"""
Install .sql preset files into cell _presets tables.

Infrastructure utility — not a domain operation. Bakes filesystem presets
(the authoring format) into cell databases (the runtime source).

Usage:
    python -m flex.manage.install_presets           # all cells
    python -m flex.manage.install_presets claude_code  # specific cell
"""
import sqlite3
import sys
from pathlib import Path

from flex.retrieve.presets import install_presets
from flex.registry import resolve_cell, list_cells

# Preset source directories
PRESET_ROOT = Path(__file__).resolve().parent.parent / "retrieve" / "presets"
GENERAL_DIR = PRESET_ROOT / "general"

# Module-specific preset directories (keyed by cell_type from registry)
MODULE_ROOT = Path(__file__).resolve().parent.parent / "modules"
MODULE_PRESETS = {
    'claude-code': [
        MODULE_ROOT / "claude_code" / "stock" / "presets",
        MODULE_ROOT / "soma"        / "stock" / "presets",
    ],
}


def _preset_dirs_for(cell_type: str | None) -> list[Path]:
    """Return preset directories for a cell type. General + module-specific."""
    dirs = [GENERAL_DIR]
    if cell_type and cell_type in MODULE_PRESETS:
        dirs.extend(MODULE_PRESETS[cell_type])
    return dirs


def install_cell(cell_name: str, preset_dirs: list[Path] = None):
    """Install presets into a single cell.

    Args:
        cell_name: Name of the cell (resolved via registry).
        preset_dirs: Override preset directories. If None, auto-detected from cell_type.
    """
    db_path = resolve_cell(cell_name)
    if db_path is None or not db_path.exists():
        print(f"  {cell_name}: SKIP (not found)")
        return

    if preset_dirs is None:
        # Detect cell_type from registry
        cell_type = None
        for cell in list_cells():
            if cell['name'] == cell_name:
                cell_type = cell.get('cell_type')
                break
        preset_dirs = _preset_dirs_for(cell_type)

    try:
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")

        # Ensure table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _presets (
                name TEXT PRIMARY KEY,
                description TEXT,
                params TEXT DEFAULT '',
                sql TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE _presets ADD COLUMN params TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass

        # Wipe existing presets — ensures orphans from cell_type changes are removed
        conn.execute("DELETE FROM _presets")

        for pd in preset_dirs:
            if pd.exists():
                install_presets(conn, pd)

        conn.close()
    except sqlite3.OperationalError as e:
        print(f"  {cell_name}: LOCKED ({e}) — retry after stopping flex-worker")


def install_all():
    """Install presets into all registered cells."""
    print("Installing presets...")
    cells = list_cells()
    if not cells:
        print("  No cells registered. Run 'flex init' first.")
        return
    for cell in cells:
        install_cell(cell['name'])
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for name in sys.argv[1:]:
            install_cell(name)
    else:
        install_all()
