"""
Install .sql preset files into cell _presets tables.

Infrastructure utility — not a domain operation. Bakes filesystem presets
(the authoring format) into cell databases (the runtime source).

Usage:
    python -m flexsearch.utils.install_presets           # all cells
    python -m flexsearch.utils.install_presets claude_code  # specific cell
"""
import sqlite3
import sys
from pathlib import Path

from flexsearch.retrieve.presets import install_presets

# Preset source directories
PRESET_ROOT = Path(__file__).resolve().parent.parent / "retrieve" / "presets"
GENERAL_DIR = PRESET_ROOT / "general"

# Module-specific preset directories
MODULE_ROOT = Path(__file__).resolve().parent.parent / "modules"
CLAUDE_CODE_DIR = MODULE_ROOT / "claude_code" / "presets"

# Cell paths
from flexsearch.registry import resolve_cell

# Which cells get which presets
CELL_CONFIG = {
    'claude_code': [GENERAL_DIR, CLAUDE_CODE_DIR],
    'claude': [GENERAL_DIR, CLAUDE_CODE_DIR],
    'qmem': [GENERAL_DIR],
    'inventory': [GENERAL_DIR],
    'thread-codebase': [GENERAL_DIR],
    'flexsearch-context': [GENERAL_DIR],
    'axpstack-context': [GENERAL_DIR],
}


def install_cell(cell_name: str, preset_dirs: list[Path] = None):
    """Install presets into a single cell.

    Args:
        cell_name: Name of the cell (must be in CELL_CONFIG).
        preset_dirs: Override preset directories. If None, uses CELL_CONFIG.
    """
    if preset_dirs is None:
        preset_dirs = CELL_CONFIG.get(cell_name, [GENERAL_DIR])

    db_path = resolve_cell(cell_name)
    if not db_path.exists():
        print(f"  {cell_name}: SKIP (not found)")
        return

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

        for pd in preset_dirs:
            if pd.exists():
                install_presets(conn, pd)

        count = conn.execute("SELECT COUNT(*) FROM _presets").fetchone()[0]
        names = [r[0] for r in conn.execute("SELECT name FROM _presets ORDER BY name").fetchall()]
        print(f"  {cell_name}: {count} presets [{', '.join(names)}]")

        conn.close()
    except sqlite3.OperationalError as e:
        print(f"  {cell_name}: LOCKED ({e}) — retry after stopping flexsearch-worker")


def install_all():
    """Install presets into all configured cells."""
    print("Installing presets...")
    for cell_name in CELL_CONFIG:
        install_cell(cell_name)
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for name in sys.argv[1:]:
            install_cell(name)
    else:
        install_all()
