"""Static Flex instructions cell.

The normal Flex path is the target cell's @orient. This cell remains a static,
exact-name note surface for legacy clients and operators.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flex.core import set_meta
from flex import registry

CELL_NAME = "instructions"
CELL_TYPE = "instructions"
DESCRIPTION = "Static Flex instruction notes. Normal path is the target cell's @orient."
SOURCE_PATH = "flex/ai/skills/flex/SKILL.md"

ORIENT_SQL = """-- @name: orient
-- @description: Return the packaged Flex instruction notes.
SELECT kind, title, content, source_path, updated_at
FROM flex_instructions
WHERE id = 'flex'
LIMIT 1;
"""


def _skill_path() -> Path:
    return Path(__file__).resolve().parent / "ai" / "skills" / "flex" / "SKILL.md"


def _source_updated_at(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()


def restore_instructions_orient(db: sqlite3.Connection) -> None:
    """Install the custom @orient preset for the instructions cell."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS _presets (
            name TEXT PRIMARY KEY,
            description TEXT,
            params TEXT DEFAULT '',
            sql TEXT
        )
        """
    )
    try:
        db.execute("ALTER TABLE _presets ADD COLUMN params TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    db.execute(
        """
        INSERT OR REPLACE INTO _presets (name, description, params, sql)
        VALUES (?, ?, ?, ?)
        """,
        (
            "orient",
            "Return the packaged Flex instruction notes.",
            "",
            ORIENT_SQL,
        ),
    )
    db.commit()


def ensure_instructions_cell() -> Path:
    """Create or refresh the static instructions cell and registry row."""
    skill_path = _skill_path()
    content = skill_path.read_text(encoding="utf-8")
    updated_at = _source_updated_at(skill_path)

    registry.CELLS_DIR.mkdir(parents=True, exist_ok=True)
    db_path = registry.CELLS_DIR / "instructions.db"

    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS flex_instructions (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                source_path TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO flex_instructions
                (id, kind, title, content, source_path, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "flex",
                "flex_instruction_notes",
                "Flex Instruction Notes",
                content,
                SOURCE_PATH,
                updated_at,
            ),
        )
        set_meta(conn, "cell_type", CELL_TYPE)
        set_meta(conn, "description", DESCRIPTION)
        restore_instructions_orient(conn)
        conn.commit()
    finally:
        conn.close()

    registry.register_cell(
        CELL_NAME,
        db_path,
        cell_type=CELL_TYPE,
        description=DESCRIPTION,
        lifecycle="static",
        active=True,
        unlisted=False,
    )
    return db_path
