"""Locate the packaged general Flex skill.

This module used to maintain ``instructions.db`` by copying the global
``SKILL.md`` into a static cell.  That duplicated an ordinary filesystem asset
and made the copy look like a second authority.  The skill file is now read at
its real address by the agent/skill runtime; Flex's MCP bootloader is enough to
tell an unskilled client to select a cell and call ``@orient``.
"""

from __future__ import annotations

from pathlib import Path


SOURCE_PATH = "flex/ai/skills/flex/SKILL.md"


def flex_skill_path() -> Path:
    """Return the single canonical filesystem address of the Flex skill."""
    return Path(__file__).resolve().parent / "ai" / "skills" / "flex" / "SKILL.md"


def read_flex_skill() -> str:
    """Read the current skill directly; there is no SQLite mirror."""
    return flex_skill_path().read_text(encoding="utf-8")
