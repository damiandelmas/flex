"""Dataview inline field extraction for Obsidian vaults."""

import re

from flex.modules.markdown.compile.tags import strip_code_blocks

# Block fields: key:: value at start of line
BLOCK_FIELD_RE = re.compile(r'(?:^|\n)(\w[\w-]*)::[ \t]+(.+?)(?:\n|$)')

# Inline fields: [key:: value] within brackets
INLINE_FIELD_RE = re.compile(r'\[(\w[\w-]*)::[ \t]+([^\]]+)\]')


def extract_dataview_fields(body: str) -> list[tuple[str, str]]:
    """Extract Dataview inline fields from markdown body.

    Returns list of (key, value) tuples.
    Keys are lowercased for consistent querying.
    """
    fields = []
    stripped = strip_code_blocks(body)

    for match in BLOCK_FIELD_RE.finditer(stripped):
        key = match.group(1).lower()
        value = match.group(2).strip()
        if value:
            fields.append((key, value))

    for match in INLINE_FIELD_RE.finditer(stripped):
        key = match.group(1).lower()
        value = match.group(2).strip()
        if value:
            fields.append((key, value))

    return fields
