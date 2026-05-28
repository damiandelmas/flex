"""
Extract MCP tool definitions from README spans.

Parses tool names and descriptions from:
- Markdown tables: | `tool_name` | Description |
- Bullet lists:    - `tool_name` - Description
- Backtick names:  - **tool_name** — Description

Stores extracted tools in _types_mcp_tools table.

Usage:
    python -m flex.modules.skills.compile.extract_tools --cell tools
"""

import re
import sqlite3
import argparse
import os
import sys

from flex.core import open_cell, log_op


# ═════════════════════════════════════════════════════
# Tool extraction patterns
# ═════════════════════════════════════════════════════

# Table row: | `tool_name` | description | or | **tool_name** | description |
_TABLE_TOOL = re.compile(
    r'\|\s*`?(\w[\w._-]*)`?\s*\|\s*(.+?)\s*\|'
)

# Bullet: - `tool_name` - description  or  - `tool_name` — description
_BULLET_TOOL = re.compile(
    r'^\s*[-*]\s+`(\w[\w._-]*)`\s*[-–—:]\s*(.+)$', re.MULTILINE
)

# Bullet bold: - **tool_name** - description
_BULLET_BOLD = re.compile(
    r'^\s*[-*]\s+\*\*(\w[\w._-]*)\*\*\s*[-–—:]\s*(.+)$', re.MULTILINE
)

# Headings that indicate a tool section
_TOOL_HEADINGS = re.compile(
    r'^#{2,4}\s+(Available\s+)?Tools|^#{2,4}\s+Functions|^#{2,4}\s+Capabilities|^#{2,4}\s+API\b',
    re.MULTILINE | re.IGNORECASE
)

# Junk tool names to skip
_SKIP_NAMES = {
    'tool', 'name', 'function', 'method', 'command', 'action',
    'description', 'type', 'input', 'output', 'example', 'usage',
    'feature', 'status', 'category', 'note', 'details', 'options',
    'parameter', 'parameters', 'returns', 'return', 'default',
    'required', 'optional', 'yes', 'no', 'true', 'false',
    'npm', 'uv', 'jq', 'git', 'node', 'python', 'pip',
    'setting', 'component', 'role', 'location', 'impact',
    'model', 'version', 'key', 'value', 'field', 'property',
    'endpoint', 'path', 'file', 'source', 'target', 'result',
    'variable', 'config', 'configuration', 'flag', 'option',
    'hook', 'event', 'trigger', 'step', 'phase', 'stage',
    'module', 'package', 'library', 'plugin', 'extension',
    'pretooluse', 'posttooluse', 'stop', 'sessionstart', 'sessionend',
}

# Tool names should look like function/API calls (snake_case or dot.notation)
# Must contain underscore or dot separator, no file extensions
_TOOL_NAME_RE = re.compile(r'^[a-z][a-z0-9]*[_.][a-z][a-z0-9_.-]*$', re.IGNORECASE)

# Reject names that look like filenames or numeric constants
_REJECT_FILE_RE = re.compile(
    r'\.(py|js|ts|md|json|yaml|yml|sh|sql)$', re.IGNORECASE
)
# ALL_CAPS env vars (no IGNORECASE!)
_REJECT_ENVVAR_RE = re.compile(r'^[A-Z][A-Z0-9_]+$')
# Starts with digit
_REJECT_DIGIT_RE = re.compile(r'^\d')

def _is_rejected_name(name: str) -> bool:
    """Reject names that are obviously not tool names."""
    return bool(
        _REJECT_FILE_RE.search(name)
        or _REJECT_ENVVAR_RE.match(name)
        or _REJECT_DIGIT_RE.match(name)
    )


def extract_tools_from_span(content: str) -> list[tuple[str, str]]:
    """Extract (tool_name, description) pairs from a README span.

    Returns deduplicated list of (name, desc) tuples.
    """
    tools = []
    seen = set()

    # Check if content has a tool-related heading
    has_tool_heading = bool(_TOOL_HEADINGS.search(content))

    # Try table format first
    for m in _TABLE_TOOL.finditer(content):
        name = m.group(1).strip().strip('*').strip('`')
        desc = m.group(2).strip().rstrip('|').strip()
        # Skip table headers and junk
        if name.lower() in _SKIP_NAMES:
            continue
        if desc.startswith('---') or desc.startswith('==='):
            continue
        if len(name) < 2 or len(name) > 80:
            continue
        # Reject obvious non-tools
        if _is_rejected_name(name):
            continue
        # Under a tool heading: accept any reasonable name
        # Outside a tool heading: require tool-like name (has _ or . separator)
        if not has_tool_heading and not _TOOL_NAME_RE.match(name):
            continue
        # Even under tool headings, reject single generic words
        if has_tool_heading and len(name.split()) > 1:
            continue  # multi-word = prose, not a tool name
        if name.lower() not in seen:
            seen.add(name.lower())
            tools.append((name, desc[:500]))

    # Try bullet format
    for pattern in (_BULLET_TOOL, _BULLET_BOLD):
        for m in pattern.finditer(content):
            name = m.group(1).strip()
            desc = m.group(2).strip()
            if name.lower() in _SKIP_NAMES:
                continue
            if _is_rejected_name(name):
                continue
            if len(name) < 2 or len(name) > 80:
                continue
            if name.lower() not in seen:
                seen.add(name.lower())
                tools.append((name, desc[:500]))

    return tools


# ═════════════════════════════════════════════════════
# Schema
# ═════════════════════════════════════════════════════

TOOLS_DDL = """
CREATE TABLE IF NOT EXISTS _types_mcp_tools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    span_chunk_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    description TEXT,
    UNIQUE(source_id, tool_name)
);
CREATE INDEX IF NOT EXISTS idx_mcp_tools_source ON _types_mcp_tools(source_id);
CREATE INDEX IF NOT EXISTS idx_mcp_tools_name ON _types_mcp_tools(tool_name);
"""


def extract_all(db: sqlite3.Connection, dry_run: bool = False):
    """Extract tool definitions from all MCP README spans."""

    if not dry_run:
        db.executescript(TOOLS_DDL)

    # Find MCP readme spans with tool-like content
    spans = db.execute("""
        SELECT c.id, c.content, es.source_id
        FROM _raw_chunks c
        JOIN _types_skills t ON c.id = t.chunk_id
        JOIN _edges_source es ON t.chunk_id = es.chunk_id
        WHERE t.is_mcp = 1 AND t.chunk_type = 'readme_span'
        AND (c.content LIKE '%## Tools%' OR c.content LIKE '%## Available Tools%'
          OR c.content LIKE '%### Tools%' OR c.content LIKE '%## Functions%'
          OR c.content LIKE '%## Capabilities%' OR c.content LIKE '%### Available Tools%'
          OR c.content LIKE '%### Available Functions%'
          OR c.content LIKE '%| `%` |%')
    """).fetchall()

    total_tools = 0
    repos_with_tools = set()

    for chunk_id, content, source_id in spans:
        tools = extract_tools_from_span(content)
        if not tools:
            continue

        repos_with_tools.add(source_id)

        if dry_run:
            total_tools += len(tools)
            continue

        for name, desc in tools:
            try:
                db.execute("""
                    INSERT OR IGNORE INTO _types_mcp_tools
                    (source_id, span_chunk_id, tool_name, description)
                    VALUES (?, ?, ?, ?)
                """, (source_id, chunk_id, name, desc))
                total_tools += 1
            except sqlite3.IntegrityError:
                pass

    if not dry_run:
        db.commit()
        log_op(db, 'extract_mcp_tools', '_types_mcp_tools',
               params={'spans_checked': len(spans), 'tools_extracted': total_tools,
                       'repos': len(repos_with_tools)},
               rows_affected=total_tools,
               source='skills/compile/extract_tools.py')
        db.commit()

    print(f"  Spans checked: {len(spans)}")
    print(f"  Repos with tools: {len(repos_with_tools)}")
    print(f"  Tools extracted: {total_tools}")

    if dry_run:
        # Show samples
        sample_spans = [(cid, content, sid) for cid, content, sid in spans
                        if extract_tools_from_span(content)][:5]
        for chunk_id, content, source_id in sample_spans:
            tools = extract_tools_from_span(content)
            print(f"\n  {source_id}:")
            for name, desc in tools[:5]:
                print(f"    {name:30s} {desc[:60]}")


def main():
    parser = argparse.ArgumentParser(description='Extract MCP tool definitions from READMEs')
    parser.add_argument('--cell', default='tools', help='Cell name')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    args = parser.parse_args()

    cell_path = args.cell
    if not cell_path.endswith('.db'):
        from flex.registry import CELLS_DIR
        cell_path = str(CELLS_DIR / f"{args.cell}.db")

    if not os.path.exists(cell_path):
        print(f"Cell not found: {cell_path}", file=sys.stderr)
        sys.exit(1)

    db = open_cell(cell_path)
    print(f"Extracting MCP tools from {cell_path}")
    extract_all(db, args.dry_run)
    db.close()
    print("Done.")


if __name__ == '__main__':
    main()
