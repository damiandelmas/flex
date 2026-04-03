#!/usr/bin/env python3
"""
SOMA Identity Audit — Coverage report with applicability-aware denominators.

Generates coverage report for identity edge tables.

Usage:
  python -m flex.modules.soma.manage.audit
"""

import sys
import sqlite3

from flex.registry import resolve_cell
from flex.modules.soma.compile import ensure_tables


COVERAGE_QUERIES = {
    'file_uuid': {
        'applicable': """
            SELECT COUNT(*) FROM _edges_tool_ops
            WHERE tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
              AND target_file IS NOT NULL
              AND target_file NOT LIKE '/tmp/%'
              AND target_file NOT LIKE '/var/tmp/%'
        """,
        'covered': """
            SELECT COUNT(DISTINCT t.chunk_id)
            FROM _edges_tool_ops t
            JOIN _edges_file_identity fi ON t.chunk_id = fi.chunk_id
            WHERE t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep')
              AND t.target_file NOT LIKE '/tmp/%'
        """,
    },
    'repo_root': {
        'applicable': """
            SELECT COUNT(*) FROM _edges_tool_ops
            WHERE tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep','Bash')
              AND target_file IS NOT NULL
              AND target_file NOT LIKE '/tmp/%'
        """,
        'covered': """
            SELECT COUNT(DISTINCT t.chunk_id)
            FROM _edges_tool_ops t
            JOIN _edges_repo_identity ri ON t.chunk_id = ri.chunk_id
            WHERE t.tool_name IN ('Write','Edit','MultiEdit','Read','Glob','Grep','Bash')
              AND t.target_file NOT LIKE '/tmp/%'
        """,
    },
    'content_hash': {
        'applicable': """
            SELECT COUNT(*) FROM _edges_tool_ops
            WHERE tool_name IN ('Write','Edit','MultiEdit')
              AND target_file IS NOT NULL
        """,
        'covered': """
            SELECT COUNT(DISTINCT t.chunk_id)
            FROM _edges_tool_ops t
            JOIN _edges_content_identity ci ON t.chunk_id = ci.chunk_id
            WHERE t.tool_name IN ('Write','Edit','MultiEdit')
        """,
    },
    'url_uuid': {
        'applicable': """
            SELECT COUNT(*) FROM _edges_tool_ops
            WHERE tool_name = 'WebFetch'
        """,
        'covered': """
            SELECT COUNT(DISTINCT t.chunk_id)
            FROM _edges_tool_ops t
            JOIN _edges_url_identity ui ON t.chunk_id = ui.chunk_id
            WHERE t.tool_name = 'WebFetch'
        """,
    },
}


def audit(conn: sqlite3.Connection) -> dict:
    """Run coverage audit. Returns dict of {field: (covered, applicable, pct)}."""
    results = {}
    for field, queries in COVERAGE_QUERIES.items():
        applicable = conn.execute(queries['applicable']).fetchone()[0]
        covered = conn.execute(queries['covered']).fetchone()[0]
        pct = (covered / applicable * 100) if applicable > 0 else 0.0
        results[field] = (covered, applicable, pct)
    return results


def main():
    cell_path = resolve_cell('claude_code')
    if not cell_path:
        print("[audit] FATAL: claude_code cell not found", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(cell_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    ensure_tables(conn)

    total_chunks = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]

    results = audit(conn)

    print(f"\nSOMA Identity Audit — claude_code cell")
    print("=" * 40)
    for field, (covered, applicable, pct) in results.items():
        print(f"  {field:15s} {covered:>8,} / {applicable:>8,}  ({pct:.1f}%)")
    print()
    print(f"  Total chunks: {total_chunks:,}")
    print(f"  Coverage floor: ~85% (remaining are deleted files, ephemeral paths)")

    conn.close()


if __name__ == "__main__":
    main()
