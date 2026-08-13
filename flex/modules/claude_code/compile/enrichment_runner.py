"""Supervised child entry point for the Claude Code enrichment cycle."""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time


DEFERRED_BUSY = 75


def run(cell_path: str, graph_threshold: int = 50) -> int:
    """Run one admitted enrichment pass in this process' own SQLite session."""
    from flex.admission import try_heavy_lease
    with try_heavy_lease(detail="claude_code enrichment") as lease:
        if not lease.acquired:
            print("[enrich] Deferred: semantic work already active", file=sys.stderr)
            return DEFERRED_BUSY
        # Import the semantic implementation only after admission.  This keeps
        # model/module globals confined to the child that actually owns work.
        from flex.core import set_meta
        from flex.modules.claude_code.compile.worker import _run_enrichment_cycle
        # An optional local aggregate enrichment may be present in richer
        # installations. Claude capture remains complete without it.
        try:
            from flex.modules.engines import refresh_corpus_graphs
        except ImportError:
            refresh_corpus_graphs = None
        conn = sqlite3.connect(cell_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            _run_enrichment_cycle(conn, graph_threshold)
            # Corpus graphs are semantic enrichment too.  Keep their complete
            # refresh inside this child-held lease, never in the capture parent.
            if refresh_corpus_graphs is not None:
                refresh_corpus_graphs()
            set_meta(conn, "last_enrichment_ts", str(time.time()))
            conn.commit()
        finally:
            conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Claude Code enrichment pass")
    parser.add_argument("--cell-path", required=True)
    parser.add_argument("--graph-threshold", type=int, default=50)
    args = parser.parse_args()
    return run(args.cell_path, args.graph_threshold)


if __name__ == "__main__":
    raise SystemExit(main())
