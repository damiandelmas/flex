"""One-shot admitted semantic catch-up for the Pi coding-agent cell."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from flex.admission import try_heavy_lease
from flex.modules.claude_code import run_enrichment
from flex.modules.claude_code.compile.worker import _batch_embed_chunks
from flex.registry import resolve_cell


def run(
    cell_name: str = "pi",
    wait_seconds: float = 3600,
    batch_chunks: int = 500,
) -> int:
    path = resolve_cell(cell_name)
    if path is None:
        print(f"[{cell_name}] cell not found", file=sys.stderr)
        return 2
    total_embedded = 0
    while True:
        with try_heavy_lease(
            detail=f"{cell_name} semantic catch-up",
            timeout_s=wait_seconds,
        ) as lease:
            if not lease.acquired:
                print(f"[{cell_name}] semantic lane unavailable", file=sys.stderr)
                return 75
            conn = sqlite3.connect(str(path), timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            try:
                remaining = int(conn.execute(
                    "SELECT COUNT(*) FROM _raw_chunks "
                    "WHERE content IS NOT NULL AND embedding IS NULL"
                ).fetchone()[0])
                if remaining == 0:
                    run_enrichment(conn, cell_type="pi")
                    conn.commit()
                    print(
                        f"[{cell_name}] semantic catch-up complete: "
                        f"embedded={total_embedded} remaining=0",
                        flush=True,
                    )
                    return 0
                embedded = _batch_embed_chunks(
                    conn,
                    quiet=True,
                    max_chunks=max(1, batch_chunks),
                )
                total_embedded += embedded
                remaining = int(conn.execute(
                    "SELECT COUNT(*) FROM _raw_chunks "
                    "WHERE content IS NOT NULL AND embedding IS NULL"
                ).fetchone()[0])
                print(
                    f"[{cell_name}] batch={embedded} "
                    f"embedded_total={total_embedded} remaining={remaining}",
                    flush=True,
                )
            finally:
                conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cell", default="pi")
    parser.add_argument("--wait-seconds", type=float, default=3600)
    parser.add_argument("--batch-chunks", type=int, default=500)
    args = parser.parse_args()
    raise SystemExit(run(args.cell, args.wait_seconds, args.batch_chunks))


if __name__ == "__main__":
    main()
