"""Batch compile pipeline for markdown vaults.

Walks a directory, parses frontmatter/tags/wikilinks, chunks by heading,
ingests via SDK, embeds, and registers for MCP.

Usage:
    from flex.modules.markdown.compile.init import compile_vault
    db = compile_vault(Path("~/vault"), name="my-vault")
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flex.sdk import create, source, ingest, link, embed, register

from flex.modules.markdown.compile.walker import walk_vault
from flex.modules.markdown.compile.frontmatter import (
    parse_frontmatter, extract_tags, extract_aliases, extract_created_date,
)
from flex.modules.markdown.compile.tags import extract_inline_tags, merge_tags
from flex.modules.markdown.compile.chunker import chunk_markdown, compute_char_offsets
from flex.modules.markdown.compile.dataview import extract_dataview_fields
from flex.modules.markdown.compile.wikilinks import extract_raw_wikilinks


SCHEMA_DDL = """\
CREATE TABLE IF NOT EXISTS _types_markdown (
    chunk_id TEXT PRIMARY KEY,
    item_type TEXT,
    note_title TEXT,
    section_title TEXT,
    heading_depth INTEGER,
    heading_chain TEXT,
    word_count INTEGER,
    char_start INTEGER,
    char_end INTEGER
);

CREATE TABLE IF NOT EXISTS _types_markdown_source (
    source_id TEXT PRIMARY KEY,
    folder TEXT,
    tags TEXT,
    aliases TEXT,
    note_created TEXT,
    file_modified TEXT
);

CREATE TABLE IF NOT EXISTS _fields_inline (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    field_key TEXT NOT NULL,
    field_value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fields_key ON _fields_inline(field_key);
CREATE INDEX IF NOT EXISTS idx_fields_source ON _fields_inline(source_id);

CREATE TABLE IF NOT EXISTS _edges_wikilink_raw (
    source_id TEXT NOT NULL,
    raw_target TEXT NOT NULL,
    PRIMARY KEY (source_id, raw_target)
);
"""


def compile_vault(
    root: Path,
    name: str,
    cell_type: str = 'markdown',
    description: str | None = None,
    exclude: list[str] | None = None,
) -> 'sqlite3.Connection':
    """Compile a markdown vault into a queryable flex cell.

    Args:
        root: Path to the vault/folder root.
        name: Cell name for MCP discovery.
        cell_type: 'obsidian' or 'markdown'.
        description: Optional cell description.
        exclude: Additional exclude patterns (dirs ending /, else fnmatch).

    Returns:
        Open sqlite3.Connection. Cell is registered and MCP-queryable.
    """
    root = Path(root).resolve()
    entries = list(walk_vault(root, exclude=exclude))

    if not entries:
        print(f"  No .md files found in {root}", file=sys.stderr)
        raise ValueError(f"No markdown files found in {root}")

    desc = description or f"{name} — {len(entries)} notes"
    print(f"  {len(entries)} markdown files found")

    db = create(name, desc, cell_type=cell_type, schema=SCHEMA_DDL)

    t0 = time.time()
    total_chunks = 0
    total_fields = 0
    total_wikilinks = 0
    skipped = 0

    for i, entry in enumerate(entries):
        try:
            raw_text = entry.path.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            skipped += 1
            continue

        if not raw_text.strip():
            skipped += 1
            continue

        # ── Parse ─────────────────────────────────────────────────────
        fm, body = parse_frontmatter(raw_text)

        fm_tags = extract_tags(fm)
        inline_tags = extract_inline_tags(body)
        all_tags = merge_tags(fm_tags, inline_tags)

        aliases = extract_aliases(fm)
        created_date = extract_created_date(fm)
        file_modified = datetime.fromtimestamp(
            entry.mtime, tz=timezone.utc
        ).isoformat()

        # ── Chunk ─────────────────────────────────────────────────────
        chunks = chunk_markdown(body, entry.stem)
        compute_char_offsets(raw_text, chunks)

        if not chunks:
            skipped += 1
            continue

        # ── Source ────────────────────────────────────────────────────
        source(db, entry.rel_path, entry.stem, timestamp=int(entry.mtime))

        # ── Ingest chunks ─────────────────────────────────────────────
        chunk_dicts = []
        for c in chunks:
            item_type = 'preamble' if c.heading_depth == 0 and not c.section_title else 'section'
            if c.heading_depth == 0 and not c.section_title and len(chunks) == 1:
                item_type = 'full_note'

            chunk_dicts.append({
                'content': c.content,
                'item_type': item_type,
                'note_title': entry.stem,
                'section_title': c.section_title,
                'heading_depth': c.heading_depth,
                'heading_chain': ' > '.join([entry.stem] + c.heading_chain) if c.heading_chain else entry.stem,
                'word_count': c.word_count,
                'char_start': c.char_start,
                'char_end': c.char_end,
            })

        n = ingest(db, entry.rel_path, chunk_dicts, types='_types_markdown')
        total_chunks += n

        # ── Source-level metadata ─────────────────────────────────────
        db.execute(
            "INSERT OR REPLACE INTO _types_markdown_source "
            "(source_id, folder, tags, aliases, note_created, file_modified) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (entry.rel_path, entry.folder, all_tags,
             ','.join(aliases), created_date, file_modified)
        )

        # ── Dataview fields ───────────────────────────────────────────
        # Extract fields from each chunk's raw body
        for ci, c in enumerate(chunks):
            fields = extract_dataview_fields(c.raw_content)
            # Reconstruct chunk_id matching what sdk.ingest generated
            chunk_id = chunk_dicts[ci].get('id')
            if not chunk_id:
                # sdk.ingest auto-generates IDs; read them back
                pass
            for key, value in fields:
                total_fields += 1

        # Need chunk IDs from the DB after ingest
        chunk_ids = [
            row[0] for row in db.execute(
                "SELECT chunk_id FROM _edges_source WHERE source_id = ? ORDER BY rowid",
                (entry.rel_path,)
            ).fetchall()
        ]

        for ci, c in enumerate(chunks):
            if ci >= len(chunk_ids):
                break
            cid = chunk_ids[ci]

            # Dataview fields
            fields = extract_dataview_fields(c.raw_content)
            for key, value in fields:
                db.execute(
                    "INSERT INTO _fields_inline (chunk_id, source_id, field_key, field_value) "
                    "VALUES (?, ?, ?, ?)",
                    (cid, entry.rel_path, key, value)
                )

        # ── Wikilinks (raw targets) ──────────────────────────────────
        targets = extract_raw_wikilinks(body)
        for target in targets:
            db.execute(
                "INSERT OR IGNORE INTO _edges_wikilink_raw (source_id, raw_target) "
                "VALUES (?, ?)",
                (entry.rel_path, target)
            )
            total_wikilinks += 1

        # ── Heading hierarchy ─────────────────────────────────────────
        if len(chunk_ids) > 1:
            parent_slots = [None] * 6
            for ci, c in enumerate(chunks):
                if ci >= len(chunk_ids):
                    break
                cid = chunk_ids[ci]
                depth = c.heading_depth

                if depth > 0:
                    parent_slots[depth - 1] = cid
                    for d in range(depth, 6):
                        parent_slots[d] = None

                    if depth > 1 and parent_slots[depth - 2]:
                        link(db, child_id=cid, parent_id=parent_slots[depth - 2],
                             relation='subsection')

        db.commit()

        # Progress
        if (i + 1) % 50 == 0 or i == len(entries) - 1:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(entries) - i - 1) / rate if rate > 0 else 0
            print(f"  {i + 1}/{len(entries)} notes ({total_chunks} chunks) — {remaining:.0f}s remaining",
                  file=sys.stderr)

    elapsed = time.time() - t0
    print(f"  Done: {total_chunks} chunks from {len(entries) - skipped} notes in {elapsed:.1f}s")
    if skipped:
        print(f"  Skipped: {skipped} files (empty or unreadable)")
    if total_wikilinks:
        print(f"  Wikilinks: {total_wikilinks} raw targets stored")
    if total_fields:
        print(f"  Dataview fields: {total_fields}")

    # ── Wikilink Resolution ───────────────────────────────────────────
    if total_wikilinks:
        print("  Resolving wikilinks...")
        from flex.modules.markdown.compile.wikilinks import resolve_all_wikilinks
        aliases_by_path = {}
        for entry in entries:
            try:
                text = entry.path.read_text(encoding='utf-8', errors='ignore')
                fm, _ = parse_frontmatter(text)
                als = extract_aliases(fm)
                if als:
                    aliases_by_path[entry.rel_path] = als
            except Exception:
                pass
        resolved, unresolved = resolve_all_wikilinks(db, entries, aliases_by_path)
        print(f"  Wikilinks: {resolved} resolved, {unresolved} unresolved")

    # ── Embed ─────────────────────────────────────────────────────────
    print("  Embedding...")
    try:
        embed(db)
    except RuntimeError as e:
        if 'model not found' in str(e).lower() or 'embedding model' in str(e).lower():
            print(
                f"  [warning] Embedding model not found. Run 'flex init' first.\n"
                f"  Cell created with {total_chunks} chunks (unembedded).",
                file=sys.stderr
            )
        else:
            raise

    # ── Combined Graph ────────────────────────────────────────────────
    print("  Building graph...")
    try:
        from flex.modules.markdown.compile.graph import build_combined_graph
        ok = build_combined_graph(db)
        if ok:
            print("  Graph built (wikilink + embedding)")
        else:
            print("  Graph skipped (too few sources or degenerate)")
    except Exception as e:
        print(f"  Graph error: {e}", file=sys.stderr)

    # ── Register with curated views + presets ─────────────────────────
    print("  Registering...")
    stock_dir = Path(__file__).resolve().parent.parent / 'stock'
    views_dir = stock_dir / 'views' if (stock_dir / 'views').exists() else None
    presets_dirs = [stock_dir / 'presets'] if (stock_dir / 'presets').exists() else None

    register(db, name, desc, cell_type=cell_type,
             views_dir=views_dir,
             presets_dirs=presets_dirs,
             lifecycle='watch',
             watch_path=str(root))
    print(f"  Cell '{name}' ready. Query with: flex search --cell {name} \"@orient\"")

    return db


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Compile a markdown vault into a flex cell")
    parser.add_argument("path", type=Path, help="Path to vault/folder root")
    parser.add_argument("--name", default=None, help="Cell name (default: directory name)")
    parser.add_argument("--exclude", action="append", default=[], help="Exclude patterns")
    args = parser.parse_args()

    vault_path = args.path.resolve()
    cell_name = args.name or vault_path.name
    cell_type = 'obsidian' if (vault_path / '.obsidian').is_dir() else 'markdown'

    compile_vault(vault_path, name=cell_name, cell_type=cell_type, exclude=args.exclude)
