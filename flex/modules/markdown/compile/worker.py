"""Incremental worker for markdown vaults. Stat-scan with content hash fast-skip."""

import hashlib
import sys
import time
from pathlib import Path

from flex.core import open_cell, log_op
from flex.registry import list_cells, discover_watched
from flex.sdk import source, ingest, link, embed

from flex.modules.markdown.compile.walker import walk_vault, VaultEntry
from flex.modules.markdown.compile.frontmatter import (
    parse_frontmatter, extract_tags, extract_aliases, extract_created_date,
)
from flex.modules.markdown.compile.tags import extract_inline_tags, merge_tags
from flex.modules.markdown.compile.chunker import chunk_markdown, compute_char_offsets
from flex.modules.markdown.compile.dataview import extract_dataview_fields
from flex.modules.markdown.compile.wikilinks import (
    extract_raw_wikilinks, resolve_all_wikilinks,
)

# Module-level size cache, persisted across daemon ticks
_size_cache: dict[str, dict[str, int]] = {}  # {cell_name: {rel_path: size}}
_hash_cache: dict[str, dict[str, str]] = {}  # {cell_name: {rel_path: sha256}}
_change_counts: dict[str, int] = {}           # {cell_name: changes since last graph}

GRAPH_STALENESS_THRESHOLD = 20


def _content_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 16), b''):
            h.update(chunk)
    return h.hexdigest()


def _index_file(db, entry: VaultEntry, embed_fn=None) -> bool:
    """Parse, delete old chunks, re-insert. Returns True if indexed."""
    try:
        raw_text = entry.path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return False

    if not raw_text.strip():
        return False

    from datetime import datetime, timezone

    fm, body = parse_frontmatter(raw_text)
    fm_tags = extract_tags(fm)
    inline_tags = extract_inline_tags(body)
    all_tags = merge_tags(fm_tags, inline_tags)
    aliases = extract_aliases(fm)
    created_date = extract_created_date(fm)
    file_modified = datetime.fromtimestamp(entry.mtime, tz=timezone.utc).isoformat()

    chunks = chunk_markdown(body, entry.stem)
    compute_char_offsets(raw_text, chunks)
    if not chunks:
        return False

    # Delete old data for this source (upsert semantics)
    sid = entry.rel_path
    old_chunk_ids = [r[0] for r in db.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id = ?", (sid,)
    ).fetchall()]

    if old_chunk_ids:
        placeholders = ','.join('?' * len(old_chunk_ids))
        db.execute(f"DELETE FROM _raw_chunks WHERE id IN ({placeholders})", old_chunk_ids)
        db.execute(f"DELETE FROM _types_markdown WHERE chunk_id IN ({placeholders})", old_chunk_ids)
        db.execute(f"DELETE FROM _fields_inline WHERE chunk_id IN ({placeholders})", old_chunk_ids)
        db.execute(f"DELETE FROM _edges_tree WHERE id IN ({placeholders})", old_chunk_ids)
        db.execute("DELETE FROM _edges_source WHERE source_id = ?", (sid,))

    # Delete wikilink edges for this source
    try:
        db.execute("DELETE FROM _edges_wikilink WHERE from_path = ?", (sid,))
        db.execute("DELETE FROM _edges_wikilink_unresolved WHERE from_path = ?", (sid,))
    except Exception:
        pass

    # Re-insert source
    source(db, sid, entry.stem, timestamp=int(entry.mtime))

    # Ingest chunks
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

    ingest(db, sid, chunk_dicts, types='_types_markdown')

    # Source-level metadata
    db.execute(
        "INSERT OR REPLACE INTO _types_markdown_source "
        "(source_id, folder, tags, aliases, note_created, file_modified) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (sid, entry.folder, all_tags, ','.join(aliases), created_date, file_modified)
    )

    # Dataview fields + tree links (need chunk IDs from DB)
    chunk_ids = [r[0] for r in db.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id = ? ORDER BY rowid", (sid,)
    ).fetchall()]

    for ci, c in enumerate(chunks):
        if ci >= len(chunk_ids):
            break
        cid = chunk_ids[ci]
        for key, value in extract_dataview_fields(c.raw_content):
            db.execute(
                "INSERT INTO _fields_inline (chunk_id, source_id, field_key, field_value) VALUES (?, ?, ?, ?)",
                (cid, sid, key, value)
            )

    # Heading hierarchy
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

    # Raw wikilinks (for re-resolution)
    try:
        db.execute("""CREATE TABLE IF NOT EXISTS _edges_wikilink_raw (
            source_id TEXT NOT NULL, raw_target TEXT NOT NULL,
            PRIMARY KEY (source_id, raw_target))""")
    except Exception:
        pass

    for target in extract_raw_wikilinks(body):
        db.execute(
            "INSERT OR IGNORE INTO _edges_wikilink_raw (source_id, raw_target) VALUES (?, ?)",
            (sid, target)
        )

    log_op(db, 'markdown_index_file', sid, source='flex/modules/markdown/compile/worker.py')
    return True


def _cleanup_stale(db, vault_root: Path, current_paths: set):
    """Remove notes whose files no longer exist on disk."""
    db_paths = {r[0] for r in db.execute("SELECT source_id FROM _raw_sources").fetchall()}
    removed = 0

    for db_path in db_paths:
        if db_path not in current_paths:
            chunk_ids = [r[0] for r in db.execute(
                "SELECT chunk_id FROM _edges_source WHERE source_id = ?", (db_path,)
            ).fetchall()]

            if chunk_ids:
                ph = ','.join('?' * len(chunk_ids))
                db.execute(f"DELETE FROM _raw_chunks WHERE id IN ({ph})", chunk_ids)
                db.execute(f"DELETE FROM _types_markdown WHERE chunk_id IN ({ph})", chunk_ids)
                db.execute(f"DELETE FROM _fields_inline WHERE chunk_id IN ({ph})", chunk_ids)
                db.execute(f"DELETE FROM _edges_tree WHERE id IN ({ph})", chunk_ids)

            db.execute("DELETE FROM _edges_source WHERE source_id = ?", (db_path,))
            db.execute("DELETE FROM _raw_sources WHERE source_id = ?", (db_path,))

            try:
                db.execute("DELETE FROM _edges_wikilink WHERE from_path = ? OR to_path = ?",
                           (db_path, db_path))
                db.execute("DELETE FROM _edges_wikilink_unresolved WHERE from_path = ?", (db_path,))
            except Exception:
                pass

            log_op(db, 'markdown_delete_note', db_path,
                   source='flex/modules/markdown/compile/worker.py')
            removed += 1

    return removed


def scan_markdown_cells(embed_fn=None) -> dict:
    """Called by the daemon on the 2-second tick.

    For each registered markdown/obsidian cell with lifecycle='watch':
        1. Walk the vault directory
        2. Compare sizes against cache
        3. Content hash fast-skip unchanged files
        4. Re-parse changed files, cleanup deleted files
        5. Re-resolve wikilinks if files added/removed (dirty-set)
        6. Embed new chunks
        7. Auto graph refresh after staleness threshold

    Returns dict with 'indexed' and 'skipped' counts.
    """
    stats = {'indexed': 0, 'skipped': 0}

    # Find markdown/obsidian cells with lifecycle='watch'
    watched = discover_watched()
    md_cells = [c for c in watched
                if c.get('cell_type') in ('markdown', 'obsidian')
                and c.get('watch_path')]

    for cell in md_cells:
        cell_name = cell['name']
        vault_root = Path(cell['watch_path'])
        if not vault_root.exists():
            continue

        db = open_cell(cell['path'])

        # Initialize caches — first tick just populates, doesn't index
        first_tick = cell_name not in _size_cache
        if first_tick:
            _size_cache[cell_name] = {}
            _hash_cache[cell_name] = {}
            _change_counts[cell_name] = 0

        sizes = _size_cache[cell_name]
        hashes = _hash_cache[cell_name]

        entries = list(walk_vault(vault_root))
        current_paths = {e.rel_path for e in entries}

        # First tick: populate cache only, don't treat everything as new
        if first_tick:
            for entry in entries:
                sizes[entry.rel_path] = entry.size
            db.close()
            continue

        files_added = []
        files_removed = []
        files_changed = []

        # Detect changes
        for entry in entries:
            key = entry.rel_path
            new_size = entry.size

            if key not in sizes:
                files_added.append(entry)
                sizes[key] = new_size
            elif sizes[key] != new_size:
                # Size changed — check content hash
                new_hash = _content_hash(entry.path)
                if hashes.get(key) != new_hash:
                    files_changed.append(entry)
                    hashes[key] = new_hash
                sizes[key] = new_size

        # Detect deletions
        for old_key in list(sizes.keys()):
            if old_key not in current_paths:
                files_removed.append(old_key)
                del sizes[old_key]
                hashes.pop(old_key, None)

        if not files_added and not files_changed and not files_removed:
            db.close()
            continue

        # Process changes
        for entry in files_added + files_changed:
            if _index_file(db, entry):
                stats['indexed'] += 1
                _change_counts[cell_name] += 1
            else:
                stats['skipped'] += 1

        # Cleanup deleted
        if files_removed:
            removed = _cleanup_stale(db, vault_root, current_paths)
            _change_counts[cell_name] += removed

        # Re-resolve wikilinks if topology changed (dirty-set)
        if files_added or files_removed:
            try:
                # Re-read entries for resolution maps
                entries_fresh = list(walk_vault(vault_root))
                aliases_by_path = {}
                for e in entries_fresh:
                    try:
                        text = e.path.read_text(encoding='utf-8', errors='ignore')
                        fm, _ = parse_frontmatter(text)
                        als = extract_aliases(fm)
                        if als:
                            aliases_by_path[e.rel_path] = als
                    except Exception:
                        pass

                resolved, unresolved = resolve_all_wikilinks(db, entries_fresh, aliases_by_path)
                if resolved or unresolved:
                    print(f"  [{cell_name}] Wikilinks: {resolved} resolved, {unresolved} unresolved",
                          file=sys.stderr)
            except Exception as e:
                print(f"  [{cell_name}] Wikilink resolution error: {e}", file=sys.stderr)

        # Embed new chunks
        try:
            embed(db)
        except Exception:
            pass

        # Auto graph refresh
        if _change_counts[cell_name] >= GRAPH_STALENESS_THRESHOLD:
            try:
                from flex.modules.markdown.compile.graph import build_combined_graph
                print(f"  [{cell_name}] Auto graph refresh ({_change_counts[cell_name]} changes)",
                      file=sys.stderr)
                build_combined_graph(db)
                _change_counts[cell_name] = 0
            except Exception as e:
                print(f"  [{cell_name}] Graph refresh error: {e}", file=sys.stderr)

        db.commit()
        db.close()

    return stats
