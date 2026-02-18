"""
Incremental indexer for Claude.ai conversation exports.

Batch runner, not daemon. Finds new files in the export directory,
parses markdown conversations into chunks, INSERTs into existing
chunk-atom tables, embeds, and optionally rebuilds the similarity graph.

Entry point:
    python -m flexsearch.modules.claude_chat.compile.worker [--export-dir PATH] [--cell NAME] [--graph] [--dry-run]
"""

import argparse
import re
import sys
import time
from pathlib import Path
from datetime import datetime

import numpy as np

from flexsearch.compile.markdown import extract_frontmatter
from flexsearch.core import open_cell, log_op

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_EXPORT_DIR = Path.home() / "projects/home/ai-sync/main/claude"

MESSAGE_RE = re.compile(r'^## Message (\d+): (USER|ASSISTANT)\s*$', re.MULTILINE)
FILE_DATE_RE = re.compile(r'^(\d{6})(?:-(\d{4}))?')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_file_date(filename):
    """YYMMDD or YYMMDD-HHMM from filename."""
    match = FILE_DATE_RE.match(filename)
    if match:
        date = match.group(1)
        t = match.group(2)
        return f"{date}-{t}" if t else date
    return None


def _title_from_stem(stem):
    """Derive title from filename stem when frontmatter missing."""
    name = re.sub(r'^\d{6}(?:-\d{4})?_?', '', stem)
    return name.replace('-', ' ').replace('_', ' ').strip() or stem


def _parse_iso_timestamp(value):
    """Convert ISO-8601 or datetime to Unix epoch. Returns None on failure."""
    if not value:
        return None
    try:
        if isinstance(value, datetime):
            return int(value.timestamp())
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return int(dt.timestamp())
    except (ValueError, AttributeError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def parse_conversation(filepath):
    """Parse a Claude.ai conversation export.

    Returns:
        (frontmatter: dict, messages: list[dict])
        Each message: {position, role, content}
    """
    content = Path(filepath).read_text(encoding='utf-8')
    frontmatter, body = extract_frontmatter(content)

    matches = list(MESSAGE_RE.finditer(body))
    if not matches:
        return frontmatter, []

    messages = []
    for i, match in enumerate(matches):
        position = int(match.group(1))
        role = match.group(2).lower()

        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        msg_content = body[start:end].strip()

        if not msg_content:
            continue

        messages.append({
            'position': position,
            'role': role,
            'content': msg_content,
        })

    return frontmatter, messages


def find_new_files(export_dir, db):
    """Find conversation files not yet indexed.

    Returns:
        list[Path] — new files, sorted by name (chronological)
    """
    export_dir = Path(export_dir)

    rows = db.execute("SELECT source_id FROM _raw_sources").fetchall()
    existing = {r[0] for r in rows}

    new_files = []
    for f in sorted(export_dir.glob('*.md')):
        if f.stem not in existing:
            new_files.append(f)

    return new_files


def ingest(new_files, db):
    """Parse and INSERT new conversations into chunk-atom tables."""
    total_sources = 0
    total_chunks = 0

    for filepath in new_files:
        try:
            frontmatter, messages = parse_conversation(filepath)
        except (UnicodeDecodeError, OSError) as e:
            print(f"  SKIP {filepath.name}: {e}")
            continue

        if not messages:
            print(f"  SKIP {filepath.name}: no messages")
            continue

        source_id = filepath.stem
        file_date = _extract_file_date(filepath.name)
        title = frontmatter.get('title', _title_from_stem(source_id))
        model = frontmatter.get('model')
        timestamp = _parse_iso_timestamp(frontmatter.get('created'))

        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, temporal, doc_type,
             model, message_count, embedding)
            VALUES (?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
        """, (source_id, title, str(filepath), file_date,
              model, len(messages)))

        for msg in messages:
            chunk_id = f"{source_id}:{msg['position']}"
            role = msg['role']

            db.execute("""
                INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
                VALUES (?, ?, NULL, ?)
            """, (chunk_id, msg['content'], timestamp))

            db.execute("""
                INSERT OR IGNORE INTO _edges_source
                (chunk_id, source_id, source_type, position)
                VALUES (?, ?, 'claude-ai', ?)
            """, (chunk_id, source_id, msg['position']))

            db.execute("""
                INSERT OR IGNORE INTO _types_message
                (chunk_id, type, role, chunk_number)
                VALUES (?, ?, ?, ?)
            """, (chunk_id, role, role, msg['position']))

            total_chunks += 1

        db.commit()
        total_sources += 1

    return total_sources, total_chunks


def embed_new(db):
    """Embed all chunks missing embeddings, then mean-pool sources."""
    from flexsearch.onnx.embed import ONNXEmbedder

    rows = db.execute(
        "SELECT id, content FROM _raw_chunks WHERE embedding IS NULL"
    ).fetchall()

    if not rows:
        return 0

    chunk_ids = [r[0] for r in rows]
    texts = [r[1] for r in rows]

    embedder = ONNXEmbedder()
    embeddings = embedder.encode(texts, batch_size=32)

    for i, chunk_id in enumerate(chunk_ids):
        blob = embeddings[i].astype(np.float32).tobytes()
        db.execute("UPDATE _raw_chunks SET embedding = ? WHERE id = ?",
                   (blob, chunk_id))
    db.commit()

    # Mean-pool new sources
    sources = db.execute("""
        SELECT DISTINCT e.source_id FROM _edges_source e
        JOIN _raw_sources s ON e.source_id = s.source_id
        WHERE s.embedding IS NULL
    """).fetchall()

    for (source_id,) in sources:
        chunk_rows = db.execute("""
            SELECT c.embedding FROM _raw_chunks c
            JOIN _edges_source e ON c.id = e.chunk_id
            WHERE e.source_id = ? AND c.embedding IS NOT NULL
        """, (source_id,)).fetchall()

        if not chunk_rows:
            continue

        vecs = [np.frombuffer(r[0], dtype=np.float32) for r in chunk_rows]
        mean_vec = np.mean(vecs, axis=0).astype(np.float32)
        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm

        db.execute("UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
                   (mean_vec.tobytes(), source_id))

    db.commit()
    return len(chunk_ids)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description='Index new Claude.ai conversations')
    parser.add_argument('--export-dir', default=str(DEFAULT_EXPORT_DIR),
                        help='Directory containing exported conversation .md files')
    parser.add_argument('--cell', default='claude_chat',
                        help='Cell name (resolved via registry)')
    parser.add_argument('--graph', action='store_true',
                        help='Rebuild similarity graph after ingest')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show new files without indexing')
    args = parser.parse_args()

    from flexsearch.registry import resolve_cell
    cell_path = resolve_cell(args.cell)
    if cell_path is None:
        print(f"Cell '{args.cell}' not found in registry")
        sys.exit(1)

    db = open_cell(str(cell_path))
    new_files = find_new_files(args.export_dir, db)

    print(f"Found {len(new_files)} new conversations")

    if args.dry_run:
        for f in new_files:
            print(f"  {f.name}")
        db.close()
        return

    if not new_files:
        print("Nothing to index.")
        db.close()
        return

    t0 = time.time()

    # Ingest
    sources, chunks = ingest(new_files, db)
    print(f"Ingested: {sources} sources, {chunks} chunks")

    # Embed
    embedded = embed_new(db)
    print(f"Embedded: {embedded} chunks")

    # Log to _ops
    log_op(db, 'claude_chat_ingest', '_raw_chunks',
           params={'sources': sources, 'chunks': chunks, 'embedded': embedded},
           rows_affected=chunks,
           source='claude_chat/compile/worker.py')
    db.commit()

    # Graph (optional)
    if args.graph:
        from flexsearch.manage.meditate import (
            build_similarity_graph, compute_scores, persist)
        print("Rebuilding similarity graph...")
        G, edge_count = build_similarity_graph(
            db, table='_raw_sources', id_col='source_id',
            threshold=0.55, center=True)
        if G is not None:
            scores = compute_scores(G)
            persist(db, scores, table='_enrich_source_graph',
                    id_col='source_id')
            print(f"Graph: {len(scores.get('hubs', []))} hubs, "
                  f"{len(set(scores.get('communities', {}).values()))} communities")

    # Regenerate views
    from flexsearch.views import regenerate_views
    regenerate_views(db)
    print("Views regenerated.")

    elapsed = time.time() - t0
    print(f"Done in {elapsed:.1f}s")

    db.close()


if __name__ == '__main__':
    main()
