"""
Docpac incremental index worker — single-file upsert into chunk-atom cells.

Drains the `pending` table in ~/.flex/queue.db.
Each row is a file path written by the flex-index.sh PostToolUse hook.

Pipeline per file:
  resolve_cell_for_path → parse_docpac_file → frontmatter → normalize →
  split_sections → embed → upsert (delete old + re-insert) → mean-pool source

Auto graph refresh when staleness threshold (20 sources) exceeded.
"""

import hashlib
import sqlite3
import sys
import time

import numpy as np
from pathlib import Path

from flex.registry import resolve_cell_for_path, FLEX_HOME
from flex.core import log_op
from flex.modules.docpac.compile.docpac import parse_docpac_file
from flex.compile.markdown import normalize_headers, extract_frontmatter, split_sections

QUEUE_DB = FLEX_HOME / "queue.db"
GRAPH_REFRESH_THRESHOLD = 20


def make_source_id(path: str) -> str:
    return hashlib.sha256(path.encode()).hexdigest()[:16]


def make_chunk_id(source_id: str, position: int) -> str:
    return f"{source_id}:{position}"


def _find_context_root(file_path: str) -> Path | None:
    """Walk up from file to find the context/ root directory."""
    p = Path(file_path)
    for parent in p.parents:
        if parent.name == 'context':
            return parent
    return None


def _embed_texts(texts: list[str], embed_fn) -> list[bytes | None]:
    """Embed texts using the shared ONNX embedder. Returns list of blobs."""
    if not texts:
        return []
    try:
        vecs = embed_fn(texts)
        if hasattr(vecs, 'shape') and len(vecs.shape) == 2:
            return [v.astype(np.float32).tobytes() for v in vecs]
        return [vecs.astype(np.float32).tobytes()]
    except Exception as e:
        print(f"[docpac-worker] embed error: {e}", file=sys.stderr)
        return [None] * len(texts)


def index_file(conn: sqlite3.Connection, file_path: str, embed_fn) -> bool:
    """Index a single markdown file into its docpac cell.

    Upsert semantics: delete old chunks for this source, re-insert.
    """
    p = Path(file_path)
    if not p.exists():
        return False

    context_root = _find_context_root(file_path)
    if not context_root:
        return False

    entry = parse_docpac_file(file_path, str(context_root))
    if entry.skip:
        return False

    try:
        content = p.read_text(encoding='utf-8')
    except (UnicodeDecodeError, FileNotFoundError):
        return False

    source_id = make_source_id(file_path)
    frontmatter, body = extract_frontmatter(content)
    normalized = normalize_headers(body)
    sections = split_sections(normalized, level=2)
    if not sections:
        sections = [('', body.strip(), 0)]

    # Embed all section texts
    section_texts = [s[1] for s in sections]
    embeddings = _embed_texts(section_texts, embed_fn)

    # --- Upsert: delete old data for this source ---
    old_chunk_ids = [r[0] for r in conn.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id = ?",
        (source_id,)
    ).fetchall()]

    if old_chunk_ids:
        ph = ','.join('?' * len(old_chunk_ids))
        conn.execute(f"DELETE FROM _raw_chunks WHERE id IN ({ph})", old_chunk_ids)
        conn.execute(f"DELETE FROM _types_docpac WHERE chunk_id IN ({ph})", old_chunk_ids)
        # _enrich_types may not exist in all cells
        try:
            conn.execute(f"DELETE FROM _enrich_types WHERE chunk_id IN ({ph})", old_chunk_ids)
        except sqlite3.OperationalError:
            pass
        conn.execute("DELETE FROM _edges_source WHERE source_id = ?", (source_id,))

    # --- Insert source ---
    conn.execute("""
        INSERT OR REPLACE INTO _raw_sources
        (source_id, file_date, temporal, doc_type, title, source_path,
         type, status, keywords)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        source_id,
        entry.file_date,
        entry.temporal,
        entry.doc_type,
        entry.title,
        file_path,
        frontmatter.get('type'),
        frontmatter.get('status'),
        ','.join(frontmatter.get('keywords', [])) if isinstance(frontmatter.get('keywords'), list)
            else frontmatter.get('keywords'),
    ))

    # --- Insert chunks + edges + types ---
    for section_title, section_content, position in sections:
        chunk_id = make_chunk_id(source_id, position)
        emb = embeddings[position] if position < len(embeddings) else None

        conn.execute("""
            INSERT OR REPLACE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, ?, ?)
        """, (chunk_id, section_content, emb, None))

        conn.execute("""
            INSERT OR REPLACE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'markdown', ?)
        """, (chunk_id, source_id, position))

        conn.execute("""
            INSERT OR REPLACE INTO _types_docpac
            (chunk_id, temporal, doc_type, facet, section_title,
             yaml_type, yaml_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            chunk_id,
            entry.temporal,
            entry.doc_type,
            None,
            section_title or None,
            frontmatter.get('type'),
            frontmatter.get('status'),
        ))

    # --- Mean-pool source embedding ---
    valid = [e for e in embeddings if e is not None]
    if valid:
        vecs = [np.frombuffer(e, dtype=np.float32) for e in valid]
        mean_vec = np.mean(vecs, axis=0).astype(np.float32)
        norm = np.linalg.norm(mean_vec)
        if norm > 0:
            mean_vec = mean_vec / norm
        conn.execute(
            "UPDATE _raw_sources SET embedding = ? WHERE source_id = ?",
            (mean_vec.tobytes(), source_id))

    # --- Log to _ops ---
    log_op(conn, 'docpac_incremental_index', '_raw_chunks',
           params={'file': str(p.name), 'sections': len(sections)},
           rows_affected=len(sections),
           source='docpac/compile/worker.py')

    return True


def _graph_stale(conn) -> bool:
    """True if enough new sources indexed since last graph build."""
    try:
        last_graph = conn.execute("""
            SELECT MAX(timestamp) FROM _ops
            WHERE operation = 'build_similarity_graph'
        """).fetchone()[0]
    except sqlite3.OperationalError:
        return False  # no _ops table yet

    if last_graph is None:
        return True  # never built

    try:
        new_sources = conn.execute("""
            SELECT COUNT(*) FROM _ops
            WHERE operation = 'docpac_incremental_index'
              AND timestamp > ?
        """, (last_graph,)).fetchone()[0]
    except sqlite3.OperationalError:
        return False

    return new_sources >= GRAPH_REFRESH_THRESHOLD


def _refresh_graph(conn, cell_name: str):
    """Rebuild similarity graph on a docpac cell."""
    from flex.manage.meditate import build_similarity_graph, compute_scores, persist
    from flex.views import regenerate_views

    print(f"[docpac] graph refresh on {cell_name}...", file=sys.stderr)
    t0 = time.time()

    G, edge_count = build_similarity_graph(
        conn, table='_raw_sources', id_col='source_id',
        threshold=0.55, center=True)

    if G is not None and G.number_of_nodes() > 0:
        scores = compute_scores(G)
        persist(conn, scores, table='_enrich_source_graph', id_col='source_id')
        log_op(conn, 'build_similarity_graph', '_enrich_source_graph',
               params={'threshold': 0.55, 'center': True,
                       'nodes': G.number_of_nodes(), 'edges': edge_count,
                       'communities': len(scores.get('communities', [])),
                       'hubs': len(scores.get('hubs', [])),
                       'trigger': 'auto_refresh'},
               rows_affected=G.number_of_nodes(),
               source='docpac/compile/worker.py')
        regenerate_views(conn)
        conn.commit()
        elapsed = time.time() - t0
        print(f"[docpac] graph refresh done: {G.number_of_nodes()} nodes, "
              f"{edge_count} edges, {elapsed:.1f}s", file=sys.stderr)


def process_queue(embed_fn) -> dict:
    """Drain the pending table. Returns stats dict."""
    stats = {'processed': 0, 'indexed': 0, 'skipped': 0}

    if not QUEUE_DB.exists():
        return stats

    qconn = sqlite3.connect(str(QUEUE_DB), timeout=5)
    qconn.execute("PRAGMA journal_mode=WAL")

    try:
        rows = qconn.execute("SELECT path, ts FROM pending ORDER BY ts").fetchall()
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        qconn.close()
        return stats

    if not rows:
        qconn.close()
        return stats

    # Group by cell via registry
    by_cell: dict[str, dict] = {}
    no_cell: list[str] = []

    for path, ts in rows:
        result = resolve_cell_for_path(path)
        if result is None:
            no_cell.append(path)
            continue
        cell_name, cell_path = result
        if cell_name not in by_cell:
            by_cell[cell_name] = {'db': str(cell_path), 'files': []}
        by_cell[cell_name]['files'].append(path)

    # Index files per cell
    processed_paths = list(no_cell)  # clear unknowns from queue too

    for cell_name, data in by_cell.items():
        conn = sqlite3.connect(data['db'], timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")

        cell_indexed = 0
        for file_path in data['files']:
            try:
                if index_file(conn, file_path, embed_fn):
                    stats['indexed'] += 1
                    cell_indexed += 1
                else:
                    stats['skipped'] += 1
            except Exception as e:
                print(f"[docpac-worker] error on {Path(file_path).name}: {e}",
                      file=sys.stderr)
                stats['skipped'] += 1
            processed_paths.append(file_path)

        conn.commit()

        # Batch summary log per cell
        if cell_indexed > 0:
            log_op(conn, 'docpac_queue_drain', 'pending',
                   params={'cell': cell_name, 'files': cell_indexed},
                   rows_affected=cell_indexed,
                   source='docpac/compile/worker.py')
            conn.commit()

        # Auto graph refresh if stale
        if cell_indexed > 0 and _graph_stale(conn):
            try:
                _refresh_graph(conn, cell_name)
            except Exception as e:
                print(f"[docpac] graph refresh error on {cell_name}: {e}",
                      file=sys.stderr)

        conn.close()
        stats['processed'] += len(data['files'])

    # Clear processed from queue
    if processed_paths:
        ph = ','.join('?' * len(processed_paths))
        qconn.execute(f"DELETE FROM pending WHERE path IN ({ph})", processed_paths)
        qconn.commit()

    qconn.close()
    return stats
