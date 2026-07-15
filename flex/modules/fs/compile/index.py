"""Atomic per-file writer for mixed filesystem cells."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from flex.modules.fs.compile.extract import ExtractionResult, extract_file
from flex.modules.fs.compile.schema import ensure_schema
from flex.modules.fs.compile.walker import FileEntry


class ExtractionFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class IndexOutcome:
    status: str
    source_id: str
    chunks: int = 0


@dataclass(frozen=True)
class _ResolutionEntry:
    rel_path: str
    stem: str


def _embedding_enabled(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM _meta WHERE key='embed'").fetchone()
    return not row or str(row[0]).strip().lower() not in {"0", "false", "no", "off"}


def _resolve_embedder(conn: sqlite3.Connection):
    from flex.compile.embed import _resolve_ingest_target
    embed_doc, _dim, _tag = _resolve_ingest_target(conn)
    return embed_doc


def _compute_embeddings(result: ExtractionResult, embed_fn) -> tuple[list[bytes], bytes | None]:
    if not result.chunks:
        return [], None
    texts = [chunk.content for chunk in result.chunks]
    try:
        matrix = embed_fn(texts, batch_size=64)
    except TypeError:
        matrix = embed_fn(texts)
    matrix = np.asarray(matrix, dtype=np.float32)
    if matrix.ndim != 2 or matrix.shape[0] != len(texts) or matrix.shape[1] == 0:
        raise RuntimeError(
            f"embedder returned shape {matrix.shape}; expected ({len(texts)}, dim)"
        )
    if not np.isfinite(matrix).all():
        raise RuntimeError("embedder returned non-finite values")
    chunk_blobs = [np.ascontiguousarray(row, dtype=np.float32).tobytes() for row in matrix]
    mean = matrix.mean(axis=0, dtype=np.float32)
    norm = float(np.linalg.norm(mean))
    if norm:
        mean = mean / norm
    return chunk_blobs, np.ascontiguousarray(mean, dtype=np.float32).tobytes()


def _old_chunk_ids(conn: sqlite3.Connection, source_id: str) -> list[str]:
    return [row[0] for row in conn.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id=?", (source_id,)
    )]


def _delete_source_rows(conn: sqlite3.Connection, source_id: str, *,
                        drop_identity: bool = False, drop_state: bool = True) -> None:
    chunk_ids = _old_chunk_ids(conn, source_id)
    if chunk_ids:
        placeholders = ",".join("?" for _ in chunk_ids)
        conn.execute(f"DELETE FROM _fields_inline WHERE chunk_id IN ({placeholders})", chunk_ids)
        conn.execute(f"DELETE FROM _edges_call WHERE caller_id IN ({placeholders})", chunk_ids)
        conn.execute(
            f"DELETE FROM _edges_tree WHERE id IN ({placeholders}) OR parent_id IN ({placeholders})",
            [*chunk_ids, *chunk_ids],
        )
        conn.execute(f"DELETE FROM _types_filesystem WHERE chunk_id IN ({placeholders})", chunk_ids)
        conn.execute(f"DELETE FROM _raw_chunks WHERE id IN ({placeholders})", chunk_ids)
    conn.execute("DELETE FROM _edges_source WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _fields_inline WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _symbols WHERE file_id=?", (source_id,))
    conn.execute("DELETE FROM _edges_import WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _types_markdown_source WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _edges_wikilink_raw WHERE source_id=?", (source_id,))
    conn.execute("DELETE FROM _edges_wikilink WHERE from_path=?", (source_id,))
    conn.execute("DELETE FROM _edges_wikilink_unresolved WHERE from_path=?", (source_id,))
    conn.execute("DELETE FROM _raw_sources WHERE source_id=?", (source_id,))
    if drop_identity:
        conn.execute("DELETE FROM _edges_fs_identity WHERE source_id=?", (source_id,))
    if drop_state:
        conn.execute("DELETE FROM _filesystem_source_state WHERE source_id=?", (source_id,))


def _mint_identity(conn: sqlite3.Connection, result: ExtractionResult) -> None:
    if conn.execute(
        "SELECT 1 FROM _edges_fs_identity WHERE source_id=?", (result.source_id,)
    ).fetchone():
        return
    try:
        from flex.modules.soma.lib.identity.file_identity import get_instance
        absolute = str(Path(result.source_path).resolve())
        file_uuid = get_instance().assign_batch([absolute]).get(absolute)
    except Exception:
        file_uuid = None
    if file_uuid:
        conn.execute(
            "INSERT INTO _edges_fs_identity(source_id,file_uuid) VALUES(?,?)",
            (result.source_id, file_uuid),
        )


def _write_state(conn: sqlite3.Connection, result: ExtractionResult, state: str) -> None:
    conn.execute(
        "INSERT INTO _filesystem_source_state "
        "(source_id,source_path,file_kind,content_hash,size_bytes,mtime_ns,"
        "source_state,extraction_state) VALUES(?,?,?,?,?,?,?,?)",
        (
            result.source_id, result.source_path, result.file_kind, result.content_hash,
            result.size_bytes, result.mtime_ns, state, result.extraction_state,
        ),
    )


def _resolve_obsidian_links(conn: sqlite3.Connection) -> None:
    """Rebuild corpus-level link side tables without committing independently."""
    from flex.modules.markdown.compile.wikilinks import build_resolution_maps, resolve_wikilink

    rows = conn.execute(
        "SELECT s.source_id, s.source_path, COALESCE(m.aliases,'') "
        "FROM _filesystem_source_state s "
        "LEFT JOIN _types_markdown_source m ON m.source_id=s.source_id "
        "WHERE s.file_kind='markdown' AND s.source_state='indexed'"
    ).fetchall()
    entries = [_ResolutionEntry(source_id, Path(source_path).stem)
               for source_id, source_path, _aliases in rows]
    aliases = {
        source_id: [value for value in alias_text.split(",") if value]
        for source_id, _source_path, alias_text in rows if alias_text
    }
    maps = build_resolution_maps(entries, aliases)
    first_chunks = dict(conn.execute(
        "SELECT es.source_id, es.chunk_id FROM _edges_source es "
        "JOIN _types_filesystem t ON t.chunk_id=es.chunk_id "
        "WHERE t.position=(SELECT MIN(t2.position) FROM _types_filesystem t2 "
        "JOIN _edges_source es2 ON es2.chunk_id=t2.chunk_id "
        "WHERE es2.source_id=es.source_id)"
    ).fetchall())
    conn.execute("DELETE FROM _edges_wikilink")
    conn.execute("DELETE FROM _edges_wikilink_unresolved")
    for source_id, target in conn.execute(
        "SELECT source_id,raw_target FROM _edges_wikilink_raw ORDER BY source_id,raw_target"
    ).fetchall():
        resolved = resolve_wikilink(target, maps, source_id)
        if resolved:
            conn.execute(
                "INSERT INTO _edges_wikilink(chunk_id,from_path,to_path) VALUES(?,?,?)",
                (first_chunks.get(source_id, source_id), source_id, resolved),
            )
        else:
            conn.execute(
                "INSERT INTO _edges_wikilink_unresolved(from_path,raw_target) VALUES(?,?)",
                (source_id, target),
            )


def _insert_result(conn: sqlite3.Connection, result: ExtractionResult,
                   chunk_embeddings: list[bytes], source_embedding: bytes | None,
                   *, obsidian: bool) -> None:
    conn.execute(
        "INSERT INTO _raw_sources(source_id,title,embedding,timestamp) VALUES(?,?,?,?)",
        (result.source_id, result.title, source_embedding, result.mtime_ns // 1_000_000_000),
    )
    for chunk, embedding in zip(result.chunks, chunk_embeddings or [None] * len(result.chunks)):
        conn.execute(
            "INSERT INTO _raw_chunks(id,content,embedding,timestamp) VALUES(?,?,?,?)",
            (chunk.id, chunk.content, embedding, result.mtime_ns // 1_000_000_000),
        )
        conn.execute(
            "INSERT INTO _edges_source(chunk_id,source_id) VALUES(?,?)",
            (chunk.id, result.source_id),
        )
        conn.execute(
            "INSERT INTO _types_filesystem "
            "(chunk_id,file_kind,chunk_kind,section_title,section_type,position,depth,"
            "container_id,content_hash,language,extraction_state) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                chunk.id, result.file_kind, chunk.chunk_kind, chunk.section_title,
                chunk.section_type, chunk.position, chunk.depth, chunk.container_id,
                chunk.content_hash, chunk.language, result.extraction_state,
            ),
        )
        if chunk.container_id:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_tree(id,parent_id,relation,depth) VALUES(?,?,?,?)",
                (chunk.id, chunk.container_id, "subsection", chunk.depth),
            )
    if result.symbols:
        conn.executemany(
            "INSERT INTO _symbols(name,def_id,file_id,kind) VALUES(?,?,?,?)",
            [(symbol.name, symbol.def_id, result.source_id, symbol.kind)
             for symbol in result.symbols],
        )
    if result.calls:
        conn.executemany(
            "INSERT OR IGNORE INTO _edges_call(caller_id,callee_name) VALUES(?,?)",
            [(call.caller_id, call.callee_name) for call in result.calls],
        )
    if result.imports:
        conn.executemany(
            "INSERT OR IGNORE INTO _edges_import(source_id,module,name) VALUES(?,?,?)",
            [(result.source_id, module, name) for module, name in result.imports],
        )
    if result.markdown:
        meta = result.markdown
        conn.execute(
            "INSERT INTO _types_markdown_source "
            "(source_id,folder,tags,aliases,note_created,file_modified) VALUES(?,?,?,?,?,?)",
            (
                result.source_id, meta.folder, ",".join(meta.tags), ",".join(meta.aliases),
                meta.note_created, meta.file_modified,
            ),
        )
        first = result.chunks[0].id if result.chunks else result.source_id
        conn.executemany(
            "INSERT INTO _fields_inline(chunk_id,source_id,field_key,field_value) "
            "VALUES(?,?,?,?)",
            [(field.chunk_id, result.source_id, field.key, field.value)
             for field in result.fields],
        )
        if obsidian:
            conn.executemany(
                "INSERT INTO _fields_inline(chunk_id,source_id,field_key,field_value) "
                "VALUES(?,?,?,?)",
                [(first, result.source_id, "tag", tag) for tag in meta.tags]
                + [(first, result.source_id, "alias", alias) for alias in meta.aliases],
            )
            conn.executemany(
                "INSERT INTO _edges_wikilink_raw(source_id,raw_target) VALUES(?,?)",
                [(result.source_id, target) for target in result.wikilinks],
            )
    _mint_identity(conn, result)
    _write_state(conn, result, "indexed")


def apply_result(conn: sqlite3.Connection, result: ExtractionResult, *,
                 chunk_embeddings: list[bytes] | None = None,
                 source_embedding: bytes | None = None, obsidian: bool = False) -> IndexOutcome:
    """Atomically replace one source with a fully prepared extraction result."""
    if result.status == "failed":
        raise ExtractionFailure(result.error or f"extraction failed: {result.source_path}")
    if chunk_embeddings is not None and len(chunk_embeddings) != len(result.chunks):
        raise ValueError(
            f"embedding count {len(chunk_embeddings)} does not match "
            f"chunk count {len(result.chunks)}"
        )
    ensure_schema(conn)
    conn.commit()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _delete_source_rows(conn, result.source_id, drop_state=True)
        if result.status == "empty":
            _mint_identity(conn, result)
            _write_state(conn, result, "empty")
        elif result.status == "indexed":
            _insert_result(
                conn, result, chunk_embeddings or [None] * len(result.chunks),
                source_embedding, obsidian=obsidian,
            )
        else:
            raise ValueError(f"unsupported extraction status: {result.status}")
        if obsidian:
            _resolve_obsidian_links(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return IndexOutcome(result.status, result.source_id, len(result.chunks))


def index_file(conn: sqlite3.Connection, entry: FileEntry, *, embed_fn=None,
               embed_enabled: bool | None = None, obsidian: bool = False) -> IndexOutcome:
    """Extract, embed, and atomically replace one discovered file."""
    if entry is None:
        raise ValueError("index_file requires a discovered FileEntry")
    result = extract_file(entry)
    if result.status == "failed":
        raise ExtractionFailure(result.error or f"extraction failed: {entry.path}")
    previous = conn.execute(
        "SELECT content_hash,file_kind,source_state FROM _filesystem_source_state "
        "WHERE source_id=?", (result.source_id,),
    ).fetchone()
    if previous and tuple(previous) == (result.content_hash, result.file_kind, result.status):
        conn.execute(
            "UPDATE _filesystem_source_state SET source_path=?,size_bytes=?,mtime_ns=? "
            "WHERE source_id=?",
            (result.source_path, result.size_bytes, result.mtime_ns, result.source_id),
        )
        conn.commit()
        return IndexOutcome("unchanged", result.source_id, len(result.chunks))
    enabled = _embedding_enabled(conn) if embed_enabled is None else embed_enabled
    chunk_embeddings: list[bytes] | None = None
    source_embedding = None
    if enabled and result.status == "indexed":
        chunk_embeddings, source_embedding = _compute_embeddings(
            result, embed_fn or _resolve_embedder(conn),
        )
    return apply_result(
        conn, result, chunk_embeddings=chunk_embeddings,
        source_embedding=source_embedding, obsidian=obsidian,
    )


def delete_source(conn: sqlite3.Connection, source_id: str, *, obsidian: bool = False) -> bool:
    """Commit deletion of one vanished source and all of its optional artifacts."""
    exists = conn.execute(
        "SELECT 1 FROM _filesystem_source_state WHERE source_id=?", (source_id,)
    ).fetchone()
    if not exists:
        return False
    conn.commit()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _delete_source_rows(conn, source_id, drop_identity=True, drop_state=True)
        if obsidian:
            _resolve_obsidian_links(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return True
