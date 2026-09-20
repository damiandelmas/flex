"""Atomic per-file writer for mixed filesystem cells."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np

from flex.modules.fs.compile.extract import ExtractionResult, extract_file
from flex.modules.fs.compile.schema import (
    DOCUMENT_PROFILE_VERSION, document_identity_required, ensure_schema,
)
from flex.modules.fs.compile.walker import FileEntry


class ExtractionFailure(RuntimeError):
    pass


class PublicationConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class IndexOutcome:
    status: str
    source_id: str
    chunks: int = 0


@dataclass(frozen=True)
class PublicationEvent:
    """One accepted source mutation, visible to a transactional sidecar.

    The callback receiving this event executes inside the compiler's
    ``BEGIN IMMEDIATE`` transaction.  It may issue SQL on ``conn`` but must not
    begin, commit, or roll back a transaction itself.
    """

    status: str
    source_id: str
    source_path: str
    file_kind: str
    content_hash: str
    size_bytes: int
    mtime_ns: int
    chunks: int
    file_uuid: str | None
    accepted_generation: int | None


SidecarCallback = Callable[[sqlite3.Connection, PublicationEvent], None]


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


def _compute_chunk_embeddings(chunks, embed_fn) -> list[bytes]:
    """Embed one bounded chunk group and return normalized fp32 blobs."""
    if not chunks:
        return []
    texts = [chunk.content for chunk in chunks]
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
    norms = np.linalg.norm(matrix, axis=1)
    if np.any(norms <= 0):
        raise RuntimeError("embedder returned a zero-length vector")
    matrix = matrix / norms[:, np.newaxis]
    return [np.ascontiguousarray(row, dtype=np.float32).tobytes() for row in matrix]


def _pool_source_embedding(chunk_blobs: list[bytes]) -> bytes | None:
    """Mean-pool one document's normalized chunk blobs into a unit vector."""
    if not chunk_blobs:
        return None
    try:
        matrix = np.stack([
            np.frombuffer(blob, dtype=np.float32) for blob in chunk_blobs
        ])
    except ValueError as exc:
        raise RuntimeError("document chunks have inconsistent vector widths") from exc
    if not np.isfinite(matrix).all():
        raise RuntimeError("document chunks contain non-finite vectors")
    mean = matrix.mean(axis=0, dtype=np.float32)
    norm = float(np.linalg.norm(mean))
    if norm <= 0:
        raise RuntimeError("document chunks produced a zero-length source vector")
    mean = mean / norm
    return np.ascontiguousarray(mean, dtype=np.float32).tobytes()


def _compute_embeddings(result: ExtractionResult, embed_fn) -> tuple[list[bytes], bytes | None]:
    chunk_blobs = _compute_chunk_embeddings(result.chunks, embed_fn)
    return chunk_blobs, _pool_source_embedding(chunk_blobs)


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
    conn.execute("DELETE FROM _fields_frontmatter WHERE source_id=?", (source_id,))
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
    required = document_identity_required(conn)
    try:
        from flex.modules.soma.lib.identity.file_identity import get_instance
        absolute = str(Path(result.source_path).resolve())
        file_uuid = get_instance().assign_batch([absolute]).get(absolute)
    except Exception as exc:
        if required:
            raise RuntimeError(
                f"required filesystem identity unavailable: {result.source_id}"
            ) from exc
        file_uuid = None
    if file_uuid:
        conn.execute(
            "INSERT INTO _edges_fs_identity(source_id,file_uuid) VALUES(?,?)",
            (result.source_id, file_uuid),
        )
    elif required:
        raise RuntimeError(
            f"required filesystem identity unavailable: {result.source_id}"
        )


def _write_state(conn: sqlite3.Connection, result: ExtractionResult, state: str,
                 *, accepted_generation: int) -> None:
    conn.execute(
        "INSERT INTO _filesystem_source_state "
        "(source_id,source_path,file_kind,content_hash,size_bytes,mtime_ns,"
        "source_state,extraction_state,accepted_generation,profile_version) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            result.source_id, result.source_path, result.file_kind, result.content_hash,
            result.size_bytes, result.mtime_ns, state, result.extraction_state,
            accepted_generation, DOCUMENT_PROFILE_VERSION,
        ),
    )


def _resolve_wikilinks(conn: sqlite3.Connection) -> None:
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
                "INSERT OR IGNORE INTO _edges_wikilink(chunk_id,from_path,to_path) VALUES(?,?,?)",
                (first_chunks.get(source_id, source_id), source_id, resolved),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO _edges_wikilink_unresolved(from_path,raw_target) VALUES(?,?)",
                (source_id, target),
            )


def refresh_wikilinks(conn: sqlite3.Connection) -> None:
    """Resolve all accepted raw Markdown links in one atomic projection pass."""
    ensure_schema(conn)
    conn.commit()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _resolve_wikilinks(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


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
        conn.executemany(
            "INSERT INTO _fields_frontmatter(source_id,field_key,field_value,position) "
            "VALUES(?,?,?,?)",
            [(result.source_id, field.key, field.value, field.position)
             for field in result.frontmatter],
        )
        conn.executemany(
            "INSERT INTO _edges_wikilink_raw(source_id,raw_target) VALUES(?,?)",
            [(result.source_id, target) for target in result.wikilinks],
        )
        if obsidian:
            conn.executemany(
                "INSERT INTO _fields_inline(chunk_id,source_id,field_key,field_value) "
                "VALUES(?,?,?,?)",
                [(first, result.source_id, "tag", tag) for tag in meta.tags]
                + [(first, result.source_id, "alias", alias) for alias in meta.aliases],
            )
    _mint_identity(conn, result)
    # State is written by apply_result() after it selects the next generation.


def _event_for_result(conn: sqlite3.Connection, result: ExtractionResult,
                      status: str, accepted_generation: int) -> PublicationEvent:
    identity = conn.execute(
        "SELECT file_uuid FROM _edges_fs_identity WHERE source_id=?",
        (result.source_id,),
    ).fetchone()
    return PublicationEvent(
        status=status,
        source_id=result.source_id,
        source_path=result.source_path,
        file_kind=result.file_kind,
        content_hash=result.content_hash,
        size_bytes=result.size_bytes,
        mtime_ns=result.mtime_ns,
        chunks=len(result.chunks),
        file_uuid=identity[0] if identity else None,
        accepted_generation=accepted_generation,
    )


def apply_result(conn: sqlite3.Connection, result: ExtractionResult, *,
                 chunk_embeddings: list[bytes] | None = None,
                 source_embedding: bytes | None = None, obsidian: bool = False,
                 sidecar_callback: SidecarCallback | None = None,
                 expected_generation: int | None = None,
                 _defer_wikilinks: bool = False,
                 _schema_ready: bool = False) -> IndexOutcome:
    """Atomically replace one source with a fully prepared extraction result."""
    if result.status == "failed":
        raise ExtractionFailure(result.error or f"extraction failed: {result.source_path}")
    if chunk_embeddings is not None and len(chunk_embeddings) != len(result.chunks):
        raise ValueError(
            f"embedding count {len(chunk_embeddings)} does not match "
            f"chunk count {len(result.chunks)}"
        )
    if not _schema_ready:
        ensure_schema(conn)
        conn.commit()
    try:
        conn.execute("BEGIN IMMEDIATE")
        previous = conn.execute(
            "SELECT accepted_generation FROM _filesystem_source_state WHERE source_id=?",
            (result.source_id,),
        ).fetchone()
        current_generation = int(previous[0]) if previous else 0
        if (expected_generation is not None
                and current_generation != expected_generation):
            raise PublicationConflict(
                f"stale source generation for {result.source_id}: expected "
                f"{expected_generation}, found {current_generation}"
            )
        accepted_generation = current_generation + 1
        _delete_source_rows(conn, result.source_id, drop_state=True)
        if result.status == "empty":
            _mint_identity(conn, result)
            _write_state(
                conn, result, "empty", accepted_generation=accepted_generation,
            )
        elif result.status == "indexed":
            _insert_result(
                conn, result, chunk_embeddings or [None] * len(result.chunks),
                source_embedding, obsidian=obsidian,
            )
            _write_state(
                conn, result, "indexed", accepted_generation=accepted_generation,
            )
        else:
            raise ValueError(f"unsupported extraction status: {result.status}")
        if result.file_kind == "markdown" and not _defer_wikilinks:
            _resolve_wikilinks(conn)
        if sidecar_callback is not None:
            sidecar_callback(
                conn,
                _event_for_result(
                    conn, result, result.status, accepted_generation,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return IndexOutcome(result.status, result.source_id, len(result.chunks))


def index_file(conn: sqlite3.Connection, entry: FileEntry, *, embed_fn=None,
               embed_enabled: bool | None = None, obsidian: bool = False,
               sidecar_callback: SidecarCallback | None = None,
               _defer_wikilinks: bool = False) -> IndexOutcome:
    """Extract, embed, and atomically replace one discovered file."""
    if entry is None:
        raise ValueError("index_file requires a discovered FileEntry")
    result = extract_file(entry)
    if result.status == "failed":
        raise ExtractionFailure(result.error or f"extraction failed: {entry.path}")
    ensure_schema(conn)
    conn.commit()
    previous = conn.execute(
        "SELECT content_hash,file_kind,source_state,profile_version,accepted_generation "
        "FROM _filesystem_source_state "
        "WHERE source_id=?", (result.source_id,),
    ).fetchone()
    if previous and tuple(previous[:4]) == (
        result.content_hash, result.file_kind, result.status, DOCUMENT_PROFILE_VERSION,
    ):
        try:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT content_hash,file_kind,source_state,profile_version,"
                "accepted_generation FROM _filesystem_source_state WHERE source_id=?",
                (result.source_id,),
            ).fetchone()
            if current is None or tuple(current) != tuple(previous):
                actual_generation = int(current[4]) if current else 0
                raise PublicationConflict(
                    f"stale source generation for {result.source_id}: expected "
                    f"{int(previous[4])}, found {actual_generation}"
                )
            _mint_identity(conn, result)
            conn.execute(
                "UPDATE _filesystem_source_state SET "
                "source_path=?,size_bytes=?,mtime_ns=?,extraction_state=? "
                "WHERE source_id=?",
                (
                    result.source_path, result.size_bytes, result.mtime_ns,
                    result.extraction_state, result.source_id,
                ),
            )
            timestamp = result.mtime_ns // 1_000_000_000
            conn.execute(
                "UPDATE _raw_sources SET timestamp=? WHERE source_id=?",
                (timestamp, result.source_id),
            )
            conn.execute(
                "UPDATE _raw_chunks SET timestamp=? WHERE id IN "
                "(SELECT chunk_id FROM _edges_source WHERE source_id=?)",
                (timestamp, result.source_id),
            )
            if result.markdown is not None:
                conn.execute(
                    "UPDATE _types_markdown_source SET file_modified=? "
                    "WHERE source_id=?",
                    (result.markdown.file_modified, result.source_id),
                )
            if sidecar_callback is not None:
                sidecar_callback(
                    conn,
                    _event_for_result(
                        conn, result, "unchanged", int(previous[4]),
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
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
        sidecar_callback=sidecar_callback,
        expected_generation=int(previous[4]) if previous else 0,
        _defer_wikilinks=_defer_wikilinks,
        _schema_ready=True,
    )


def delete_source(conn: sqlite3.Connection, source_id: str, *, obsidian: bool = False,
                  sidecar_callback: SidecarCallback | None = None,
                  expected_generation: int | None = None,
                  _defer_wikilinks: bool = False) -> bool:
    """Commit deletion of one vanished source and all of its optional artifacts."""
    ensure_schema(conn)
    conn.commit()
    try:
        conn.execute("BEGIN IMMEDIATE")
        previous = conn.execute(
            "SELECT source_path,file_kind,content_hash,size_bytes,mtime_ns,"
            "accepted_generation FROM _filesystem_source_state WHERE source_id=?",
            (source_id,),
        ).fetchone()
        if not previous:
            if expected_generation not in (None, 0):
                raise PublicationConflict(
                    f"stale source generation for {source_id}: expected "
                    f"{expected_generation}, found 0"
                )
            conn.rollback()
            return False
        current_generation = int(previous[5])
        if (expected_generation is not None
                and current_generation != expected_generation):
            raise PublicationConflict(
                f"stale source generation for {source_id}: expected "
                f"{expected_generation}, found {current_generation}"
            )
        identity = conn.execute(
            "SELECT file_uuid FROM _edges_fs_identity WHERE source_id=?", (source_id,)
        ).fetchone()
        chunks = conn.execute(
            "SELECT COUNT(*) FROM _edges_source WHERE source_id=?", (source_id,)
        ).fetchone()[0]
        _delete_source_rows(conn, source_id, drop_identity=True, drop_state=True)
        if previous[1] == "markdown" and not _defer_wikilinks:
            _resolve_wikilinks(conn)
        if sidecar_callback is not None:
            sidecar_callback(conn, PublicationEvent(
                status="removed",
                source_id=source_id,
                source_path=previous[0],
                file_kind=previous[1],
                content_hash=previous[2],
                size_bytes=int(previous[3]),
                mtime_ns=int(previous[4]),
                chunks=int(chunks),
                file_uuid=identity[0] if identity else None,
                accepted_generation=current_generation + 1,
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return True
