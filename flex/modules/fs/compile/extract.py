"""Pure file extraction for the mixed filesystem compiler."""

from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from flex.modules.fs.compile.walker import FileEntry


@dataclass(frozen=True)
class ExtractedChunk:
    id: str
    content: str
    chunk_kind: str
    section_title: str | None
    section_type: str | None
    position: int
    depth: int
    container_id: str | None
    content_hash: str
    language: str | None


@dataclass(frozen=True)
class ExtractedSymbol:
    name: str
    def_id: str
    kind: str | None = None


@dataclass(frozen=True)
class ExtractedCall:
    caller_id: str
    callee_name: str


@dataclass(frozen=True)
class ExtractedField:
    chunk_id: str
    key: str
    value: str


@dataclass(frozen=True)
class MarkdownMetadata:
    folder: str
    tags: tuple[str, ...]
    aliases: tuple[str, ...]
    note_created: str | None
    file_modified: str


@dataclass(frozen=True)
class ExtractionResult:
    source_id: str
    source_path: str
    title: str
    file_kind: str
    status: str
    extraction_state: str
    content_hash: str
    size_bytes: int
    mtime_ns: int
    chunks: tuple[ExtractedChunk, ...] = ()
    symbols: tuple[ExtractedSymbol, ...] = ()
    calls: tuple[ExtractedCall, ...] = ()
    imports: tuple[tuple[str, str | None], ...] = ()
    markdown: MarkdownMetadata | None = None
    fields: tuple[ExtractedField, ...] = ()
    wikilinks: tuple[str, ...] = ()
    error: str | None = None


def _chunk(source_id: str, content: str, position: int, *, chunk_kind: str,
           title: str | None = None, section_type: str | None = None,
           depth: int = 0, parent: str | None = None,
           language: str | None = None) -> ExtractedChunk:
    from flex.sdk import _make_chunk_id
    cid = _make_chunk_id(source_id, position, content)
    return ExtractedChunk(
        id=cid, content=content, chunk_kind=chunk_kind, section_title=title,
        section_type=section_type, position=position, depth=depth,
        container_id=parent, content_hash=hashlib.sha256(content.encode()).hexdigest(),
        language=language,
    )


def _markdown(entry: FileEntry, text: str, digest: str) -> ExtractionResult:
    from flex.modules.markdown.compile.chunker import chunk_markdown, compute_char_offsets
    from flex.modules.markdown.compile.dataview import extract_dataview_fields
    from flex.modules.markdown.compile.frontmatter import (
        extract_aliases, extract_created_date, extract_tags, parse_frontmatter,
    )
    from flex.modules.markdown.compile.tags import extract_inline_tags, merge_tags
    from flex.modules.markdown.compile.wikilinks import extract_raw_wikilinks

    fm, body = parse_frontmatter(text)
    parsed = chunk_markdown(body, entry.path.stem)
    compute_char_offsets(text, parsed)
    chunks = []
    parent_at_depth: dict[int, str] = {}
    fields = []
    for pos, part in enumerate(parsed):
        depth = int(part.heading_depth or 0)
        shallower = [known for known in parent_at_depth if known < depth]
        parent = parent_at_depth[max(shallower)] if shallower else None
        kind = "full_note" if len(parsed) == 1 else (
            "preamble" if depth == 0 and not part.section_title else "section"
        )
        item = _chunk(
            entry.source_id, part.content, pos, chunk_kind=kind,
            title=part.section_title, section_type="markdown", depth=depth,
            parent=parent, language="markdown",
        )
        chunks.append(item)
        if depth > 0:
            parent_at_depth[depth] = item.id
            for old_depth in [d for d in parent_at_depth if d > depth]:
                del parent_at_depth[old_depth]
        fields.extend(
            ExtractedField(item.id, key, value)
            for key, value in extract_dataview_fields(part.raw_content)
        )
    merged_tags = merge_tags(extract_tags(fm), extract_inline_tags(body))
    tags = tuple(tag for tag in merged_tags.split(",") if tag)
    aliases = extract_aliases(fm)
    meta = MarkdownMetadata(
        folder=Path(entry.rel_path).parent.as_posix() if "/" in entry.rel_path else "",
        tags=tags, aliases=tuple(aliases), note_created=extract_created_date(fm),
        file_modified=datetime.fromtimestamp(
            entry.mtime_ns / 1_000_000_000, tz=timezone.utc
        ).isoformat(),
    )
    return ExtractionResult(
        entry.source_id, str(entry.path), entry.path.stem, "markdown", "indexed", "ok",
        digest, entry.size_bytes, entry.mtime_ns, tuple(chunks), markdown=meta,
        fields=tuple(fields), wikilinks=tuple(extract_raw_wikilinks(body)),
    )


def _code(entry: FileEntry, text: str, digest: str) -> ExtractionResult:
    from flex.compile.chunkers import _build_code_tree, _build_code_tree_ts
    from flex.modules.fs.compile.index_code import _extract_imports

    ext = entry.path.suffix.lower().lstrip(".")
    degraded = False
    if ext == "py":
        try:
            ast.parse(text)
        except SyntaxError:
            degraded = True
        nodes = _build_code_tree(entry.source_id, text)
    else:
        nodes = _build_code_tree_ts(entry.source_id, text, ext)
        try:
            from flex.compile.chunkers import _ts_language
            from tree_sitter import Parser
            language = _ts_language(ext)
            degraded = language is None or Parser(language).parse(text.encode()).root_node.has_error
        except Exception:
            degraded = True
    chunks = tuple(
        _chunk(
            entry.source_id, node.get("content", ""), pos, chunk_kind="code",
            title=node.get("section_title"), section_type="code",
            depth=int(node.get("depth") or 0), parent=node.get("container_id"),
            language=ext,
        )
        for pos, node in enumerate(nodes)
    )
    # Normalize node ids because _chunk owns deterministic common ids.
    node_ids = {node.get("id"): chunks[pos].id for pos, node in enumerate(nodes)}
    chunks = tuple(
        ExtractedChunk(
            c.id, c.content, c.chunk_kind, c.section_title, c.section_type, c.position,
            c.depth, node_ids.get(c.container_id, c.container_id), c.content_hash, c.language,
        ) for c in chunks
    )
    symbols = tuple(
        ExtractedSymbol(node.get("section_title"), chunks[pos].id)
        for pos, node in enumerate(nodes)
        if node.get("section_title") and not node["section_title"].startswith("(")
    )
    calls = tuple(
        ExtractedCall(chunks[pos].id, name)
        for pos, node in enumerate(nodes) for name in node.get("_calls", ())
    )
    imports = tuple(
        (module, name)
        for _source, module, name in _extract_imports(str(entry.path), ext, text=text)
    )
    return ExtractionResult(
        entry.source_id, str(entry.path), entry.path.name, "code", "indexed",
        "degraded" if degraded else "ok", digest, entry.size_bytes, entry.mtime_ns,
        chunks, symbols, calls, imports,
    )


def extract_file(entry: FileEntry) -> ExtractionResult:
    """Extract one discovered file without touching SQLite or embedding state."""
    try:
        raw = entry.path.read_bytes()
    except OSError as exc:
        return ExtractionResult(
            entry.source_id, str(entry.path), entry.path.name, entry.file_kind, "failed",
            "failed", "", entry.size_bytes, entry.mtime_ns, error=str(exc),
        )
    digest = hashlib.sha256(raw).hexdigest()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return ExtractionResult(
            entry.source_id, str(entry.path), entry.path.name, entry.file_kind, "failed",
            "failed", digest, entry.size_bytes, entry.mtime_ns, error=str(exc),
        )
    if not text.strip():
        return ExtractionResult(
            entry.source_id, str(entry.path), entry.path.name, entry.file_kind, "empty", "ok",
            digest, entry.size_bytes, entry.mtime_ns,
        )
    if entry.file_kind == "markdown":
        return _markdown(entry, text, digest)
    if entry.file_kind == "code":
        return _code(entry, text, digest)
    chunk = _chunk(
        entry.source_id, text, 0, chunk_kind="text", title=entry.path.name,
        section_type="text", language=entry.path.suffix.lower().lstrip(".") or None,
    )
    return ExtractionResult(
        entry.source_id, str(entry.path), entry.path.name, "text", "indexed", "ok",
        digest, entry.size_bytes, entry.mtime_ns, (chunk,),
    )
