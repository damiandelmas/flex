"""Additive Obsidian enrichment over a compiled docpac cell.

Obsidian vaults ride the docpac engine (chunk-model A: per-heading). The
obsidian-specific metadata — wikilinks, dataview fields, tags, aliases — is a
UNIFORM ADDITIVE ENRICHMENT applied as a post-compile pass. It only ADDS rows to
the four enrichment tables below; it MUST NEVER mutate `_raw_chunks`
content/boundaries, `_edges_source`, `_types_docpac`, or `_raw_sources` content.
This is the parity guardrail: the invariant projection (source_path, position,
content) is byte-identical before and after enrichment.

The only tables this module writes:
  - _fields_inline          (dataview fields, tags, aliases)
  - _edges_wikilink_raw     (transient — consumed + dropped by resolution)
  - _edges_wikilink         (resolved links)
  - _edges_wikilink_unresolved (ghost links)

Extractors are REUSED verbatim from flex.modules.markdown.compile — nothing is
reimplemented here.

Grain note (why the resolution adapter exists)
----------------------------------------------
In a markdown cell, `source_id == rel_path`, so `resolve_all_wikilinks` writes
`from_path`/`to_path` that are simultaneously paths and source ids, and
`build_combined_graph` joins them straight against `_raw_sources.source_id`. A
docpac cell keys sources by a content hash, not the path, so the two id-spaces
diverge. To keep the resolved edges in the cell's OWN id-space (joinable to
`_edges_source`/`_raw_sources`, graph-compatible), the thin adapter feeds
`resolve_all_wikilinks` VaultEntry-like objects whose `.rel_path` IS the docpac
`source_id` and whose `.stem` is the note stem. Obsidian's native resolution is
basename/alias/title-first (the `[[Note Name]]` model), which the stem drives
exactly; exact-path resolution simply falls through to basename. The function
itself is reused unmodified.
"""

from pathlib import Path

from flex.modules.markdown.compile.frontmatter import (
    parse_frontmatter,
    extract_tags,
    extract_aliases,
)
from flex.modules.markdown.compile.tags import extract_inline_tags
from flex.modules.markdown.compile.dataview import extract_dataview_fields
from flex.modules.markdown.compile.wikilinks import (
    extract_raw_wikilinks,
    resolve_all_wikilinks,
)


# ── Enrichment-table DDL ──────────────────────────────────────────────────────
# _fields_inline + _edges_wikilink_raw copied verbatim from
# flex/modules/markdown/compile/init.py:SCHEMA_DDL. _edges_wikilink and
# _edges_wikilink_unresolved copied verbatim from
# flex/modules/markdown/compile/wikilinks.py:resolve_all_wikilinks (which also
# creates them itself — mirrored here so inserts are always safe). These are the
# ONLY tables this pass touches.
_ENRICH_DDL = """
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

CREATE TABLE IF NOT EXISTS _edges_wikilink (
    chunk_id TEXT NOT NULL,
    from_path TEXT NOT NULL,
    to_path TEXT NOT NULL,
    PRIMARY KEY (from_path, to_path, chunk_id)
);
CREATE INDEX IF NOT EXISTS idx_wikilink_to ON _edges_wikilink(to_path);

CREATE TABLE IF NOT EXISTS _edges_wikilink_unresolved (
    from_path TEXT NOT NULL,
    raw_target TEXT NOT NULL,
    PRIMARY KEY (from_path, raw_target)
);
CREATE INDEX IF NOT EXISTS idx_unresolved_target ON _edges_wikilink_unresolved(raw_target);
"""


class _ResolvEntry:
    """VaultEntry-like shim for build_resolution_maps / resolve_all_wikilinks.

    Only `.rel_path` and `.stem` are consumed by the resolver. We deliberately
    set `.rel_path` to the docpac `source_id` so resolved edges land in the
    cell's own id-space (see module docstring).
    """

    __slots__ = ("rel_path", "stem")

    def __init__(self, rel_path: str, stem: str):
        self.rel_path = rel_path
        self.stem = stem


def _entry_path(entry) -> str | None:
    """Normalize a caller-supplied entry to an absolute path string.

    Accepts a str/os.PathLike, or any object exposing `.path` (DocPacEntry has
    a str `.path`; VaultEntry has a Path `.path`).
    """
    p = getattr(entry, "path", entry)
    if p is None:
        return None
    return str(p)


def _resolve_source(conn, path: str):
    """Map a disk path to its (source_id, source_path) row in _raw_sources.

    docpac stores the exact `file_path` string it was handed as `source_path`,
    so we match on that first, then on the resolved absolute path as a fallback
    for callers that pass a differently-spelled but equivalent path.
    """
    row = conn.execute(
        "SELECT source_id, source_path FROM _raw_sources WHERE source_path = ?",
        (path,),
    ).fetchone()
    if row:
        return row[0], row[1]
    resolved = str(Path(path).resolve())
    if resolved != path:
        row = conn.execute(
            "SELECT source_id, source_path FROM _raw_sources WHERE source_path = ?",
            (resolved,),
        ).fetchone()
        if row:
            return row[0], row[1]
    return None


def _first_chunk_id(conn, source_id: str) -> str | None:
    """The note-level (position 0) chunk_id for a source, via _edges_source.

    Tags/aliases are SOURCE-grain, so they key to the source's first chunk.
    """
    row = conn.execute(
        "SELECT chunk_id FROM _edges_source WHERE source_id = ? "
        "ORDER BY position, rowid LIMIT 1",
        (source_id,),
    ).fetchone()
    return row[0] if row else None


def _ordered_chunks(conn, source_id: str) -> list[tuple[str, str]]:
    """(chunk_id, content) for a source, in document (position) order."""
    return conn.execute(
        "SELECT es.chunk_id, rc.content "
        "FROM _edges_source es JOIN _raw_chunks rc ON rc.id = es.chunk_id "
        "WHERE es.source_id = ? ORDER BY es.position, es.rowid",
        (source_id,),
    ).fetchall()


def enrich_obsidian(conn, entries, corpus_root=None) -> dict:
    """Additive obsidian enrichment over a compiled docpac cell.

    Reads source bodies from disk (via `_raw_sources.source_path`) and adds:
      - dataview  → per chunk: extract_dataview_fields(chunk_content) rows in
                    _fields_inline.
      - tags      → per source: dedup(extract_tags(frontmatter) +
                    extract_inline_tags(body)), one _fields_inline row per tag
                    (field_key='tag'), keyed to the source's position-0 chunk.
      - aliases   → per source: extract_aliases(frontmatter), one _fields_inline
                    row per alias (field_key='alias'), keyed like tags.
      - wikilinks → per source: extract_raw_wikilinks(body) → _edges_wikilink_raw,
                    then resolve_all_wikilinks(...) → _edges_wikilink /
                    _edges_wikilink_unresolved.

    Args:
        conn: open sqlite3 connection to a compiled docpac cell.
        entries: iterable of the vault's sources — each a path str/PathLike or an
            object with a `.path` attribute (DocPacEntry / VaultEntry).
        corpus_root: accepted for call-compatibility; source bodies resolve from
            the absolute `source_path` stored on each row, so it is not required.

    Returns:
        counts dict: {wikilink, wikilink_unresolved, dataview, tag, alias}.

    ADDITIVE ONLY — the sole tables written are _fields_inline,
    _edges_wikilink_raw (transient), _edges_wikilink, _edges_wikilink_unresolved.
    """
    conn.executescript(_ENRICH_DDL)

    counts = {
        "wikilink": 0,
        "wikilink_unresolved": 0,
        "dataview": 0,
        "tag": 0,
        "alias": 0,
    }

    # Resolve caller entries → cell sources. Skip anything not in this cell.
    sources = []  # (source_id, disk_path)
    seen_ids = set()
    for entry in entries or []:
        path = _entry_path(entry)
        if not path:
            continue
        resolved = _resolve_source(conn, path)
        if not resolved:
            continue
        source_id, source_path = resolved
        if source_id in seen_ids:
            continue
        seen_ids.add(source_id)
        # Prefer the exact stored source_path for disk reads.
        sources.append((source_id, source_path or path))

    if not sources:
        return counts

    resolv_entries = []          # _ResolvEntry list for wikilink resolution
    aliases_by_path = {}         # {source_id: [alias, ...]} for alias resolution

    for source_id, disk_path in sources:
        try:
            raw_text = Path(disk_path).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        fm, body = parse_frontmatter(raw_text)
        stem = Path(disk_path).stem
        resolv_entries.append(_ResolvEntry(rel_path=source_id, stem=stem))

        note_chunk_id = _first_chunk_id(conn, source_id)

        # ── dataview (per chunk) ──────────────────────────────────────────
        for chunk_id, content in _ordered_chunks(conn, source_id):
            for key, value in extract_dataview_fields(content or ""):
                conn.execute(
                    "INSERT INTO _fields_inline "
                    "(chunk_id, source_id, field_key, field_value) VALUES (?, ?, ?, ?)",
                    (chunk_id, source_id, key, value),
                )
                counts["dataview"] += 1

        # ── tags (per source, frontmatter YAML + body #tag) ───────────────
        # dedup preserves order; both extractors casefold, so exact-dedup is safe.
        tags = list(dict.fromkeys(extract_tags(fm) + extract_inline_tags(body)))
        if note_chunk_id is not None:
            for tag in tags:
                conn.execute(
                    "INSERT INTO _fields_inline "
                    "(chunk_id, source_id, field_key, field_value) VALUES (?, ?, 'tag', ?)",
                    (note_chunk_id, source_id, tag),
                )
                counts["tag"] += 1

            # ── aliases (per source, frontmatter YAML) ────────────────────
            aliases = extract_aliases(fm)
            if aliases:
                aliases_by_path[source_id] = aliases
            for alias in aliases:
                conn.execute(
                    "INSERT INTO _fields_inline "
                    "(chunk_id, source_id, field_key, field_value) VALUES (?, ?, 'alias', ?)",
                    (note_chunk_id, source_id, alias),
                )
                counts["alias"] += 1
        else:
            # No chunk to key source-grain fields to — still capture aliases for
            # wikilink resolution even though we can't store rows.
            aliases = extract_aliases(fm)
            if aliases:
                aliases_by_path[source_id] = aliases

        # ── wikilinks (raw targets) ───────────────────────────────────────
        for target in extract_raw_wikilinks(body):
            conn.execute(
                "INSERT OR IGNORE INTO _edges_wikilink_raw (source_id, raw_target) "
                "VALUES (?, ?)",
                (source_id, target),
            )

    # ── wikilink resolution (reused verbatim) ─────────────────────────────
    # resolve_all_wikilinks reads _edges_wikilink_raw, writes _edges_wikilink /
    # _edges_wikilink_unresolved, and drops the raw table. Because resolv_entries
    # carry rel_path == source_id, the resolved from_path/to_path stay in the
    # cell's own source-id space.
    resolved, unresolved = resolve_all_wikilinks(conn, resolv_entries, aliases_by_path)
    counts["wikilink"] = resolved
    counts["wikilink_unresolved"] = unresolved

    conn.commit()
    return counts
