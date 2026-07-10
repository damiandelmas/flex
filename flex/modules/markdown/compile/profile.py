"""
IngestProfile — the seam object for the one markdown ingestion pipeline.

A profile carries *how to label* (classify) and *which types table*, plus the
per-flavor hooks (boundary frame-reset, sidecar). It is pure declaration +
callables: no DB handle, no embedding, no pipeline internals (substrate
guardrail #1 — the profile/resolver is the knob, the pipeline is fixed).

Three flavors of one pipeline:
  - markdown_profile()  default; today's compile_vault behavior (no-op seam).
  - obsidian_profile()  markdown + wikilinks/dataview/vault-exclude.
  - docpac_profile()    temporal/doc_type spine, boundary frame-reset, sidecar.

docpac_profile() lazy-imports the docpac classify/parse modules inside the
factory body so the PUBLIC markdown module stays importable without docpac
present (markdown/obsidian factories never touch docpac). compile_vault is wired
to honor a profile in S3; this node only defines the contract + factories.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional


@dataclass(frozen=True)
class IngestProfile:
    """The single axis of variation over the markdown pipeline.

    classify_source(entry, frontmatter) -> dict   → source-level typed columns
    classify_chunk(entry, frontmatter, chunk) -> dict → types_table row
    boundary_hook  walker plugin (e.g. docpac boundary frame-reset) or None
    sidecar_loader (entries, root, get_file_uuid, ...) -> int, or None
    """
    cell_type: str
    types_table: str
    schema_extra: str
    classify_source: Callable
    classify_chunk: Callable
    boundary_hook: Optional[Callable] = None
    sidecar_loader: Optional[Callable] = None
    views_dir: Optional[Path] = None
    presets_dirs: tuple = field(default_factory=tuple)
    split_level: int = 1
    graph_threshold: float = 0.55


# ════════════════════════════════════════════════════════════════════
# markdown / obsidian — the public default flavors
# ════════════════════════════════════════════════════════════════════

def _markdown_classify_chunk(entry, frontmatter, chunk) -> dict:
    """Reproduce compile_vault's per-chunk `_types_markdown` row.

    `chunk` is a markdown ChunkEntry; `entry` a VaultEntry.
    """
    section_title = getattr(chunk, 'section_title', None)
    heading_depth = getattr(chunk, 'heading_depth', 0)
    heading_chain = getattr(chunk, 'heading_chain', None) or []
    stem = getattr(entry, 'stem', None)
    single = getattr(chunk, '_only_chunk', False)
    if heading_depth == 0 and not section_title:
        item_type = 'full_note' if single else 'preamble'
    else:
        item_type = 'section'
    return {
        'item_type': item_type,
        'note_title': stem,
        'section_title': section_title,
        'heading_depth': heading_depth,
        'heading_chain': ' > '.join([stem] + heading_chain) if heading_chain else stem,
        'word_count': getattr(chunk, 'word_count', None),
        'char_start': getattr(chunk, 'char_start', None),
        'char_end': getattr(chunk, 'char_end', None),
    }


def _markdown_classify_source(entry, frontmatter) -> dict:
    """Reproduce compile_vault's `_types_markdown_source` row."""
    return {
        'folder': getattr(entry, 'folder', None),
        'tags': frontmatter.get('_all_tags'),      # merged tags, passed by caller
        'aliases': frontmatter.get('_aliases'),
        'note_created': frontmatter.get('_created_date'),
        'file_modified': frontmatter.get('_file_modified'),
    }


_MARKDOWN_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _types_markdown (
    chunk_id TEXT PRIMARY KEY,
    item_type TEXT, note_title TEXT, section_title TEXT,
    heading_depth INTEGER, heading_chain TEXT, word_count INTEGER,
    char_start INTEGER, char_end INTEGER
);
CREATE TABLE IF NOT EXISTS _types_markdown_source (
    source_id TEXT PRIMARY KEY,
    folder TEXT, tags TEXT, aliases TEXT, note_created TEXT, file_modified TEXT
);
"""


def _markdown_stock():
    stock = Path(__file__).resolve().parent.parent / 'stock'
    views = stock / 'views' if (stock / 'views').exists() else None
    presets = (stock / 'presets',) if (stock / 'presets').exists() else ()
    return views, presets


def markdown_profile() -> IngestProfile:
    """The default — today's compile_vault behavior, no-op seam."""
    views, presets = _markdown_stock()
    return IngestProfile(
        cell_type='markdown',
        types_table='_types_markdown',
        schema_extra=_MARKDOWN_SCHEMA,
        classify_source=_markdown_classify_source,
        classify_chunk=_markdown_classify_chunk,
        views_dir=views,
        presets_dirs=presets,
        split_level=1,
    )


def obsidian_profile() -> IngestProfile:
    """Markdown + wikilinks/dataview/vault-exclude. Same pipeline, vault flavor.

    Differs from markdown only in cell_type today; the wikilink/dataview/heading
    side-tables are carried by compile_vault's existing flavor steps, gated on
    profile.cell_type in S3. Kept a distinct profile so the flag stops being
    magic and becomes the knob.
    """
    base = markdown_profile()
    return IngestProfile(
        cell_type='obsidian',
        types_table=base.types_table,
        schema_extra=base.schema_extra,
        classify_source=base.classify_source,
        classify_chunk=base.classify_chunk,
        views_dir=base.views_dir,
        presets_dirs=base.presets_dirs,
        split_level=base.split_level,
    )


# ════════════════════════════════════════════════════════════════════
# docpac — the temporal/doc_type flavor (parity-gated)
# ════════════════════════════════════════════════════════════════════

_DOCPAC_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _types_docpac (
    chunk_id TEXT PRIMARY KEY,
    temporal TEXT, doc_type TEXT, facet TEXT, section_title TEXT,
    yaml_type TEXT, yaml_status TEXT
);
"""


def _docpac_classify_source(entry, frontmatter) -> dict:
    """Reproduce docpac/init.py's `_raw_sources` typed columns.

    Applies the highest-priority frontmatter doc_type override (mutating entry
    so classify_chunk sees the merged temporal/doc_type), then merges
    frontmatter > sidecar (_meta) > defaults for the metric columns. Lifted
    verbatim from docpac/init.py main() so parity holds.
    """
    from flex.modules.docpac.compile.classify import derive_temporal

    fm_doc_type = frontmatter.get('doc_type')
    if fm_doc_type:
        entry.doc_type = fm_doc_type
        entry.temporal = derive_temporal(fm_doc_type, entry.temporal)

    _m = getattr(entry, '_meta', {}) or {}
    keywords = frontmatter.get('keywords', _m.get('keywords'))
    return {
        'file_date':  entry.file_date,
        'temporal':   entry.temporal,
        'doc_type':   entry.doc_type,
        'title':      entry.title,
        'source_path': entry.path,
        'file_uuid':  _m.get('file_uuid'),
        'type':       frontmatter.get('type', _m.get('type')),
        'status':     frontmatter.get('status'),
        'keywords':   ','.join(keywords) if isinstance(keywords, list) else keywords,
        'summary':    frontmatter.get('summary', _m.get('summary')),
        'confidence': frontmatter.get('confidence', _m.get('confidence', 1.0)),
        'validity':   frontmatter.get('validity', _m.get('validity', 1.0)),
        'maturity':   frontmatter.get('maturity', _m.get('maturity', 1.0)),
    }


def _docpac_classify_chunk(entry, frontmatter, chunk) -> dict:
    """Reproduce docpac/init.py's `_types_docpac` row.

    `chunk` is a `(section_title, content, position)` tuple from split_sections.
    yaml_type/yaml_status come from frontmatter DIRECTLY (not the sidecar-merged
    value) — matching init.py's _types_docpac write exactly.
    """
    if isinstance(chunk, (tuple, list)):
        section_title = chunk[0]
    else:
        section_title = getattr(chunk, 'section_title', None)
    return {
        'temporal':    entry.temporal,
        'doc_type':    entry.doc_type,
        'facet':       None,             # assigned by human, not docpac
        'section_title': section_title or None,
        'yaml_type':   frontmatter.get('type'),
        'yaml_status': frontmatter.get('status'),
    }


def docpac_profile() -> IngestProfile:
    """Temporal/doc_type spine: boundary frame-reset + `_flex_types.json` sidecar.

    Lazy-imports docpac so the public markdown module imports without docpac
    present. Called only from docpac's own context until the physical merge (S6).
    """
    from flex.modules.docpac.compile.docpac import parse_docpac
    from flex.modules.docpac.compile.classify import apply_sidecar_overrides

    stock = Path(__file__).resolve().parent.parent.parent / 'docpac' / 'stock'
    views = stock / 'views' if (stock / 'views').exists() else None
    presets = (stock / 'presets',) if (stock / 'presets').exists() else ()

    return IngestProfile(
        cell_type='docpac',
        types_table='_types_docpac',
        schema_extra=_DOCPAC_SCHEMA,
        classify_source=_docpac_classify_source,
        classify_chunk=_docpac_classify_chunk,
        boundary_hook=parse_docpac,
        sidecar_loader=apply_sidecar_overrides,
        views_dir=views,
        presets_dirs=presets,
        split_level=1,  # lossless default: split at every heading (return_depth)
    )
