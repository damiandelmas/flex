"""Heading-aware markdown chunker with breadcrumb context and char offsets."""

import re
from dataclasses import dataclass, field

from flex.compile.markdown import normalize_headers, split_sections

from flex.modules.markdown.compile.tags import strip_code_blocks, strip_tags_for_embedding

# ─── Constants ────────────────────────────────────────────────────────────────

MIN_NOTE_SIZE = 200       # notes shorter than this become a single chunk
MIN_CHUNK_CONTENT = 30    # sections below this merge into previous
MAX_SECTION_CHARS = 4000  # sliding window threshold (Latin)
MAX_SECTION_CJK = 2000    # sliding window threshold (CJK-dominant)
SLIDE_OVERLAP = 200       # overlap chars for sliding window

SKIP_PATTERNS = [
    re.compile(r'^#{1,6}\s*$'),
    re.compile(r'^-{3,}$'),
    re.compile(r'^(TODO|FIXME|NOTE):?\s*$'),
    re.compile(r'^\[\[.+\]\]$'),
    re.compile(r'^!\[.*\]\(.+\)$'),
]

TEMPLATER_RE = re.compile(r'<%[\s\S]*?%>')
HANDLEBARS_RE = re.compile(r'\{\{[\s\S]*?\}\}')
FOOTNOTE_DEF_RE = re.compile(r'^\[\^[^\]]+\]:.*$', re.MULTILINE)
WIKILINK_DISPLAY_RE = re.compile(r'\[\[([^\]|]+)(?:\|([^\]]+))?\]\]')
HEADING_RE = re.compile(r'^(#{1,6})\s')


# ─── Dataclass ────────────────────────────────────────────────────────────────

@dataclass
class ChunkEntry:
    content: str            # heading chain prefix + cleaned body (for embedding)
    raw_content: str        # original markdown with heading prefix (for storage)
    section_title: str      # heading text (empty for preamble)
    heading_depth: int      # 0=preamble, 1-6
    heading_chain: list     # full breadcrumb trail
    position: int           # sequential index within the file
    word_count: int = 0
    char_start: int = 0
    char_end: int = 0


# ─── Cleaning (ephemeral — never modifies source files) ──────────────────────

def strip_template_syntax(text: str) -> str:
    text = TEMPLATER_RE.sub('', text)
    text = HANDLEBARS_RE.sub('', text)
    return text


def strip_footnotes_for_embedding(text: str) -> str:
    return FOOTNOTE_DEF_RE.sub('', text)


def resolve_wikilinks_for_display(text: str) -> str:
    """Replace [[wikilinks]] with display text for embedding."""
    def _replace(m):
        target = m.group(1)
        alias = m.group(2)
        if alias:
            return alias
        if '#' in target:
            parts = target.split('#', 1)
            return f"{parts[0]} > {parts[1]}" if parts[0] else parts[1]
        return target
    return WIKILINK_DISPLAY_RE.sub(_replace, text)


def _clean_for_embedding(text: str) -> str:
    """Full cleaning pipeline for embedding input. Ephemeral."""
    text = strip_template_syntax(text)
    text = resolve_wikilinks_for_display(text)
    text = strip_tags_for_embedding(text)
    text = strip_footnotes_for_embedding(text)
    return text.strip()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_cjk_dominant(text: str) -> bool:
    """True if >30% of chars are CJK."""
    if not text:
        return False
    cjk = sum(1 for c in text if '\u4E00' <= c <= '\u9FFF'
              or '\uAC00' <= c <= '\uD7A3'
              or '\u3040' <= c <= '\u30FF')
    return cjk / len(text) > 0.3


def _should_skip(body: str) -> bool:
    """True if section body is trivial / non-content."""
    stripped = body.strip()
    if len(stripped) < MIN_CHUNK_CONTENT:
        return True
    for pat in SKIP_PATTERNS:
        if pat.match(stripped):
            return True
    return False


def _sliding_window(text: str, note_title: str, heading_chain: list,
                    heading_depth: int, section_title: str,
                    start_position: int) -> list[ChunkEntry]:
    """Split oversized section into overlapping windows."""
    max_chars = MAX_SECTION_CJK if is_cjk_dominant(text) else MAX_SECTION_CHARS
    chunks = []
    prefix = " > ".join([note_title] + heading_chain) if heading_chain else note_title
    offset = 0
    pos = start_position

    while offset < len(text):
        end = min(offset + max_chars, len(text))
        window = text[offset:end]

        cleaned = _clean_for_embedding(window)
        content = f"{prefix}\n{cleaned}" if prefix else cleaned

        chunks.append(ChunkEntry(
            content=content,
            raw_content=f"{prefix}\n{window}" if prefix else window,
            section_title=section_title,
            heading_depth=heading_depth,
            heading_chain=list(heading_chain),
            position=pos,
            word_count=len(window.split()),
        ))
        pos += 1

        if end >= len(text):
            break
        offset = end - SLIDE_OVERLAP

    return chunks


# ─── Main ─────────────────────────────────────────────────────────────────────

def chunk_markdown(body: str, note_title: str) -> list[ChunkEntry]:
    """Split markdown body into heading-aware chunks.

    Uses split_sections(return_depth=True) from flex.compile.markdown.
    Applies 6-slot heading tracker for breadcrumb chains.
    Content is cleaned for embedding; raw_content keeps original markdown.
    """
    # Small notes → single chunk
    content_len = len(strip_code_blocks(body).strip())
    if content_len < MIN_NOTE_SIZE:
        cleaned = _clean_for_embedding(body)
        content = f"{note_title}\n{cleaned}" if note_title else cleaned
        return [ChunkEntry(
            content=content,
            raw_content=f"{note_title}\n{body}" if note_title else body,
            section_title='',
            heading_depth=0,
            heading_chain=[],
            position=0,
            word_count=len(body.split()),
        )]

    normalized = normalize_headers(body)
    sections = split_sections(normalized, level=1, return_depth=True)

    if not sections:
        cleaned = _clean_for_embedding(body)
        content = f"{note_title}\n{cleaned}" if note_title else cleaned
        return [ChunkEntry(
            content=content,
            raw_content=f"{note_title}\n{body}" if note_title else body,
            section_title='',
            heading_depth=0,
            heading_chain=[],
            position=0,
            word_count=len(body.split()),
        )]

    chunks = []
    heading_slots = [None] * 6  # H1-H6
    position = 0

    for title, section_body, _pos, depth in sections:
        # Update heading slots
        if depth > 0:
            heading_slots[depth - 1] = title
            for i in range(depth, 6):
                heading_slots[i] = None

        heading_chain = [h for h in heading_slots if h is not None]

        # Skip trivial sections — merge intent handled by caller if needed
        if _should_skip(section_body) and position > 0:
            continue

        # Check for oversized sections
        max_chars = MAX_SECTION_CJK if is_cjk_dominant(section_body) else MAX_SECTION_CHARS
        if len(section_body) > max_chars:
            window_chunks = _sliding_window(
                section_body, note_title, heading_chain,
                depth, title, position
            )
            chunks.extend(window_chunks)
            position += len(window_chunks)
            continue

        # Normal chunk
        prefix = " > ".join([note_title] + heading_chain) if heading_chain else note_title
        cleaned = _clean_for_embedding(section_body)
        content = f"{prefix}\n{cleaned}" if prefix else cleaned

        item_type = 'preamble' if depth == 0 and not title else 'section'

        chunks.append(ChunkEntry(
            content=content,
            raw_content=f"{prefix}\n{section_body}" if prefix else section_body,
            section_title=title,
            heading_depth=depth,
            heading_chain=list(heading_chain),
            position=position,
            word_count=len(section_body.split()),
        ))
        position += 1

    return chunks


# ─── Char Offsets (decoupled from cleaning pipeline) ─────────────────────────

def compute_char_offsets(raw_text: str, chunks: list[ChunkEntry]) -> None:
    """Assign char_start/char_end from the raw source file.

    Uses heading positions as anchors, matched sequentially with chunks.
    Mutates chunks in-place.
    """
    # Find all heading positions in the raw file (outside code fences and math blocks)
    heading_positions = []
    in_code = False
    in_math = False

    for m in re.finditer(r'^(```|#{1,6}\s|\$\$)', raw_text, re.MULTILINE):
        token = m.group(1)
        if token == '```':
            in_code = not in_code
        elif token == '$$':
            in_math = not in_math
        elif not in_code and not in_math and token.startswith('#'):
            heading_positions.append(m.start())

    # Match headings to chunks
    h_idx = 0
    for chunk in chunks:
        if chunk.heading_depth == 0 and chunk.position == 0:
            # Preamble: starts at 0
            chunk.char_start = 0
            chunk.char_end = heading_positions[0] if heading_positions else len(raw_text)
        elif h_idx < len(heading_positions):
            chunk.char_start = heading_positions[h_idx]
            h_idx += 1
            chunk.char_end = heading_positions[h_idx] if h_idx < len(heading_positions) else len(raw_text)
        else:
            # Fallback for sliding window sub-chunks
            chunk.char_start = chunks[chunk.position - 1].char_end if chunk.position > 0 else 0
            chunk.char_end = len(raw_text)
