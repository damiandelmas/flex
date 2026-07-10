"""Tag extraction and merging for markdown files."""

import re

# Unicode-aware: [^\W\d] matches any Unicode letter or underscore (no digits).
# Supports CJK, Arabic, Korean, Latin, etc.
#
# A tag is `#` + a letter-led [\w-/] run, where `#` is NOT preceded by a word char
# or another `#`. The zero-width lookbehind admits a tag after ANY non-word boundary
# (start, whitespace, `[`, `(`, `>`, `|`, `"`, `-`, table cells, …) — the previous
# fixed `[\s,;(]` prefix missed ~45% of real inline tags. It still excludes mid-word
# `word#x` (URL fragments), `##heading`, and `#1` (issue refs — digit-led).
INLINE_TAG_RE = re.compile(r'(?<![\w#])#([^\W\d][\w\-/]*)', re.UNICODE)

_FENCED_CODE_RE = re.compile(r'```[\s\S]*?```')
# Inline code is single-line: `[^`\n]+`. Spanning newlines let ONE stray backtick eat
# a multi-line span (incl. real #tags) on backtick-heavy vaults — the tag-under-capture.
_INLINE_CODE_RE = re.compile(r'`[^`\n]+`')
# A list-item line (so an INDENTED list item is not mistaken for an indented code block
# and its #tags survive). Matches `- `, `* `, `+ `, `1. `, `1) `.
_LIST_MARKER_RE = re.compile(r'^(?:[-*+]|\d+[.)])\s')


def _strip_indented_code(text: str) -> str:
    """Remove indented code blocks (a run of >=4-space / tab-indented, non-list lines
    set off by a blank line — the CommonMark indented-code form). Bash tests like
    `[[ -z "$x" ]]` live here on code-heavy vaults and must not become wikilinks. The
    blank-line-precedence + list-marker guards protect real (contiguous / list) content:
    a nested list item stays, only genuine set-off code blocks are dropped."""
    out, prev_blank, in_code = [], True, False
    for ln in text.split('\n'):
        blank = not ln.strip()
        indented = ln[:4] == '    ' or ln[:1] == '\t'
        is_list = bool(_LIST_MARKER_RE.match(ln.strip()))
        if indented and not blank and not is_list and (prev_blank or in_code):
            in_code = True
            out.append('')            # drop the code line
        else:
            if not blank:
                in_code = False        # a non-blank, non-code line ends the block
            out.append(ln)
        prev_blank = blank
    return '\n'.join(out)


def strip_code_blocks(text: str) -> str:
    """Remove fenced (``` ```), inline (` `), and indented (4-space/tab) code."""
    text = _FENCED_CODE_RE.sub('', text)
    text = _INLINE_CODE_RE.sub('', text)
    text = _strip_indented_code(text)
    return text


# CSS hex colors (#d4763c, #ffffff) are letter-led so INLINE_TAG_RE captures them as
# tags. Drop the canonical 3/6 all-hex shape — a real tag being exactly 3-or-6 all-hex
# chars is vanishingly rare (#faced is 5, #cafe is 4). Only prose/inline hex reaches
# here (code hex is already code-stripped).
_HEX_SHAPE_RE = re.compile(r'(?:[0-9a-f]{3}|[0-9a-f]{6})$', re.IGNORECASE)


def _is_hex_color(tag: str) -> bool:
    return bool(re.fullmatch(r'[0-9a-f]{3}|[0-9a-f]{6}', tag, re.IGNORECASE))


def extract_inline_tags(body: str) -> list[str]:
    """Extract #tags from markdown body, excluding code blocks and CSS hex colors."""
    stripped = strip_code_blocks(body)
    return list(dict.fromkeys(
        m.casefold() for m in INLINE_TAG_RE.findall(stripped)
        if m and not _is_hex_color(m)
    ))


def merge_tags(frontmatter_tags: list[str], inline_tags: list[str]) -> str:
    """Deduplicate, sort, return comma-joined string for types table."""
    seen = {}
    for t in frontmatter_tags + inline_tags:
        norm = t.casefold().strip()
        if norm:
            seen[norm] = True
    return ','.join(sorted(seen))


def strip_tags_for_embedding(text: str) -> str:
    """Remove #tags from text (tags stored separately in types table). The match is
    now exactly `#tag` (zero-width lookbehind), so surrounding text/whitespace is
    preserved without special-casing a captured separator char."""
    return INLINE_TAG_RE.sub('', text)
