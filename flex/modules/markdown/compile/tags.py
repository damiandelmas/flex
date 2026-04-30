"""Tag extraction and merging for markdown files."""

import re

# Unicode-aware: [^\W\d] matches any Unicode letter or underscore (no digits).
# Supports CJK, Arabic, Korean, Latin, etc.
INLINE_TAG_RE = re.compile(
    r'(?:^|[\s,;(])#([^\W\d][\w\-/]*)', re.MULTILINE | re.UNICODE
)

_FENCED_CODE_RE = re.compile(r'```[\s\S]*?```')
_INLINE_CODE_RE = re.compile(r'`[^`]+`')


def strip_code_blocks(text: str) -> str:
    """Remove fenced (``` ```) and inline (` `) code from text."""
    text = _FENCED_CODE_RE.sub('', text)
    text = _INLINE_CODE_RE.sub('', text)
    return text


def extract_inline_tags(body: str) -> list[str]:
    """Extract #tags from markdown body, excluding code blocks."""
    stripped = strip_code_blocks(body)
    return list(dict.fromkeys(
        m.casefold() for m in INLINE_TAG_RE.findall(stripped) if m
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
    """Remove #tags from text (tags stored separately in types table)."""
    return INLINE_TAG_RE.sub(lambda m: m.group(0)[0] if m.group(0)[0] != '#' else '', text)
