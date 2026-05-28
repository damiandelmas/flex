"""
LaTeX document parser for flex.

Splits LaTeX source into sections, preserving heading hierarchy.
Analogous to flex.compile.markdown.split_sections() but for .tex files.

Key difference from the markdown parser: this emits tree edges.
Each section knows its parent via heading depth. The hierarchy is:
  \\section > \\subsection > \\subsubsection > \\paragraph

Non-destructive: raw heading text is preserved exactly as written.
Canonical section_type mapping (abstract, methodology, etc.) happens
at the VIEW level, not here.
"""

import re
from dataclasses import dataclass


# LaTeX heading commands in order of depth
HEADING_COMMANDS = [
    (r"\\section\*?\{",        1),
    (r"\\subsection\*?\{",     2),
    (r"\\subsubsection\*?\{",  3),
    (r"\\paragraph\*?\{",      4),
]

# Compiled pattern matching any heading command
_HEADING_RE = re.compile(
    r"^\\((?:sub)*section|paragraph)\*?\{(.+?)\}\s*$",
    re.MULTILINE,
)

# LaTeX inline noise to strip from heading text (labels, refs, etc.)
_LABEL_RE = re.compile(r"\\label\{[^}]*\}")
_TEXCMD_RE = re.compile(r"\\(?:textbf|textit|emph|textrm|textsf)\{([^}]*)\}")


def _clean_heading(text: str) -> str:
    """Strip LaTeX noise from heading text. Non-destructive: raw stored separately."""
    text = _LABEL_RE.sub("", text)
    text = _TEXCMD_RE.sub(r"\1", text)  # unwrap formatting commands
    return text.strip()

# Abstract environment
_ABSTRACT_BEGIN = re.compile(r"\\begin\{abstract\}")
_ABSTRACT_END = re.compile(r"\\end\{abstract\}")

# Document body boundaries
_DOC_BEGIN = re.compile(r"\\begin\{document\}")
_DOC_END = re.compile(r"\\end\{document\}")

# Common LaTeX noise to strip for content (not from raw)
_COMMENT_RE = re.compile(r"(?<!\\)%.*$", re.MULTILINE)


@dataclass
class LatexSection:
    """A section extracted from LaTeX source."""
    heading: str         # raw heading text, e.g. "Experimental Setup"
    heading_command: str  # e.g. "section", "subsection", "subsubsection"
    depth: int           # 1=section, 2=subsection, 3=subsubsection, 4=paragraph
    content: str         # full section content including heading
    position: int        # 0-indexed position in document
    line_start: int      # line number where this section starts


def _heading_depth(command: str) -> int:
    """Map LaTeX command name to depth."""
    depths = {
        "section": 1,
        "subsection": 2,
        "subsubsection": 3,
        "paragraph": 4,
    }
    return depths.get(command, 1)


def extract_body(latex: str) -> str:
    """Extract content between \\begin{document} and \\end{document}.

    If no document environment found, returns the full text
    (some papers are fragments without the preamble).
    """
    begin = _DOC_BEGIN.search(latex)
    end = _DOC_END.search(latex)

    if begin and end:
        return latex[begin.end():end.start()]
    elif begin:
        return latex[begin.end():]
    return latex


def extract_abstract(latex: str) -> str | None:
    """Extract abstract text from \\begin{abstract}...\\end{abstract}.

    Returns None if no abstract environment found.
    """
    begin = _ABSTRACT_BEGIN.search(latex)
    end = _ABSTRACT_END.search(latex)

    if begin and end:
        raw = latex[begin.end():end.start()].strip()
        # Strip comments but preserve everything else
        return _COMMENT_RE.sub("", raw).strip()
    return None


def extract_preamble_metadata(latex: str) -> dict:
    """Extract metadata from LaTeX preamble (before \\begin{document}).

    Non-destructive: returns raw values as-is from the source.
    """
    begin = _DOC_BEGIN.search(latex)
    preamble = latex[:begin.start()] if begin else latex[:2000]

    meta = {}

    # Title
    m = re.search(r"\\title\{(.+?)\}", preamble, re.DOTALL)
    if m:
        meta["title"] = " ".join(m.group(1).split())

    # Authors (simple extraction — LaTeX author markup varies wildly)
    m = re.search(r"\\author\{(.+?)\}", preamble, re.DOTALL)
    if m:
        meta["authors_raw"] = m.group(1).strip()

    return meta


def split_sections(latex: str) -> list[LatexSection]:
    """Split LaTeX document into sections by heading commands.

    Preserves full hierarchy: section > subsection > subsubsection > paragraph.
    Each section includes its heading line and all content until the next
    heading at the same or higher level.

    The abstract (if present as \\begin{abstract}...\\end{abstract}) is
    emitted as position 0 with heading_command='abstract' and depth=0.

    Returns:
        List of LatexSection ordered by position in document.
    """
    body = extract_body(latex)
    lines = body.split("\n")

    sections: list[LatexSection] = []
    position = 0

    # Extract abstract first (special handling — it's an environment, not a heading)
    abstract_text = extract_abstract(latex)
    if abstract_text:
        sections.append(LatexSection(
            heading="Abstract",
            heading_command="abstract",
            depth=0,
            content=abstract_text,
            position=position,
            line_start=0,
        ))
        position += 1

    # Find all headings with line numbers
    heading_positions = []
    for line_num, line in enumerate(lines):
        stripped = line.strip()
        m = _HEADING_RE.match(stripped)
        if m:
            command = m.group(1)
            heading_text = _clean_heading(m.group(2))
            depth = _heading_depth(command)
            heading_positions.append((line_num, command, heading_text, depth))

    # Split content between headings
    for i, (line_num, command, heading_text, depth) in enumerate(heading_positions):
        # Content runs from this heading to the next heading (or end of document)
        if i + 1 < len(heading_positions):
            end_line = heading_positions[i + 1][0]
        else:
            end_line = len(lines)

        content = "\n".join(lines[line_num:end_line]).strip()

        # Strip LaTeX comments from content (for embedding quality) but keep structure
        clean_content = _COMMENT_RE.sub("", content).strip()

        if clean_content:
            sections.append(LatexSection(
                heading=heading_text,
                heading_command=command,
                depth=depth,
                content=clean_content,
                position=position,
                line_start=line_num,
            ))
            position += 1

    # If no sections found (no headings), treat the whole body as one chunk
    if not sections and body.strip():
        content = _COMMENT_RE.sub("", body).strip()
        if content:
            sections.append(LatexSection(
                heading="",
                heading_command="body",
                depth=0,
                content=content,
                position=0,
                line_start=0,
            ))

    return sections


def build_tree_edges(sections: list[LatexSection], source_id: str
                     ) -> list[tuple[str, str | None, str | None, str, int]]:
    """Build _edges_tree rows from section hierarchy.

    Uses a stack to track the current parent at each depth level.
    Each section's parent is the most recent section at a shallower depth.

    Returns:
        List of (id, parent_id, branch_at, relation, depth) tuples.
        id and parent_id are chunk_ids (source_id:position).
    """
    edges = []
    # Stack: list of (depth, chunk_id) — tracks current ancestor at each level
    stack: list[tuple[int, str]] = []

    for section in sections:
        chunk_id = f"{source_id}:{section.position}"

        # Pop stack until we find a parent at a shallower depth
        while stack and stack[-1][0] >= section.depth:
            stack.pop()

        parent_id = stack[-1][1] if stack else None
        # branch_at = the parent chunk itself (the section where this branches from)
        branch_at = parent_id

        edges.append((
            chunk_id,
            parent_id,
            branch_at,
            "child",
            section.depth,
        ))

        stack.append((section.depth, chunk_id))

    return edges
