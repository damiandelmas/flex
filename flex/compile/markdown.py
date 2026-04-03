"""
Markdown COMPILE primitives.

Reusable building blocks. Three functions:
- normalize_headers(): ephemeral header promotion (source files never modified)
- extract_frontmatter(): YAML frontmatter extraction
- split_sections(): split on ## headers into chunk-sized sections

"""

import re
import yaml
from typing import Optional


def normalize_headers(content: str) -> str:
    """Ephemeral header normalization. Source files never modified.

    If the content has no H1 but has H2s, promotes all headers up one level:
    ## -> #, ### -> ##, etc. This ensures we always split on ## after normalization.

    If content already has an H1, returns unchanged.
    """
    lines = content.split('\n')

    has_h1 = any(line.startswith('# ') and not line.startswith('## ') for line in lines)

    if has_h1:
        return content

    # Check if there are any headers at all
    has_headers = any(line.startswith('#') for line in lines)
    if not has_headers:
        return content

    # Promote: remove one # from each header line
    result = []
    for line in lines:
        if line.startswith('## '):
            result.append(line[1:])  # ## -> #
        elif line.startswith('### '):
            result.append(line[1:])  # ### -> ##
        elif line.startswith('#### '):
            result.append(line[1:])  # #### -> ###
        elif line.startswith('##### '):
            result.append(line[1:])  # ##### -> ####
        else:
            result.append(line)

    return '\n'.join(result)


def extract_frontmatter(content: str) -> tuple[dict, str]:
    """Extract YAML frontmatter from markdown content.

    Returns:
        (frontmatter_dict, body) — frontmatter is empty dict if none found.
        Body has the frontmatter stripped.
    """
    if not content.startswith('---'):
        return {}, content

    # Find closing ---
    end = content.find('---', 3)
    if end == -1:
        return {}, content

    # Make sure the closing --- is on its own line
    yaml_block = content[3:end].strip()
    body = content[end + 3:].lstrip('\n')

    try:
        frontmatter = yaml.safe_load(yaml_block)
        if not isinstance(frontmatter, dict):
            # Empty YAML (None) or non-dict (list) — treat as no frontmatter
            return {}, body if yaml_block == '' else content
        return frontmatter, body
    except yaml.YAMLError:
        return {}, content


def split_sections(content: str, level: int = 2,
                    return_depth: bool = False) -> list:
    """Split markdown content on header boundaries.

    Args:
        content: Markdown text (should be normalized first via normalize_headers)
        level: Header level to split on (2 = ##)
        return_depth: If True, split on ALL heading levels >= level and return
            (title, content, position, depth) tuples with heading depth.
            If False (default), return (title, content, position) tuples
            split only at the exact target level (backwards compatible).

    Returns:
        List of (title, content, position[, depth]) tuples.
        - title: the header text (empty string for preamble before first header)
        - content: the section body including the header line
        - position: 0-indexed section number
        - depth: heading level (1=#, 2=##, 3=###, etc.) — only when return_depth=True
    """
    lines = content.split('\n')

    if return_depth:
        # Multi-level mode: split on every heading >= level, track depth
        sections = []
        current_title = ''
        current_depth = 0
        current_lines = []
        position = 0

        for line in lines:
            # Detect any heading at or below the target level
            heading_depth = 0
            if line.startswith('#'):
                for ch in line:
                    if ch == '#':
                        heading_depth += 1
                    else:
                        break
                # Must be followed by a space and be at/below target level
                if heading_depth >= level and len(line) > heading_depth and line[heading_depth] == ' ':
                    # Flush previous section
                    if current_lines or position > 0:
                        body = '\n'.join(current_lines).strip()
                        if body:
                            sections.append((current_title, body, position, current_depth))
                            position += 1

                    current_title = line[heading_depth + 1:].strip()
                    current_depth = heading_depth
                    current_lines = [line]
                    continue

            current_lines.append(line)

        # Flush final section
        body = '\n'.join(current_lines).strip()
        if body:
            sections.append((current_title, body, position, current_depth))

        return sections

    # Original mode: split only at exact target level
    prefix = '#' * level + ' '

    sections = []
    current_title = ''
    current_lines = []
    position = 0

    for line in lines:
        if line.startswith(prefix) and not line.startswith('#' * (level + 1) + ' '):
            # Found a header at exactly the target level
            if current_lines or position > 0:
                # Flush previous section
                body = '\n'.join(current_lines).strip()
                if body:
                    sections.append((current_title, body, position))
                    position += 1

            current_title = line[len(prefix):].strip()
            current_lines = [line]
        else:
            current_lines.append(line)

    # Flush final section
    body = '\n'.join(current_lines).strip()
    if body:
        sections.append((current_title, body, position))

    return sections
