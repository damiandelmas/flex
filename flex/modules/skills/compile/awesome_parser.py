"""
Awesome-list markdown parser for the skills cell.

Parses awesome-list READMEs into structured entry dicts.
Universal format: `- [Name](url) - Description.` with heading hierarchy for categories.
Handles punkpeye emoji badge variant and markdown table format.

Entry point:
    from flex.modules.skills.compile.awesome_parser import parse_awesome_list
    entries = parse_awesome_list('hesreallyhim/awesome-claude-code')
"""

import re
import json
import sys
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass, asdict


@dataclass
class AwesomeEntry:
    """A single entry parsed from an awesome list."""
    name: str               # tool/project name
    url: str                # GitHub URL or homepage
    description: str        # one-line description
    category: str           # raw ## heading text
    subcategory: str | None  # raw ### heading text (None if flat)
    github_owner: str | None  # parsed from URL if GitHub
    github_repo: str | None   # parsed from URL if GitHub
    author: str | None      # if present (hesreallyhim format)
    emoji_badges: str | None  # raw emoji string (punkpeye format)
    position: int           # 0-indexed position in the list
    heading_depth: int      # 2 for ##-level category, 3 for ###-level


# ═════════════════════════════════════════════════════
# Heading filter — skip non-entry sections
# ═════════════════════════════════════════════════════

SKIP_HEADINGS = {
    'contents', 'table of contents', 'toc',
    'contributing', 'contributors', 'contribute',
    'license', 'licence',
    'acknowledgments', 'acknowledgements',
    'related', 'related projects', 'related lists',
    'resources', 'tutorials', 'legend',
    'tips and tricks', 'tips',
    'star history', 'stargazers',
    'sponsors', 'support',
    'about', 'disclaimer',
}


# ═════════════════════════════════════════════════════
# Entry patterns
# ═════════════════════════════════════════════════════

# With author: - [Name](url) by [Author](url) - Description
_PATTERN_AUTHOR = re.compile(
    r'^\s*[-*]\s+\[([^\]]+)\]\(([^)]+)\)\s+by\s+\[([^\]]+)\]\([^)]+\)\s*[-–—:]\s*(.+)$'
)

# Emoji badges (punkpeye): - [owner/repo](url) 📇🐍☁️ - Description
_PATTERN_EMOJI = re.compile(
    r'^\s*[-*]\s+\[([^\]]+)\]\(([^)]+)\)\s+([^\s\-–—][^\-–—]*?)\s*[-–—]\s*(.+)$'
)

# Standard: - [Name](url) - Description
_PATTERN_STANDARD = re.compile(
    r'^\s*[-*]\s+\[([^\]]+)\]\(([^)]+)\)\s*[-–—:]\s*(.+)$'
)

# Bare link (no description): - [Name](url)
_PATTERN_BARE = re.compile(
    r'^\s*[-*]\s+\[([^\]]+)\]\(([^)]+)\)\s*$'
)

# Table row: | **[Name](url)** | Description | or | [Name](url) | Description |
_PATTERN_TABLE = re.compile(
    r'\|\s*\*?\*?\[([^\]]+)\]\(([^)]+)\)\*?\*?\s*\|\s*(.+?)\s*\|'
)

# GitHub URL parser
_GITHUB_URL_RE = re.compile(
    r'https?://(?:www\.)?github\.com/([^/\s#?]+)/([^/\s#?]+)'
)


# ═════════════════════════════════════════════════════
# URL parsing
# ═════════════════════════════════════════════════════

def _parse_github_url(url: str) -> tuple[str | None, str | None]:
    """Extract owner/repo from a GitHub URL.

    Handles:
        https://github.com/owner/repo
        https://github.com/owner/repo/tree/main/subdir
        https://github.com/owner/repo#readme
    Returns (owner, repo) or (None, None) if not a GitHub URL.
    """
    m = _GITHUB_URL_RE.match(url)
    if not m:
        return None, None
    owner = m.group(1)
    repo = m.group(2)
    # Strip trailing .git
    if repo.endswith('.git'):
        repo = repo[:-4]
    return owner, repo


# ═════════════════════════════════════════════════════
# README fetching
# ═════════════════════════════════════════════════════

def _fetch_readme(repo: str, token: str | None = None) -> str:
    """Fetch raw README.md from GitHub API."""
    url = f"https://api.github.com/repos/{repo}/readme"
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github.raw+json")
    req.add_header("User-Agent", "flex-skills/1.0")
    if token:
        req.add_header("Authorization", f"token {token}")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        print(f"  [awesome_parser] Failed to fetch {repo}: HTTP {e.code}",
              file=sys.stderr)
        return ""
    except Exception as e:
        print(f"  [awesome_parser] Failed to fetch {repo}: {e}",
              file=sys.stderr)
        return ""


# ═════════════════════════════════════════════════════
# Emoji detection
# ═════════════════════════════════════════════════════

def _is_emoji_string(s: str) -> bool:
    """Check if a string is primarily emoji/symbol characters."""
    s = s.strip()
    if not s:
        return False
    # Count emoji-like chars (non-ASCII, non-alphanumeric)
    emoji_count = sum(1 for c in s if ord(c) > 127 or c in '🔧📇🐍☁️💻🖥️📦⚡🌐🔌')
    alpha_count = sum(1 for c in s if c.isalpha())
    return emoji_count > 0 and emoji_count >= alpha_count


# ═════════════════════════════════════════════════════
# Line parsing
# ═════════════════════════════════════════════════════

def _is_real_url(url: str) -> bool:
    """Reject anchor links and relative paths — not real entries."""
    if not url:
        return False
    if url.startswith('#'):       # section header anchor
        return False
    if url.startswith('./') or url.startswith('../'):  # relative path
        return False
    # Bare relative paths (no scheme) — e.g. "categories/foo.md"
    if not url.startswith('http') and '/' in url and not url.startswith('//'):
        return False
    return True


def _parse_entry_line(line: str, category: str, subcategory: str | None,
                      heading_depth: int, position: int) -> AwesomeEntry | None:
    """Try to parse a single line as an awesome-list entry."""

    # Skip nested list items (indented more than one level)
    stripped = line.lstrip()
    indent = len(line) - len(stripped)
    if indent > 4:  # deeply nested
        return None

    # Try author pattern first (most specific)
    m = _PATTERN_AUTHOR.match(line)
    if m:
        name, url, author, desc = m.group(1), m.group(2), m.group(3), m.group(4)
        if not _is_real_url(url):
            return None
        owner, repo = _parse_github_url(url)
        return AwesomeEntry(
            name=name.strip(), url=url.strip(),
            description=desc.strip().rstrip('.'),
            category=category, subcategory=subcategory,
            github_owner=owner, github_repo=repo,
            author=author.strip(), emoji_badges=None,
            position=position, heading_depth=heading_depth,
        )

    # Try emoji pattern (punkpeye)
    m = _PATTERN_EMOJI.match(line)
    if m:
        name, url, badges, desc = m.group(1), m.group(2), m.group(3), m.group(4)
        if _is_emoji_string(badges) and _is_real_url(url):
            owner, repo = _parse_github_url(url)
            return AwesomeEntry(
                name=name.strip(), url=url.strip(),
                description=desc.strip().rstrip('.'),
                category=category, subcategory=subcategory,
                github_owner=owner, github_repo=repo,
                author=None, emoji_badges=badges.strip(),
                position=position, heading_depth=heading_depth,
            )

    # Try standard pattern
    m = _PATTERN_STANDARD.match(line)
    if m:
        name, url, desc = m.group(1), m.group(2), m.group(3)
        if not _is_real_url(url):
            return None
        owner, repo = _parse_github_url(url)
        return AwesomeEntry(
            name=name.strip(), url=url.strip(),
            description=desc.strip().rstrip('.'),
            category=category, subcategory=subcategory,
            github_owner=owner, github_repo=repo,
            author=None, emoji_badges=None,
            position=position, heading_depth=heading_depth,
        )

    # Try bare link (no description)
    m = _PATTERN_BARE.match(line)
    if m:
        name, url = m.group(1), m.group(2)
        if not _is_real_url(url):
            return None
        owner, repo = _parse_github_url(url)
        return AwesomeEntry(
            name=name.strip(), url=url.strip(),
            description='',
            category=category, subcategory=subcategory,
            github_owner=owner, github_repo=repo,
            author=None, emoji_badges=None,
            position=position, heading_depth=heading_depth,
        )

    return None


def _parse_table_row(line: str, category: str, subcategory: str | None,
                     heading_depth: int, position: int) -> AwesomeEntry | None:
    """Try to parse a markdown table row as an entry."""
    m = _PATTERN_TABLE.search(line)
    if not m:
        return None
    name, url, desc = m.group(1), m.group(2), m.group(3)
    # Skip table header separators
    if desc.strip().startswith('---'):
        return None
    owner, repo = _parse_github_url(url)
    return AwesomeEntry(
        name=name.strip(), url=url.strip(),
        description=desc.strip().rstrip('.').rstrip('|').strip(),
        category=category, subcategory=subcategory,
        github_owner=owner, github_repo=repo,
        author=None, emoji_badges=None,
        position=position, heading_depth=heading_depth,
    )


# ═════════════════════════════════════════════════════
# Main parser
# ═════════════════════════════════════════════════════

def parse_readme(content: str) -> list[AwesomeEntry]:
    """Parse awesome-list markdown content into structured entries.

    Args:
        content: Raw markdown content of the awesome list README.

    Returns:
        List of AwesomeEntry with parsed metadata.
    """
    if not content:
        return []

    lines = content.split('\n')
    entries = []
    position = 0

    category = ''
    subcategory = None
    heading_depth = 2
    in_entry_section = False

    for line in lines:
        stripped = line.strip()

        # Track heading hierarchy
        if stripped.startswith('## ') and not stripped.startswith('### '):
            heading_text = stripped[3:].strip()
            if heading_text.lower() in SKIP_HEADINGS:
                in_entry_section = False
                continue
            category = heading_text
            subcategory = None
            heading_depth = 2
            in_entry_section = True
            continue

        if stripped.startswith('### '):
            heading_text = stripped[4:].strip()
            if heading_text.lower() in SKIP_HEADINGS:
                in_entry_section = False
                continue
            subcategory = heading_text
            heading_depth = 3
            in_entry_section = True
            continue

        if stripped.startswith('#### '):
            heading_text = stripped[5:].strip()
            if heading_text.lower() in SKIP_HEADINGS:
                continue
            subcategory = heading_text
            heading_depth = 4
            continue

        # Skip lines before first category heading
        if not category and not stripped.startswith(('-', '*', '|')):
            continue

        # Try parsing as entry
        if stripped.startswith(('-', '*')):
            entry = _parse_entry_line(line, category, subcategory,
                                      heading_depth, position)
            if entry:
                entries.append(entry)
                position += 1
                continue

        # Try parsing as table row
        if '|' in stripped and '[' in stripped:
            entry = _parse_table_row(line, category, subcategory,
                                     heading_depth, position)
            if entry:
                entries.append(entry)
                position += 1

    return entries


def parse_awesome_list(repo: str, token: str | None = None) -> list[AwesomeEntry]:
    """Parse an awesome-list GitHub repo into structured entries.

    Args:
        repo: GitHub 'owner/repo' string
        token: Optional GitHub API token for authenticated requests

    Returns:
        List of AwesomeEntry dicts with parsed metadata.
    """
    if token is None:
        import os
        token = os.environ.get('GITHUB_TOKEN')

    content = _fetch_readme(repo, token)
    if not content:
        return []

    entries = parse_readme(content)
    print(f"  [awesome_parser] {repo}: {len(entries)} entries parsed")
    return entries
