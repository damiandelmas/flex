"""
GitHub API client for the skills cell.

Fetches repo metadata (stars, language, topics, license, last_commit),
raw README.md content, and Claude Code skill artifacts (SKILL.md, agents, hooks).

Authentication via GITHUB_TOKEN env var gives 5,000 requests/hour.
Without auth: 60/hour — unusable for bulk enrichment.

Entry point:
    from flex.modules.skills.compile.github_api import get_repo_metadata, get_readme
    meta = get_repo_metadata('owner', 'repo')
    readme = get_readme('owner', 'repo')
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from dataclasses import dataclass


# ═════════════════════════════════════════════════════
# Constants
# ═════════════════════════════════════════════════════

RATE_LIMIT_DELAY = 0.1       # seconds between requests (default)
RATE_LIMIT_BACKOFF = 60      # seconds to wait on 403/429
MAX_RETRIES = 3
USER_AGENT = "flex-skills/1.0"

SKILL_PATHS = [
    '.claude/skills',           # official skill directory
    '.claude/agents',           # official agent directory
    '.claude/commands',         # legacy command directory
]


# ═════════════════════════════════════════════════════
# HTTP client
# ═════════════════════════════════════════════════════

def _get_token() -> str | None:
    """Get GitHub token from environment."""
    return os.environ.get('GITHUB_TOKEN')


def _github_wait_cap() -> int:
    """Return max seconds to sleep inside GitHub API retry handling."""
    raw = os.environ.get('FLEX_GITHUB_RATE_WAIT_MAX_SEC', '0')
    try:
        return max(0, int(raw))
    except ValueError:
        return 5


def _github_max_retries() -> int:
    """Return retry count for GitHub API calls."""
    raw = os.environ.get('FLEX_GITHUB_MAX_RETRIES', '1')
    try:
        return max(1, int(raw))
    except ValueError:
        return MAX_RETRIES


def _request(url: str, token: str | None = None,
             accept: str = 'application/vnd.github+json') -> dict | str | None:
    """Make authenticated GitHub API request with rate limit handling.

    Returns parsed JSON dict, raw string (for raw accept), or None on error.
    """
    if token is None:
        token = _get_token()

    max_retries = _github_max_retries()
    wait_cap = _github_wait_cap()

    for attempt in range(max_retries):
        req = urllib.request.Request(url)
        req.add_header("Accept", accept)
        req.add_header("User-Agent", USER_AGENT)
        if token:
            req.add_header("Authorization", f"token {token}")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                # Check rate limit headers
                remaining = resp.headers.get('X-RateLimit-Remaining')
                if remaining and int(remaining) < 10:
                    reset_ts = int(resp.headers.get('X-RateLimit-Reset', 0))
                    wait = max(reset_ts - int(time.time()), 1)
                    wait = min(wait, wait_cap)
                    print(f"  [github_api] Rate limit low ({remaining}), "
                          f"waiting {wait}s", file=sys.stderr)
                    if wait > 0:
                        time.sleep(wait)

                data = resp.read()
                if 'raw' in accept:
                    return data.decode('utf-8', errors='replace')
                return json.loads(data)

        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (403, 429):
                # Rate limited — back off
                reset_ts = int(e.headers.get('X-RateLimit-Reset', 0))
                wait = max(reset_ts - int(time.time()), RATE_LIMIT_BACKOFF)
                wait = min(wait, wait_cap)
                print(f"  [github_api] Rate limited (HTTP {e.code}), "
                      f"waiting {wait}s (attempt {attempt+1}/{max_retries})",
                      file=sys.stderr)
                if wait > 0:
                    time.sleep(wait)
                continue
            print(f"  [github_api] HTTP {e.code} for {url}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  [github_api] Error for {url}: {e}", file=sys.stderr)
            return None

    return None


# ═════════════════════════════════════════════════════
# Repo metadata
# ═════════════════════════════════════════════════════

def _normalize_repo(data: dict) -> dict:
    """Flatten GitHub API repo response to clean dict."""
    return {
        'name': data['name'],
        'full_name': data['full_name'],
        'description': data.get('description') or '',
        'stars': data['stargazers_count'],
        'language': data.get('language'),
        'license': (data.get('license') or {}).get('spdx_id')
                   if data.get('license') else None,
        'topics': ','.join(data.get('topics', [])),
        'last_commit': data.get('pushed_at'),
        'open_issues': data.get('open_issues_count', 0),
        'created_at': data['created_at'],
        'homepage': data.get('homepage') or '',
        'archived': data.get('archived', False),
        'fork': data.get('fork', False),
        'default_branch': data.get('default_branch', 'main'),
        'github_id': data['id'],  # immutable integer, survives renames
    }


def get_repo_metadata(owner: str, repo: str,
                      token: str | None = None) -> dict | None:
    """Fetch repository metadata from GitHub API.

    Returns normalized dict with keys:
        name, full_name, description, stars, language, license,
        topics, last_commit, open_issues, created_at, homepage,
        archived, fork, default_branch
    Returns None if repo not found (404).
    """
    url = f"https://api.github.com/repos/{owner}/{repo}"
    data = _request(url, token)
    if not data or not isinstance(data, dict):
        return None
    try:
        return _normalize_repo(data)
    except (KeyError, TypeError):
        return None


# ═════════════════════════════════════════════════════
# README
# ═════════════════════════════════════════════════════

def get_readme(owner: str, repo: str,
               token: str | None = None) -> tuple[str, str | None] | None:
    """Fetch README.md content and blob hash from GitHub API.

    Returns (content, blob_hash) tuple, or None if no README exists.
    blob_hash is the GitHub blob SHA-1 (same as git hash-object).
    """
    import base64
    url = f"https://api.github.com/repos/{owner}/{repo}/readme"
    result = _request(url, token)  # JSON response gives both content + sha
    if result and isinstance(result, dict):
        blob_hash = result.get('sha')
        # Decode base64 content
        encoded = result.get('content')
        if encoded:
            try:
                content = base64.b64decode(encoded).decode('utf-8', errors='replace')
                return (content, blob_hash)
            except Exception:
                pass
    return None


# ═════════════════════════════════════════════════════
# Repo search (GitHub Search API)
# ═════════════════════════════════════════════════════

# Default queries that cover the Claude Code ecosystem.
# Each query is a GitHub search string (q= parameter).
DEFAULT_SEARCH_QUERIES = [
    'topic:claude-code',
    'topic:claude-skills',
    'topic:claude-agent',
    'topic:mcp-server',
    'filename:SKILL.md path:.claude/skills',
]


def search_repos(query: str, min_stars: int = 100,
                 max_pages: int | None = None,
                 token: str | None = None) -> list[dict]:
    """Search GitHub repos via the Search API.

    Paginates through results, returns normalized metadata dicts
    (same shape as get_repo_metadata output).

    GitHub Search API returns max 1,000 results per query.
    At 30 per page, max_pages=34 would exhaust that.

    Args:
        query: GitHub search query (e.g. 'topic:claude-code')
        min_stars: Minimum star count filter (appended to query)
        max_pages: Maximum pages to fetch (30 results per page)
        token: GitHub API token (required for reasonable rate limits)

    Returns:
        List of normalized repo metadata dicts (same as _normalize_repo).
    """
    if token is None:
        token = _get_token()
    if max_pages is None:
        raw_pages = os.environ.get('FLEX_SKILLS_SEARCH_PAGES', '1')
        try:
            max_pages = max(1, int(raw_pages))
        except ValueError:
            max_pages = 1

    results = []
    q = f"{query} stars:>={min_stars}" if min_stars > 0 else query
    q_encoded = urllib.parse.quote(q)

    for page in range(1, max_pages + 1):
        url = (f"https://api.github.com/search/repositories"
               f"?q={q_encoded}&sort=stars&order=desc"
               f"&per_page=100&page={page}")
        data = _request(url, token)
        time.sleep(RATE_LIMIT_DELAY)

        if not data or not isinstance(data, dict):
            break

        items = data.get('items', [])
        if not items:
            break

        for repo_data in items:
            try:
                results.append(_normalize_repo(repo_data))
            except (KeyError, TypeError):
                continue

        # Check if we've exhausted results
        total = data.get('total_count', 0)
        if page * 100 >= min(total, 1000):
            break

        if (page) % 5 == 0:
            print(f"  [search] {query}: page {page}, "
                  f"{len(results)}/{total} repos", file=sys.stderr)

    return results


# ═════════════════════════════════════════════════════
# Batch enrichment
# ═════════════════════════════════════════════════════

def enrich_repos(repos: list[tuple[str, str]],
                 token: str | None = None,
                 delay: float = RATE_LIMIT_DELAY) -> dict[str, dict]:
    """Batch enrich multiple repos with metadata.

    Args:
        repos: List of (owner, repo) tuples
        token: GitHub API token
        delay: Seconds between requests (rate limiting)

    Returns:
        Dict mapping 'owner/repo' to metadata dict.
        Missing/private repos omitted from result.
    """
    results = {}
    for i, (owner, repo) in enumerate(repos):
        meta = get_repo_metadata(owner, repo, token)
        if meta:
            key = f"{owner}/{repo}"
            # Use full_name from API (handles renames)
            if meta.get('full_name'):
                key = meta['full_name']
            results[key] = meta

        if delay > 0 and i < len(repos) - 1:
            time.sleep(delay)

        if (i + 1) % 100 == 0:
            print(f"  [github_api] Enriched {i+1}/{len(repos)} repos")

    return results


# ═════════════════════════════════════════════════════
# Skill artifact discovery
# ═════════════════════════════════════════════════════

@dataclass
class SkillArtifact:
    """A Claude Code skill artifact found in a GitHub repo."""
    path: str                   # e.g., '.claude/skills/deploy/SKILL.md'
    artifact_type: str          # 'skill' | 'agent' | 'command' | 'hook'
    content: str                # raw file content
    frontmatter: dict | None    # parsed YAML frontmatter (if present)
    blob_hash: str | None = None  # GitHub blob SHA-1 (git hash-object)


def _parse_frontmatter(content: str) -> tuple[dict | None, str]:
    """Extract YAML frontmatter from markdown file.

    Returns (frontmatter_dict, body_content).
    Returns (None, content) if no frontmatter found.
    """
    if not content.startswith('---'):
        return None, content

    end = content.find('---', 3)
    if end == -1:
        return None, content

    yaml_block = content[3:end].strip()
    body = content[end + 3:].lstrip('\n')

    try:
        import yaml
        fm = yaml.safe_load(yaml_block)
        if isinstance(fm, dict):
            return fm, body
    except Exception:
        pass

    return None, content


def _list_directory(owner: str, repo: str, path: str,
                    token: str | None = None) -> list[dict] | None:
    """List contents of a directory via GitHub Contents API.

    Returns list of file/dir info dicts, or None if path doesn't exist.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    data = _request(url, token)
    if data and isinstance(data, list):
        return data
    return None


def _fetch_file(owner: str, repo: str, path: str,
                token: str | None = None) -> tuple[str, str | None] | None:
    """Fetch file content and blob hash via GitHub Contents API.

    Returns (content, blob_hash) tuple, or None if file doesn't exist.
    """
    import base64
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    result = _request(url, token)  # JSON gives content + sha in one call
    if result and isinstance(result, dict):
        blob_hash = result.get('sha')
        encoded = result.get('content')
        if encoded:
            try:
                content = base64.b64decode(encoded).decode('utf-8', errors='replace')
                return (content, blob_hash)
            except Exception:
                pass
    return None


def discover_skill_artifacts(owner: str, repo: str,
                             token: str | None = None) -> list[SkillArtifact]:
    """Discover Claude Code skill artifacts in a GitHub repo.

    Checks known paths (.claude/skills/, .claude/agents/, .claude/commands/)
    via the GitHub Contents API. Fetches and parses each artifact.

    Returns list of SkillArtifact, or empty list if no .claude/ directory.
    """
    if token is None:
        token = _get_token()

    artifacts = []

    # Check if .claude/ directory exists
    claude_dir = _list_directory(owner, repo, '.claude', token)
    time.sleep(RATE_LIMIT_DELAY)
    if not claude_dir:
        return artifacts

    # Check skills directory
    skills_dir = _list_directory(owner, repo, '.claude/skills', token)
    time.sleep(RATE_LIMIT_DELAY)
    if skills_dir:
        for item in skills_dir:
            if item.get('type') == 'dir':
                # Check for SKILL.md inside subdirectory
                skill_path = f".claude/skills/{item['name']}/SKILL.md"
                result = _fetch_file(owner, repo, skill_path, token)
                time.sleep(RATE_LIMIT_DELAY)
                if result:
                    content, blob_hash = result
                    fm, body = _parse_frontmatter(content)
                    artifacts.append(SkillArtifact(
                        path=skill_path,
                        artifact_type='skill',
                        content=content,
                        frontmatter=fm,
                        blob_hash=blob_hash,
                    ))
            elif item.get('name', '').endswith('.md'):
                # Direct .md file in skills/
                skill_path = f".claude/skills/{item['name']}"
                result = _fetch_file(owner, repo, skill_path, token)
                time.sleep(RATE_LIMIT_DELAY)
                if result:
                    content, blob_hash = result
                    fm, body = _parse_frontmatter(content)
                    artifacts.append(SkillArtifact(
                        path=skill_path,
                        artifact_type='skill',
                        content=content,
                        frontmatter=fm,
                        blob_hash=blob_hash,
                    ))

    # Check agents directory
    agents_dir = _list_directory(owner, repo, '.claude/agents', token)
    time.sleep(RATE_LIMIT_DELAY)
    if agents_dir:
        for item in agents_dir:
            if item.get('name', '').endswith('.md'):
                agent_path = f".claude/agents/{item['name']}"
                result = _fetch_file(owner, repo, agent_path, token)
                time.sleep(RATE_LIMIT_DELAY)
                if result:
                    content, blob_hash = result
                    fm, body = _parse_frontmatter(content)
                    artifacts.append(SkillArtifact(
                        path=agent_path,
                        artifact_type='agent',
                        content=content,
                        frontmatter=fm,
                        blob_hash=blob_hash,
                    ))

    # Check legacy commands directory
    commands_dir = _list_directory(owner, repo, '.claude/commands', token)
    time.sleep(RATE_LIMIT_DELAY)
    if commands_dir:
        for item in commands_dir:
            if item.get('name', '').endswith('.md'):
                cmd_path = f".claude/commands/{item['name']}"
                result = _fetch_file(owner, repo, cmd_path, token)
                time.sleep(RATE_LIMIT_DELAY)
                if result:
                    content, blob_hash = result
                    fm, body = _parse_frontmatter(content)
                    artifacts.append(SkillArtifact(
                        path=cmd_path,
                        artifact_type='command',
                        content=content,
                        frontmatter=fm,
                        blob_hash=blob_hash,
                    ))

    # Check settings.json for hooks
    settings_result = _fetch_file(owner, repo, '.claude/settings.json', token)
    time.sleep(RATE_LIMIT_DELAY)
    if settings_result:
        settings_content, _ = settings_result
        try:
            settings = json.loads(settings_content)
            hooks = settings.get('hooks', {})
            if hooks:
                artifacts.append(SkillArtifact(
                    path='.claude/settings.json',
                    artifact_type='hook',
                    content=json.dumps(hooks, indent=2),
                    frontmatter=None,
                    # No blob_hash — content is extracted subset of settings.json
                ))
        except (json.JSONDecodeError, AttributeError):
            pass

    return artifacts
