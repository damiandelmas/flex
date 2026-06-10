"""
GitHub Issues API client.

Reusable module for pulling issues and comments from GitHub.
Used by both the one-shot worker and the incremental refresh script.

API docs: https://docs.github.com/en/rest/issues
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import urllib.parse
from datetime import datetime, timezone


BASE_URL = "https://api.github.com"
USER_AGENT = "flex-github/1.0"
BATCH_SIZE = 100
DEFAULT_MAX_ISSUES = 50
DEFAULT_MAX_COMMENTS_PER_ISSUE = 25

# Dual rate limits
SEARCH_DELAY = 2.0   # 30 requests/min for search API
REST_DELAY = 0.5      # 5000 requests/hr for REST API

# Default repos
DEFAULT_REPOS = [
    "anthropics/claude-code",
    "hesreallyhim/awesome-claude-code",
    "affaan-m/everything-claude-code",
    "punkpeye/awesome-mcp-servers",
]

# Default cross-repo search queries
DEFAULT_QUERIES = [
    "search session history",
    "conversation history",
    "memory between sessions",
    "search past conversations",
    "MCP memory server",
    "session context lost",
    "vector search sqlite",
    "semantic search local",
]


def _get_token() -> str | None:
    """Get GitHub token from env var or gh CLI."""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _auth_headers() -> dict:
    """Build auth + accept headers."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github.v3+json",
    }
    token = _get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


# Set when any fetch hits the API rate limit; callers surface it so an
# empty pull is never reported as a healthy cell.
rate_limited = False


def api_fetch(url: str) -> tuple[dict | list, dict]:
    """Fetch from GitHub API. Returns (parsed_json, response_headers_dict)."""
    global rate_limited
    headers = _auth_headers()
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            return data, resp_headers
    except urllib.error.HTTPError as e:
        if e.code == 403 and e.headers.get("X-RateLimit-Remaining") == "0":
            rate_limited = True
            reset = e.headers.get("X-RateLimit-Reset")
            when = ""
            if reset and reset.isdigit():
                when = datetime.fromtimestamp(int(reset), tz=timezone.utc).strftime(" (resets %H:%M UTC)")
            print(f"  [!] GitHub rate limit exhausted{when} — "
                  f"set GITHUB_TOKEN for 5000 req/h", file=sys.stderr)
        else:
            print(f"  [!] {url} — {e}", file=sys.stderr)
        return {}, {}
    except Exception as e:
        print(f"  [!] {url} — {e}", file=sys.stderr)
        return {}, {}


def _parse_link_header(headers: dict) -> str | None:
    """Extract next page URL from Link header."""
    link = headers.get("link", "")
    for part in link.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip().strip("<>")
            return url
    return None


def pull_issues(queries: list[str] | None = None,
                repos: list[str] | None = None,
                after_ts: int = 0,
                quiet: bool = False,
                max_issues: int | None = DEFAULT_MAX_ISSUES,
                max_comments_per_issue: int | None = DEFAULT_MAX_COMMENTS_PER_ISSUE) -> list[dict]:
    """Pull issues via search API and per-repo listing.

    Returns list of normalized issue dicts (with comments pre-loaded).
    """
    all_issues = {}  # keyed by source_id to dedup

    after_date = ""
    if after_ts:
        dt = datetime.fromtimestamp(after_ts, tz=timezone.utc)
        after_date = dt.strftime("%Y-%m-%d")

    # Pull from specific repos
    repos = DEFAULT_REPOS if repos is None else repos
    for repo in repos:
        if max_issues is not None and len(all_issues) >= max_issues:
            break
        if not quiet:
            print(f"  Repo: {repo}")
        owner, name = repo.split("/", 1)
        remaining = None if max_issues is None else max_issues - len(all_issues)
        issues = _pull_repo_issues(
            owner, name, after_date, quiet,
            max_issues=remaining,
            max_comments_per_issue=max_comments_per_issue,
        )
        for issue in issues:
            sid = issue["source_id"]
            if sid not in all_issues:
                all_issues[sid] = issue
                if max_issues is not None and len(all_issues) >= max_issues:
                    break

    # Pull via search queries (cross-repo)
    queries = queries or []
    for query in queries:
        if max_issues is not None and len(all_issues) >= max_issues:
            break
        if not quiet:
            print(f"  Query: {query}")
        remaining = None if max_issues is None else max_issues - len(all_issues)
        issues = _pull_search_issues(
            query, after_date, quiet,
            max_issues=remaining,
            max_comments_per_issue=max_comments_per_issue,
        )
        for issue in issues:
            sid = issue["source_id"]
            if sid not in all_issues:
                all_issues[sid] = issue
                if max_issues is not None and len(all_issues) >= max_issues:
                    break

    result = list(all_issues.values())
    if not quiet:
        print(f"  Total unique issues: {len(result)}")
    return result


def _pull_repo_issues(owner: str, repo: str, after_date: str,
                      quiet: bool = False,
                      max_issues: int | None = None,
                      max_comments_per_issue: int | None = None) -> list[dict]:
    """Pull issues from a single repo via REST API."""
    issues = []
    page = 1

    while True:
        if max_issues is not None and len(issues) >= max_issues:
            break
        params = {
            "state": "all",
            "per_page": BATCH_SIZE,
            "sort": "created",
            "direction": "desc",
            "page": page,
        }
        if after_date:
            params["since"] = f"{after_date}T00:00:00Z"

        qs = urllib.parse.urlencode(params)
        url = f"{BASE_URL}/repos/{owner}/{repo}/issues?{qs}"
        data, headers = api_fetch(url)

        if not data or not isinstance(data, list):
            break

        # Filter out pull requests (GitHub issues endpoint returns PRs too)
        batch = [item for item in data if "pull_request" not in item]

        for item in batch:
            if max_issues is not None and len(issues) >= max_issues:
                break
            normalized = normalize_issue(item, owner, repo)
            if normalized:
                issues.append(normalized)

        if not quiet:
            print(f"    issues: {len(issues)} (page {page})", end="\r")

        time.sleep(REST_DELAY)

        if len(data) < BATCH_SIZE:
            break

        next_url = _parse_link_header(headers)
        if not next_url:
            break
        page += 1

    if not quiet:
        print(f"    issues: {len(issues)} total{' ' * 20}")

    # Pull comments for each issue
    for issue in issues:
        if issue.get("num_comments", 0) > 0:
            comments = pull_comments_for_issue(
                owner, repo, issue["issue_number"], quiet,
                max_comments=max_comments_per_issue)
            issue["_comments"] = comments
        else:
            issue["_comments"] = []

    return issues


def _pull_search_issues(query: str, after_date: str,
                        quiet: bool = False,
                        max_issues: int | None = None,
                        max_comments_per_issue: int | None = None) -> list[dict]:
    """Pull issues via GitHub search API."""
    issues = []
    page = 1

    q_parts = [query, "type:issue"]
    if after_date:
        q_parts.append(f"created:>{after_date}")
    q_string = " ".join(q_parts)

    while True:
        if max_issues is not None and len(issues) >= max_issues:
            break
        params = {
            "q": q_string,
            "sort": "created",
            "order": "desc",
            "per_page": BATCH_SIZE,
            "page": page,
        }
        qs = urllib.parse.urlencode(params)
        url = f"{BASE_URL}/search/issues?{qs}"
        data, headers = api_fetch(url)

        if not data or not isinstance(data, dict):
            break

        batch = data.get("items", [])
        if not batch:
            break

        for item in batch:
            if max_issues is not None and len(issues) >= max_issues:
                break
            # Parse owner/repo from repository_url
            repo_url = item.get("repository_url", "")
            parts = repo_url.rstrip("/").split("/")
            if len(parts) >= 2:
                owner = parts[-2]
                repo = parts[-1]
            else:
                continue

            normalized = normalize_issue(item, owner, repo)
            if normalized:
                issues.append(normalized)

        if not quiet:
            print(f"    search results: {len(issues)} (page {page})", end="\r")

        time.sleep(SEARCH_DELAY)

        if len(batch) < BATCH_SIZE:
            break

        next_url = _parse_link_header(headers)
        if not next_url:
            break
        page += 1

    if not quiet:
        print(f"    search results: {len(issues)} total{' ' * 20}")

    # Pull comments for each issue
    for issue in issues:
        repo_url = issue.get("_repository_url", "")
        parts = repo_url.rstrip("/").split("/")
        if len(parts) >= 2:
            owner, repo = parts[-2], parts[-1]
            if issue.get("num_comments", 0) > 0:
                comments = pull_comments_for_issue(
                    owner, repo, issue["issue_number"], quiet,
                    max_comments=max_comments_per_issue)
                issue["_comments"] = comments
            else:
                issue["_comments"] = []

    return issues


def pull_comments_for_issue(owner: str, repo: str, number: int,
                            quiet: bool = False,
                            max_comments: int | None = DEFAULT_MAX_COMMENTS_PER_ISSUE) -> list[dict]:
    """Pull all comments for a single issue."""
    comments = []
    page = 1

    while True:
        if max_comments is not None and len(comments) >= max_comments:
            break
        params = {"per_page": BATCH_SIZE, "page": page}
        qs = urllib.parse.urlencode(params)
        url = f"{BASE_URL}/repos/{owner}/{repo}/issues/{number}/comments?{qs}"
        data, headers = api_fetch(url)

        if not data or not isinstance(data, list):
            break

        for item in data:
            if max_comments is not None and len(comments) >= max_comments:
                break
            normalized = normalize_comment(item, owner, repo, number)
            if normalized:
                comments.append(normalized)

        time.sleep(REST_DELAY)

        if len(data) < BATCH_SIZE:
            break

        next_url = _parse_link_header(headers)
        if not next_url:
            break
        page += 1

    return comments


def normalize_issue(item: dict, owner: str, repo: str) -> dict | None:
    """Normalize a raw GitHub issue into Flex-indexable format."""
    number = item.get("number")
    if number is None:
        return None

    created_str = item.get("created_at", "")
    created_utc = 0
    if created_str:
        try:
            dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            created_utc = int(dt.timestamp())
        except (ValueError, OSError):
            pass

    title = item.get("title", "") or ""
    body = item.get("body", "") or ""
    content = f"{title}\n\n{body}".strip()

    user = item.get("user") or {}
    author = user.get("login", "")

    reactions = item.get("reactions") or {}
    score = reactions.get("total_count", 0)

    labels_raw = item.get("labels") or []
    labels = json.dumps([lb["name"] for lb in labels_raw if isinstance(lb, dict) and "name" in lb])

    state = item.get("state", "open")
    html_url = item.get("html_url", "")
    num_comments = item.get("comments", 0)
    repo_full = f"{owner}/{repo}"
    source_id = f"gh_{owner}_{repo}_{number}"

    return {
        "id": source_id,
        "source_id": source_id,
        "type": "issue",
        "title": title,
        "body": body,
        "content": content,
        "author": author,
        "score": score,
        "num_comments": num_comments,
        "url": html_url,
        "created_utc": created_utc,
        "repo": repo_full,
        "issue_number": number,
        "state": state,
        "labels": labels,
        "_repository_url": item.get("repository_url", ""),
        "_comments": [],
    }


def normalize_comment(item: dict, owner: str, repo: str,
                      issue_number: int) -> dict | None:
    """Normalize a raw GitHub comment into Flex-indexable format."""
    comment_id = item.get("id")
    if comment_id is None:
        return None

    created_str = item.get("created_at", "")
    created_utc = 0
    if created_str:
        try:
            dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            created_utc = int(dt.timestamp())
        except (ValueError, OSError):
            pass

    body = item.get("body", "") or ""
    user = item.get("user") or {}
    author = user.get("login", "")

    reactions = item.get("reactions") or {}
    score = reactions.get("total_count", 0)

    html_url = item.get("html_url", "")
    repo_full = f"{owner}/{repo}"

    return {
        "id": str(comment_id),
        "type": "comment",
        "title": "",
        "body": body,
        "content": body,
        "author": author,
        "score": score,
        "num_comments": 0,
        "url": html_url,
        "created_utc": created_utc,
        "repo": repo_full,
        "issue_number": issue_number,
        "state": "",
        "labels": "[]",
    }
