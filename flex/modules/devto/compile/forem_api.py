"""
Dev.to (Forem) API client.

Reusable module for pulling articles and comments from Dev.to.
Used by both the one-shot worker and the incremental refresh script.

API docs: https://developers.forem.com/api/v1
"""

import html
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone


BASE_URL = "https://dev.to/api"
USER_AGENT = "flex-devto/1.0"
BATCH_SIZE = 100
DELAY = 0.3  # 10 req/sec limit, 0.3s is safe

DEFAULT_TAGS = [
    "claudecode", "claude", "anthropic",
    "ai", "aitools",
    "mcp", "semanticsearch", "vectorsearch",
    "sqlite", "rag", "llm", "cli", "devtools",
]


def api_fetch(endpoint: str, params: dict | None = None) -> list | dict:
    """Fetch from Dev.to API. Returns parsed JSON."""
    url = f"{BASE_URL}/{endpoint}"
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  [!] {endpoint} — {e}", file=sys.stderr)
        return []


def strip_html(html_text: str) -> str:
    """Strip HTML tags and unescape entities. For comment body_html."""
    if not html_text:
        return ""
    text = re.sub(r'<[^>]+>', '', html.unescape(html_text))
    return text.strip()


def parse_iso_timestamp(iso_str: str) -> int:
    """Parse ISO 8601 timestamp to unix epoch seconds."""
    if not iso_str:
        return 0
    try:
        # Handle both Z and +00:00 suffixes
        iso_str = iso_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(iso_str)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return 0


def pull_articles(tag: str, after_ts: int = 0,
                  quiet: bool = False,
                  limit: int | None = None) -> list[dict]:
    """Pull articles for a tag, paginating until empty.

    For each article, fetches the full body_markdown via N+1 request.
    Returns list of normalized article dicts.
    """
    all_articles = []
    page = 1

    while True:
        params = {
            "tag": tag,
            "per_page": BATCH_SIZE,
            "page": page,
        }

        batch = api_fetch("articles", params)

        if not batch or not isinstance(batch, list):
            break

        # Filter by timestamp if after_ts set
        if after_ts:
            batch = [a for a in batch
                     if parse_iso_timestamp(a.get("published_at", "")) >= after_ts]

        if not batch:
            break

        if limit is not None:
            remaining = max(limit - len(all_articles), 0)
            if remaining <= 0:
                break
            batch = batch[:remaining]

        for article in batch:
            article_id = article.get("id")
            if not article_id:
                continue

            # N+1 fetch for full body_markdown
            time.sleep(DELAY)
            full = api_fetch(f"articles/{article_id}")
            if isinstance(full, dict) and full.get("body_markdown"):
                article["body_markdown"] = full["body_markdown"]

            normalized = normalize_article(article)
            all_articles.append(normalized)

        if not quiet:
            print(f"  tag={tag}: {len(all_articles)} articles (page {page})",
                  end="\r")
        time.sleep(DELAY)

        if limit is not None and len(all_articles) >= limit:
            break

        if len(batch) < BATCH_SIZE:
            break
        page += 1

        # Safety cap: max 1000 articles per tag
        if page > 10:
            break

    if not quiet:
        print(f"  tag={tag}: {len(all_articles)} articles total{' ' * 20}")
    return all_articles


def pull_articles_by_author(username: str, after_ts: int = 0,
                            quiet: bool = False,
                            limit: int | None = None) -> list[dict]:
    """Pull articles authored by `username`, paginating until empty.

    Uses `articles?username=<u>`. For each article, fetches full body_markdown
    via N+1 request. Filters by published_at >= after_ts.
    """
    all_articles = []
    page = 1

    while True:
        params = {
            "username": username,
            "per_page": BATCH_SIZE,
            "page": page,
        }

        batch = api_fetch("articles", params)

        if not batch or not isinstance(batch, list):
            break

        if after_ts:
            batch = [a for a in batch
                     if parse_iso_timestamp(a.get("published_at", "")) >= after_ts]

        if not batch:
            break

        if limit is not None:
            remaining = max(limit - len(all_articles), 0)
            if remaining <= 0:
                break
            batch = batch[:remaining]

        for article in batch:
            article_id = article.get("id")
            if not article_id:
                continue

            time.sleep(DELAY)
            full = api_fetch(f"articles/{article_id}")
            if isinstance(full, dict) and full.get("body_markdown"):
                article["body_markdown"] = full["body_markdown"]

            normalized = normalize_article(article)
            all_articles.append(normalized)

        if not quiet:
            print(f"  {username} articles: {len(all_articles)} (page {page})",
                  end="\r")
        time.sleep(DELAY)

        if limit is not None and len(all_articles) >= limit:
            break

        if len(batch) < BATCH_SIZE:
            break
        page += 1
        if page > 10:
            break

    if not quiet:
        print(f"  {username} articles: {len(all_articles)} total{' ' * 20}")
    return all_articles


def pull_comments(article_id: int, quiet: bool = False,
                  limit: int | None = None) -> list[dict]:
    """Pull comments for an article. Returns flattened list of normalized comments."""
    params = {"a_id": str(article_id)}
    time.sleep(DELAY)
    tree = api_fetch("comments", params)

    if not tree or not isinstance(tree, list):
        return []

    flat = flatten_comments(tree)
    if limit is not None:
        return flat[:limit]
    return flat


def flatten_comments(tree: list, parent_id: str | None = None) -> list[dict]:
    """Recursively flatten nested comment tree via DFS.

    Each comment has a 'children' array that may contain nested replies.
    Returns flat list of normalized comment dicts.
    """
    flat = []
    for comment in tree:
        normalized = normalize_comment(comment, parent_id)
        flat.append(normalized)
        children = comment.get("children", [])
        if children:
            flat.extend(flatten_comments(children, comment.get("id_code", "")))
    return flat


def normalize_article(article: dict) -> dict:
    """Normalize a raw Dev.to article into Flex-indexable format."""
    created_utc = parse_iso_timestamp(article.get("published_at", ""))
    title = article.get("title", "") or ""
    body = article.get("body_markdown", "") or article.get("description", "") or ""
    tag_list = article.get("tag_list", [])
    if isinstance(tag_list, str):
        tag_list = [t.strip() for t in tag_list.split(",") if t.strip()]

    return {
        "id": article.get("id", ""),
        "type": "article",
        "author": article.get("user", {}).get("username", ""),
        "title": title,
        "body": body,
        "content": f"{title}\n\n{body}".strip(),
        "score": article.get("positive_reactions_count", 0),
        "num_comments": article.get("comments_count", 0),
        "url": article.get("url", ""),
        "created_utc": created_utc,
        "tags": json.dumps(tag_list),
        "reading_time": article.get("reading_time_minutes", 0),
    }


def normalize_comment(comment: dict, parent_id: str | None = None) -> dict:
    """Normalize a raw Dev.to comment into Flex-indexable format."""
    created_utc = parse_iso_timestamp(comment.get("created_at", ""))
    body_html = comment.get("body_html", "") or ""
    content = strip_html(body_html)
    user = comment.get("user", {}) or {}

    return {
        "id": comment.get("id_code", ""),
        "type": "comment",
        "author": user.get("username", ""),
        "title": "",
        "body": content,
        "content": content,
        "score": 0,  # Dev.to comments don't have scores in API
        "num_comments": 0,
        "url": "",
        "created_utc": created_utc,
        "parent_id": parent_id or "",
        "tags": "[]",
        "reading_time": 0,
    }
