"""
Hacker News Algolia API client.

Reusable module for pulling stories and comments from HN via the Algolia search API.
Used by both the one-shot worker and the incremental refresh script.

API docs: https://hn.algolia.com/api
"""

import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone


BASE_URL = "https://hn.algolia.com/api/v1"
USER_AGENT = "flex-hn/1.0"
HITS_PER_PAGE = 100
MAX_PAGES = 10  # 10 pages * 100 = 1000 hits max per query
DELAY = 0.5  # be respectful


def api_fetch(endpoint: str, params: dict) -> dict:
    """Fetch from HN Algolia API. Returns parsed JSON."""
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/{endpoint}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  [!] {endpoint} — {e}", file=sys.stderr)
        return {"hits": [], "nbPages": 0}


def _page_limit(max_pages: int | None) -> int:
    return MAX_PAGES if max_pages is None else max(0, int(max_pages))


def _hits_limit(hits_per_page: int | None) -> int:
    return HITS_PER_PAGE if hits_per_page is None else max(1, int(hits_per_page))


def pull_stories(query: str, after_ts: int = 0,
                 quiet: bool = False, max_pages: int | None = None,
                 hits_per_page: int | None = None) -> list[dict]:
    """Pull stories matching query, created after after_ts.

    Paginates via page=N. Returns list of normalized story dicts.
    """
    all_stories = []

    for page in range(_page_limit(max_pages)):
        params = {
            "query": query,
            "tags": "story",
            "hitsPerPage": _hits_limit(hits_per_page),
            "page": page,
        }
        if after_ts:
            params["numericFilters"] = f"created_at_i>{after_ts}"

        data = api_fetch("search", params)
        batch = data.get("hits", [])
        nb_pages = data.get("nbPages", 0)

        if not batch:
            break

        normalized = [normalize_story(s) for s in batch]
        all_stories.extend(normalized)

        if not quiet:
            latest_ts = batch[-1].get("created_at_i", 0)
            if latest_ts:
                print(f"  stories: {len(all_stories)} (latest: "
                      f"{datetime.fromtimestamp(latest_ts, tz=timezone.utc).date()})",
                      end="\r")

        time.sleep(DELAY)

        if page + 1 >= nb_pages:
            break

    if not quiet:
        print(f"  stories: {len(all_stories)} total{' ' * 30}")
    return all_stories


def pull_comments_for_story(story_id: str, after_ts: int = 0,
                            quiet: bool = False, max_pages: int | None = None,
                            hits_per_page: int | None = None) -> list[dict]:
    """Pull all comments for a given story ID.

    Uses tags=comment,story_STORYID for server-side filtering.
    Paginates via page=N. Returns list of normalized comment dicts.
    """
    all_comments = []

    for page in range(_page_limit(max_pages)):
        params = {
            "tags": f"comment,story_{story_id}",
            "hitsPerPage": _hits_limit(hits_per_page),
            "page": page,
        }
        if after_ts:
            params["numericFilters"] = f"created_at_i>{after_ts}"

        data = api_fetch("search", params)
        batch = data.get("hits", [])
        nb_pages = data.get("nbPages", 0)

        if not batch:
            break

        normalized = [normalize_comment(c, story_id) for c in batch]
        all_comments.extend(normalized)

        time.sleep(DELAY)

        if page + 1 >= nb_pages:
            break

    if not quiet:
        print(f"    comments for story {story_id}: {len(all_comments)}")
    return all_comments


def pull_stories_by_author(author: str, after_ts: int = 0,
                           quiet: bool = False, max_pages: int | None = None,
                           hits_per_page: int | None = None) -> list[dict]:
    """Pull stories authored by `author`, created after after_ts.

    Uses Algolia tag `author_<name>` for server-side filtering.
    """
    all_stories = []

    for page in range(_page_limit(max_pages)):
        params = {
            "tags": f"story,author_{author}",
            "hitsPerPage": _hits_limit(hits_per_page),
            "page": page,
        }
        if after_ts:
            params["numericFilters"] = f"created_at_i>{after_ts}"

        data = api_fetch("search", params)
        batch = data.get("hits", [])
        nb_pages = data.get("nbPages", 0)

        if not batch:
            break

        normalized = [normalize_story(s) for s in batch]
        all_stories.extend(normalized)

        if not quiet:
            latest_ts = batch[-1].get("created_at_i", 0)
            if latest_ts:
                print(f"  stories by {author}: {len(all_stories)} (latest: "
                      f"{datetime.fromtimestamp(latest_ts, tz=timezone.utc).date()})",
                      end="\r")

        time.sleep(DELAY)

        if page + 1 >= nb_pages:
            break

    if not quiet:
        print(f"  stories by {author}: {len(all_stories)} total{' ' * 30}")
    return all_stories


def pull_comments_by_author(author: str, after_ts: int = 0,
                            quiet: bool = False, max_pages: int | None = None,
                            hits_per_page: int | None = None) -> list[dict]:
    """Pull comments authored by `author`, created after after_ts.

    Uses Algolia tag `author_<name>` for server-side filtering.
    """
    all_comments = []

    for page in range(_page_limit(max_pages)):
        params = {
            "tags": f"comment,author_{author}",
            "hitsPerPage": _hits_limit(hits_per_page),
            "page": page,
        }
        if after_ts:
            params["numericFilters"] = f"created_at_i>{after_ts}"

        data = api_fetch("search", params)
        batch = data.get("hits", [])
        nb_pages = data.get("nbPages", 0)

        if not batch:
            break

        # story_id comes from the comment payload itself
        normalized = [normalize_comment(c) for c in batch]
        all_comments.extend(normalized)

        if not quiet:
            latest_ts = batch[-1].get("created_at_i", 0)
            if latest_ts:
                print(f"  comments by {author}: {len(all_comments)} (latest: "
                      f"{datetime.fromtimestamp(latest_ts, tz=timezone.utc).date()})",
                      end="\r")

        time.sleep(DELAY)

        if page + 1 >= nb_pages:
            break

    if not quiet:
        print(f"  comments by {author}: {len(all_comments)} total{' ' * 30}")
    return all_comments


def normalize_story(story: dict) -> dict:
    """Normalize a raw Algolia story hit into Flex-indexable format."""
    object_id = story.get("objectID", "")
    created = story.get("created_at_i", 0)
    title = story.get("title", "") or ""
    story_text = story.get("story_text", "") or ""
    url = story.get("url", "") or ""

    # Content: title + story_text for self-posts
    if story_text:
        content = f"{title}\n\n{story_text}"
    else:
        content = title

    return {
        "id": object_id,
        "type": "story",
        "author": story.get("author", ""),
        "title": title,
        "body": story_text,
        "content": content.strip(),
        "score": story.get("points", 0) or 0,
        "num_comments": story.get("num_comments", 0) or 0,
        "url": f"https://news.ycombinator.com/item?id={object_id}",
        "hn_url": url,  # external link URL
        "created_utc": created,
        "story_id": object_id,
        "parent_id": None,
    }


def normalize_comment(comment: dict, story_id: str = "") -> dict:
    """Normalize a raw Algolia comment hit into Flex-indexable format."""
    object_id = comment.get("objectID", "")
    created = comment.get("created_at_i", 0)
    comment_text = comment.get("comment_text", "") or ""
    # story_id from the comment payload, fall back to parameter
    c_story_id = str(comment.get("story_id", "")) or story_id
    parent_id = str(comment.get("parent_id", "")) or ""

    return {
        "id": object_id,
        "type": "comment",
        "author": comment.get("author", ""),
        "title": comment.get("story_title", "") or "",
        "body": comment_text,
        "content": comment_text.strip(),
        "score": comment.get("points", 0) or 0,
        "num_comments": 0,
        "url": f"https://news.ycombinator.com/item?id={object_id}",
        "hn_url": "",
        "created_utc": created,
        "story_id": c_story_id,
        "parent_id": parent_id,
    }
