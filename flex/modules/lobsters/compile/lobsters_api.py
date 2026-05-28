"""
Lobsters API client.

Reusable module for pulling data from Lobsters.
Used by both the one-shot worker and the incremental refresh script.

API docs: https://lobste.rs/about
"""

import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone


BASE_URL = "https://lobste.rs"
USER_AGENT = "flex-lobsters/1.0"
DELAY = 2.0  # small site — be respectful

DEFAULT_TAGS = [
    "ai", "ml", "python", "programming",
    "devops", "databases", "practices",
    "security",
]


def api_fetch(endpoint: str, params: dict | None = None) -> list | dict:
    """Fetch from Lobsters API. Returns parsed JSON (list or dict)."""
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{BASE_URL}/{endpoint}?{qs}"
    else:
        url = f"{BASE_URL}/{endpoint}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  [!] {endpoint} — {e}", file=sys.stderr)
        return []


def _parse_iso(ts_str: str) -> int:
    """Parse ISO 8601 timestamp to unix epoch seconds."""
    if not ts_str:
        return 0
    try:
        # Handle various ISO formats
        ts_str = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return 0


def pull_tag_feed(tag: str, quiet: bool = False) -> list[dict]:
    """Pull stories from a tag feed.

    Returns list of normalized story dicts (no comments).
    Tag feeds return ~25 items, no pagination.
    """
    if not quiet:
        print(f"  Tag: {tag}...", end="\r")

    data = api_fetch(f"t/{tag}.json")
    if not isinstance(data, list):
        return []

    stories = [normalize_story(s) for s in data]
    if not quiet:
        print(f"  Tag: {tag} — {len(stories)} stories{' ' * 20}")
    return stories


def pull_newest(pages: int = 3, after: int = 0,
                quiet: bool = False) -> list[dict]:
    """Pull newest stories, paginating through pages.

    Returns list of normalized story dicts.
    Stops when stories are older than `after` timestamp.
    """
    all_stories = []

    for page in range(1, pages + 1):
        if not quiet:
            print(f"  Newest page {page}...", end="\r")

        data = api_fetch("newest.json", {"page": page})
        if not isinstance(data, list) or not data:
            break

        batch = [normalize_story(s) for s in data]

        # Filter by after timestamp
        if after:
            batch = [s for s in batch if s["created_utc"] >= after]

        all_stories.extend(batch)

        # Stop if we've gone past our time window
        if after and batch and batch[-1]["created_utc"] < after:
            break

        time.sleep(DELAY)

    if not quiet:
        print(f"  Newest: {len(all_stories)} stories{' ' * 30}")
    return all_stories


def pull_story_details(short_id: str, quiet: bool = False) -> dict | None:
    """Fetch a single story with its full comment tree.

    Returns normalized story dict with 'comments' key containing
    flattened comment list, or None on failure.
    """
    data = api_fetch(f"s/{short_id}.json")
    if not isinstance(data, dict) or "short_id" not in data:
        return None

    story = normalize_story(data)
    raw_comments = data.get("comments", [])
    story["comments"] = flatten_comment_tree(raw_comments)

    if not quiet:
        print(f"  Story {short_id}: {len(story['comments'])} comments", end="\r")
    return story


def flatten_comment_tree(comments: list[dict]) -> list[dict]:
    """Flatten nested comment tree via DFS.

    Lobsters returns comments already flattened with depth/parent_comment
    fields, so we just normalize each one.
    """
    flat = []
    for c in comments:
        flat.append(normalize_comment(c))
    return flat


def normalize_story(story: dict) -> dict:
    """Normalize a raw Lobsters story into Flex-indexable format."""
    short_id = story.get("short_id", "")
    title = story.get("title", "") or ""
    description = story.get("description_plain", "") or story.get("description", "") or ""
    content = f"{title}\n\n{description}".strip() if description else title

    # Nested author
    submitter = story.get("submitter_user", {})
    if isinstance(submitter, dict):
        author = submitter.get("username", "")
    else:
        author = str(submitter) if submitter else ""

    tags = story.get("tags", [])
    if not isinstance(tags, list):
        tags = []

    return {
        "id": short_id,
        "type": "story",
        "author": author,
        "title": title,
        "body": description,
        "content": content,
        "score": story.get("score", 0),
        "num_comments": story.get("comment_count", 0),
        "url": story.get("comments_url", f"{BASE_URL}/s/{short_id}"),
        "story_url": story.get("url", ""),
        "created_utc": _parse_iso(story.get("created_at", "")),
        "tags": tags,
    }


def normalize_comment(comment: dict) -> dict:
    """Normalize a raw Lobsters comment into Flex-indexable format."""
    short_id = comment.get("short_id", "")
    body = comment.get("comment_plain", "") or comment.get("comment", "") or ""

    # Nested author
    commenter = comment.get("commenting_user", {})
    if isinstance(commenter, dict):
        author = commenter.get("username", "")
    else:
        author = str(commenter) if commenter else ""

    return {
        "id": short_id,
        "type": "comment",
        "author": author,
        "title": "",
        "body": body,
        "content": body,
        "score": comment.get("score", 0),
        "num_comments": 0,
        "url": comment.get("url", ""),
        "created_utc": _parse_iso(comment.get("created_at", "")),
        "depth": comment.get("depth", 0),
        "parent_id": comment.get("parent_comment", ""),
        "tags": [],
    }


def pull_stories_with_comments(tags: list[str] | None = None,
                                pages: int = 3, after: int = 0,
                                limit: int | None = None,
                                quiet: bool = False) -> list[dict]:
    """Pull stories from tag feeds and newest, then fetch details for
    stories with comments.

    Returns list of story dicts, each with a 'comments' key.
    This is the main entry point for the worker and refresh scripts.
    """
    if tags is None:
        tags = DEFAULT_TAGS

    seen_ids = set()
    all_stories = []

    # Pull from tag feeds
    for tag in tags:
        stories = pull_tag_feed(tag, quiet=quiet)
        for s in stories:
            if s["id"] not in seen_ids:
                if after and s["created_utc"] < after:
                    continue
                seen_ids.add(s["id"])
                all_stories.append(s)
                if limit and len(all_stories) >= limit:
                    break
        if limit and len(all_stories) >= limit:
            break
        time.sleep(DELAY)

    # Pull from newest
    if not limit or len(all_stories) < limit:
        newest = pull_newest(pages=pages, after=after, quiet=quiet)
        for s in newest:
            if s["id"] not in seen_ids:
                seen_ids.add(s["id"])
                all_stories.append(s)
                if limit and len(all_stories) >= limit:
                    break

    if not quiet:
        print(f"\n  Total unique stories: {len(all_stories)}")
        stories_with_comments = sum(1 for s in all_stories if s["num_comments"] > 0)
        print(f"  Stories with comments: {stories_with_comments}")
        print(f"  Fetching story details...")

    # Fetch details for stories with comments
    for i, story in enumerate(all_stories):
        if story["num_comments"] > 0:
            details = pull_story_details(story["id"], quiet=quiet)
            if details:
                story["comments"] = details.get("comments", [])
            time.sleep(DELAY)
        else:
            story["comments"] = []

    if not quiet:
        total_comments = sum(len(s.get("comments", [])) for s in all_stories)
        print(f"\n  Fetched details: {len(all_stories)} stories, "
              f"{total_comments} comments{' ' * 20}")

    return all_stories
