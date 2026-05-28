"""
Arctic Shift API client for Reddit data.

Reusable module for pulling posts and comments from the Arctic Shift archive.
Used by both the one-shot worker and the incremental refresh script.

API docs: https://arctic-shift.photon-reddit.com/

Rate limits (dynamic, returned via x-ratelimit headers):
  - Hard limit: ~2000 req/min (varies)
  - "Slow down a little" error on expensive queries
  - Settings modeled after BAScraper (MIT), the most-used Arctic Shift client.
"""

import json
import random
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone


BASE_URL = "https://arctic-shift.photon-reddit.com/api"
USER_AGENT = "flex-reddit/1.0"
BATCH_SIZE = 100
BASE_DELAY = 1.0       # seconds between requests (BAScraper default)
BACKOFF_BASE = 3.0     # base backoff on failure
MAX_RETRIES = 5        # retries before giving up on a request
JITTER = 0.3           # random jitter factor (0-30% added to delays)

# Adaptive state — updated from x-ratelimit headers
_rate_state = {
    "delay": BASE_DELAY,
    "remaining": None,
    "reset_at": None,
}


def _parse_rate_headers(resp) -> None:
    """Read x-ratelimit headers and adapt pacing."""
    remaining = resp.headers.get("x-ratelimit-remaining")
    reset = resp.headers.get("x-ratelimit-reset")

    if remaining is not None:
        _rate_state["remaining"] = int(float(remaining))
    if reset is not None:
        _rate_state["reset_at"] = time.time() + int(float(reset))

    # Adaptive: slow down when remaining is low
    if _rate_state["remaining"] is not None:
        if _rate_state["remaining"] < 50:
            _rate_state["delay"] = 3.0
        elif _rate_state["remaining"] < 200:
            _rate_state["delay"] = 2.0
        elif _rate_state["remaining"] < 500:
            _rate_state["delay"] = 1.5
        else:
            _rate_state["delay"] = BASE_DELAY


def _sleep():
    """Sleep with adaptive delay + jitter."""
    delay = _rate_state["delay"]
    delay += delay * random.uniform(0, JITTER)
    time.sleep(delay)


def api_fetch(endpoint: str, params: dict) -> dict:
    """Fetch from Arctic Shift API with retry + exponential backoff."""
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/{endpoint}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                _parse_rate_headers(resp)
                data = json.loads(resp.read().decode())
                # Arctic Shift returns error key on overload
                if data.get("error"):
                    raise RuntimeError(data["error"])
                return data
        except Exception as e:
            is_last = attempt >= MAX_RETRIES
            err_str = str(e)
            if is_last:
                print(f"  [!] {endpoint} — FAILED after {MAX_RETRIES + 1} "
                      f"attempts: {err_str}", file=sys.stderr)
                return {"data": []}

            # Exponential backoff with jitter
            wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1)
            print(f"  [~] {endpoint} — {err_str}, retry {attempt + 1}/"
                  f"{MAX_RETRIES} in {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)


def pull_posts(subreddit: str, after: int = 0, before: int = 0,
               quiet: bool = False) -> list[dict]:
    """Pull posts from a subreddit, paginating by created_utc.

    Returns list of normalized post dicts.
    """
    all_posts = []
    cursor = after

    while True:
        params = {
            "subreddit": subreddit,
            "limit": BATCH_SIZE,
            "sort": "asc",
        }
        if cursor:
            params["after"] = cursor
        if before:
            params["before"] = before

        data = api_fetch("posts/search", params)
        batch = data.get("data", [])

        if not batch:
            break

        normalized = [normalize_post(p, subreddit) for p in batch]
        all_posts.extend(normalized)
        cursor = batch[-1].get("created_utc", 0)

        if not quiet:
            print(f"  posts: {len(all_posts)} (latest: "
                  f"{datetime.fromtimestamp(cursor, tz=timezone.utc).date()})",
                  end="\r")
        _sleep()

        if len(batch) < BATCH_SIZE:
            break

    if not quiet:
        print(f"  posts: {len(all_posts)} total{' ' * 30}")
    return all_posts


def pull_comments(subreddit: str, after: int = 0, before: int = 0,
                  out_path: str | None = None,
                  quiet: bool = False) -> list[dict]:
    """Pull comments from a subreddit, paginating by created_utc.

    If out_path is given, streams JSONL incrementally (memory-safe for huge subs).
    Returns list of normalized comment dicts (empty if streaming to file).
    """
    all_comments = []
    cursor = after
    total = 0
    out_file = open(out_path, "w") if out_path else None

    try:
        while True:
            params = {
                "subreddit": subreddit,
                "limit": BATCH_SIZE,
                "sort": "asc",
            }
            if cursor:
                params["after"] = cursor
            if before:
                params["before"] = before

            data = api_fetch("comments/search", params)
            batch = data.get("data", [])

            if not batch:
                break

            normalized = [normalize_comment(c, subreddit) for c in batch]
            total += len(normalized)

            if out_file:
                for item in normalized:
                    out_file.write(json.dumps(item, default=str) + "\n")
                out_file.flush()
            else:
                all_comments.extend(normalized)

            cursor = batch[-1].get("created_utc", 0)
            if not quiet:
                print(f"  comments: {total} (latest: "
                      f"{datetime.fromtimestamp(cursor, tz=timezone.utc).date()})",
                      end="\r")
            _sleep()

            if len(batch) < BATCH_SIZE:
                break
    finally:
        if out_file:
            out_file.close()

    if not quiet:
        print(f"  comments: {total} total{' ' * 30}")
    return all_comments


def pull_posts_by_ids(post_ids: list[str], quiet: bool = False) -> list[dict]:
    """Batch-fetch posts by ID via posts/ids endpoint.

    Used to backfill parent threads for author comments on external threads.
    Returns list of normalized post dicts. Missing IDs are silently dropped.
    """
    if not post_ids:
        return []

    # Arctic Shift accepts comma-separated IDs, cap at 100 per request
    results = []
    for i in range(0, len(post_ids), 100):
        chunk_ids = post_ids[i:i + 100]
        params = {"ids": ",".join(chunk_ids)}
        data = api_fetch("posts/ids", params)
        batch = data.get("data", [])
        for p in batch:
            sub = p.get("subreddit", "")
            results.append(normalize_post(p, sub))

        if not quiet:
            print(f"  parent posts: {len(results)}/{len(post_ids)}", end="\r")
        _sleep()

    if not quiet:
        print(f"  parent posts: {len(results)}/{len(post_ids)} found{' ' * 20}")
    return results


def pull_posts_by_author(author: str, after: int = 0, before: int = 0,
                         quiet: bool = False) -> list[dict]:
    """Pull posts authored by a user across all subreddits, paginating by created_utc.

    Returns list of normalized post dicts with subreddit from each post's own record.
    """
    all_posts = []
    cursor = after

    while True:
        params = {
            "author": author,
            "limit": BATCH_SIZE,
            "sort": "asc",
        }
        if cursor:
            params["after"] = cursor
        if before:
            params["before"] = before

        data = api_fetch("posts/search", params)
        batch = data.get("data", [])

        if not batch:
            break

        # Each post carries its own subreddit — normalize per-item
        normalized = [normalize_post(p, p.get("subreddit", "")) for p in batch]
        all_posts.extend(normalized)
        cursor = batch[-1].get("created_utc", 0)

        if not quiet:
            print(f"  posts by u/{author}: {len(all_posts)} (latest: "
                  f"{datetime.fromtimestamp(cursor, tz=timezone.utc).date()})",
                  end="\r")
        _sleep()

        if len(batch) < BATCH_SIZE:
            break

    if not quiet:
        print(f"  posts by u/{author}: {len(all_posts)} total{' ' * 30}")
    return all_posts


def pull_comments_by_author(author: str, after: int = 0, before: int = 0,
                            quiet: bool = False) -> list[dict]:
    """Pull comments authored by a user across all subreddits, paginating by created_utc.

    Returns list of normalized comment dicts with subreddit from each comment's own record.
    """
    all_comments = []
    cursor = after

    while True:
        params = {
            "author": author,
            "limit": BATCH_SIZE,
            "sort": "asc",
        }
        if cursor:
            params["after"] = cursor
        if before:
            params["before"] = before

        data = api_fetch("comments/search", params)
        batch = data.get("data", [])

        if not batch:
            break

        normalized = [normalize_comment(c, c.get("subreddit", "")) for c in batch]
        all_comments.extend(normalized)
        cursor = batch[-1].get("created_utc", 0)

        if not quiet:
            print(f"  comments by u/{author}: {len(all_comments)} (latest: "
                  f"{datetime.fromtimestamp(cursor, tz=timezone.utc).date()})",
                  end="\r")
        _sleep()

        if len(batch) < BATCH_SIZE:
            break

    if not quiet:
        print(f"  comments by u/{author}: {len(all_comments)} total{' ' * 30}")
    return all_comments


def normalize_post(post: dict, subreddit: str) -> dict:
    """Normalize a raw Arctic Shift post into Flex-indexable format."""
    created = post.get("created_utc", 0)
    selftext = post.get("selftext", "") or ""
    title = post.get("title", "") or ""

    return {
        "id": post.get("id", ""),
        "type": "post",
        "subreddit": subreddit,
        "author": post.get("author", "[deleted]"),
        "title": title,
        "body": selftext,
        "content": f"{title}\n\n{selftext}".strip(),
        "score": post.get("score", 0),
        "num_comments": post.get("num_comments", 0),
        "url": f"https://www.reddit.com/r/{subreddit}/comments/{post.get('id', '')}/",
        "permalink": post.get("permalink", ""),
        "created_utc": created,
        "link_id": f"t3_{post.get('id', '')}",
        "depth": 0,
    }


def normalize_comment(comment: dict, subreddit: str) -> dict:
    """Normalize a raw Arctic Shift comment into Flex-indexable format."""
    created = comment.get("created_utc", 0)
    body = comment.get("body", "") or ""

    return {
        "id": comment.get("id", ""),
        "type": "comment",
        "subreddit": subreddit,
        "author": comment.get("author", "[deleted]"),
        "title": "",
        "body": body,
        "content": body,
        "score": comment.get("score", 0),
        "num_comments": 0,
        "url": f"https://www.reddit.com{comment.get('permalink', '')}",
        "permalink": comment.get("permalink", ""),
        "created_utc": created,
        "parent_id": comment.get("parent_id", ""),
        "link_id": comment.get("link_id", ""),
        "depth": comment.get("depth", 0),
    }
