"""
Scope — tombstone filter for reddit cells at ingest time.

Three filtering levels for reddit:

    JSONL -> [SCOPE]   -> parse -> chunks
          -> [NOISE]   -> _enrich_* columns, graph where clause
          -> [SURFACE] -> views filtered by _meta thresholds

SCOPE is minimal on purpose. It only drops content that carries ZERO signal:
empty bodies, [deleted]/[removed] tombstones. Nothing else.

Quality thresholds (score, author class, subreddit scope) live in `_meta` and
are applied at the SURFACE and NOISE levels via views and graph filters. That
makes them tunable at any time without re-ingest — `_meta` is the lever.

Loaded once at import time. No user config on this level — it's a hard floor,
not a policy.
"""

from typing import Optional


TOMBSTONE_BODIES: set[str] = {
    '',
    '[deleted]',
    '[removed]',
    '[unavailable]',
}


def _is_tombstone(body: Optional[str]) -> bool:
    if body is None:
        return True
    return body.strip() in TOMBSTONE_BODIES


def should_skip_post(post: dict) -> bool:
    """Drop a post only if it has no recoverable content.

    A post with an empty/deleted body but a real title is still ingestable
    (the title becomes the chunk body downstream). This function returns True
    only when both the body AND title are tombstones.
    """
    body = post.get('content') or post.get('body') or ''
    title = post.get('title') or ''
    return _is_tombstone(body) and _is_tombstone(title)


def should_skip_comment(comment: dict) -> bool:
    """Drop a comment only if its body is empty or a tombstone."""
    body = comment.get('content') or comment.get('body') or ''
    return _is_tombstone(body)
