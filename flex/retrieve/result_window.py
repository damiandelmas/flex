"""Bounded, lossless delivery windows for large JSON query results.

The query executor has already produced an immutable JSON result by the time
this module is called.  A :class:`ResultWindowStore` keeps that snapshot for a
short period and exposes it as deterministic structural fragments.  The first
window and every continuation contain disjoint value data; callers never need
to rerun a query merely to receive the remainder of its result.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Iterator


_MAX_PIECE_CHARS = 1200
_STRING_CHUNK_CHARS = 900


def _encoded_chars(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _pieces(value: Any, path: tuple[Any, ...] = ()) -> Iterator[dict[str, Any]]:
    """Yield lossless, ordered pieces whose value payloads never overlap."""
    if _encoded_chars(value) <= _MAX_PIECE_CHARS:
        yield {"path": list(path), "value": value}
        return

    if isinstance(value, dict):
        yield {"path": list(path), "container": "object", "length": len(value)}
        for key, child in value.items():
            yield from _pieces(child, (*path, key))
        return

    if isinstance(value, list):
        yield {"path": list(path), "container": "array", "length": len(value)}
        for index, child in enumerate(value):
            yield from _pieces(child, (*path, index))
        return

    if isinstance(value, str):
        for start in range(0, len(value), _STRING_CHUNK_CHARS):
            end = min(start + _STRING_CHUNK_CHARS, len(value))
            yield {
                "path": list(path),
                "string_fragment": value[start:end],
                "char_start": start,
                "char_end": end,
                "value_chars": len(value),
            }
        return

    # JSON scalars cannot normally exceed the piece bound, but retain a total
    # fallback so unusual integer/string adapters still produce valid output.
    yield {"path": list(path), "value": value}


@dataclass
class _Snapshot:
    result_id: str
    owner: str
    cell: str
    query: str
    fingerprint: str
    row_count: int
    estimated_tokens: int
    original_chars: int
    pieces: list[dict[str, Any]]
    cursor: int
    created_at: float
    touched_at: float


class ResultWindowStore:
    """Caller-scoped TTL/LRU cache of already-executed JSON results."""

    def __init__(
        self,
        *,
        ttl_seconds: float = 300.0,
        max_entries: int = 32,
        max_result_chars: int = 350_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self.max_result_chars = max_result_chars
        self.clock = clock
        self._items: OrderedDict[tuple[str, str, str], _Snapshot] = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def _key(owner: str, cell: str, query: str) -> tuple[str, str, str]:
        return owner, cell, query.strip()

    def _expire(self, now: float) -> None:
        expired = [
            key for key, item in self._items.items()
            if now - item.touched_at > self.ttl_seconds
        ]
        for key in expired:
            self._items.pop(key, None)

    def discard(self, owner: str, cell: str, query: str) -> None:
        with self._lock:
            self._items.pop(self._key(owner, cell, query), None)

    def start(
        self,
        *,
        owner: str,
        cell: str,
        query: str,
        result_json: str,
        row_count: int,
        estimated_tokens: int,
        page_chars: int,
    ) -> dict[str, Any]:
        """Store one immutable result and return its first delivery window."""
        key = self._key(owner, cell, query)
        now = self.clock()
        with self._lock:
            self._expire(now)
            self._items.pop(key, None)
            if len(result_json) > self.max_result_chars:
                return {
                    "status": "result_too_large",
                    "row_count": row_count,
                    "estimated_tokens": estimated_tokens,
                    "result_chars": len(result_json),
                    "limit_chars": self.max_result_chars,
                    "has_more": False,
                    "hint": "Narrow the SQL with WHERE/LIMIT before requesting data.",
                }
            try:
                parsed = json.loads(result_json)
            except (TypeError, ValueError, json.JSONDecodeError):
                return {
                    "status": "invalid_result_json",
                    "has_more": False,
                }

            snapshot = _Snapshot(
                result_id=uuid.uuid4().hex,
                owner=owner,
                cell=cell,
                query=query.strip(),
                fingerprint=hashlib.sha256(result_json.encode("utf-8")).hexdigest(),
                row_count=row_count,
                estimated_tokens=estimated_tokens,
                original_chars=len(result_json),
                pieces=list(_pieces(parsed)),
                cursor=0,
                created_at=now,
                touched_at=now,
            )
            self._items[key] = snapshot
            self._items.move_to_end(key)
            while len(self._items) > self.max_entries:
                self._items.popitem(last=False)
            return self._page(snapshot, page_chars=page_chars)

    def continue_result(
        self,
        *,
        owner: str,
        cell: str,
        query: str,
        page_chars: int,
    ) -> dict[str, Any]:
        """Return only pieces not delivered by earlier windows."""
        key = self._key(owner, cell, query)
        now = self.clock()
        with self._lock:
            self._expire(now)
            snapshot = self._items.get(key)
            if snapshot is None:
                return {
                    "status": "continuation_unavailable",
                    "has_more": False,
                    "hint": "Run the query without ! to create a new result snapshot.",
                }
            snapshot.touched_at = now
            self._items.move_to_end(key)
            return self._page(snapshot, page_chars=page_chars)

    @staticmethod
    def _page(snapshot: _Snapshot, *, page_chars: int) -> dict[str, Any]:
        start = snapshot.cursor
        fragments: list[dict[str, Any]] = []
        used = 0
        while snapshot.cursor < len(snapshot.pieces):
            piece = snapshot.pieces[snapshot.cursor]
            piece_chars = _encoded_chars(piece)
            if fragments and used + piece_chars > page_chars:
                break
            fragments.append(piece)
            used += piece_chars
            snapshot.cursor += 1

        return {
            "status": "window",
            "result_id": snapshot.result_id,
            "result_fingerprint": snapshot.fingerprint,
            "row_count": snapshot.row_count,
            "estimated_tokens": snapshot.estimated_tokens,
            "result_chars": snapshot.original_chars,
            "fragment_start": start,
            "fragment_end": snapshot.cursor,
            "fragments_total": len(snapshot.pieces),
            "fragments": fragments,
            "has_more": snapshot.cursor < len(snapshot.pieces),
            "continuation": "Prefix the identical query with !" if snapshot.cursor < len(snapshot.pieces) else None,
        }


def reconstruct_fragments(fragments: list[dict[str, Any]]) -> Any:
    """Reconstruct a JSON value from a complete ordered fragment stream.

    This helper is intentionally public for contract tests and downstream
    clients that want to mechanically verify lossless traversal.
    """
    root: Any = None
    initialized = False

    def assign(path: list[Any], value: Any, *, append_string: bool = False) -> None:
        nonlocal root, initialized
        if not path:
            if append_string:
                root = (root or "") + value
            else:
                root = value
            initialized = True
            return
        if not initialized:
            raise ValueError("fragment stream has no root container")
        parent = root
        for part in path[:-1]:
            parent = parent[part]
        leaf = path[-1]
        if append_string:
            current = parent.get(leaf, "") if isinstance(parent, dict) else parent[leaf]
            parent[leaf] = (current or "") + value
        else:
            parent[leaf] = value

    for piece in fragments:
        path = piece["path"]
        if "container" in piece:
            value: Any = {} if piece["container"] == "object" else [None] * piece["length"]
            assign(path, value)
        elif "string_fragment" in piece:
            assign(path, piece["string_fragment"], append_string=True)
        else:
            assign(path, piece.get("value"))
    return root
