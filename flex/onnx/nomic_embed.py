"""
Nomic API embedder — drop-in replacement for ONNXEmbedder during backfill.

Uses the Nomic API (nomic-embed-text-v1.5) instead of local ONNX inference.
Same model, same 128d Matryoshka vectors, 100% compatible with cells embedded
by ONNXEmbedder. Intended for the initial backfill pass on CPU-only machines
where local ONNX would take hours.

Pure stdlib (urllib) — no nomic SDK required.
"""
import json
import time
import urllib.error
import urllib.request

import numpy as np
from typing import Callable, List, Optional, Union

_NOMIC_EMBED_URL = "https://api-atlas.nomic.ai/v1/embedding/text"


class NomicEmbedder:
    """Calls Nomic embed API directly via urllib. Same .encode() interface as ONNXEmbedder."""

    def __init__(self, api_key: str, model: str = "nomic-embed-text-v1.5"):
        self.api_key = api_key
        self.model = model
        self._batch_size = 64  # ~64 chunks keeps payload well under 1MB
        self._last_req_time = 0.0  # throttle tracker

    def validate(self) -> "str | None":
        """Test the key with a single embed. Returns error string or None on success."""
        try:
            self._post(["test"], timeout=10)
            return None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                return None  # key is valid, just rate limited
            try:
                detail = json.loads(e.read().decode())
                msg = detail.get("detail") or detail.get("message") or e.reason
            except Exception:
                msg = e.reason
            return f"HTTP {e.code}: {msg}"
        except Exception as e:
            return str(e)

    def _post(self, texts: List[str], timeout: int = 30) -> List[List[float]]:
        """Single HTTP POST to the Nomic embed endpoint. Returns raw embeddings list."""
        # Truncate + sanitize: strip null bytes and replace invalid UTF-8. Avoids 400/413.
        texts = [t[:2048].replace('\x00', '').encode('utf-8', errors='replace').decode('utf-8') for t in texts]
        body = json.dumps({
            "texts": texts,
            "model": self.model,
            "task_type": "search_document",
            "dimensionality": 128,
        }).encode()
        req = urllib.request.Request(
            _NOMIC_EMBED_URL,
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())["embeddings"]

    def _embed_batch(
        self,
        texts: List[str],
        on_wait: Optional[Callable[[float], None]] = None,
    ) -> np.ndarray:
        # Throttle: Nomic limit is 1200 req / 5 min = 4 req/sec. Stay at ~3.5.
        gap = time.time() - self._last_req_time
        if gap < 0.28:
            time.sleep(0.28 - gap)

        delay = 15  # start at 15s on 429 — Nomic rolling window resets slowly
        for attempt in range(6):
            try:
                self._last_req_time = time.time()
                embeddings = self._post(texts)
                return np.array(embeddings, dtype=np.float32)
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 5:
                    if on_wait:
                        on_wait(delay)
                    time.sleep(delay)
                    delay = min(delay * 2, 60)  # cap at 60s
                elif e.code == 400:
                    # Bad request — return empty array so worker zip skips the write, chunks stay NULL
                    return np.empty((0, 128), dtype=np.float32)
                else:
                    raise
        raise RuntimeError("Nomic API: max retries exceeded (429)")

    def encode(
        self,
        sentences: Union[str, List[str]],
        prefix: str = "search_document: ",  # accepted for API compat; task_type handles it
        matryoshka_dim: int = 128,
        on_wait: Optional[Callable[[float], None]] = None,
        **kwargs,
    ) -> np.ndarray:
        """Encode sentences via Nomic API. Returns (n, 128) float32 array."""
        if isinstance(sentences, str):
            sentences = [sentences]
        all_embs = []
        for i in range(0, len(sentences), self._batch_size):
            batch = sentences[i : i + self._batch_size]
            result = self._embed_batch(batch, on_wait=on_wait)
            if len(result) > 0:
                all_embs.append(result)
        if not all_embs:
            return np.empty((0, 128), dtype=np.float32)
        return np.vstack(all_embs)  # API returns unit-normalized vectors
