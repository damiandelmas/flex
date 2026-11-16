"""Loopback client for the single semantic-model owner in flex-mcp."""

from __future__ import annotations

import base64
import json
import os
import socket
import urllib.error
import urllib.request

import numpy as np

# The owner adds the prefix server-side and then cuts the combined string to
# the embedder's MAX_CHARS (4096), so sending MAX_CHARS characters is at least
# every character the model reads.
SERVICE_TEXT_CHARS = 4096


def service_only_enabled() -> bool:
    """Whether this process must use the shared semantic owner."""
    return os.environ.get("FLEX_EMBED_SERVICE_ONLY", "").lower() in {
        "1", "true", "yes",
    }


def encode_via_service(
    texts: str | list[str],
    *,
    model: str,
    mode: str,
    dim: int,
    timeout: float = 120,
) -> np.ndarray:
    """Encode through the local long-lived model owner.

    There is deliberately no silent in-process fallback: that would recreate
    a second ~910MB fp32 ONNX owner in the capture worker.
    """
    single = isinstance(texts, str)
    # The owner's embedder keeps only the first MAX_CHARS (4096) characters
    # after its prefix (flex/onnx/embed.py), so clipping here leaves every
    # vector byte-identical. Unclipped, one oversized row aborted its whole
    # batch on the request size limit and the same rows failed every retry.
    values = [v[:SERVICE_TEXT_CHARS] if isinstance(v, str) else v
              for v in ([texts] if single else list(texts))]
    if not values:
        raise ValueError("at least one text is required")
    payload = json.dumps({
        "model": model,
        "mode": mode,
        "dim": int(dim),
        "texts": values,
    }, ensure_ascii=False).encode("utf-8")
    # Ingest may hand us a whole commit (500 rows), while the shared owner
    # admits at most 64 texts and 1 MB per request. Split before admission,
    # preserving order and the caller's all-or-error publication contract.
    if len(values) > 64 or len(payload) > 1_000_000:
        if len(values) == 1:
            raise ValueError("one embedding text exceeds the service request byte limit")
        midpoint = min(64, max(1, len(values) // 2))
        return np.concatenate([
            encode_via_service(values[:midpoint], model=model, mode=mode, dim=dim, timeout=timeout),
            encode_via_service(values[midpoint:], model=model, mode=mode, dim=dim, timeout=timeout),
        ], axis=0)
    request = urllib.request.Request(
        os.environ.get("FLEX_EMBED_ENDPOINT", "http://127.0.0.1:7134/internal/embed"),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            decoded = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read()).get("error")
        except Exception:
            detail = None
        raise RuntimeError(
            f"semantic model service unavailable: {detail or exc.reason}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        reason = getattr(exc, "reason", exc)
        raise RuntimeError(f"semantic model service unavailable: {reason}") from exc
    if not isinstance(decoded, dict) or decoded.get("dtype") != "float32":
        raise RuntimeError("semantic model service returned an invalid response")
    try:
        shape = tuple(int(value) for value in decoded["shape"])
        raw = base64.b64decode(decoded["data"], validate=True)
        result = np.frombuffer(raw, dtype=np.float32).reshape(shape).copy()
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("semantic model service returned invalid vector bytes") from exc
    if result.shape != (len(values), int(dim)) or not np.isfinite(result).all():
        raise RuntimeError(
            "semantic model service returned an invalid vector shape or value"
        )
    return result[0] if single else result


def document_encoder(model: str | None, dim: int):
    """Build an ingest-compatible callable backed by the local service."""
    model_name = model or "minilm"

    def encode(texts, **_ignored):
        return encode_via_service(
            texts,
            model=model_name,
            mode="document",
            dim=dim,
        )

    return encode
