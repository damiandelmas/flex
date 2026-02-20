"""Claude Code session fingerprint — HDBSCAN config and fingerprint builder.

Builds a flat chronological navigational index for each session:
strange attractor sentences (from chunk-level HDBSCAN on pre-computed embeddings)
interleaved with every tool call. No collapsing. No truncation. No dedup. Raw output.

Selection ethic: extremes, not centroids. The chunk farthest from each cluster's
centroid — where thinking deviated, pivoted, or crystallized. Within that chunk,
the highest-entropy sentence surfaces. Noise chunks are first-class.

Zero ONNX at enrichment time — uses _raw_chunks.embedding (already in DB).

Structure: chronological index with [N] message_number prefixes. Tool calls
interleaved. Gaps between numbers reveal rhythm.
"""

import math
import os
import re
from collections import Counter

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


# ---------------------------------------------------------------------------
# HDBSCAN — lazy import
# ---------------------------------------------------------------------------

_hdbscan_mod = None

def _get_hdbscan():
    global _hdbscan_mod
    if _hdbscan_mod is None:
        import hdbscan
        _hdbscan_mod = hdbscan
    return _hdbscan_mod


# ---------------------------------------------------------------------------
# HDBSCAN parameters — tuned for claude-code session sizes
# ---------------------------------------------------------------------------

HDBSCAN_MIN_CHUNKS = 20
HDBSCAN_MIN_CLUSTER_SIZE = 5
HDBSCAN_MIN_SAMPLES = 3
HDBSCAN_METRIC = 'euclidean'

# ---------------------------------------------------------------------------
# Fingerprint config
# ---------------------------------------------------------------------------

SKIP_TOOLS = {'UserPrompt', 'TaskOutput', 'BashOutput'}
SPAN_MIN_LEN = 50
MAX_NOISE_REPS = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_content_chunk(chunk):
    """True if chunk carries human-readable text (not a tool call)."""
    tool = chunk.get('tool_name')
    if tool and tool not in ('UserPrompt',):
        return False
    return True


def _text_entropy(text):
    """Shannon entropy of word distribution. Higher = more diverse vocabulary."""
    words = text.lower().split()
    if not words:
        return 0.0
    counts = Counter(words)
    total = len(words)
    return -sum((c / total) * math.log2(c / total) for c in counts.values())


# ---------------------------------------------------------------------------
# Shatter — decompose content chunks into sentence-level spans
# ---------------------------------------------------------------------------

def shatter_spans(chunks):
    """Shatter content chunks into sentence-level spans.

    Each span inherits its parent chunk's message_number.
    Only content-bearing chunks are shattered (user_prompt + assistant text).
    Tool call chunks pass through to Phase 2 (tool line formatting) unchanged.

    Returns list of {text, message_number, chunk_idx, span_idx}
    """
    spans = []
    for i, chunk in enumerate(chunks):
        if not _is_content_chunk(chunk):
            continue
        content = chunk.get('content', '').strip()
        if not content:
            continue

        sents = re.split(r'(?<=[.!?])\s+', content)
        for j, sent in enumerate(sents):
            sent = sent.strip()
            if len(sent) > SPAN_MIN_LEN:
                spans.append({
                    'text': sent,
                    'message_number': chunk.get('message_number', 0),
                    'chunk_idx': i,
                    'span_idx': j,
                })
    return spans


# ---------------------------------------------------------------------------
# Chunk-level attractor selection + entropy-based span picking
# ---------------------------------------------------------------------------

def _best_span_from_chunk(chunk):
    """Shatter one chunk, pick the highest-entropy span."""
    spans = shatter_spans([chunk])
    if not spans:
        content = chunk.get('content', '').strip()
        if len(content) > SPAN_MIN_LEN:
            return {
                'text': content[:200],
                'message_number': chunk.get('message_number', 0),
            }
        return None
    return max(spans, key=lambda s: _text_entropy(s['text']))


def select_representatives(content_items, embeddings, labels):
    """Chunk-level attractor selection + entropy-based span picking.

    Per cluster: find chunk farthest from centroid (the attractor),
    shatter it, pick the highest-entropy sentence.
    Noise chunks: same treatment.

    content_items: list of (original_index, chunk_dict)
    embeddings: np.array of chunk embeddings
    labels: HDBSCAN labels array

    Returns list of {message_number, text, source}.
    """
    reps = []
    unique_labels = set(labels)
    unique_labels.discard(-1)

    for lbl in sorted(unique_labels):
        indices = [i for i, l in enumerate(labels) if l == lbl]
        vecs = embeddings[indices]
        centroid = vecs.mean(axis=0)
        centroid /= (np.linalg.norm(centroid) + 1e-10)

        sims = cosine_similarity(centroid.reshape(1, -1), vecs)[0]
        farthest_local = np.argmin(sims)
        farthest_idx = indices[farthest_local]

        _, chunk = content_items[farthest_idx]
        span = _best_span_from_chunk(chunk)
        if span:
            reps.append({
                'message_number': span['message_number'],
                'text': span['text'],
                'source': f'cluster_{lbl}',
            })

    # Noise chunks — capped by entropy, not dumped raw
    noise_candidates = []
    for i, l in enumerate(labels):
        if l == -1:
            _, chunk = content_items[i]
            span = _best_span_from_chunk(chunk)
            if span:
                noise_candidates.append({
                    'message_number': span['message_number'],
                    'text': span['text'],
                    'source': 'noise',
                    '_entropy': _text_entropy(span['text']),
                })

    # Keep top N by entropy — the most informationally dense noise
    noise_candidates.sort(key=lambda x: x['_entropy'], reverse=True)
    for nc in noise_candidates[:MAX_NOISE_REPS]:
        reps.append({k: v for k, v in nc.items() if not k.startswith('_')})

    return reps


# ---------------------------------------------------------------------------
# Tool call formatting — one line per call, no collapsing
# ---------------------------------------------------------------------------

def format_tool_line(chunk):
    """Format a single tool call chunk into one line. Returns str or None."""
    tool = chunk.get('tool_name')
    if not tool or tool in SKIP_TOOLS:
        return None

    target = chunk.get('target_file')
    content = chunk.get('content', '') or ''

    if tool in ('Read', 'Write', 'Edit', 'MultiEdit'):
        basename = os.path.basename(target) if target else ''
        return f"{tool} {basename}" if basename else tool

    if tool == 'Bash':
        cmd = content.strip()
        if cmd.startswith('Bash:'):
            cmd = cmd[5:].strip()
        if cmd and cmd != 'Bash' and len(cmd) > 3:
            return f"Bash: {cmd[:60]}"
        return "Bash"

    if tool == 'Task':
        desc = content[:60].strip()
        return f'Task: "{desc}"' if desc else "Task"

    if tool in ('WebSearch', 'WebFetch'):
        query = content[:60].strip()
        return f"{tool}: {query}" if query else tool

    if tool.startswith('mcp__'):
        parts = tool.split('__')
        short = parts[-1] if len(parts) > 2 else 'MCP tool'
        return f"{short} query"

    return tool


# ---------------------------------------------------------------------------
# Fingerprint composition — zero ONNX, chunk-level HDBSCAN
# ---------------------------------------------------------------------------

def build_fingerprint(chunks, labels_unused=None, embed_fn=None, span_embeddings=None):
    """Build session fingerprint from chunk-level HDBSCAN + entropy span selection.

    Uses pre-computed chunk embeddings from _raw_chunks. Zero ONNX cost.
    Per cluster: farthest chunk from centroid, then highest-entropy sentence.

    Returns flat chronological text or None if session too sparse.
    """
    # Extract content chunks with embeddings
    content_items = []
    for i, ch in enumerate(chunks):
        if _is_content_chunk(ch) and ch.get('content', '').strip():
            emb = ch.get('embedding')
            if emb is not None:
                content_items.append((i, ch))

    if len(content_items) < HDBSCAN_MIN_CHUNKS:
        return build_short_fingerprint(chunks)

    # HDBSCAN on pre-computed chunk embeddings (zero ONNX)
    embeddings = np.array([ch['embedding'] for _, ch in content_items])

    hdb = _get_hdbscan()
    clusterer = hdb.HDBSCAN(
        min_cluster_size=HDBSCAN_MIN_CLUSTER_SIZE,
        min_samples=HDBSCAN_MIN_SAMPLES,
        metric=HDBSCAN_METRIC,
    )
    chunk_labels = clusterer.fit_predict(embeddings)

    unique = set(chunk_labels)
    unique.discard(-1)
    if not unique:
        return build_short_fingerprint(chunks)

    # Select representative spans via chunk-level geometry + entropy
    reps = select_representatives(content_items, embeddings, chunk_labels)

    # Tool calls — one line each
    tool_lines = []
    for ch in chunks:
        formatted = format_tool_line(ch)
        if formatted:
            tool_lines.append({
                'message_number': ch.get('message_number', 0),
                'content': formatted,
            })

    # Merge + sort + format — no dedup, ship raw
    lines = []
    for r in reps:
        lines.append((r['message_number'], f'[{r["message_number"]}] "{r["text"]}"'))
    for t in tool_lines:
        lines.append((t['message_number'], f'[{t["message_number"]}] {t["content"]}'))

    lines.sort(key=lambda x: x[0])

    if not lines:
        return None

    return '\n'.join(line for _, line in lines)


def build_short_fingerprint(chunks):
    """Fingerprint for sessions too small for HDBSCAN.

    Lists all content spans + all tool calls chronologically. No dedup.
    No embedding needed — just sentence-split and list.
    """
    lines = []

    for ch in chunks:
        msg_num = ch.get('message_number', 0)
        if _is_content_chunk(ch) and ch.get('content', '').strip():
            sents = re.split(r'(?<=[.!?])\s+', ch['content'].strip())
            for sent in sents:
                sent = sent.strip()
                if len(sent) > SPAN_MIN_LEN:
                    lines.append((msg_num, f'[{msg_num}] "{sent}"'))
        else:
            formatted = format_tool_line(ch)
            if formatted:
                lines.append((msg_num, f'[{msg_num}] {formatted}'))

    lines.sort(key=lambda x: x[0])

    if not lines:
        return None

    return '\n'.join(line for _, line in lines)
