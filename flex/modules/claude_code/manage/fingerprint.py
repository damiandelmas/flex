"""Claude Code session fingerprint — HDBSCAN config and fingerprint builder.

Builds a compact chronological navigational index for each session:
strange attractor sentences (from chunk-level HDBSCAN on pre-computed embeddings)
interleaved with collapsed tool runs. Target: ~30-50 lines per session, max 70.

Content lines: [N] "quoted text" — what was said/decided.
Tool lines:    > [N-M] 5x op:Read, 2x op:Bash — what was done.

Compression: tool runs merge between content fence posts, content truncated
to 3 sentences / 200 chars, max 3 reps per message_number, max 70 total lines.

Zero ONNX at enrichment time — uses _raw_chunks.embedding (already in DB).
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
MAX_REPS_PER_POSITION = 3   # dedup when >3 reps share a message_number
MAX_CONTENT_CHARS = 200      # truncate content reps
MAX_CONTENT_SENTS = 3        # max sentences per content rep
MAX_LINES = 70               # safety net — trim lowest-entropy lines


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


def _truncate_content(text):
    """Truncate to MAX_CONTENT_SENTS sentences / MAX_CONTENT_CHARS chars."""
    sents = re.split(r'(?<=[.!?])\s+', text.strip())
    sents = sents[:MAX_CONTENT_SENTS]
    result = ' '.join(sents)
    if len(result) > MAX_CONTENT_CHARS:
        result = result[:MAX_CONTENT_CHARS - 3] + '...'
    return result


def _dedup_reps(reps):
    """Cap reps per message_number at MAX_REPS_PER_POSITION, keep highest entropy."""
    by_pos = {}
    for r in reps:
        pos = r['message_number']
        if pos not in by_pos:
            by_pos[pos] = []
        by_pos[pos].append(r)

    deduped = []
    for pos in sorted(by_pos):
        group = by_pos[pos]
        if len(group) <= MAX_REPS_PER_POSITION:
            deduped.extend(group)
        else:
            # Keep top N by entropy
            scored = sorted(group, key=lambda r: _text_entropy(r['text']), reverse=True)
            deduped.extend(scored[:MAX_REPS_PER_POSITION])
    return deduped


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
# Tool call formatting — one line per call
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
        return f"op:{tool} `{basename}`" if basename else f"op:{tool}"

    if tool == 'Bash':
        cmd = content.strip()
        if cmd.startswith('Bash:'):
            cmd = cmd[5:].strip()
        if cmd and cmd != 'Bash' and len(cmd) > 3:
            return f"op:Bash `{cmd[:60]}`"
        return "op:Bash"

    if tool == 'Task':
        desc = content[:60].strip()
        return f'op:Task "{desc}"' if desc else "op:Task"

    if tool in ('WebSearch', 'WebFetch'):
        query = content[:60].strip()
        return f"op:{tool} `{query}`" if query else f"op:{tool}"

    if tool.startswith('mcp__'):
        parts = tool.split('__')
        short = parts[-1] if len(parts) > 2 else 'MCP tool'
        return f"op:{short}"

    return f"op:{tool}"


# ---------------------------------------------------------------------------
# Tool call collapsing — group by message_number, then merge consecutive runs
# ---------------------------------------------------------------------------

def _collapse_tool_lines(tool_lines, content_positions=None):
    """Collapse tool calls — group by message_number, merge consecutive runs.

    Phase 1: Group by message_number → "[8] 4x Read"
    Phase 2: Merge consecutive tool groups into runs → "[8-60] 15x Read, 3x Bash"
             Content rep positions act as fence posts that break runs.

    Single tool call keeps detail: "[50] Bash: pytest tests/"

    Returns list of {sort_key, line}.
    """
    if not tool_lines:
        return []
    if content_positions is None:
        content_positions = set()

    # Phase 1: group by message_number
    groups = {}
    for t in tool_lines:
        msg = t['message_number']
        if msg not in groups:
            groups[msg] = []
        groups[msg].append(t)

    sorted_msgs = sorted(groups)

    # Phase 2: merge consecutive groups into runs
    # A content rep between two tool groups breaks the run.
    runs = []
    run_start = None
    run_end = None
    run_tools = []

    def _flush():
        nonlocal run_start, run_end, run_tools
        if not run_tools:
            return
        if len(run_tools) == 1:
            line = f'> [{run_start}] {run_tools[0]["content"]}'
        else:
            counts = Counter(t['tool_name'] for t in run_tools)
            parts = [f'{c}x op:{name}' for name, c in counts.most_common()]
            tag = ', '.join(parts)
            prefix = f'[{run_start}]' if run_start == run_end else f'[{run_start}-{run_end}]'
            line = f'> {prefix} {tag}'
        runs.append({'sort_key': run_start, 'line': line})
        run_start = None
        run_end = None
        run_tools = []

    for msg_num in sorted_msgs:
        # Check if a content rep sits between current run and this group
        if run_start is not None:
            fence = any(p for p in content_positions if run_end < p < msg_num)
            if fence:
                _flush()

        if run_start is None:
            run_start = msg_num
        run_end = msg_num
        run_tools.extend(groups[msg_num])

    _flush()
    return runs


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

    # Dedup: cap reps per message_number, truncate content
    reps = _dedup_reps(reps)
    for r in reps:
        r['text'] = _truncate_content(r['text'])

    # Tool calls — collect raw, collapse later
    tool_lines = []
    for ch in chunks:
        formatted = format_tool_line(ch)
        if formatted:
            # Extract tool name: "op:Read `file.py`" → "Read"
            m = re.match(r'op:(\w+)', formatted)
            tool_name = m.group(1) if m else formatted.split()[0]
            tool_lines.append({
                'message_number': ch.get('message_number', 0),
                'tool_name': tool_name,
                'content': formatted,
            })

    # Collapse tool calls — content rep positions are fence posts
    content_positions = {r['message_number'] for r in reps}
    collapsed_tools = _collapse_tool_lines(tool_lines, content_positions)

    # Merge content reps + collapsed tools, sort chronologically
    lines = []
    for r in reps:
        lines.append((r['message_number'], f'[{r["message_number"]}] "{r["text"]}"'))
    for t in collapsed_tools:
        lines.append((t['sort_key'], t['line']))

    lines.sort(key=lambda x: x[0])

    if not lines:
        return None

    # Safety net — trim to MAX_LINES, keeping tool lines (structural)
    # and highest-entropy content lines
    if len(lines) > MAX_LINES:
        tool = [(pos, line) for pos, line in lines if line.startswith('>')]
        content = [(pos, line) for pos, line in lines if not line.startswith('>')]
        # Keep all tool lines, trim content by entropy
        content_scored = sorted(content,
            key=lambda x: _text_entropy(x[1]), reverse=True)
        budget = MAX_LINES - len(tool)
        if budget < 1:
            budget = 1
        content = content_scored[:budget]
        lines = sorted(tool + content, key=lambda x: x[0])

    return '\n'.join(line for _, line in lines)


def build_short_fingerprint(chunks):
    """Fingerprint for sessions too small for HDBSCAN.

    Lists all content spans + collapsed tool calls chronologically.
    No embedding needed — just sentence-split and list.
    """
    content_lines = []
    tool_lines = []

    for ch in chunks:
        msg_num = ch.get('message_number', 0)
        if _is_content_chunk(ch) and ch.get('content', '').strip():
            truncated = _truncate_content(ch['content'].strip())
            if len(truncated) > SPAN_MIN_LEN:
                content_lines.append((msg_num, f'[{msg_num}] "{truncated}"'))
        else:
            formatted = format_tool_line(ch)
            if formatted:
                # Extract tool name: "op:Read `file.py`" → "Read"
                m = re.match(r'op:(\w+)', formatted)
                tool_name = m.group(1) if m else formatted.split()[0]
                tool_lines.append({
                    'message_number': msg_num,
                    'tool_name': tool_name,
                    'content': formatted,
                })

    content_positions = {pos for pos, _ in content_lines}
    collapsed = _collapse_tool_lines(tool_lines, content_positions)

    lines = list(content_lines)
    for t in collapsed:
        lines.append((t['sort_key'], t['line']))

    lines.sort(key=lambda x: x[0])

    if not lines:
        return None

    return '\n'.join(line for _, line in lines)
