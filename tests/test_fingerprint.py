"""Tests for session fingerprint builder (summary.py).

Covers:
  - shatter_spans: sentence decomposition
  - select_representatives: strange attractor selection
  - format_tool_line: tool call formatting
  - build_fingerprint: full pipeline (mocked HDBSCAN)
  - build_short_fingerprint: small session fallback
  - sessions view column presence
"""

import struct
import sys
from pathlib import Path

import numpy as np
import pytest

FLEX_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flex.modules.claude_code.manage.fingerprint import (
    HDBSCAN_MIN_CHUNKS,
    MAX_NOISE_REPS,
    SKIP_TOOLS,
    SPAN_MIN_LEN,
    _is_content_chunk,
    _text_entropy,
    build_fingerprint,
    build_short_fingerprint,
    format_tool_line,
    select_representatives,
    shatter_spans,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk(content='', tool_name=None, target_file=None, msg_num=0):
    """Build a minimal chunk dict."""
    emb = np.random.randn(384).astype(np.float32)
    return {
        'id': f'chunk_{msg_num}',
        'embedding': emb,
        'content': content,
        'tool_name': tool_name,
        'target_file': target_file,
        'message_number': msg_num,
    }


def _embed_fn(texts):
    """Deterministic fake embedder — each text gets a unique-ish vector."""
    vecs = []
    for t in texts:
        np.random.seed(hash(t) % 2**31)
        vecs.append(np.random.randn(384).astype(np.float32))
    return np.array(vecs)


# ---------------------------------------------------------------------------
# shatter_spans
# ---------------------------------------------------------------------------

class TestShatterSpans:

    def test_basic_sentence_split(self):
        chunks = [
            _chunk("First sentence is here now and long enough to pass the filter easily. Second sentence is also here and long enough to pass. Third one.", msg_num=5),
        ]
        spans = shatter_spans(chunks)
        assert len(spans) == 2  # "Third one." is < SPAN_MIN_LEN, others > 50
        assert all(s['message_number'] == 5 for s in spans)

    def test_tool_chunks_excluded(self):
        chunks = [
            _chunk("Some user text here that is definitely long enough to pass the fifty char filter.", tool_name=None, msg_num=1),
            _chunk("Read /path/to/file.py", tool_name='Read', target_file='/path/to/file.py', msg_num=2),
        ]
        spans = shatter_spans(chunks)
        # Only the content chunk produces spans
        assert all(s['chunk_idx'] == 0 for s in spans)

    def test_empty_content_skipped(self):
        chunks = [_chunk("", msg_num=1)]
        assert shatter_spans(chunks) == []

    def test_short_fragments_filtered(self):
        chunks = [_chunk("Hi. Yes. No. This sentence is long enough to survive the fifty character minimum filter.", msg_num=1)]
        spans = shatter_spans(chunks)
        # Only spans > SPAN_MIN_LEN survive
        assert len(spans) == 1
        for s in spans:
            assert len(s['text']) > SPAN_MIN_LEN

    def test_inherits_message_number(self):
        chunks = [
            _chunk("First sentence that is absolutely long enough to pass the fifty char filter.", msg_num=42),
            _chunk("Second sentence that is also absolutely long enough to pass the filter.", msg_num=99),
        ]
        spans = shatter_spans(chunks)
        msg_nums = {s['message_number'] for s in spans}
        assert 42 in msg_nums
        assert 99 in msg_nums


# ---------------------------------------------------------------------------
# select_representatives
# ---------------------------------------------------------------------------

class TestSelectRepresentatives:

    def test_one_rep_per_cluster(self):
        # content_items: list of (original_index, chunk_dict)
        content_items = [
            (i, _chunk(f'Sentence number {i} which is definitely long enough to pass the fifty char filter easily.', msg_num=i))
            for i in range(15)
        ]
        embeddings = np.random.randn(15, 384).astype(np.float32)
        labels = np.array([0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1])

        reps = select_representatives(content_items, embeddings, labels)

        cluster_reps = [r for r in reps if r['source'].startswith('cluster_')]
        noise_reps = [r for r in reps if r['source'] == 'noise']

        assert len(cluster_reps) == 2  # clusters 0 and 1
        assert len(noise_reps) == MAX_NOISE_REPS  # capped

    def test_all_noise_returns_empty_clusters(self):
        content_items = [
            (i, _chunk(f'Noise span {i} that is long enough text to survive the fifty char filter easily.', msg_num=i))
            for i in range(5)
        ]
        embeddings = np.random.randn(5, 384).astype(np.float32)
        labels = np.array([-1, -1, -1, -1, -1])

        reps = select_representatives(content_items, embeddings, labels)
        cluster_reps = [r for r in reps if r['source'].startswith('cluster_')]
        assert len(cluster_reps) == 0
        assert len(reps) == 5  # all noise (5 <= MAX_NOISE_REPS)

    def test_noise_capped_at_max(self):
        """More than MAX_NOISE_REPS noise chunks → only top N by entropy kept."""
        content_items = [
            (i, _chunk(f'Noise chunk number {i} with enough diverse vocabulary words to pass the entropy filter and length check.', msg_num=i))
            for i in range(MAX_NOISE_REPS + 10)
        ]
        embeddings = np.random.randn(MAX_NOISE_REPS + 10, 384).astype(np.float32)
        labels = np.array([-1] * (MAX_NOISE_REPS + 10))

        reps = select_representatives(content_items, embeddings, labels)
        assert len(reps) == MAX_NOISE_REPS


# ---------------------------------------------------------------------------
# format_tool_line
# ---------------------------------------------------------------------------

class TestFormatToolLine:

    def test_read(self):
        ch = _chunk(tool_name='Read', target_file='/home/user/project/file.py')
        assert format_tool_line(ch) == 'Read file.py'

    def test_write(self):
        ch = _chunk(tool_name='Write', target_file='/home/user/project/out.md')
        assert format_tool_line(ch) == 'Write out.md'

    def test_edit(self):
        ch = _chunk(tool_name='Edit', target_file='/foo/bar.py')
        assert format_tool_line(ch) == 'Edit bar.py'

    def test_bash_with_command(self):
        ch = _chunk(content='pytest tests/test_vec_ops.py -v', tool_name='Bash')
        result = format_tool_line(ch)
        assert result.startswith('Bash: ')
        assert 'pytest' in result

    def test_bash_empty(self):
        ch = _chunk(content='', tool_name='Bash')
        assert format_tool_line(ch) == 'Bash'

    def test_task_with_desc(self):
        ch = _chunk(content='Read soma architecture and codebase', tool_name='Task')
        result = format_tool_line(ch)
        assert result.startswith('Task: "')
        assert 'soma' in result

    def test_websearch(self):
        ch = _chunk(content='flexmem npm availability', tool_name='WebSearch')
        assert format_tool_line(ch) == 'WebSearch: flexmem npm availability'

    def test_mcp_tool(self):
        ch = _chunk(tool_name='mcp__flexsearch__flex')
        result = format_tool_line(ch)
        assert result == 'flex query'

    def test_skip_tools(self):
        for tool in SKIP_TOOLS:
            ch = _chunk(tool_name=tool)
            assert format_tool_line(ch) is None

    def test_no_action(self):
        ch = _chunk(tool_name=None)
        assert format_tool_line(ch) is None


# ---------------------------------------------------------------------------
# build_short_fingerprint
# ---------------------------------------------------------------------------

class TestBuildShortFingerprint:

    def test_basic(self):
        chunks = [
            _chunk("User asked a question that is definitely long enough to pass the fifty character minimum.", msg_num=2),
            _chunk(tool_name='Read', target_file='/path/file.py', msg_num=3),
            _chunk("Agent responded with a detailed explanation here that also passes the fifty char minimum.", msg_num=4),
        ]
        result = build_short_fingerprint(chunks)
        assert result is not None
        lines = result.split('\n')
        assert len(lines) >= 2

        # Check [N] prefix format
        for line in lines:
            assert line.startswith('[')

    def test_empty_session(self):
        assert build_short_fingerprint([]) is None

    def test_chronological_order(self):
        chunks = [
            _chunk("Later message that is long enough to pass the fifty character filter easily.", msg_num=50),
            _chunk("Earlier message that is also long enough to pass the fifty character filter.", msg_num=10),
        ]
        result = build_short_fingerprint(chunks)
        lines = result.split('\n')
        assert '[10]' in lines[0]
        assert '[50]' in lines[1]

    def test_no_dedup(self):
        """Multiple tool calls at same message_number all appear."""
        chunks = [
            _chunk(tool_name='Read', target_file='/a.py', msg_num=8),
            _chunk(tool_name='Read', target_file='/b.py', msg_num=8),
            _chunk(tool_name='Read', target_file='/c.py', msg_num=8),
        ]
        result = build_short_fingerprint(chunks)
        lines = result.split('\n')
        assert len(lines) == 3
        assert all('[8]' in line for line in lines)


# ---------------------------------------------------------------------------
# build_fingerprint
# ---------------------------------------------------------------------------

class TestBuildFingerprint:

    def test_falls_back_to_short_when_few_spans(self):
        """Sessions with < HDBSCAN_MIN_CHUNKS spans use short fingerprint."""
        chunks = [
            _chunk("A short session with just a few chunks here but long enough to pass the minimum.", msg_num=1),
            _chunk(tool_name='Read', target_file='/file.py', msg_num=2),
        ]
        result = build_fingerprint(chunks, None, _embed_fn)
        assert result is not None
        assert '[1]' in result or '[2]' in result

    def test_output_has_message_numbers(self):
        """All output lines have [N] prefix."""
        # Build enough content chunks to potentially trigger HDBSCAN
        chunks = []
        for i in range(30):
            chunks.append(_chunk(
                f"This is sentence number {i} with enough words to be a real span in the session and pass the minimum.",
                msg_num=i * 2
            ))
        # Add some tool calls
        chunks.append(_chunk(tool_name='Read', target_file='/test.py', msg_num=5))
        chunks.append(_chunk(tool_name='Write', target_file='/out.py', msg_num=25))

        result = build_fingerprint(chunks, None, _embed_fn)
        assert result is not None
        for line in result.split('\n'):
            assert line.startswith('[')

    def test_tool_calls_and_quotes_mixed(self):
        """Output contains both quoted text and unquoted tool calls."""
        chunks = [
            _chunk("User said something meaningful and long enough to keep past the fifty char minimum.", msg_num=1),
            _chunk(tool_name='Read', target_file='/foo.py', msg_num=2),
            _chunk("Agent explained the architecture in detail here now and this is long enough too.", msg_num=3),
        ]
        result = build_fingerprint(chunks, None, _embed_fn)
        assert result is not None

        has_quoted = any('"' in line for line in result.split('\n'))
        has_tool = any('Read' in line for line in result.split('\n'))
        assert has_quoted
        assert has_tool


# ---------------------------------------------------------------------------
# _is_content_chunk
# ---------------------------------------------------------------------------

class TestIsContentChunk:

    def test_no_tool_is_content(self):
        assert _is_content_chunk({'tool_name': None}) is True

    def test_user_prompt_is_content(self):
        assert _is_content_chunk({'tool_name': 'UserPrompt'}) is True

    def test_read_is_not_content(self):
        assert _is_content_chunk({'tool_name': 'Read'}) is False

    def test_bash_is_not_content(self):
        assert _is_content_chunk({'tool_name': 'Bash'}) is False


# ---------------------------------------------------------------------------
# _text_entropy
# ---------------------------------------------------------------------------

class TestTextEntropy:

    def test_empty(self):
        assert _text_entropy('') == 0.0

    def test_single_word(self):
        assert _text_entropy('hello') == 0.0

    def test_diverse_higher(self):
        uniform = _text_entropy('a b c d e f g h')
        repeated = _text_entropy('a a a a a a a a')
        assert uniform > repeated


# ---------------------------------------------------------------------------
# Sessions view integration
# ---------------------------------------------------------------------------

class TestSessionsViewColumns:
    """Verify sessions.sql curated view has correct columns."""

    def test_sessions_sql_has_fingerprint(self):
        sql_path = FLEX_ROOT / 'views' / 'claude_code' / 'sessions.sql'
        content = sql_path.read_text()
        assert 'fingerprint_index' in content
        assert 'topic_summary' not in content
        assert 'community_label' not in content

    def test_orient_sql_uses_substr_fingerprint(self):
        sql_path = FLEX_ROOT / 'flex' / 'modules' / 'claude_code' / 'presets' / 'orient.sql'
        content = sql_path.read_text()
        assert 'substr(ess.fingerprint_index' in content
        assert 'topic_summary' not in content
