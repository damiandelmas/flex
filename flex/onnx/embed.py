"""
ONNX-based embedding model — Nomic embed-text-v1.5 (768-dim, Matryoshka).

Drop-in replacement for sentence-transformers. Uses ONNX runtime.
No PyTorch dependency. ~137MB int8 model.

Task prefixes (mandatory for Nomic):
  search_document:  — index time (default)
  search_query:     — query time
  clustering:       — clustering tasks
  classification:   — classification tasks

Memory-safe adaptive batching:
  Attention is O(seq_len²). One long text in a batch pads the entire batch
  to max_seq_len, spiking memory. We sort by tokenized length and scale
  batch_size inversely with sequence length. Budget: 512MB attention ceiling.

Performance:
    Adaptive batching: sorts inputs by tokenized length so short texts batch
    together (fast, low padding) and long texts batch together (predictable).
    Attention memory stays under 512MB at any input mix. Results returned in
    original order.

Usage:
    from flex.onnx import ONNXEmbedder

    model = ONNXEmbedder()
    embeddings = model.encode(["text1", "text2"])                    # index
    embeddings = model.encode(["query"], prefix='search_query: ')    # search
"""
import numpy as np
from pathlib import Path
from typing import List, Union

# Lazy imports
_ort = None
_tokenizer = None

ONNX_DIR = Path(__file__).parent


def _resolve_model_path() -> Path:
    """Bundled first, then $FLEX_HOME/models/ (FLEX_HOME-aware)."""
    import os
    bundled = ONNX_DIR / "model.onnx"
    if bundled.exists():
        return bundled
    flex_home = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
    user = flex_home / "models" / "model.onnx"
    if user.exists():
        return user
    raise RuntimeError(
        "Embedding model not found.\n"
        f"  Checked: {bundled}\n"
        f"  Checked: {user}\n"
        "  Run 'flex init' to download it."
    )


# Attention memory budget: batch × 12_heads × seq² × 4_bytes ≤ ATTN_BUDGET_BYTES
# 512MB keeps us safe on 8GB machines with headroom for hidden states + OS.
ATTN_BUDGET_BYTES = 512 * 1024 * 1024
ATTN_HEADS = 12
MAX_BATCH = 256
MAX_LENGTH = 512
MAX_CHARS = MAX_LENGTH * 8  # pre-truncate before tokenizer sees the text


def _safe_batch_size(seq_len: int) -> int:
    """Max batch size that keeps attention intermediates under budget."""
    if seq_len <= 0:
        return MAX_BATCH
    max_bs = ATTN_BUDGET_BYTES // (ATTN_HEADS * seq_len * seq_len * 4)
    return max(1, min(max_bs, MAX_BATCH))


def _get_onnxruntime():
    global _ort
    if _ort is None:
        import onnxruntime as ort
        _ort = ort
    return _ort


def _get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        from tokenizers import Tokenizer
        _tokenizer = Tokenizer.from_file(str(ONNX_DIR / "tokenizer.json"))
    return _tokenizer


class ONNXEmbedder:
    """ONNX-based sentence embedder compatible with sentence-transformers API."""

    def __init__(self, model_path: Path = None):
        self.model_path = model_path or _resolve_model_path()
        self._session = None
        self._tokenizer = None

    @property
    def session(self):
        if self._session is None:
            ort = _get_onnxruntime()
            opts = ort.SessionOptions()
            # ORT_ENABLE_ALL: fuses QKV attention, layer norm, GELU, embedding
            # layers into single kernels. 2-5x speedup on transformers.
            opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
            # intra_op=0 lets ONNX use all physical cores (default heuristic).
            # inter_op=1 because we run one model sequentially.
            opts.intra_op_num_threads = 0
            opts.inter_op_num_threads = 1
            self._session = ort.InferenceSession(
                str(self.model_path),
                sess_options=opts,
                providers=["CPUExecutionProvider"]
            )
        return self._session

    @property
    def tokenizer(self):
        if self._tokenizer is None:
            self._tokenizer = _get_tokenizer()
        return self._tokenizer

    def _encode_batch(self, batch: list, normalize: bool) -> np.ndarray:
        """Tokenize and run inference on a single pre-sorted batch."""
        tok = self.tokenizer
        encoded = tok.encode_batch(batch)
        input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
        attention_mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)

        outputs = self.session.run(
            None,
            {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
                "token_type_ids": np.zeros_like(input_ids),
            }
        )

        # Mean pooling
        last_hidden = outputs[0]
        mask_expanded = np.expand_dims(attention_mask, -1).astype(np.float32)
        sum_embeddings = np.sum(last_hidden * mask_expanded, axis=1)
        sum_mask = np.sum(mask_expanded, axis=1)
        embeddings = sum_embeddings / np.maximum(sum_mask, 1e-9)

        if normalize:
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            embeddings = embeddings / np.maximum(norms, 1e-9)

        return embeddings

    def encode(
        self,
        sentences: Union[str, List[str]],
        batch_size: int = 32,
        normalize: bool = True,
        prefix: str = 'search_document: ',
        show_progress_bar: bool = False,  # noqa: ARG002 — sentence-transformers API compat
    ) -> np.ndarray:
        """
        Encode sentences to embeddings with adaptive batching.

        Sorts inputs by tokenized length so short texts batch together (fast)
        and long texts get smaller batches (safe). Attention memory stays under
        512MB regardless of input mix. Results returned in original order.

        Args:
            sentences: Single sentence or list of sentences
            batch_size: Max batch size hint (actual size may be smaller for long texts)
            normalize: Whether to L2-normalize embeddings
            prefix: Task prefix for Nomic (default 'search_document: ' for indexing,
                    use 'search_query: ' for retrieval). Empty string for no prefix.
            show_progress_bar: Ignored. Exists for sentence-transformers API compatibility.

        Returns:
            numpy array of shape (n_sentences, 768)
        """
        if isinstance(sentences, str):
            sentences = [sentences]
        if prefix:
            sentences = [prefix + s for s in sentences]
        # Pre-truncate: no point tokenizing 214K chars to keep 512 tokens
        sentences = [s[:MAX_CHARS] for s in sentences]

        n = len(sentences)
        if n == 0:
            return np.empty((0, 768), dtype=np.float32)

        tok = self.tokenizer
        tok.enable_truncation(max_length=MAX_LENGTH)
        tok.enable_padding()

        # Tokenize all to get lengths for sorting + adaptive batch sizing
        pre_encoded = tok.encode_batch(sentences)
        lengths = [len(e.ids) for e in pre_encoded]

        # Sort by length: short texts first, long texts last
        order = np.argsort(lengths)
        sorted_sentences = [sentences[i] for i in order]
        sorted_lengths = [lengths[i] for i in order]

        all_embeddings = []
        i = 0
        while i < n:
            # Adaptive batch size from longest text in this slice
            max_seq = sorted_lengths[min(i + batch_size - 1, n - 1)]
            safe_bs = min(batch_size, _safe_batch_size(max_seq))
            end = min(i + safe_bs, n)
            batch = sorted_sentences[i:end]

            all_embeddings.append(self._encode_batch(batch, normalize))
            i = end

        stacked = np.vstack(all_embeddings)

        # Unsort back to original order
        result = np.empty_like(stacked)
        result[order] = stacked
        return result


# Singleton
_model = None


def get_model() -> ONNXEmbedder:
    """Get singleton ONNX embedder instance."""
    global _model
    if _model is None:
        _model = ONNXEmbedder()
    return _model


def encode(sentences: Union[str, List[str]], prefix: str = 'search_document: ', **kwargs) -> np.ndarray:
    """Convenience function to encode sentences."""
    return get_model().encode(sentences, prefix=prefix, **kwargs)
