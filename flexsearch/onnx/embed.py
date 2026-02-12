"""
ONNX-based embedding model.

Drop-in replacement for sentence-transformers. Uses ONNX runtime.
No PyTorch dependency. ~90MB instead of ~400MB.

Usage:
    from flexsearch.onnx import ONNXEmbedder

    model = ONNXEmbedder()
    embeddings = model.encode(["text1", "text2"])
"""
import numpy as np
from pathlib import Path
from typing import List, Union

# Lazy imports
_ort = None
_tokenizer = None

ONNX_DIR = Path(__file__).parent


def _get_onnxruntime():
    global _ort
    if _ort is None:
        import onnxruntime as ort
        _ort = ort
    return _ort


def _get_tokenizer():
    global _tokenizer
    if _tokenizer is None:
        from transformers import AutoTokenizer
        _tokenizer = AutoTokenizer.from_pretrained(str(ONNX_DIR))
    return _tokenizer


class ONNXEmbedder:
    """ONNX-based sentence embedder compatible with sentence-transformers API."""

    def __init__(self, model_path: Path = None):
        self.model_path = model_path or ONNX_DIR / "model.onnx"
        self._session = None
        self._tokenizer = None

    @property
    def session(self):
        if self._session is None:
            ort = _get_onnxruntime()
            self._session = ort.InferenceSession(
                str(self.model_path),
                providers=["CPUExecutionProvider"]
            )
        return self._session

    @property
    def tokenizer(self):
        if self._tokenizer is None:
            self._tokenizer = _get_tokenizer()
        return self._tokenizer

    def encode(
        self,
        sentences: Union[str, List[str]],
        batch_size: int = 32,
        normalize: bool = True,
        show_progress_bar: bool = False,  # noqa: ARG002 — sentence-transformers API compat
    ) -> np.ndarray:
        """
        Encode sentences to embeddings.

        Args:
            sentences: Single sentence or list of sentences
            batch_size: Batch size for encoding
            normalize: Whether to L2-normalize embeddings
            show_progress_bar: Ignored. Exists for sentence-transformers API compatibility.

        Returns:
            numpy array of shape (n_sentences, 384)
        """
        if isinstance(sentences, str):
            sentences = [sentences]

        all_embeddings = []

        for i in range(0, len(sentences), batch_size):
            batch = sentences[i:i + batch_size]

            # Tokenize
            inputs = self.tokenizer(
                batch,
                return_tensors="np",
                padding=True,
                truncation=True,
                max_length=256
            )

            # Run ONNX inference
            outputs = self.session.run(
                None,
                {
                    "input_ids": inputs["input_ids"],
                    "attention_mask": inputs["attention_mask"]
                }
            )

            # Mean pooling
            last_hidden = outputs[0]
            attention_mask = inputs["attention_mask"]
            mask_expanded = np.expand_dims(attention_mask, -1).astype(np.float32)
            sum_embeddings = np.sum(last_hidden * mask_expanded, axis=1)
            sum_mask = np.sum(mask_expanded, axis=1)
            embeddings = sum_embeddings / np.maximum(sum_mask, 1e-9)

            if normalize:
                norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
                embeddings = embeddings / np.maximum(norms, 1e-9)

            all_embeddings.append(embeddings)

        return np.vstack(all_embeddings)


# Singleton
_model = None


def get_model() -> ONNXEmbedder:
    """Get singleton ONNX embedder instance."""
    global _model
    if _model is None:
        _model = ONNXEmbedder()
    return _model


def encode(sentences: Union[str, List[str]], **kwargs) -> np.ndarray:
    """Convenience function to encode sentences."""
    return get_model().encode(sentences, **kwargs)
