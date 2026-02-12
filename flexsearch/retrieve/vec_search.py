"""
Flexsearch Vector Cache

Matrix-based semantic search. Trades memory for speed.
Loads all vectors once, queries in <1ms via BLAS matmul.

Three numpy-only operations that SQL cannot do:
- Matrix multiply (corpus-wide cosine similarity in 0.012ms)
- Contrastive (second matmul against negative query)
- MMR diversity (iterative pairwise selection)

Everything else (graph boost, temporal decay, metadata filter)
is SQL arithmetic on the candidate rows vec_search returns.

Performance:
    1k docs:   0.1ms
    10k docs:  0.5ms
    100k docs: 5ms
    367k docs: 12ms

Memory: ~15MB per 10k docs (384-dim vectors)
"""

import numpy as np
from typing import Optional, List, Dict, Any


class VectorCache:
    """
    In-memory vector cache for fast semantic search via matrix multiplication.

    Usage:
        cache = VectorCache()
        cache.load_from_db(db, '_raw_chunks', 'embedding', 'id')
        results = cache.search(query_vec, limit=10)

        # With pre-filtering (SQL decides what to search)
        mask = cache.get_mask_for_ids(['chunk1', 'chunk2'])
        results = cache.search(query_vec, limit=10, mask=mask)
    """

    def __init__(self):
        self.ids: List[str] = []
        self.matrix: Optional[np.ndarray] = None  # (n, dims), normalized
        self._id_to_idx: Dict[str, int] = {}
        self.loaded_at: Optional[float] = None
        self.dims: int = 0

    def load_from_db(self, db, table: str, embedding_col: str = 'embedding',
                     id_col: str = 'id') -> 'VectorCache':
        """
        Load vectors from SQLite BLOB column into numpy matrix.

        Args:
            db: SQLite connection
            table: Table name
            embedding_col: Column with BLOB embeddings
            id_col: Column with document IDs
        """
        import time
        start = time.time()

        rows = db.execute(
            f"SELECT [{id_col}], [{embedding_col}] FROM [{table}] "
            f"WHERE [{embedding_col}] IS NOT NULL"
        ).fetchall()

        if not rows:
            return self

        self.ids = []
        vectors = []

        for row in rows:
            self.ids.append(row[0])
            vectors.append(np.frombuffer(row[1], dtype=np.float32))

        # Stack into matrix
        self.matrix = np.vstack(vectors)  # (n, dims)
        self.dims = self.matrix.shape[1]

        # Normalize for cosine similarity (in-place)
        norms = np.linalg.norm(self.matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1
        self.matrix /= norms

        # Build index
        self._id_to_idx = {id_: i for i, id_ in enumerate(self.ids)}

        self.loaded_at = time.time()
        elapsed = (self.loaded_at - start) * 1000
        print(f"VectorCache: {len(self.ids)} vectors ({self.dims}d) in {elapsed:.1f}ms")

        return self

    def search(self, query_vec: np.ndarray, *, not_like_vec: np.ndarray = None,
               diverse: bool = False, limit: int = 10, oversample: int = 200,
               mask: np.ndarray = None, threshold: float = 0.0,
               mmr_lambda: float = 0.7) -> List[Dict[str, Any]]:
        """
        Search for similar vectors. The three numpy-only operations:

        1. Matrix multiply: corpus-wide cosine similarity
        2. Contrastive (not_like_vec): penalize similarity to negative query
        3. MMR diversity: iterative selection maximizing relevance - redundancy

        Args:
            query_vec: Query embedding (dims,)
            not_like_vec: Negative query embedding for contrastive
            diverse: Enable MMR diversity selection
            limit: Max results to return
            oversample: Candidate pool size for diversity/contrastive
            mask: Boolean mask (n,) - True = include in search
            threshold: Minimum cosine similarity cutoff
            mmr_lambda: Relevance vs diversity tradeoff (0-1). Higher = more relevant, lower = more diverse.

        Returns:
            List of {id, score} sorted by score desc
        """
        if self.matrix is None or len(self.ids) == 0:
            return []

        # Validate dimensions
        if query_vec.shape != (self.dims,):
            raise ValueError(
                f"Query vector dimension {query_vec.shape} doesn't match "
                f"cache dimension ({self.dims},)"
            )

        # Normalize query
        query_vec = query_vec.astype(np.float32)
        query_norm = np.linalg.norm(query_vec)
        if query_norm > 0:
            query_vec = query_vec / query_norm

        # 1. Matrix multiply — all similarities at once
        similarities = self.matrix @ query_vec

        # Apply mask
        if mask is not None:
            similarities = np.where(mask, similarities, -np.inf)

        # Apply threshold
        if threshold > 0:
            similarities = np.where(similarities >= threshold, similarities, -np.inf)

        # 2. Contrastive — penalize similarity to negative query
        if not_like_vec is not None:
            not_like_vec = not_like_vec.astype(np.float32)
            nl_norm = np.linalg.norm(not_like_vec)
            if nl_norm > 0:
                not_like_vec = not_like_vec / nl_norm
            neg_sims = self.matrix @ not_like_vec
            similarities -= 0.5 * neg_sims

        # Get candidate pool
        pool_size = oversample if diverse else limit
        if pool_size >= len(similarities):
            top_indices = np.argsort(similarities)[::-1]
        else:
            top_indices = np.argpartition(similarities, -pool_size)[-pool_size:]
            top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]

        # Filter -inf
        top_indices = [i for i in top_indices if similarities[i] != -np.inf]

        # 3. MMR diversity — iterative selection
        if diverse and len(top_indices) > limit:
            selected_indices = self._mmr_select(top_indices, similarities, limit,
                                                   lambda_=mmr_lambda)
        else:
            selected_indices = top_indices[:limit]

        # Build results
        results = []
        for idx in selected_indices:
            results.append({
                'id': self.ids[idx],
                'score': float(similarities[idx])
            })

        return results

    def _mmr_select(self, candidates: list, similarities: np.ndarray,
                    k: int, lambda_: float = 0.7) -> list:
        """MMR: iteratively select for relevance minus redundancy."""
        if not candidates:
            return []

        selected = [candidates[0]]
        remaining = list(candidates[1:])

        while len(selected) < k and remaining:
            best_idx, best_score = -1, -float('inf')

            for i, cand in enumerate(remaining):
                cand_vec = self.matrix[cand]

                # Max similarity to any already selected
                max_sim = 0.0
                for sel in selected:
                    sim = float(np.dot(cand_vec, self.matrix[sel]))
                    max_sim = max(max_sim, sim)

                # MMR: lambda * relevance - (1-lambda) * redundancy
                mmr = lambda_ * similarities[cand] - (1 - lambda_) * max_sim
                if mmr > best_score:
                    best_score = mmr
                    best_idx = i

            if best_idx >= 0:
                selected.append(remaining.pop(best_idx))
            else:
                break

        return selected

    def get_mask_for_ids(self, ids: List[str]) -> np.ndarray:
        """Create boolean mask for specific IDs."""
        mask = np.zeros(len(self.ids), dtype=bool)
        for id_ in ids:
            if id_ in self._id_to_idx:
                mask[self._id_to_idx[id_]] = True
        return mask

    def get_mask_from_db(self, db, table: str, where: str,
                         params: tuple = ()) -> np.ndarray:
        """
        Create boolean mask from SQL WHERE clause.

        Example:
            mask = cache.get_mask_from_db(db, '_raw_chunks',
                "doc_id IN (SELECT source_id FROM _edges_source WHERE project = ?)",
                ('thread',))
        """
        rows = db.execute(
            f"SELECT id FROM [{table}] WHERE {where}", params
        ).fetchall()
        ids = [r[0] for r in rows]
        return self.get_mask_for_ids(ids)

    def get_vector(self, doc_id: str) -> Optional[np.ndarray]:
        """Return the embedding vector for an ID."""
        if doc_id in self._id_to_idx:
            return self.matrix[self._id_to_idx[doc_id]]
        return None

    @property
    def size(self) -> int:
        return len(self.ids)

    @property
    def memory_mb(self) -> float:
        if self.matrix is None:
            return 0.0
        return self.matrix.nbytes / (1024 * 1024)

    def __repr__(self):
        return f"VectorCache({self.size} vectors, {self.dims}d, {self.memory_mb:.1f}MB)"
