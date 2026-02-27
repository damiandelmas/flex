"""
Flex Vector Cache

Matrix-based semantic search. Trades memory for speed.
Loads all vectors once, queries in <1ms via BLAS matmul.

Three retrieval layers:
  vec_ops (numpy)   — cosine matmul + candidate shaping
  @structure (numpy) — ephemeral micro-graph on candidates (future)
  SQL                — everything else (ORDER BY, WHERE, JOIN)

Pipeline: SQL pre-filter → vec_ops (numpy) → SQL composition.

vec_ops modulations (applied on full N array before top-k):
- Matrix multiply (corpus-wide cosine similarity)
- Temporal decay: scores *= 1 / (1 + days_ago / half_life)
- Contrastive (second matmul against negative query)
- MMR diversity (iterative pairwise selection, returns MMR scores)
- Centroid (like:id1,id2) — query-by-example
- Trajectory (from:TEXT to:TEXT) — directional search

Hub/bridge boost moved to SQL: ORDER BY v.score * (1 + m.centrality)

SQL usage:
    vec_ops('_raw_chunks', 'auth')                    -- raw cosine
    vec_ops('_raw_chunks', 'auth', 'recent:7 diverse unlike:jwt')
    vec_ops('_raw_chunks', 'auth', 'diverse', 'SELECT chunk_id FROM _types_message WHERE type = ''user_prompt''')

Performance:
    1k docs:   0.1ms
    10k docs:  0.5ms
    100k docs: 5ms
    367k docs: 12ms

Memory: ~30MB per 10k docs (768-dim vectors)
"""

import json
import re
import sys
import time
import uuid

import numpy as np
from typing import Optional, List, Dict, Any


def parse_modifiers(modifier_str: str) -> dict:
    """Parse a modifier string into modulation parameters.

    Tokens (space-separated, composable):
        diverse              MMR diversity selection
        recent[:N]           temporal decay (optional N-day half-life)
        unlike:TEXT          contrastive — demote similarity to TEXT
        like:id1,id2,...     centroid of example chunks
        from:TEXT to:TEXT    trajectory — direction through embedding space
        local_communities    per-query Louvain on candidates, adds _community
        limit:N              candidate count (default 500)

    Dead tokens (silently ignored): kind:TYPE, community:N
    Alias: detect_communities → local_communities
    Unknown tokens silently ignored (forward-compatible).
    """
    result = {
        'recent': False,
        'recent_days': None,
        'unlike': None,
        'diverse': False,
        'limit': None,
        'like': None,
        'trajectory_from': None,
        'trajectory_to': None,
        'local_communities': False,
    }

    if not modifier_str:
        return result

    # Extract trajectory (spans tokens) before splitting
    traj_match = re.search(
        r'from:(.*?)\s+to:(.*?)(?=\s+(?:diverse|recent:|unlike:|like:|limit:|local_communities|detect_communities)\b|\s*$)',
        modifier_str
    )
    if traj_match:
        result['trajectory_from'] = traj_match.group(1).strip()
        result['trajectory_to'] = traj_match.group(2).strip()
        modifier_str = modifier_str[:traj_match.start()] + modifier_str[traj_match.end():]

    for token in modifier_str.strip().split():
        if token == 'diverse':
            result['diverse'] = True
        elif token == 'recent':
            result['recent'] = True
        elif token.startswith('recent:'):
            result['recent'] = True
            try:
                result['recent_days'] = int(token.split(':', 1)[1])
            except ValueError:
                result['recent_days'] = None
        elif token.startswith('unlike:'):
            result['unlike'] = token.split(':', 1)[1]
        elif token.startswith('limit:'):
            try:
                result['limit'] = int(token.split(':', 1)[1])
            except ValueError:
                pass
        elif token.startswith('like:'):
            result['like'] = token.split(':', 1)[1].split(',')
        elif token in ('local_communities', 'detect_communities'):
            result['local_communities'] = True
        # kind: and community: silently ignored (dead tokens)

    return result


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
        # Column arrays for landscape modulation (N,), aligned with self.ids
        self.timestamps: Optional[np.ndarray] = None    # (N,) float64, epoch seconds

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

        # Detect dominant dimension and filter outliers (guards against mixed-model migrations)
        dims = [v.shape[0] for v in vectors]
        dominant_dim = max(set(dims), key=dims.count)
        skipped = sum(1 for d in dims if d != dominant_dim)
        if skipped:
            print(f"VectorCache: skipping {skipped} vectors with dim != {dominant_dim} (mixed-model artifacts)",
                  file=sys.stderr)
            filtered = [(id_, v) for id_, v, d in zip(self.ids, vectors, dims) if d == dominant_dim]
            self.ids, vectors = zip(*filtered) if filtered else ([], [])
            self.ids = list(self.ids)
            vectors = list(vectors)

        if not vectors:
            return self

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
        self._load_msg = f"VectorCache: {len(self.ids)} vectors ({self.dims}d) in {elapsed:.1f}ms"

        return self

    def load_columns(self, db, table: str, id_col: str = 'id'):
        """Load timestamp arrays from DB, aligned with self.ids.

        Timestamps used for recent:N temporal decay modulation.
        Must be called AFTER load_from_db().
        """
        if not self.ids:
            return

        N = len(self.ids)

        # --- Timestamps: direct from table (if column exists) ---
        self.timestamps = np.zeros(N, dtype=np.float64)
        try:
            cols = {r[1] for r in db.execute(f"PRAGMA table_info([{table}])").fetchall()}
            if 'timestamp' in cols:
                rows = db.execute(
                    f"SELECT [{id_col}], timestamp FROM [{table}] "
                    f"WHERE timestamp IS NOT NULL"
                ).fetchall()
                for row in rows:
                    idx = self._id_to_idx.get(row[0])
                    if idx is not None:
                        self.timestamps[idx] = float(row[1])
        except Exception as e:
            print(f"VectorCache: timestamps load failed: {e}", file=sys.stderr)


    def search(self, query_vec: np.ndarray, *, pre_filter_ids: set = None,
               not_like_vec: np.ndarray = None,
               diverse: bool = False, limit: int = 10, oversample: int = 200,
               mask: np.ndarray = None, threshold: float = 0.0,
               mmr_lambda: float = 0.7,
               modifiers: dict = None, config: dict = None,
               embed_fn=None, embed_doc_fn=None) -> List[Dict[str, Any]]:
        """
        Search for similar vectors with optional landscape modulations.

        Pipeline: SQL pre-filter → numpy operations → SQL composition.

        Args:
            query_vec: Query embedding (dims,)
            pre_filter_ids: Set of chunk IDs to restrict search (from SQL 4th arg)
            not_like_vec: Negative query embedding for contrastive
            diverse: Enable MMR diversity selection
            limit: Max results to return
            oversample: Candidate pool size for diversity/contrastive
            mask: Boolean mask (n,) - True = include in search
            threshold: Minimum cosine similarity cutoff
            mmr_lambda: Relevance vs diversity tradeoff (0-1)
            modifiers: Parsed modifier dict from parse_modifiers()
            config: Cell config dict from _meta (vec:* keys)
            embed_fn: Embedding function for query-space text (unlike:TEXT, main query)
            embed_doc_fn: Embedding function for document-space text (trajectory from:/to:).
                          Falls back to embed_fn if not provided. Required for asymmetric
                          models (e.g. Nomic) where trajectory direction must be computed
                          in document space to match stored embeddings.

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

        # === CENTROID: like:id1,id2,... replaces or blends with query_vec ===
        like_ids = modifiers.get('like') if modifiers else None
        if like_ids:
            valid_indices = [self._id_to_idx[id_] for id_ in like_ids if id_ in self._id_to_idx]
            if not valid_indices:
                return []  # All IDs unknown
            vecs = self.matrix[np.array(valid_indices)]
            centroid = vecs.mean(axis=0)
            c_norm = np.linalg.norm(centroid)
            if c_norm > 0:
                centroid /= c_norm
            # If query text was provided, blend 50/50; otherwise pure centroid
            if query_norm > 0:
                query_vec = 0.5 * query_vec + 0.5 * centroid
                q_norm = np.linalg.norm(query_vec)
                if q_norm > 0:
                    query_vec /= q_norm
            else:
                query_vec = centroid

        # === TRAJECTORY: from:TEXT to:TEXT biases query via score combination ===
        # Scores = 0.7 * cosine(query) + 0.3 * cosine(direction)
        # Query defines the topic; direction is a reranking nudge.
        # Score combination keeps each component in its own embedding space —
        # avoids normalization weirdness from adding vectors (critical for
        # asymmetric models like Nomic where query/doc spaces differ).
        traj_from = modifiers.get('trajectory_from') if modifiers else None
        traj_to = modifiers.get('trajectory_to') if modifiers else None
        _traj_direction = None
        if traj_from and traj_to and embed_fn:
            # Direction must be in document space to match stored embeddings.
            # Use embed_doc_fn if provided (asymmetric models like Nomic),
            # else fall back to embed_fn (symmetric models like MiniLM).
            _embed_for_traj = embed_doc_fn if embed_doc_fn is not None else embed_fn
            start_vec = np.squeeze(_embed_for_traj(traj_from)).astype(np.float32)
            end_vec = np.squeeze(_embed_for_traj(traj_to)).astype(np.float32)
            direction = end_vec - start_vec
            d_norm = np.linalg.norm(direction)
            if d_norm > 0:
                direction /= d_norm
            _traj_direction = direction  # stored, applied after cosine scores

        # === SQL PRE-FILTER: fancy-index into warm matrix ===
        if pre_filter_ids is not None:
            indices = np.array([
                self._id_to_idx[id_] for id_ in pre_filter_ids
                if id_ in self._id_to_idx
            ], dtype=np.int64)
            if len(indices) == 0:
                return []
            # Slice warm matrix — subset arrays for all downstream code
            active_matrix = self.matrix[indices]
            active_ids = [self.ids[i] for i in indices]
            active_timestamps = self.timestamps[indices] if self.timestamps is not None else None
            active_id_to_idx = {id_: i for i, id_ in enumerate(active_ids)}
        else:
            # Full corpus path
            active_matrix = self.matrix
            active_ids = self.ids
            active_timestamps = self.timestamps
            active_id_to_idx = self._id_to_idx
            indices = None

        # 1. Matrix multiply — all similarities at once
        similarities = active_matrix @ query_vec

        # Trajectory blend: 0.7 * query_score + 0.3 * direction_score
        if _traj_direction is not None:
            traj_scores = active_matrix @ _traj_direction
            similarities = 0.7 * similarities + 0.3 * traj_scores

        # === LANDSCAPE MODULATIONS (on active array, before candidate selection) ===
        if modifiers:
            cfg = config or {}

            # Temporal decay: scores *= 1 / (1 + days_ago / half_life)
            if modifiers.get('recent') and active_timestamps is not None:
                if np.any(active_timestamps > 0):
                    half_life = float(
                        modifiers.get('recent_days')
                        or cfg.get('vec:recent:half_life', 30)
                    )
                    days_ago = np.maximum(
                        (time.time() - active_timestamps) / 86400.0, 0.0
                    )
                    similarities = similarities * (1.0 / (1.0 + days_ago / half_life))

            # Contrastive from modifier string
            if modifiers.get('unlike') and embed_fn is not None:
                not_like_vec = np.squeeze(embed_fn(modifiers['unlike']))

            # Override diverse/limit from modifiers
            if modifiers.get('diverse'):
                diverse = True
            if modifiers.get('limit'):
                limit = modifiers['limit']

        # Apply mask
        if mask is not None:
            if pre_filter_ids is not None:
                # Mask is for full corpus — remap to subset
                sub_mask = mask[indices]
                similarities = np.where(sub_mask, similarities, -np.inf)
            else:
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
            neg_sims = active_matrix @ not_like_vec
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

        # Local communities on candidate set (query-time Louvain)
        detected_communities = None
        local_comm_key = 'local_communities' if modifiers and 'local_communities' in modifiers else 'detect_communities'
        if modifiers and modifiers.get(local_comm_key) and len(top_indices) >= 3:
            import networkx as nx
            comm_pool = min(len(top_indices), limit)
            cand_indices = np.array(top_indices[:comm_pool])
            cand_vecs = active_matrix[cand_indices]
            sims = cand_vecs @ cand_vecs.T
            comm_threshold = 0.5
            rows, cols = np.where(np.triu(sims > comm_threshold, k=1))
            G = nx.Graph()
            G.add_nodes_from(range(len(cand_indices)))
            G.add_weighted_edges_from(
                (int(r), int(c), float(sims[r, c])) for r, c in zip(rows, cols)
            )
            if G.number_of_edges() > 0:
                comms = nx.community.louvain_communities(G)
                detected_communities = {}
                for ci, comm in enumerate(comms):
                    for node in comm:
                        detected_communities[int(node)] = ci

        # 3. MMR diversity — iterative selection (returns MMR scores)
        if diverse and len(top_indices) > limit:
            mmr_results = self._mmr_select_on(
                top_indices, similarities, active_matrix, limit, lambda_=mmr_lambda)
            results = [{'id': active_ids[idx], 'score': float(score)}
                       for idx, score in mmr_results]
            if detected_communities is not None:
                cand_list = list(cand_indices)
                for r in results:
                    orig_idx = active_id_to_idx.get(r['id'])
                    if orig_idx is not None and orig_idx in cand_list:
                        pos = cand_list.index(orig_idx)
                        r['_community'] = detected_communities.get(pos)
            return results

        # Build results (cosine/modulated scores)
        results = [{'id': active_ids[idx], 'score': float(similarities[idx])}
                   for idx in top_indices[:limit]]
        if detected_communities is not None:
            cand_list = list(cand_indices)
            for r in results:
                orig_idx = active_id_to_idx.get(r['id'])
                if orig_idx is not None and orig_idx in cand_list:
                    pos = cand_list.index(orig_idx)
                    r['_community'] = detected_communities.get(pos)
        return results

    def _mmr_select_on(self, candidates: list, similarities: np.ndarray,
                       matrix: np.ndarray, k: int, lambda_: float = 0.7) -> list:
        """MMR: iteratively select for relevance minus redundancy.

        Args:
            candidates: indices into the active arrays (not necessarily self.matrix)
            similarities: score array aligned with active arrays
            matrix: the active matrix (full or pre-filtered subset)
            k: number of items to select
            lambda_: relevance vs diversity tradeoff (0-1)

        Returns list of (index, mmr_score) tuples.
        """
        if not candidates:
            return []

        # Pre-compute pairwise similarities for all candidates (one matmul)
        cand_vecs = matrix[candidates]  # (n_cand, dims)
        cand_sims = cand_vecs @ cand_vecs.T  # (n_cand, n_cand)

        n = len(candidates)
        # Track max similarity to any selected item per candidate
        max_sim_to_selected = np.full(n, -np.inf)
        selected_mask = np.zeros(n, dtype=bool)

        # Pre-compute relevance scores once (not per iteration)
        relevance = similarities[candidates]  # numpy fancy indexing, (n,)

        # First item: highest cosine, MMR score = lambda * relevance
        selected = [(candidates[0], lambda_ * float(relevance[0]))]
        selected_mask[0] = True
        max_sim_to_selected = np.maximum(max_sim_to_selected, cand_sims[0])

        for _ in range(k - 1):
            if selected_mask.all():
                break

            # MMR score for all unselected candidates (vectorized)
            mmr_scores = lambda_ * relevance - (1 - lambda_) * max_sim_to_selected
            mmr_scores[selected_mask] = -np.inf  # exclude already selected

            best = np.argmax(mmr_scores)
            if mmr_scores[best] == -np.inf:
                break

            selected.append((candidates[best], float(mmr_scores[best])))
            selected_mask[best] = True
            # Update max similarities with newly selected item
            max_sim_to_selected = np.maximum(max_sim_to_selected, cand_sims[best])

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
                ('claude_code',))
        """
        rows = db.execute(
            f"SELECT id FROM [{table}] WHERE {where}", params
        ).fetchall()
        ids = [r[0] for r in rows]
        return self.get_mask_for_ids(ids)

    def get_vectors(self, ids: list) -> np.ndarray:
        """Return embedding matrix for a batch of IDs. Unknown IDs skipped."""
        indices = [self._id_to_idx[id_] for id_ in ids if id_ in self._id_to_idx]
        if not indices:
            return np.empty((0, self.dims), dtype=np.float32)
        return self.matrix[np.array(indices, dtype=np.int64)]

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


def materialize_vec_ops(db, sql: str) -> str:
    """Transparently materialize vec_ops() as a temp table.

    AI writes:  FROM vec_ops('_raw_chunks', 'query') v
    Becomes:    FROM _vec_results v  (temp table with id TEXT, score REAL)

    Returns original SQL unchanged if no vec_ops table source found.
    Returns JSON error string if vec_ops returns an error (bad pre-filter, etc).
    Skips if wrapped in json_each() (backward compat).
    Only triggers when vec_ops appears as a table source (after FROM/JOIN).
    """
    lower = sql.lower()

    # json_each(vec_ops(...)) — explicit pattern, don't touch
    if 'json_each' in lower:
        return sql

    # Find vec_ops(...) call — balanced paren matching for quoted strings
    start = re.search(r'vec_ops\s*\(', sql)
    if not start:
        return sql

    # Only materialize when used as a table source
    before = sql[:start.start()].rstrip().upper()
    if not (before.endswith('FROM') or before.endswith('JOIN') or before.endswith(',')):
        return json.dumps({"error":
            "vec_ops must be used as a table source (after FROM or JOIN), "
            "not as a scalar expression. "
            "Correct: SELECT v.id, v.score FROM vec_ops('_raw_chunks', 'query') v"})

    # Find the matching close paren (handles quoted strings with escaped '' quotes)
    paren_start = start.end() - 1
    depth = 0
    in_quote = False
    end_pos = None
    i = paren_start
    while i < len(sql):
        c = sql[i]
        if in_quote:
            if c == "'":
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    i += 2  # skip escaped quote '', stay in string
                    continue
                else:
                    in_quote = False
        else:
            if c == "'":
                in_quote = True
            elif c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth == 0:
                    end_pos = i + 1
                    break
        i += 1
    if end_pos is None:
        return sql

    # Execute the vec_ops call as a scalar to get JSON
    call_expr = sql[start.start():end_pos]
    try:
        row = db.execute(f"SELECT {call_expr}").fetchone()
        if not row or not row[0]:
            return sql
        results = json.loads(row[0])
    except Exception as e:
        return json.dumps({"error": f"vec_ops execution failed: {e}"})

    # Handle error JSON from vec_ops — surface it directly
    if not isinstance(results, list):
        if isinstance(results, dict) and 'error' in results:
            return json.dumps(results)
        return sql
    if not results:
        return json.dumps({"error": "vec_ops returned 0 results — pre-filter may have matched no chunks. Check your WHERE clause."})

    # Populate temp table (unique name per call for HTTP concurrency)
    tmp_name = f"_vec_results_{uuid.uuid4().hex[:8]}"
    has_community = '_community' in results[0]
    if has_community:
        db.execute(f"CREATE TEMP TABLE [{tmp_name}] (id TEXT PRIMARY KEY, score REAL, _community INT)")
        db.executemany(
            f"INSERT INTO [{tmp_name}] VALUES (?, ?, ?)",
            [(r['id'], r['score'], r.get('_community')) for r in results]
        )
    else:
        db.execute(f"CREATE TEMP TABLE [{tmp_name}] (id TEXT PRIMARY KEY, score REAL)")
        db.executemany(
            f"INSERT INTO [{tmp_name}] VALUES (?, ?)",
            [(r['id'], r['score']) for r in results]
        )

    # Rewrite: replace vec_ops(...) with temp table
    return sql[:start.start()] + tmp_name + sql[end_pos:]


def register_vec_ops(conn, caches: dict, embed_fn, cell_config: dict = None,
                     embed_doc_fn=None):
    """Register vec_ops as a SQL-callable function with modifier support.

    Args:
        conn: SQLite connection
        caches: {table_name: VectorCache}
        embed_fn: callable(text) -> np.ndarray (768d)
        cell_config: dict of vec:* keys from _meta (optional)

    SQL usage:
        vec_ops('_raw_chunks', 'auth')                              -- raw cosine
        vec_ops('_raw_chunks', 'auth', 'recent:7 diverse unlike:jwt')
    """
    import json
    cfg = cell_config or {}

    def vec_ops_fn(*args):
        if len(args) < 2:
            return json.dumps({"error": "vec_ops requires at least 2 args: table, query_text"})

        try:
            return _vec_ops_inner(*args)
        except Exception as e:
            return json.dumps({"error": f"vec_ops failed: {e}"})

    def _vec_ops_inner(*args):
        table = args[0]
        query_text = args[1]
        modifier_str = args[2] if len(args) > 2 else None
        pre_filter_sql = args[3] if len(args) > 3 else None

        cache = caches.get(table)
        if cache is None or cache.matrix is None:
            return json.dumps([])

        # Diagnostic mode: return cache state
        if query_text == '__diag__':
            return json.dumps({
                'size': cache.size,
                'has_timestamps': cache.timestamps is not None,
            })

        modifiers = parse_modifiers(modifier_str) if modifier_str else None

        # SQL pre-filter: execute 4th arg to get chunk IDs
        # Authorizer whitelist: pure SELECT only (READ=20, SELECT=21, FUNCTION=31, RECURSIVE=33)
        _SQLITE_OK, _SQLITE_DENY = 0, 1
        _SELECT_ONLY = {20, 21, 31, 33}

        def _read_only_authorizer(action, arg1, arg2, db_name, trigger_name):
            return _SQLITE_OK if action in _SELECT_ONLY else _SQLITE_DENY

        pre_filter_ids = None
        if pre_filter_sql:
            try:
                conn.set_authorizer(_read_only_authorizer)
                rows = conn.execute(pre_filter_sql).fetchall()
                pre_filter_ids = {str(r[0]) for r in rows}
            except Exception as e:
                return json.dumps({"error": f"vec_ops pre-filter SQL failed: {e}"})
            finally:
                conn.set_authorizer(None)

        # Handle NULL query text (for like: or from:to: tokens)
        if query_text is None:
            # Check if modifiers provide an alternative query vector
            if modifiers and (modifiers.get('like') or modifiers.get('trajectory_from')):
                # Use a zero vector as placeholder — centroid will replace it, or
                # trajectory blend will weight query_scores=0 (pure direction mode)
                query_vec = np.zeros(cache.dims, dtype=np.float32)
            else:
                return json.dumps({"error": "vec_ops: query_text is NULL and no like: or from:to: token"})
        else:
            query_vec = np.squeeze(embed_fn(query_text))

        limit = 500
        if modifiers and modifiers.get('limit'):
            limit = modifiers['limit']

        results = cache.search(
            query_vec,
            pre_filter_ids=pre_filter_ids,
            modifiers=modifiers,
            config=cfg,
            embed_fn=embed_fn,
            embed_doc_fn=embed_doc_fn,
            diverse=bool(modifiers.get('diverse')) if modifiers else False,
            limit=limit,
            oversample=min(limit * 3, cache.size),
        )
        return json.dumps([
            {k: (round(v, 4) if k == 'score' else v)
             for k, v in r.items()}
            for r in results
        ])

    conn.create_function("vec_ops", -1, vec_ops_fn)
