"""
FlexSearch Vector Cache

Matrix-based semantic search. Trades memory for speed.
Loads all vectors once, queries in <1ms via BLAS matmul.

Three retrieval layers:
  vec_search (numpy)  — cosine matmul + candidate shaping
  @structure (numpy)  — ephemeral micro-graph on candidates (future)
  SQL                 — everything else (ORDER BY, WHERE, JOIN)

vec_search modulations (applied on full N array before top-k):
- Pre-filter masks: community:N, kind:TYPE (zero non-matching)
- Matrix multiply (corpus-wide cosine similarity)
- Temporal decay: scores *= 1 / (1 + days_ago / half_life)
- Contrastive (second matmul against negative query)
- MMR diversity (iterative pairwise selection, returns MMR scores)

Hub/bridge boost moved to SQL: ORDER BY v.score * (1 + m.centrality)
Eventually replaced by @structure query-local PageRank.

SQL usage:
    vec_search('_raw_chunks', 'auth')                    -- raw cosine
    vec_search('_raw_chunks', 'auth', 'recent:7 diverse unlike:jwt')
    vec_search('_raw_chunks', 'auth', 'kind:delegation community:12')

Performance:
    1k docs:   0.1ms
    10k docs:  0.5ms
    100k docs: 5ms
    367k docs: 12ms

Memory: ~15MB per 10k docs (384-dim vectors)
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
        recent          temporal decay (cell-configured half-life)
        recent:N        temporal decay with N-day half-life
        unlike:TEXT     contrastive — demote similarity to TEXT
        diverse         MMR diversity selection
        limit:N         override default candidate limit
        community:N     pre-filter to community ID
        kind:TYPE       pre-filter to semantic kind

    Returns dict with keys: recent, recent_days, unlike, diverse,
    limit, community, kind.
    Unknown tokens silently ignored (forward-compatible).
    """
    result = {
        'recent': False,
        'recent_days': None,
        'unlike': None,
        'diverse': False,
        'limit': None,
        'community': None,
        'kind': None,
    }

    if not modifier_str:
        return result

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
        elif token.startswith('community:'):
            try:
                result['community'] = int(token.split(':', 1)[1])
            except ValueError:
                pass
        elif token.startswith('kind:'):
            result['kind'] = token.split(':', 1)[1]

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
        # Pre-filter arrays (N,), aligned with self.ids
        self.community_ids: Optional[np.ndarray] = None  # (N,) int32, -1 = unmapped
        self.kinds: Optional[np.ndarray] = None           # (N,) object, '' = unmapped

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

    def load_columns(self, db, table: str, id_col: str = 'id'):
        """Load modulation column arrays from DB, aligned with self.ids.

        Loads centrality (min-max normalized to 0-1) and timestamps for each
        vector in the cache. Sources without graph data get centrality=0.
        Chunks without timestamps get timestamp=0.

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

        # --- Graph columns: community_id (for pre-filter mask) ---
        self.community_ids = np.full(N, -1, dtype=np.int32)

        try:
            rows = db.execute("""
                SELECT e.chunk_id, g.community_id
                FROM _edges_source e
                JOIN _enrich_source_graph g ON e.source_id = g.source_id
                WHERE g.community_id IS NOT NULL
            """).fetchall()
            for row in rows:
                idx = self._id_to_idx.get(row[0])
                if idx is not None:
                    self.community_ids[idx] = int(row[1])
        except Exception as e:
            print(f"VectorCache: community_ids load failed: {e}", file=sys.stderr)

        # --- Kinds: chunk -> _enrich_types ---
        self.kinds = np.empty(N, dtype=object)
        self.kinds[:] = ''
        try:
            rows = db.execute("""
                SELECT chunk_id, semantic_role
                FROM _enrich_types
                WHERE semantic_role IS NOT NULL
            """).fetchall()
            for row in rows:
                idx = self._id_to_idx.get(row[0])
                if idx is not None:
                    self.kinds[idx] = row[1]
        except Exception as e:
            print(f"VectorCache: kinds load failed: {e}", file=sys.stderr)

    def search(self, query_vec: np.ndarray, *, not_like_vec: np.ndarray = None,
               diverse: bool = False, limit: int = 10, oversample: int = 200,
               mask: np.ndarray = None, threshold: float = 0.0,
               mmr_lambda: float = 0.7,
               modifiers: dict = None, config: dict = None,
               embed_fn=None) -> List[Dict[str, Any]]:
        """
        Search for similar vectors with optional landscape modulations.

        Landscape modulations (applied on full N array before top-k):
        1. Matrix multiply: corpus-wide cosine similarity
        2. Temporal decay: scores *= 1 / (1 + days_ago / half_life)
        3. Contrastive (not_like_vec): penalize similarity to negative query
        4. MMR diversity: iterative selection maximizing relevance - redundancy

        Hub/bridge boost moved to SQL: ORDER BY v.score * (1 + m.centrality)

        Args:
            query_vec: Query embedding (dims,)
            not_like_vec: Negative query embedding for contrastive
            diverse: Enable MMR diversity selection
            limit: Max results to return
            oversample: Candidate pool size for diversity/contrastive
            mask: Boolean mask (n,) - True = include in search
            threshold: Minimum cosine similarity cutoff
            mmr_lambda: Relevance vs diversity tradeoff (0-1)
            modifiers: Parsed modifier dict from parse_modifiers()
            config: Cell config dict from _meta (vec:* keys)
            embed_fn: Embedding function for unlike:TEXT in modifiers

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

        # === PRE-FILTER MASKS (exclude non-matching before modulations) ===
        if modifiers:
            if modifiers.get('community') is not None and self.community_ids is not None:
                similarities[self.community_ids != modifiers['community']] = -np.inf
            if modifiers.get('kind') and self.kinds is not None:
                similarities[self.kinds != modifiers['kind']] = -np.inf

        # === LANDSCAPE MODULATIONS (full N array, before candidate selection) ===
        if modifiers:
            cfg = config or {}

            # Temporal decay: scores *= 1 / (1 + days_ago / half_life)
            if modifiers.get('recent') and self.timestamps is not None:
                if np.any(self.timestamps > 0):
                    half_life = float(
                        modifiers.get('recent_days')
                        or cfg.get('vec:recent:half_life', 30)
                    )
                    days_ago = np.maximum(
                        (time.time() - self.timestamps) / 86400.0, 0.0
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

        # 3. MMR diversity — iterative selection (returns MMR scores)
        if diverse and len(top_indices) > limit:
            mmr_results = self._mmr_select(top_indices, similarities, limit,
                                           lambda_=mmr_lambda)
            return [{'id': self.ids[idx], 'score': float(score)}
                    for idx, score in mmr_results]

        # Build results (cosine/modulated scores)
        return [{'id': self.ids[idx], 'score': float(similarities[idx])}
                for idx in top_indices[:limit]]

    def _mmr_select(self, candidates: list, similarities: np.ndarray,
                    k: int, lambda_: float = 0.7) -> list:
        """MMR: iteratively select for relevance minus redundancy.

        Returns list of (index, mmr_score) tuples. MMR scores monotonically
        decrease by construction, so ORDER BY score DESC in SQL preserves
        the diversity ordering.
        """
        if not candidates:
            return []

        # First item: highest cosine, MMR score = lambda * relevance
        first = candidates[0]
        selected = [(first, lambda_ * float(similarities[first]))]
        remaining = list(candidates[1:])

        while len(selected) < k and remaining:
            best_idx, best_score = -1, -float('inf')

            for i, cand in enumerate(remaining):
                cand_vec = self.matrix[cand]

                # Max similarity to any already selected
                max_sim = 0.0
                for sel_idx, _ in selected:
                    sim = float(np.dot(cand_vec, self.matrix[sel_idx]))
                    max_sim = max(max_sim, sim)

                # MMR: lambda * relevance - (1-lambda) * redundancy
                mmr = lambda_ * similarities[cand] - (1 - lambda_) * max_sim
                if mmr > best_score:
                    best_score = mmr
                    best_idx = i

            if best_idx >= 0:
                selected.append((remaining.pop(best_idx), best_score))
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


def materialize_vec_search(db, sql: str) -> str:
    """Transparently materialize vec_search() as a temp table.

    AI writes:  FROM vec_search('_raw_chunks', 'query') v
    Becomes:    FROM _vec_results v  (temp table with id TEXT, score REAL)

    Skips if wrapped in json_each() (backward compat).
    Only triggers when vec_search appears as a table source (after FROM/JOIN).
    """
    lower = sql.lower()

    # json_each(vec_search(...)) — explicit pattern, don't touch
    if 'json_each' in lower:
        return sql

    # Find vec_search(...) call — balanced paren matching for quoted strings
    start = re.search(r'vec_search\s*\(', sql)
    if not start:
        return sql

    # Only materialize when used as a table source
    before = sql[:start.start()].rstrip().upper()
    if not (before.endswith('FROM') or before.endswith('JOIN') or before.endswith(',')):
        return sql

    # Find the matching close paren (handles quoted strings with parens)
    paren_start = start.end() - 1
    depth = 0
    in_quote = False
    end_pos = None
    for i in range(paren_start, len(sql)):
        c = sql[i]
        if in_quote:
            if c == "'" and (i + 1 >= len(sql) or sql[i + 1] != "'"):
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
    if end_pos is None:
        return sql

    # Execute the vec_search call as a scalar to get JSON
    call_expr = sql[start.start():end_pos]
    try:
        row = db.execute(f"SELECT {call_expr}").fetchone()
        if not row or not row[0]:
            return sql
        results = json.loads(row[0])
    except Exception:
        return sql  # let original SQL fail naturally

    # Populate temp table (unique name per call for HTTP concurrency)
    tmp_name = f"_vec_results_{uuid.uuid4().hex[:8]}"
    db.execute(f"CREATE TEMP TABLE [{tmp_name}] (id TEXT PRIMARY KEY, score REAL)")
    if results:
        db.executemany(
            f"INSERT INTO [{tmp_name}] VALUES (?, ?)",
            [(r['id'], r['score']) for r in results]
        )

    # Rewrite: replace vec_search(...) with temp table
    return sql[:start.start()] + tmp_name + sql[end_pos:]


def register_vec_search(conn, caches: dict, embed_fn, cell_config: dict = None):
    """Register vec_search as a SQL-callable function with modifier support.

    Args:
        conn: SQLite connection
        caches: {table_name: VectorCache}
        embed_fn: callable(text) -> np.ndarray (384d)
        cell_config: dict of vec:* keys from _meta (optional)

    SQL usage:
        vec_search('_raw_chunks', 'auth')                              -- raw cosine
        vec_search('_raw_chunks', 'auth', 'recent:7 diverse unlike:jwt')
    """
    import json
    cfg = cell_config or {}

    def vec_search_fn(table, query_text, modifier_str=None):
        cache = caches.get(table)
        if cache is None or cache.matrix is None:
            return json.dumps([])

        # Diagnostic mode: return cache state
        if query_text == '__diag__':
            unique_kinds = set(cache.kinds[:50].tolist()) if cache.kinds is not None else None
            n_kinds_set = int((cache.kinds != '').sum()) if cache.kinds is not None else 0
            n_comm_set = int((cache.community_ids != -1).sum()) if cache.community_ids is not None else 0
            return json.dumps({
                'size': cache.size,
                'has_kinds': cache.kinds is not None,
                'kinds_populated': n_kinds_set,
                'kinds_sample': list(unique_kinds) if unique_kinds else None,
                'has_community_ids': cache.community_ids is not None,
                'community_ids_populated': n_comm_set,
                'has_timestamps': cache.timestamps is not None,
            })

        query_vec = np.squeeze(embed_fn(query_text))
        modifiers = parse_modifiers(modifier_str) if modifier_str else None

        limit = 500
        if modifiers and modifiers.get('limit'):
            limit = modifiers['limit']

        results = cache.search(
            query_vec,
            modifiers=modifiers,
            config=cfg,
            embed_fn=embed_fn,
            diverse=bool(modifiers.get('diverse')) if modifiers else False,
            limit=limit,
            oversample=min(limit * 3, cache.size),
        )
        return json.dumps([
            {'id': r['id'], 'score': round(r['score'], 4)}
            for r in results
        ])

    conn.create_function("vec_search", -1, vec_search_fn)
