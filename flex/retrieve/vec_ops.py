"""
Flex Vector Operations — SQL-accessible semantic search.

Bridges the scoring engine into SQLite via virtual table registration.
The scoring engine lives in score.

SQL usage (new — token-based):
    vec_ops('similar:auth')                                   -- cosine search
    vec_ops('similar:auth diverse suppress:jwt decay:7')       -- composed
    vec_ops('centroid:id1,id2 diverse')                       -- centroid search
    vec_ops('similar:auth diverse', 'SELECT id FROM chunks WHERE type = ''file''')

SQL usage (legacy — still supported):
    vec_ops('_raw_chunks', 'auth')
    vec_ops('_raw_chunks', 'auth', 'diverse unlike:jwt', 'SELECT id FROM ...')
"""

import json
import re
import sys
import time
import uuid
from datetime import datetime

import numpy as np
from typing import Optional, List, Dict, Any

from flex.retrieve.score import parse_modifiers, score_candidates, _mmr_select

_TOKEN_RESOLVER = None
_EXTRA_BOUNDARIES = None
try:
    from flex.modules.query import resolve_token, registered_token_names
    _TOKEN_RESOLVER = resolve_token
    _EXTRA_BOUNDARIES = registered_token_names()
except ImportError:
    pass


def _registered_token_names():
    if _TOKEN_RESOLVER is None:
        return _EXTRA_BOUNDARIES
    try:
        from flex.modules.query import registered_token_names
        return registered_token_names()
    except ImportError:
        return _EXTRA_BOUNDARIES


def _coerce_timestamp(value) -> Optional[float]:
    """Best-effort timestamp coercion for mixed-format legacy cells."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        pass

    # Legacy markdown/doc cells sometimes stored human-readable timestamps.
    cleaned = re.sub(r'\s*\([^)]+\)\s*', ' ', text).strip()
    for fmt in (
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(cleaned, fmt).timestamp()
        except ValueError:
            continue

    print(f"VectorCache: skipping non-epoch timestamp {text!r}", file=sys.stderr)
    return None


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
        """Load vectors from SQLite BLOB column into numpy matrix."""
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
        """Load timestamp arrays from DB, aligned with self.ids."""
        if not self.ids:
            return

        N = len(self.ids)

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
                        ts = _coerce_timestamp(row[1])
                        if ts is not None:
                            self.timestamps[idx] = ts
        except Exception as e:
            print(f"VectorCache: timestamps load failed: {e}", file=sys.stderr)

    def search(self, query_vec: np.ndarray, *, pre_filter_ids: set = None,
               not_like_vec: np.ndarray = None,
               diverse: bool = False, limit: int = 10, oversample: int = 200,
               mask: np.ndarray = None, threshold: float = 0.0,
               mmr_lambda: float = 0.7,
               modifiers: dict = None, config: dict = None,
               embed_fn=None, embed_doc_fn=None) -> List[Dict[str, Any]]:
        """Search for similar vectors with optional landscape modulations.

        Delegates to the scoring engine (score.score_candidates).
        """
        if self.matrix is None or len(self.ids) == 0:
            return []

        return score_candidates(
            matrix=self.matrix,
            ids=self.ids,
            id_to_idx=self._id_to_idx,
            query_vec=query_vec,
            timestamps=self.timestamps,
            pre_filter_ids=pre_filter_ids,
            not_like_vec=not_like_vec,
            diverse=diverse,
            limit=limit,
            oversample=oversample,
            mask=mask,
            threshold=threshold,
            mmr_lambda=mmr_lambda,
            modifiers=modifiers,
            config=config,
            embed_fn=embed_fn,
            embed_doc_fn=embed_doc_fn,
            token_resolver=_TOKEN_RESOLVER,
        )

    def _mmr_select_on(self, candidates: list, similarities: np.ndarray,
                       matrix: np.ndarray, k: int, lambda_: float = 0.7) -> list:
        """MMR: iteratively select for relevance minus redundancy."""
        return _mmr_select(candidates, similarities, matrix, k, lambda_)

    def get_mask_for_ids(self, ids: List[str]) -> np.ndarray:
        """Create boolean mask for specific IDs."""
        mask = np.zeros(len(self.ids), dtype=bool)
        for id_ in ids:
            if id_ in self._id_to_idx:
                mask[self._id_to_idx[id_]] = True
        return mask

    def get_mask_from_db(self, db, table: str, where: str,
                         params: tuple = ()) -> np.ndarray:
        """Create boolean mask from SQL WHERE clause."""
        rows = db.execute(
            f"SELECT id FROM [{table}] WHERE {where}", params
        ).fetchall()
        ids = [r[0] for r in rows]
        return self.get_mask_for_ids(ids)

    def get_vectors(self, ids: list) -> np.ndarray:
        """Return embedding matrix for a batch of IDs."""
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
            "Correct: SELECT v.id, v.score FROM vec_ops('similar:query text') v"})

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
    # Dynamic column construction: discover all _-prefixed columns
    # and build the schema automatically. Any enrichment can emit columns.
    tmp_name = f"_vec_results_{uuid.uuid4().hex[:8]}"

    base_cols = [('id', 'TEXT PRIMARY KEY'), ('score', 'REAL')]
    extra_cols = []
    if results:
        for key in sorted(results[0].keys()):
            if key.startswith('_'):
                # Find first non-None value across results for type inference
                val = next((r[key] for r in results if r.get(key) is not None), None)
                if val is None or isinstance(val, (bool, int)):
                    extra_cols.append((key, 'INTEGER'))
                elif isinstance(val, float):
                    extra_cols.append((key, 'REAL'))
                else:
                    extra_cols.append((key, 'TEXT'))

    all_cols = base_cols + extra_cols
    col_defs = ', '.join(f'[{name}] {typ}' for name, typ in all_cols)
    db.execute(f"CREATE TEMP TABLE [{tmp_name}] ({col_defs})")

    col_names = [c[0] for c in all_cols]
    placeholders = ', '.join('?' * len(col_names))
    db.executemany(
        f"INSERT INTO [{tmp_name}] ({', '.join(f'[{c}]' for c in col_names)}) VALUES ({placeholders})",
        [tuple(r.get(c) for c in col_names) for r in results]
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

    SQL usage (new form):
        vec_ops('similar:auth')
        vec_ops('similar:auth diverse suppress:jwt decay:7')
        vec_ops('centroid:id1,id2 diverse', 'SELECT id FROM chunks WHERE type = ''file''')

    SQL usage (legacy form — backward compatible):
        vec_ops('_raw_chunks', 'auth')
        vec_ops('_raw_chunks', 'auth', 'recent:7 diverse unlike:jwt')
        vec_ops('_raw_chunks', 'auth', 'diverse', 'SELECT id FROM ...')
    """
    import json
    cfg = cell_config or {}

    def vec_ops_fn(*args):
        if len(args) < 1:
            return json.dumps({"error": "vec_ops requires at least 1 arg: token string"})

        try:
            return _vec_ops_inner(*args)
        except Exception as e:
            return json.dumps({"error": f"vec_ops failed: {e}"})

    def _vec_ops_inner(*args):
        # Detect legacy vs new form:
        # Legacy: first arg is a table name in caches (e.g. '_raw_chunks')
        # New: first arg is a token string (e.g. 'similar:auth diverse')
        if len(args) >= 2 and args[0] in caches:
            # Legacy form: vec_ops('_raw_chunks', 'query', 'tokens', 'prefilter')
            table = args[0]
            query_text = args[1]
            modifier_str = args[2] if len(args) > 2 else None
            pre_filter_sql = args[3] if len(args) > 3 else None
        else:
            # New form: vec_ops('similar:auth diverse', 'prefilter')
            table = '_raw_chunks'
            token_str = args[0]
            pre_filter_sql = args[1] if len(args) > 1 else None

            # Parse tokens to extract query_text from similar: token
            modifiers_preview = parse_modifiers(token_str, extra_boundaries=_registered_token_names())
            query_text = modifiers_preview.get('similar')
            modifier_str = token_str

        cache = caches.get(table)
        if cache is None or cache.matrix is None:
            return json.dumps([])

        modifiers = parse_modifiers(modifier_str, extra_boundaries=_registered_token_names()) if modifier_str else None

        # SQL pre-filter: execute to get chunk IDs
        # Authorizer whitelist: pure SELECT only (READ=20, SELECT=21, FUNCTION=31, RECURSIVE=33)
        # PRAGMA(19) data_version is allowed — FTS5 vtable constructor needs it to initialize.
        _SQLITE_OK, _SQLITE_DENY = 0, 1
        _SELECT_ONLY = {20, 21, 29, 31, 33}  # 29=CREATE_VTABLE (FTS5 read access)

        def _read_only_authorizer(action, arg1, arg2, db_name, trigger_name):
            if action == 19 and arg1 == 'data_version':
                return _SQLITE_OK
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

        # Handle NULL/empty query text (for centroid: or from:to: tokens)
        if query_text is None or query_text == '':
            if modifiers and (modifiers.get('like') or modifiers.get('trajectory_from')):
                query_vec = np.zeros(cache.dims, dtype=np.float32)
            else:
                return json.dumps({"error": "vec_ops: no similar: text and no centroid: or from:to: token provided"})
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
