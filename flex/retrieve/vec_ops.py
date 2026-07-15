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


# ── Modulation-token validation (Issue 2) ──────────────────────────────────
# Directive names that take an integer value.
_INT_DIRECTIVES = {'decay', 'pool', 'recent', 'limit'}
# All recognized directive names (canonical + legacy aliases).
_KNOWN_DIRECTIVES = {
    'similar', 'decay', 'suppress', 'centroid', 'pool', 'from', 'to',
    'diverse', 'recent', 'unlike', 'like', 'limit',
}
# Names eligible for typo (edit-distance) matching. Short names (from, to, pool,
# like) are excluded — they false-positive against ordinary colon tokens
# (e.g. 'foo:' is edit-distance 2 from 'pool'). Only names len>=5 participate,
# and only candidate tokens len>=5 are checked, so kind:/community:/frobnicate:
# stay treated as query text, not flagged.
_FUZZY_DIRECTIVES = {
    'similar', 'suppress', 'centroid', 'diverse', 'recent', 'unlike', 'decay',
    'limit',
}


def _is_int(value: str) -> bool:
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1,
                           prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _strip_text_directive_bodies(token_str, known_names):
    """Remove the multi-word free-text bodies of the text directives
    (similar:/suppress:/centroid: and the from:…to: trajectory) up to the next
    directive boundary, mirroring parse_modifiers()'s span extraction. Those
    bodies are query text, not structured tokens, so the validator must not
    inspect the `word:value` shapes inside them (e.g. 'similar:car decal:sticker
    printing' — 'decal:sticker' is query text, not a typo'd directive)."""
    s = token_str.replace('unlike:', 'suppress:').replace('like:', 'centroid:') \
                 .replace('limit:', 'pool:')
    s = re.sub(r'\brecent:', 'decay:', s)
    core = r'diverse|decay:|suppress:|centroid:|pool:|from:|similar:'
    if known_names:
        core = core + '|' + '|'.join(re.escape(n) for n in known_names)
    # from:…to:… trajectory (spans two directives) then the single-body directives
    s = re.sub(r'from:.*?\s+to:.*?(?=\s+(?:' + core + r')\b|\s*$)', ' ', s)
    for name in ('similar', 'suppress'):
        s = re.sub(name + r':.*?(?=\s+(?:' + core + r')\b|\s*$)', ' ', s)
    # centroid: is a SINGLE whitespace-delimited token (centroid:id1,id2), not a
    # multi-word span — mirror parse_modifiers()'s split() so a typo directly
    # following it (centroid:id1 decey:5) is not absorbed and still gets flagged.
    s = re.sub(r'\bcentroid:\S*', ' ', s)
    return s


def _validate_modulation_tokens(token_str, known_names):
    """Detect typo'd or malformed modulation tokens BEFORE they silently
    degrade a vec_ops query (Issue 2). Returns an error dict, or None if clean.

    Flags only high-confidence mistakes:
      - a known integer directive with a non-integer value (decay:notanumber),
      - a colon token (len>=5) that is edit-distance <=2 from a known directive
        but not an exact match (decey: -> decay, supress: -> suppress).

    Arbitrary unknown colon tokens far from any directive (kind:, community:,
    frobnicate:) and the multi-word values of text directives (similar:/suppress:
    /from:/to:/centroid:) are left untouched as query text.
    """
    if not token_str:
        return None
    # Text-directive bodies are free query text — strip them first so their
    # word:value spans aren't validated as structured tokens (parity with parser).
    token_str = _strip_text_directive_bodies(token_str, known_names)
    known = set(_KNOWN_DIRECTIVES)
    known.update(n.lower() for n in (known_names or []))
    problems = []
    for tok in token_str.split():
        if ':' not in tok:
            continue
        name, _, value = tok.partition(':')
        name_l = name.lower()
        if name_l in _INT_DIRECTIVES:
            if value != '' and not _is_int(value):
                problems.append({
                    "token": tok,
                    "reason": f"'{name}:' expects an integer value, got '{value}'",
                })
            continue
        if name_l in known:
            continue  # recognized directive or registered structural token
        if len(name_l) >= 5:
            best_d, best_name = min(
                ((_levenshtein(name_l, d), d) for d in _FUZZY_DIRECTIVES),
                default=(99, None))
            if best_d <= 2:
                problems.append({
                    "token": tok,
                    "reason": f"unknown directive '{name}:' — did you mean "
                              f"'{best_name}:'?",
                })
    if problems:
        return {
            "error": "vec_ops: unrecognized or malformed modulation token(s). "
                     "Fix or remove the token — it would otherwise be embedded "
                     "as query noise or silently ignored.",
            "unrecognized_tokens": problems,
        }
    return None


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
        # Stored (pre-slice) column width, e.g. 768 for a native-Nomic column
        # served at serve_dim=128. Equals self.dims when there is no Matryoshka
        # slice (e.g. MiniLM@128). append_from_db matches new rows on THIS width
        # (the actual blob length in the column) then slices to self.dims —
        # matching on self.dims would silently match zero rows whenever
        # serve_dim < stored width, permanently defeating incremental append.
        self._stored_dim: int = 0
        # Column arrays for landscape modulation (N,), aligned with self.ids
        self.timestamps: Optional[np.ndarray] = None    # (N,) float64, epoch seconds
        # Incremental-append cursor state (see append_from_db)
        self.max_rowid: int = 0          # highest rowid seen among loaded embedded rows
        self.embedded_count: int = 0     # rows in the matrix (post dim-filter)

    def load_from_db(self, db, table: str, embedding_col: str = 'embedding',
                     id_col: str = 'id', serve_dim: int | None = None) -> 'VectorCache':
        """Load vectors from SQLite BLOB column into numpy matrix.

        serve_dim: optional Matryoshka slice applied AFTER the dominant-dim
        guard and BEFORE normalization. No-op when serve_dim >= the loaded
        matrix width (e.g. MiniLM@128 stays 128)."""
        start = time.time()

        rows = db.execute(
            f"SELECT rowid, [{id_col}], [{embedding_col}] FROM [{table}] "
            f"WHERE [{embedding_col}] IS NOT NULL"
        ).fetchall()

        if not rows:
            return self

        self.ids = []
        vectors = []

        for row in rows:
            if row[0] is not None and row[0] > self.max_rowid:
                self.max_rowid = row[0]
            self.ids.append(row[1])
            vectors.append(np.frombuffer(row[2], dtype=np.float32))

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
        self._stored_dim = self.matrix.shape[1]   # pre-slice column width

        if serve_dim and serve_dim < self.matrix.shape[1]:        # Matryoshka slice
            self.matrix = np.ascontiguousarray(self.matrix[:, :serve_dim])

        self.dims = self.matrix.shape[1]

        # Normalize for cosine similarity (in-place slice is a fresh contiguous
        # array from the copy above; the unsliced path keeps the original
        # vstack buffer, so /= is still safe there too)
        norms = np.linalg.norm(self.matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1
        self.matrix = self.matrix / norms

        # Build index
        self._id_to_idx = {id_: i for i, id_ in enumerate(self.ids)}

        self.embedded_count = len(self.ids)

        self.loaded_at = time.time()
        elapsed = (self.loaded_at - start) * 1000
        self._load_msg = f"VectorCache: {len(self.ids)} vectors ({self.dims}d) in {elapsed:.1f}ms"

        self._memmap_matrix(db, table)
        return self

    def _memmap_matrix(self, db, table: str) -> None:
        """turbovec (opt-in via FLEX_VEC_MEMMAP): persist the normalized matrix to
        a flat .npy and replace self.matrix with a read-only memmap of it. Resident
        RAM drops to the working set (kernel pages hot rows); cold cells cost ~0 RSS.

        Default OFF — no behavior change unless the env flag is set. Scoring is
        unchanged: `matrix @ q` and `matrix[indices]` work identically on a memmap
        (fancy-indexing copies only the selected subset — the pushdown path)."""
        import os
        import tempfile
        if not os.environ.get('FLEX_VEC_MEMMAP') or self.matrix is None:
            return
        if isinstance(self.matrix, np.memmap):
            return
        base = None
        try:
            for r in db.execute("PRAGMA database_list").fetchall():
                if r[1] == 'main' and r[2]:
                    base = r[2]
                    break
        except Exception:
            pass
        if base:
            path = f"{base}.vec_{table}_{self.dims}.npy"
        else:  # in-memory / unknown db (e.g. tests) → per-process temp file
            path = os.path.join(
                tempfile.gettempdir(),
                f"flexvec_{table}_{self.dims}_{os.getpid()}_{id(self)}.npy")
        try:
            tmp = f"{path}.{os.getpid()}.tmp"
            with open(tmp, 'wb') as f:                       # file handle → no .npy munging
                np.save(f, np.ascontiguousarray(self.matrix, dtype=np.float32))
            os.replace(tmp, path)                            # atomic: safe across MCP/worker
            self.matrix = np.load(path, mmap_mode='r')       # resident → working set
        except Exception as e:
            print(f"VectorCache: memmap persist failed ({e}); staying in RAM",
                  file=sys.stderr)

    def append_from_db(self, db, table: str, embedding_col: str = 'embedding',
                       id_col: str = 'id') -> 'VectorCache | None | int':
        """Incremental refresh: build a SUCCESSOR cache with rows appended.

        Returns the successor VectorCache (swap it in with one assignment —
        never mutate this instance, in-flight searches hold it), 0 if there
        is nothing new (caller may treat the cache as fresh), or None if a
        full rebuild is required.

        None (rebuild) when: cache never loaded; embedded-row count for this
        dim dropped (deletes happened — ghost risk); or the new-row batch is
        large enough that a rebuild is cheaper.

        Known residual: a delete + insert that keeps the count equal within
        one refresh window can leave a ghost row (id that joins to nothing)
        until the periodic full-rebuild floor (engine.refresh_vec_state)
        forces a reload. Pool pollution only — never wrong content.
        """
        if self.matrix is None or not self.ids or self.dims == 0:
            return None

        # Match new rows on the STORED (pre-slice) column width, not the served
        # width — a nomic column stores native 768d while self.dims is the
        # Matryoshka serve_dim (e.g. 128). Matching on self.dims would match
        # zero rows whenever serve_dim < stored width, forcing a full rebuild
        # on every refresh. Falls back to self.dims if _stored_dim is somehow
        # unset (defensive; load_from_db always sets it).
        stored_dim = self._stored_dim or self.dims
        byte_len = stored_dim * 4  # float32 blobs; length() on BLOB = bytes
        try:
            total = db.execute(
                f"SELECT COUNT(*) FROM [{table}] "
                f"WHERE [{embedding_col}] IS NOT NULL AND length([{embedding_col}]) = ?",
                (byte_len,),
            ).fetchone()[0]
        except Exception:
            return None
        if total < self.embedded_count:
            return None  # deletes detected — heal via full rebuild

        try:
            rows = db.execute(
                f"SELECT rowid, [{id_col}], [{embedding_col}] FROM [{table}] "
                f"WHERE rowid > ? AND [{embedding_col}] IS NOT NULL",
                (self.max_rowid,),
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return 0
        if len(rows) > max(10_000, len(self.ids) // 4):
            return None  # rebuild is cheaper than a mega-concat

        new_max = self.max_rowid
        new_ids: list = []
        vecs: list = []
        for rowid, id_, blob in rows:
            if rowid is not None and rowid > new_max:
                new_max = rowid
            v = np.frombuffer(blob, dtype=np.float32)
            if v.shape[0] != stored_dim:
                continue  # mixed-model artifact, same rule as load_from_db
            if id_ in self._id_to_idx:
                continue  # already present (e.g. re-scanned row)
            if stored_dim > self.dims:                # Matryoshka slice, mirrors
                v = v[:self.dims]                      # load_from_db (renorm below)
            new_ids.append(id_)
            vecs.append(v)

        succ = VectorCache()
        succ.dims = self.dims
        succ._stored_dim = self._stored_dim
        succ.loaded_at = self.loaded_at  # full-load age survives appends (rebuild floor)
        succ.max_rowid = new_max
        succ.embedded_count = self.embedded_count + len(new_ids)
        succ.ids = self.ids + new_ids
        succ._id_to_idx = dict(self._id_to_idx)
        base = len(self.ids)
        for i, id_ in enumerate(new_ids):
            succ._id_to_idx[id_] = base + i

        if vecs:
            block = np.vstack(vecs)
            norms = np.linalg.norm(block, axis=1, keepdims=True)
            norms[norms == 0] = 1
            block = block / norms
            succ.matrix = np.concatenate([self.matrix, block])
        else:
            # All fetched rows were dupes/dim-skipped; matrix unchanged.
            # Sharing is safe: matrices are immutable by convention.
            succ.matrix = self.matrix

        if self.timestamps is not None:
            succ.timestamps = np.concatenate(
                [self.timestamps, np.zeros(len(new_ids), dtype=np.float64)])
            if new_ids:
                try:
                    cols = {r[1] for r in db.execute(
                        f"PRAGMA table_info([{table}])").fetchall()}
                    if 'timestamp' in cols:
                        for chunk_start in range(0, len(new_ids), 900):
                            chunk = new_ids[chunk_start:chunk_start + 900]
                            ph = ','.join('?' * len(chunk))
                            for id_, ts_raw in db.execute(
                                f"SELECT [{id_col}], timestamp FROM [{table}] "
                                f"WHERE [{id_col}] IN ({ph}) AND timestamp IS NOT NULL",
                                tuple(chunk),
                            ).fetchall():
                                idx = succ._id_to_idx.get(id_)
                                if idx is not None and idx >= base:
                                    ts = _coerce_timestamp(ts_raw)
                                    if ts is not None:
                                        succ.timestamps[idx] = ts
                except Exception as e:
                    print(f"VectorCache: append timestamps load failed: {e}",
                          file=sys.stderr)

        succ._memmap_matrix(db, table)   # re-persist+mmap so appends keep the RAM win
        return succ

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


def _mask_sql_data(sql: str) -> str:
    """Blank SQL strings, quoted identifiers, and comments without moving bytes.

    Materializers only need to recognize calls in executable SQL.  Keeping the
    result the same length lets matches found in the mask slice the original SQL
    directly, avoiding a parser dependency and the false positives caused by
    documentation literals in presets.
    """
    masked = list(sql)
    i = 0
    while i < len(sql):
        c = sql[i]
        if c == '-' and i + 1 < len(sql) and sql[i + 1] == '-':
            end = sql.find('\n', i + 2)
            end = len(sql) if end < 0 else end
            masked[i:end] = ' ' * (end - i)
            i = end
            continue
        if c == '/' and i + 1 < len(sql) and sql[i + 1] == '*':
            end = sql.find('*/', i + 2)
            end = len(sql) if end < 0 else end + 2
            masked[i:end] = ' ' * (end - i)
            i = end
            continue
        if c in ("'", '"', '`'):
            quote = c
            start = i
            i += 1
            while i < len(sql):
                if sql[i] == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            masked[start:i] = ' ' * (i - start)
            continue
        if c == '[':
            start = i
            end = sql.find(']', i + 1)
            i = len(sql) if end < 0 else end + 1
            masked[start:i] = ' ' * (i - start)
            continue
        i += 1
    return ''.join(masked)


def materialize_vec_ops(db, sql: str) -> str:
    """Transparently materialize vec_ops() as a temp table.

    AI writes:  FROM vec_ops('_raw_chunks', 'query') v
    Becomes:    FROM _vec_results v  (temp table with id TEXT, score REAL)

    Returns original SQL unchanged if no vec_ops table source found.
    Returns JSON error string if vec_ops returns an error (bad pre-filter, etc).
    Skips if wrapped in json_each() (backward compat).
    Only triggers when vec_ops appears as a table source (after FROM/JOIN).
    """
    code = _mask_sql_data(sql)
    lower = code.lower()

    # json_each(vec_ops(...)) — explicit pattern, don't touch
    if 'json_each' in lower:
        return sql

    # Find vec_ops(...) call — balanced paren matching for quoted strings
    start = re.search(r'\bvec_ops\s*\(', code, re.IGNORECASE)
    if not start:
        return sql

    # Only materialize when used as a table source
    before = code[:start.start()].rstrip().upper()
    if not (before.endswith('FROM') or before.endswith('JOIN') or before.endswith(',')):
        return json.dumps({"error":
            "vec_ops must be used as a table source (after FROM or JOIN), "
            "not as a scalar expression. "
            "Correct: SELECT v.id, v.score FROM vec_ops('similar:query text') v"})

    # Parentheses inside data/comments are blank in ``code``, so balance only
    # executable punctuation while slicing the corresponding original text.
    paren_start = start.end() - 1
    depth = 0
    end_pos = None
    i = paren_start
    while i < len(code):
        c = code[i]
        if c == '(':
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

        # Validate modulation tokens before doing any work — fail loud on typo'd
        # or malformed directives instead of silently degrading the query.
        token_problem = _validate_modulation_tokens(
            modifier_str, _registered_token_names())
        if token_problem is not None:
            return json.dumps(token_problem)

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
