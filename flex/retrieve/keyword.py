"""FTS5 keyword materializer — peer primitive to vec_ops.

AI writes:  FROM keyword('search term') k
            FROM keyword('search term', 'SELECT id FROM chunks WHERE type = ''user_prompt''') k
Becomes:    FROM _kw_results_xxxx k  (temp table with id, rank, snippet)

The optional second argument is a pre-filter SQL query that restricts which
chunk IDs are eligible for BM25 ranking.  Without it, keyword() searches the
entire FTS index — classic pool starvation on scoped queries.  The pre-filter
is executed with a read-only SQLite authorizer (same pattern as vec_ops).

Modifiers (limit:N) can appear as the 2nd arg when no pre-filter is used,
or as the 3rd arg when a pre-filter is present.
"""

import json
import re
import sqlite3
import uuid


# Authorizer whitelist — pure SELECT only.
# Matches vec_ops pattern: READ=20, SELECT=21, CREATE_VTABLE=29, FUNCTION=31, RECURSIVE=33.
# PRAGMA(19) data_version allowed for FTS5 vtable constructor.
_SQLITE_OK, _SQLITE_DENY = 0, 1
_SELECT_ONLY = {20, 21, 29, 31, 33}


def _read_only_authorizer(action, arg1, arg2, db_name, trigger_name):
    if action == 19 and arg1 == 'data_version':
        return _SQLITE_OK
    return _SQLITE_OK if action in _SELECT_ONLY else _SQLITE_DENY


def materialize_keyword(db, sql: str) -> str:
    """Transparently materialize keyword() as a temp table.

    Returns original SQL unchanged if no keyword() table source found.
    Returns JSON error string on failure.
    """
    # Find keyword(...) call
    start = re.search(r'keyword\s*\(', sql)
    if not start:
        return sql

    # Only materialize when used as a table source (FROM/JOIN position)
    before = sql[:start.start()].rstrip().upper()
    if not (before.endswith('FROM') or before.endswith('JOIN') or before.endswith(',')):
        return sql

    # Balanced-paren extraction (handles quoted strings with escaped '' quotes)
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
                    i += 2
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

    # Extract args from keyword('term', 'pre_filter', 'modifiers')
    inner = sql[paren_start + 1:end_pos - 1].strip()
    args = _split_args(inner)
    if not args:
        return json.dumps({"error": "keyword() requires a non-empty search term"})

    term = args[0].strip()
    # Strip surrounding quotes
    if len(term) >= 2 and term[0] == "'" and term[-1] == "'":
        term = term[1:-1].replace("''", "'")

    if not term or not term.strip():
        return json.dumps({"error": "keyword() requires a non-empty search term"})

    # Sanitize for FTS5: strip punctuation that breaks MATCH syntax,
    # then OR the remaining words. Natural language queries like
    # "What degree did I graduate with?" become "What OR degree OR did OR ..."
    # which is more forgiving than FTS5's default AND semantics.
    sanitized = _sanitize_fts5(term)

    # Parse remaining args: detect pre-filter (starts with SELECT) vs modifiers
    pre_filter_sql = None
    limit = 200
    for arg in args[1:]:
        val = arg.strip()
        if len(val) >= 2 and val[0] == "'" and val[-1] == "'":
            val = val[1:-1].replace("''", "'")
        stripped = val.strip()
        if stripped.upper().startswith('SELECT'):
            pre_filter_sql = stripped
        else:
            m = re.search(r'limit:(\d+)', stripped)
            if m:
                limit = int(m.group(1)) or 200

    # Execute pre-filter to get eligible chunk IDs
    scope_table = None
    if pre_filter_sql:
        try:
            db.set_authorizer(_read_only_authorizer)
            pf_rows = db.execute(pre_filter_sql).fetchall()
        except Exception as e:
            return json.dumps({"error": f"keyword() pre-filter SQL failed: {e}"})
        finally:
            db.set_authorizer(None)

        if not pf_rows:
            # Pre-filter matched nothing — return empty results
            tmp_name = f"_kw_results_{uuid.uuid4().hex[:8]}"
            db.execute(f"CREATE TEMP TABLE [{tmp_name}] (id TEXT PRIMARY KEY, rank REAL, snippet TEXT)")
            return sql[:start.start()] + tmp_name + sql[end_pos:]

        # Materialize pre-filter IDs into a temp table so FTS can JOIN against it.
        # This pushes the scope into the FTS query itself — no over-fetch needed.
        scope_table = f"_kw_scope_{uuid.uuid4().hex[:8]}"
        db.execute(f"CREATE TEMP TABLE [{scope_table}] (id TEXT PRIMARY KEY)")
        db.executemany(
            f"INSERT OR IGNORE INTO [{scope_table}] VALUES (?)",
            [(str(r[0]),) for r in pf_rows]
        )

    # Execute FTS5 query — raw-first, quote-on-error fallback
    if scope_table is not None:
        # Scoped: JOIN FTS results against pre-filter IDs directly.
        # BM25 ranks only within the scoped set — no pool starvation.
        fts_sql = (
            "SELECT c.id, "
            "  -bm25(chunks_fts) as rank, "
            "  snippet(chunks_fts, 0, '>>>', '<<<', '...', 30) as snippet "
            "FROM chunks_fts "
            "JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid "
            f"JOIN [{scope_table}] s ON c.id = s.id "
            "WHERE chunks_fts MATCH ? "
            "ORDER BY bm25(chunks_fts) "
            "LIMIT ?"
        )
    else:
        fts_sql = (
            "SELECT c.id, "
            "  -bm25(chunks_fts) as rank, "
            "  snippet(chunks_fts, 0, '>>>', '<<<', '...', 30) as snippet "
            "FROM chunks_fts "
            "JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid "
            "WHERE chunks_fts MATCH ? "
            "ORDER BY bm25(chunks_fts) "
            "LIMIT ?"
        )

    try:
        try:
            rows = db.execute(fts_sql, (sanitized, limit)).fetchall()
            # AND returned nothing — fall back to OR for broader matching
            if not rows and ' ' in sanitized and 'OR' not in sanitized:
                or_query = ' OR '.join(sanitized.split())
                rows = db.execute(fts_sql, (or_query, limit)).fetchall()
        except sqlite3.OperationalError:
            # Fallback: double-quote each word with OR for literal matching
            words = re.sub(r'[^\w\s]', '', term).split()
            if words:
                escaped = ' OR '.join(f'"{w}"' for w in words if len(w) > 1)
                rows = db.execute(fts_sql, (escaped or '""', limit)).fetchall()
            else:
                rows = []
    except Exception as e:
        return json.dumps({"error": f"keyword() search failed: {e}"})
    finally:
        # Clean up scope temp table
        if scope_table:
            try:
                db.execute(f"DROP TABLE IF EXISTS [{scope_table}]")
            except Exception:
                pass

    # Min-max normalize ranks to [0, 1] so keyword scores compose with
    # vec_ops cosine scores (~[0,1]) via simple addition in hybrid queries.
    if len(rows) >= 2:
        ranks = [r[1] for r in rows]
        lo, hi = min(ranks), max(ranks)
        span = hi - lo
        if span > 0:
            rows = [(r[0], (r[1] - lo) / span, r[2]) for r in rows]
        else:
            rows = [(r[0], 0.5, r[2]) for r in rows]  # equal scores → 0.5 (uncertain)
    elif len(rows) == 1:
        rows = [(rows[0][0], 0.5, rows[0][2])]  # single result → 0.5 (uncertain)

    # Create temp table (always — even on empty results)
    tmp_name = f"_kw_results_{uuid.uuid4().hex[:8]}"
    db.execute(f"CREATE TEMP TABLE [{tmp_name}] (id TEXT PRIMARY KEY, rank REAL, snippet TEXT)")
    if rows:
        db.executemany(
            f"INSERT INTO [{tmp_name}] VALUES (?, ?, ?)",
            [(r[0], r[1], r[2]) for r in rows]
        )

    # Rewrite: replace keyword(...) with temp table name
    return sql[:start.start()] + tmp_name + sql[end_pos:]


def _sanitize_fts5(term: str) -> str:
    """Sanitize a search term for FTS5 MATCH syntax.

    Strips punctuation that breaks FTS5 (?, !, @, etc.), drops single-char
    words, and space-joins (FTS5 default = AND). If the term already contains
    FTS5 operators (AND, OR, NOT, NEAR, *), it's passed through as-is to
    preserve intentional FTS5 syntax.

    AND semantics are correct for most queries ("net revenue 2022" should
    require all terms). The fallback path uses OR when AND returns 0 results.
    """
    # Pass through if it already contains FTS5 operators
    if re.search(r'\b(AND|OR|NOT|NEAR)\b', term) or '*' in term:
        return term

    # Strip non-alphanumeric (except spaces and dots for decimals),
    # split, drop single-char words
    words = re.sub(r'[^\w\s.]', '', term).split()
    words = [w.strip('.') for w in words]
    words = [w for w in words if len(w) > 1]
    if not words:
        return term  # fall back to raw term, let FTS5 error handling catch it

    # Space-join = FTS5 implicit AND (all terms must match)
    return ' '.join(words)


def _split_args(inner: str) -> list[str]:
    """Split comma-separated args respecting single-quoted strings."""
    args = []
    current = []
    in_quote = False
    i = 0
    while i < len(inner):
        c = inner[i]
        if in_quote:
            current.append(c)
            if c == "'":
                if i + 1 < len(inner) and inner[i + 1] == "'":
                    current.append("'")
                    i += 2
                    continue
                else:
                    in_quote = False
        else:
            if c == "'":
                in_quote = True
                current.append(c)
            elif c == ',':
                args.append(''.join(current))
                current = []
            else:
                current.append(c)
        i += 1
    if current:
        args.append(''.join(current))
    return args
