"""FTS5 keyword materializer — peer primitive to vec_ops.

AI writes:  FROM keyword('search term') k
            FROM keyword('search term', 'SELECT id FROM chunks WHERE type = ''user_prompt''') k
Becomes:    FROM _kw_results_xxxx k  (temp table with id, rank, snippet)

The optional second argument is a pre-filter SQL query that restricts which
chunk IDs are eligible for BM25 ranking.  Without it, keyword() searches the
entire FTS index — classic pool starvation on scoped queries.  The pre-filter
is executed with a read-only SQLite authorizer (same pattern as vec_ops).

Modifiers (limit:N) can appear as the 2nd arg when no pre-filter is used,
or as the 3rd arg when a pre-filter is present. There is no implicit result
limit: without ``limit:N`` the materializer stages the complete native FTS
match set and lets the surrounding SQL query decide how much to return.
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

    # A trusted Meta retrieval declaration is an explicit dispatch boundary.
    # Ordinary attachments do not install it, so the long-standing one-cell
    # implementation below remains the exact default behavior.
    try:
        from flex.meta import retrieval_world_members

        world_members = retrieval_world_members(db)
    except (ImportError, RuntimeError, sqlite3.DatabaseError):
        world_members = ()
    if world_members:
        return _materialize_world_keyword(
            db,
            sql,
            call_start=start.start(),
            call_end=end_pos,
            inner=sql[paren_start + 1:end_pos - 1].strip(),
            members=world_members,
        )

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
    sanitized = sanitize_fts5(term)

    # Parse remaining args: detect pre-filter (starts with SELECT) vs modifiers
    pre_filter_sql = None
    # Do not silently truncate the native FTS relation. A caller can request
    # an explicit materialization bound with limit:N; otherwise the complete
    # match set is staged and the outer SQL LIMIT remains authoritative.
    limit = None
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
                limit = int(m.group(1))

    # Execute pre-filter to get eligible chunk IDs
    scope_table = None
    hidden_visibility_ids = None
    hidden_visibility_table = None
    try:
        from flex.modules.claude_code.source_visibility import hidden_ids
        hidden_visibility_ids = hidden_ids(db, "_raw_chunks")
    except Exception as exc:
        return json.dumps({"error": f"keyword() visibility policy failed: {exc}"})
    if hidden_visibility_ids is not None:
        hidden_visibility_table = f"_kw_hidden_{uuid.uuid4().hex[:8]}"
        db.execute(
            f"CREATE TEMP TABLE [{hidden_visibility_table}] (id TEXT PRIMARY KEY)"
        )
        db.executemany(
            f"INSERT OR IGNORE INTO [{hidden_visibility_table}] VALUES (?)",
            [(id_,) for id_ in hidden_visibility_ids],
        )
    if pre_filter_sql:
        try:
            db.set_authorizer(_read_only_authorizer)
            pf_rows = db.execute(pre_filter_sql).fetchall()
        except Exception as e:
            return json.dumps({"error": f"keyword() pre-filter SQL failed: {e}"})
        finally:
            db.set_authorizer(None)

        pf_ids = {str(r[0]) for r in pf_rows}
        if hidden_visibility_ids is not None:
            pf_ids -= hidden_visibility_ids

        if not pf_ids:
            # Pre-filter matched nothing — return empty results
            tmp_name = f"_kw_results_{uuid.uuid4().hex[:8]}"
            _create_keyword_result_table(db, tmp_name)
            if hidden_visibility_table:
                db.execute(f"DROP TABLE IF EXISTS [{hidden_visibility_table}]")
            return sql[:start.start()] + tmp_name + sql[end_pos:]

        # Materialize pre-filter IDs into a temp table so FTS can JOIN against it.
        # This pushes the scope into the FTS query itself — no over-fetch needed.
        scope_table = f"_kw_scope_{uuid.uuid4().hex[:8]}"
        db.execute(f"CREATE TEMP TABLE [{scope_table}] (id TEXT PRIMARY KEY)")
        db.executemany(
            f"INSERT OR IGNORE INTO [{scope_table}] VALUES (?)",
            [(id_,) for id_ in pf_ids]
        )
    # Execute FTS5 query — raw-first, quote-on-error fallback
    visibility_predicate = (
        f"AND c.id NOT IN (SELECT id FROM [{hidden_visibility_table}]) "
        if hidden_visibility_table else ""
    )
    limit_clause = " LIMIT ?" if limit is not None else ""
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
            f"{visibility_predicate}"
            "ORDER BY bm25(chunks_fts) "
            f"{limit_clause}"
        )
    else:
        fts_sql = (
            "SELECT c.id, "
            "  -bm25(chunks_fts) as rank, "
            "  snippet(chunks_fts, 0, '>>>', '<<<', '...', 30) as snippet "
            "FROM chunks_fts "
            "JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid "
            "WHERE chunks_fts MATCH ? "
            f"{visibility_predicate}"
            "ORDER BY bm25(chunks_fts) "
            f"{limit_clause}"
        )

    matched_words: list = []
    dropped_words: list = []
    try:
        try:
            params = (sanitized,) if limit is None else (sanitized, limit)
            rows = db.execute(fts_sql, params).fetchall()
            # AND returned nothing — fall back to OR for broader matching
            if not rows and ' ' in sanitized and 'OR' not in sanitized:
                or_query = ' OR '.join(sanitized.split())
                params = (or_query,) if limit is None else (or_query, limit)
                rows = db.execute(fts_sql, params).fetchall()
        except sqlite3.OperationalError:
            # Fallback: double-quote each word with OR for literal matching
            words = re.sub(r'[^\w\s]', '', term).split()
            if words:
                escaped = ' OR '.join(f'"{w}"' for w in words if len(w) > 1)
                fallback_query = escaped or '""'
                params = ((fallback_query,) if limit is None
                          else (fallback_query, limit))
                rows = db.execute(fts_sql, params).fetchall()
            else:
                rows = []
        # Per-token match probe — surfaces which tokens matched ZERO rows so a
        # bare multi-word query that silently collapsed to its matchable subset
        # (e.g. 'zzzgrumblefish stenographer' driven entirely by 'stenographer')
        # is no longer indistinguishable from a real match. Runs while the scope
        # temp table is still alive (before the finally drops it).
        matched_words, dropped_words = _probe_keyword_tokens(
            db, term, sanitized, scope_table, hidden_visibility_table)
    except Exception as e:
        return json.dumps({"error": f"keyword() search failed: {e}"})
    finally:
        # Search results are already buffered; release temporary eligibility sets.
        for temp_table in (scope_table, hidden_visibility_table):
            if temp_table:
                try:
                    db.execute(f"DROP TABLE IF EXISTS [{temp_table}]")
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
    # matched_tokens / dropped_tokens are query-level facts repeated on every
    # row (JSON arrays). dropped_tokens names the terms that matched nothing in
    # the corpus — the in-band signal Issue 1 was missing.
    matched_json = json.dumps(matched_words)
    dropped_json = json.dumps(dropped_words)
    tmp_name = f"_kw_results_{uuid.uuid4().hex[:8]}"
    _create_keyword_result_table(db, tmp_name)
    if rows:
        db.executemany(
            f"INSERT INTO [{tmp_name}] VALUES (?, ?, ?, ?, ?)",
            [(r[0], r[1], r[2], matched_json, dropped_json) for r in rows]
        )

    # Rewrite: replace keyword(...) with temp table name
    return sql[:start.start()] + tmp_name + sql[end_pos:]


def _create_keyword_result_table(db, tmp_name: str, *, world: bool = False):
    """Create a staged keyword relation and its rank-ordered access path.

    Keyword results are commonly joined to a wide provider view and then
    ordered/limited by rank. The primary key keeps identity lookups cheap;
    this covering rank index makes SQLite drive that join from the already
    materialized FTS hits instead of scanning the wide view first.
    """
    columns = (
        "id TEXT PRIMARY KEY, rank REAL, snippet TEXT, "
        "matched_tokens TEXT, dropped_tokens TEXT"
    )
    if world:
        columns += ", cell_id TEXT NOT NULL, native_id TEXT NOT NULL"
    db.execute(f"CREATE TEMP TABLE [{tmp_name}] ({columns})")
    db.execute(
        f"CREATE INDEX [{tmp_name}_rank] "
        f"ON [{tmp_name}](rank DESC, id)"
    )


def _world_result_table(db, sql: str, call_start: int, call_end: int,
                        rows=(), matched_words=(), dropped_words=()) -> str:
    """Stage world keyword results and rewrite one table-source call."""
    tmp_name = f"_kw_results_{uuid.uuid4().hex[:8]}"
    _create_keyword_result_table(db, tmp_name, world=True)
    if rows:
        matched_json = json.dumps(list(matched_words))
        dropped_json = json.dumps(list(dropped_words))
        db.executemany(
            f"INSERT INTO [{tmp_name}] VALUES (?,?,?,?,?,?,?)",
            [
                (
                    row["id"], row["rank"], row["snippet"],
                    matched_json, dropped_json, row["cell_id"], row["native_id"],
                )
                for row in rows
            ],
        )
    return sql[:call_start] + tmp_name + sql[call_end:]


def _world_fts_sql(member, *, scoped: bool, default_scope: bool = False,
                   coord_table: str | None = None, probe: bool = False,
                   limit: int | None = None) -> str:
    from flex.meta import (
        RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS,
        RETRIEVAL_WORLD_OBJECTS,
    )

    alias = '"' + member.schema_alias.replace('"', '""') + '"'
    selected = "1" if probe else (
        "c.id, -bm25(chunks_fts) AS rank, "
        "snippet(chunks_fts, 0, '>>>', '<<<', '...', 30) AS snippet"
    )
    if coord_table is not None:
        selected = (
            "coord.id, coord.cell_id, coord.native_id, -bm25(chunks_fts) AS rank, "
            "snippet(chunks_fts, 0, '>>>', '<<<', '...', 30) AS snippet"
        )
        join_scope = "JOIN temp.[{scope}] coord ON coord.cell_id=? AND coord.native_id=c.id "
        scope_predicate = ""
    elif scoped:
        join_scope = "JOIN temp.[{scope}] s ON s.cell_id=? AND s.native_id=c.id "
        scope_predicate = ""
    elif default_scope:
        # The default world membership is already indexed by
        # (cell_id,native_id,object_kind). Join it directly instead of copying
        # every eligible object into a new temp table for each query. The
        # exclusion table is keyed by the stable world object id, so native
        # FTS remains authoritative while the world retains its acceptance
        # boundary.
        join_scope = (
            f"JOIN temp.{RETRIEVAL_WORLD_OBJECTS} o "
            "ON o.object_kind='chunk' AND o.cell_id=? AND o.native_id=c.id "
            f"LEFT JOIN temp.{RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS} x "
            "ON x.object_kind='chunk' AND x.id=o.id "
        )
        scope_predicate = "x.id IS NULL AND "
    else:
        join_scope = ""
        scope_predicate = ""
    result_limit = " LIMIT ?" if limit is not None else ""
    return (
        f"SELECT {selected} FROM {alias}.chunks_fts "
        f"JOIN {alias}._raw_chunks c ON chunks_fts.rowid=c.rowid "
        + join_scope
        + f"WHERE {scope_predicate}chunks_fts MATCH ? "
        + ("LIMIT 1" if probe
           else f"ORDER BY bm25(chunks_fts),c.id{result_limit}")
    )


def _world_token_probe(db, term: str, sanitized: str, members, scope_table,
                       *, default_scope: bool = False):
    if re.search(r'\b(AND|OR|NOT|NEAR)\b', term) or '*' in term:
        return [], []
    words = [word for word in sanitized.split() if word]
    if len(words) < 2:
        return words, []
    matched, dropped = [], []
    for word in words:
        found = False
        for member in members:
            query = _world_fts_sql(
                member,
                scoped=scope_table is not None,
                default_scope=default_scope,
                probe=True,
            )
            if scope_table is not None:
                query = query.format(scope=scope_table)
                params = (member.cell_id, word)
            elif default_scope:
                params = (member.cell_id, word)
            else:
                params = (word,)
            try:
                found = db.execute(query, params).fetchone() is not None
            except sqlite3.OperationalError:
                # Preserve the one-cell contract: a token that cannot be probed
                # is not confidently reported as absent.
                found = True
            if found:
                break
        (matched if found else dropped).append(word)
    return matched, dropped


def _materialize_world_keyword(db, sql: str, *, call_start: int, call_end: int,
                               inner: str, members) -> str:
    """Search every declared native FTS index as one lexical world."""
    from flex.meta import (
        RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS,
        RETRIEVAL_WORLD_OBJECTS,
    )

    args = _split_args(inner)
    if not args:
        return json.dumps({"error": "keyword() requires a non-empty search term"})
    term = args[0].strip()
    if len(term) >= 2 and term[0] == "'" and term[-1] == "'":
        term = term[1:-1].replace("''", "'")
    if not term or not term.strip():
        return json.dumps({"error": "keyword() requires a non-empty search term"})
    sanitized = sanitize_fts5(term)

    pre_filter_sql = None
    # World dispatch follows the same no-hidden-truncation contract as a
    # native cell. ``limit:N`` is an explicit opt-in bound; otherwise every
    # member contributes its complete native FTS match set to the RRF merge.
    limit = None
    for arg in args[1:]:
        value = arg.strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            value = value[1:-1].replace("''", "'")
        stripped = value.strip()
        if stripped.upper().startswith(("SELECT", "WITH")):
            pre_filter_sql = stripped
        else:
            match = re.search(r'limit:(\d+)', stripped)
            if match:
                limit = int(match.group(1))

    scope_table = None
    default_scope = False
    coordinate_table = None
    try:
        if pre_filter_sql:
            try:
                db.set_authorizer(_read_only_authorizer)
                eligible = {
                    str(row[0]) for row in db.execute(pre_filter_sql).fetchall()
                    if row[0] is not None
                }
            except Exception as exc:
                return json.dumps({"error": f"keyword() pre-filter SQL failed: {exc}"})
            finally:
                db.set_authorizer(None)
            if not eligible:
                return _world_result_table(db, sql, call_start, call_end)
            scope_table = f"_kw_world_scope_{uuid.uuid4().hex[:8]}"
            db.execute(
                f"CREATE TEMP TABLE [{scope_table}]("
                "cell_id TEXT NOT NULL,native_id TEXT NOT NULL,"
                "PRIMARY KEY(cell_id,native_id))"
            )
            world_ids = f"_kw_world_ids_{uuid.uuid4().hex[:8]}"
            db.execute(f"CREATE TEMP TABLE [{world_ids}](id TEXT PRIMARY KEY)")
            db.executemany(
                f"INSERT OR IGNORE INTO [{world_ids}] VALUES(?)",
                [(value,) for value in eligible],
            )
            db.execute(
                f"INSERT OR IGNORE INTO [{scope_table}] "
                f"SELECT o.cell_id,o.native_id FROM temp.{RETRIEVAL_WORLD_OBJECTS} o "
                f"JOIN [{world_ids}] i ON i.id=o.id WHERE o.object_kind='chunk'"
            )
            db.execute(f"DROP TABLE [{world_ids}]")
            if db.execute(f"SELECT 1 FROM [{scope_table}] LIMIT 1").fetchone() is None:
                return _world_result_table(db, sql, call_start, call_end)
        else:
            # Product worlds may preserve lossless native coordinates while
            # excluding historical/provisional objects from their ordinary
            # retrieval surface. Keep that boundary as an indexed join in
            # each native FTS query; do not copy the accepted world.
            default_scope = True

        coordinate_table = f"_kw_world_coords_{uuid.uuid4().hex[:8]}"
        db.execute(
            f"CREATE TEMP TABLE [{coordinate_table}]("
            "id TEXT PRIMARY KEY, cell_id TEXT NOT NULL, native_id TEXT NOT NULL)"
        )
        if scope_table is not None:
            db.execute(
                f"INSERT INTO [{coordinate_table}] "
                f"SELECT o.id,o.cell_id,o.native_id FROM temp.{RETRIEVAL_WORLD_OBJECTS} o "
                f"JOIN [{scope_table}] s ON s.cell_id=o.cell_id AND s.native_id=o.native_id "
                "WHERE o.object_kind='chunk'"
            )
        else:
            db.execute(
                f"INSERT INTO [{coordinate_table}] "
                f"SELECT o.id,o.cell_id,o.native_id FROM temp.{RETRIEVAL_WORLD_OBJECTS} o "
                f"LEFT JOIN temp.{RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS} x "
                "ON x.object_kind='chunk' AND x.id=o.id "
                "WHERE o.object_kind='chunk' AND x.id IS NULL"
            )
        if db.execute(f"SELECT 1 FROM [{coordinate_table}] LIMIT 1").fetchone() is None:
            return _world_result_table(db, sql, call_start, call_end)

        def search_all(match_query: str):
            found = []
            for member in members:
                statement = _world_fts_sql(
                    member,
                    scoped=False,
                    coord_table=coordinate_table,
                    limit=limit,
                )
                statement = statement.format(scope=coordinate_table)
                params = ((member.cell_id, match_query) if limit is None
                          else (member.cell_id, match_query, limit))
                for ordinal_rank, row in enumerate(
                    db.execute(statement, params).fetchall(), start=1
                ):
                    found.append({
                        "id": str(row[0]),
                        "rank": 1.0 / (60.0 + ordinal_rank),
                        "snippet": row[4],
                        "cell_id": str(row[1]),
                        "native_id": str(row[2]),
                        "member_ordinal": member.ordinal,
                    })
            return found

        try:
            rows = search_all(sanitized)
            if not rows and ' ' in sanitized and 'OR' not in sanitized:
                rows = search_all(' OR '.join(sanitized.split()))
        except sqlite3.OperationalError:
            words = re.sub(r'[^\w\s]', '', term).split()
            escaped = ' OR '.join(f'"{word}"' for word in words if len(word) > 1)
            rows = search_all(escaped or '""') if words else []
        except Exception as exc:
            return json.dumps({"error": f"keyword() search failed: {exc}"})

        matched, dropped = _world_token_probe(
            db, term, sanitized, members, scope_table,
            default_scope=default_scope,
        )
        rows.sort(key=lambda row: (
            -row["rank"], row["member_ordinal"], row["native_id"]
        ))
        return _world_result_table(
            db, sql, call_start, call_end,
            rows=(rows if limit is None else rows[:limit]),
            matched_words=matched, dropped_words=dropped,
        )
    except Exception as exc:
        return json.dumps({"error": f"keyword() search failed: {exc}"})
    finally:
        if scope_table:
            try:
                db.execute(f"DROP TABLE IF EXISTS [{scope_table}]")
            except sqlite3.DatabaseError:
                pass
        if coordinate_table:
            try:
                db.execute(f"DROP TABLE IF EXISTS [{coordinate_table}]")
            except sqlite3.DatabaseError:
                pass


def _probe_keyword_tokens(db, term: str, sanitized: str, scope_table,
                          hidden_visibility_table: str | None = None):
    """Probe each query token individually to find which matched ZERO rows.

    Returns (matched, dropped) word lists. This is the in-band signal for the
    silent-drop failure: under the AND→OR fallback an unmatchable token simply
    contributes nothing and the query collapses to its matchable subset with no
    trace. Probing each token against the (scoped) FTS index recovers that fact.

    Two real tokens that never co-occur in one chunk still both report as
    matched (dropped=[]) — the AND→OR fallback fires for them too, but neither
    token is actually absent from the index, so neither is "dropped".

    Returns ([], []) when the query uses explicit FTS5 operators (cannot be
    decomposed into independent tokens) or is a single token (a zero-match
    single token already returns an empty, loud result set).
    """
    if re.search(r'\b(AND|OR|NOT|NEAR)\b', term) or '*' in term:
        return [], []
    words = [w for w in sanitized.split() if w]
    if len(words) < 2:
        return words, []

    visibility_predicate = (
        f"AND c.id NOT IN (SELECT id FROM [{hidden_visibility_table}]) "
        if hidden_visibility_table else ""
    )
    if scope_table is not None:
        probe_sql = (
            "SELECT 1 FROM chunks_fts "
            "JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid "
            f"JOIN [{scope_table}] s ON c.id = s.id "
            f"WHERE chunks_fts MATCH ? {visibility_predicate}LIMIT 1"
        )
    elif hidden_visibility_table:
        probe_sql = (
            "SELECT 1 FROM chunks_fts "
            "JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid "
            f"WHERE chunks_fts MATCH ? {visibility_predicate}LIMIT 1"
        )
    else:
        probe_sql = "SELECT 1 FROM chunks_fts WHERE chunks_fts MATCH ? LIMIT 1"

    matched, dropped = [], []
    for w in words:
        try:
            hit = db.execute(probe_sql, (w,)).fetchone()
        except sqlite3.OperationalError:
            # Token isn't independently MATCH-able (rare) — don't claim it dropped.
            matched.append(w)
            continue
        (matched if hit else dropped).append(w)
    return matched, dropped


def sanitize_fts5(term: str) -> str:
    """Sanitize a search term for FTS5 MATCH syntax.

    Strips punctuation that breaks FTS5 (?, !, @, etc.), drops single-char
    words, and space-joins (FTS5 default = AND). If the term already contains
    FTS5 operators (AND, OR, NOT, NEAR, *), it's passed through as-is to
    preserve intentional FTS5 syntax.

    AND semantics are correct for most queries ("net revenue 2022" should
    require all terms). The fallback path uses OR when AND returns 0 results.

    Public interface modules call this directly when they lower whole
    expressions to SQL and cannot nest inside the keyword() materializer.
    """
    # Pass through if it already contains FTS5 operators
    if re.search(r'\b(AND|OR|NOT|NEAR)\b', term) or '*' in term:
        return term

    # Apostrophes join a word (what's -> whats); other punctuation is a token
    # boundary (vec:model -> vec model), not deletion (vecmodel).
    normalized = re.sub(r"['\u2019]", '', term)
    words = re.sub(r'[^\w\s.]', ' ', normalized).split()
    words = [w.strip('.') for w in words]
    words = [w for w in words if len(w) > 1]
    if not words:
        return term  # fall back to raw term, let FTS5 error handling catch it

    # Space-join = FTS5 implicit AND (all terms must match)
    return ' '.join(words)


# Backwards-compatible alias for pre-public callers.
_sanitize_fts5 = sanitize_fts5


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
