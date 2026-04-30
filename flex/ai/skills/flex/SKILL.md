---
name: flex
description: Search knowledge cells via the flex MCP server. Use when the user asks to flex, search conversations, memories, changes, documentation, session history, or knowledge bases.
allowed-tools:
  - mcp__flex__flex_search
user-invocable: true
argument-hint: "what to search for (e.g., 'history of auth.py', 'what did we build this week', 'why did we switch to Postgres')"
---

# Flex Search

Flex indexes the user's conversations and knowledge bases. Each cell is a self-describing SQLite database with chunks, embeddings, and graph intelligence. Use when the user asks to "flex" or search their conversations, memories, changes, documentation, or knowledge.

Single endpoint: `mcp__flex__flex_search`. Two params: `query` (SQL or `@preset`) and `cell` (cell name).

Every query must be valid SQL or a `@preset`. Plain text is not accepted; wrap it in `keyword()` or `vec_ops()`.

# RETRIEVAL

Pipeline: SQL -> vec_ops -> SQL. Phase 1 narrows with SQL. Phase 2 scores with embeddings. Phase 3 composes with SQL.

Scores are ordinal within a query. Do not compare scores across queries with different tokens.

## PHASE 1: SQL PRE-FILTER

The second argument to `vec_ops` narrows candidates **before** scoring. Push every known constraint here.

A `WHERE` clause after `vec_ops` filters **after** the pool is filled; sparse post-filters (hitting <5% of chunks) cause pool starvation, for example 500 candidates scored, then `WHERE` drops 498. Pre-filter prevents this.

```sql
SELECT id FROM chunks WHERE type = 'user_prompt'
SELECT id FROM chunks WHERE session_id LIKE 'abc123%'
SELECT id FROM chunks WHERE tool_name = 'Edit'
SELECT id FROM chunks WHERE created_at >= date('now', '-7 days')
SELECT id FROM chunks WHERE type = 'user_prompt'
```

## PHASE 2: VECTOR OPERATIONS (vec_ops)

Scores candidates using embeddings. Tokens reshape scoring before selection: spread across subtopics, suppress a dominant theme, weight recency, search from examples, or trace a direction.

Tokens compose freely in one pass. Returns `(id, score)` pairs for Phase 3.

```sql
vec_ops('similar:query_text tokens', 'pre_filter_sql')
```

**IMPORTANT:** `vec_ops` is a table source; always after `FROM` or `JOIN`:

```sql
FROM vec_ops('similar:how the authentication system evolved over time diverse decay:7') v
```

The `similar:` text is embedded. Bare keywords cast a wide net; natural language (5-15 words) narrows focus.

### Modulation Tokens

Tokens compose:

```text
similar:how we handle auth and token refresh diverse suppress:JWT rotation boilerplate decay:7
```

#### diverse

MMR; penalizes similarity to already-selected results. Use for breadth across subtopics.

#### suppress:TEXT

Embeds `TEXT`, demotes chunks similar to it. Suppresses the dominant signal so edges surface.

Stack multiple: `suppress:deployment pipeline suppress:CI/CD configuration`.

Aim at the dominant cluster theme, not the whole topic.

#### decay:N

Temporal decay. N-day half-life. `decay:7` = weekly. `decay:1` = aggressive. `decay:0` = disabled.

#### centroid:id1,id2,...

Mean embedding of given chunk IDs as query. Use when examples define a concept better than words.

#### from:TEXT to:TEXT

Direction vector through embedding space. Finds content along the conceptual arc between two ideas.

Anchors should be contrasting concepts: `from:quick hacky prototype to:principled production system`.

#### pool:N

Candidate pool size (default 500). Increase if post-filter `WHERE` is sparse: `pool:2000`.

### Examples

Broad survey with recency:

```sql
SELECT v.id, v.score, c.content, c.session_id
FROM vec_ops('similar:key decisions tradeoffs and design choices we made this week diverse decay:7',
  'SELECT id FROM chunks WHERE type = ''user_prompt''') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 15
```

Suppress dominant theme to find edges:

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:what else happened beyond the main refactor this week diverse suppress:deployment pipeline suppress:CI configuration',
  'SELECT id FROM chunks WHERE created_at >= date(''now'', ''-7 days'')') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

Trajectory; conceptual evolution:

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:how the system architecture evolved from:monolithic worker to:cell-based design') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

### Edge Cases

- `diverse` is boolean; no parameter.
- `decay:0` disables decay, `pool:0` falls back to default (500).
- One `vec_ops` per query. For multiple, use CTEs: `WITH a AS (SELECT * FROM vec_ops(...) v) SELECT * FROM a`.
- Some sessions have `NULL` graph columns (`centrality`, `community`); enrichment runs every 30 min. Use `COALESCE`.

## PHASE 3: SQL COMPOSITION

Join scored results back to views, boost with graph metadata, group, filter, paginate.

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:authentication patterns and middleware design') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

# RECIPES

**Structural** (no embeddings; free):

```sql
SELECT project, COUNT(*) as sessions
FROM sessions GROUP BY project ORDER BY sessions DESC
```

**Semantic search** (the skeleton):

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:YOUR TOPIC IN NATURAL LANGUAGE', 'PRE_FILTER') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

Pre-filter examples:

- Session scope: `'SELECT id FROM chunks WHERE session_id LIKE ''abc%'''`
- User messages: `'SELECT id FROM chunks WHERE type = ''user_prompt'''`
- Recent: `'SELECT id FROM chunks WHERE created_at >= date(''now'', ''-7 days'')'`
- By project: `'SELECT id FROM chunks WHERE session_id IN (SELECT session_id FROM sessions WHERE project = ''myapp'')'`
- Files only: `'SELECT id FROM chunks WHERE type = ''file'''`
- No filter: omit or `''`

**Exact term** (FTS5; filename, error, function name, UUID):

```sql
SELECT k.id, k.rank, k.snippet, c.content
FROM keyword('term') k
JOIN chunks c ON k.id = c.id
ORDER BY k.rank DESC LIMIT 10
-- keyword() is a table source — always after FROM or JOIN.
-- Returns (id, rank, snippet). rank is positive — higher = better.
```

**Scoped keyword** (pre-filter restricts which chunks BM25 ranks; prevents pool starvation):

```sql
SELECT k.id, k.rank, k.snippet, c.content
FROM keyword('authentication', 'SELECT id FROM chunks WHERE type = ''user_prompt''') k
JOIN chunks c ON k.id = c.id
ORDER BY k.rank DESC LIMIT 10
-- 2nd arg = pre-filter SQL (must start with SELECT). Same pattern as vec_ops.
-- Without pre-filter, BM25 ranks globally — scoped post-filters starve the pool.
```

**Hybrid intersection** (BOTH keyword and semantic; empty results mean no overlap, not broken syntax):

```sql
SELECT k.id, k.rank, v.score, c.content
FROM keyword('sdk') k
JOIN vec_ops('similar:cell creation pipeline and programmatic ingest workflow') v ON k.id = v.id
JOIN chunks c ON k.id = c.id
ORDER BY v.score DESC LIMIT 10
```

**FTS as pre-filter** (semantic results scoped to keyword matches):

```sql
SELECT v.id, v.score, c.content
FROM vec_ops('similar:error handling and retry patterns',
  'SELECT c.id FROM chunks_fts f JOIN _raw_chunks c ON f.rowid = c.rowid
   WHERE chunks_fts MATCH ''timeout''') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

**Hub navigation** (most connected sessions):

```sql
SELECT v.id, v.score, s.title, s.centrality
FROM vec_ops('similar:the main architectural decisions and system design') v
JOIN chunks c ON v.id = c.id
JOIN sessions s ON c.session_id = s.session_id
WHERE s.is_hub = 1
ORDER BY s.centrality DESC LIMIT 5
```

**File search:**

```sql
-- By path (structural, no embeddings)
SELECT file, section, ext, substr(content, 1, 200)
FROM chunks WHERE type = 'file' AND file LIKE '%/changes/code/2603%'
ORDER BY created_at DESC LIMIT 10
```

```sql
-- Semantic within files
SELECT v.id, v.score, c.file, c.section, substr(c.content, 1, 200)
FROM vec_ops('similar:how authentication middleware validates tokens', 'SELECT id FROM chunks WHERE type = ''file''') v
JOIN chunks c ON v.id = c.id
ORDER BY v.score DESC LIMIT 10
```

**One-liners:**

- Session: `WHERE session_id LIKE 'd332a1a0%'`
- Type: `WHERE type = 'file'` / `'user_prompt'` / `'tool_call'`
- Extension: `WHERE ext = 'py'`
- Drill-down: `@story session=d332a1a0`

# METHODOLOGY

**Right mode for the task:**

- Known path/name -> `WHERE file LIKE '%pattern%'` (structural, free)
- Known exact term -> `keyword('term')` (FTS5)
- Conceptual/fuzzy -> `vec_ops('similar:...')` (semantic)

Start with `@orient`. Then `PRAGMA table_info(chunks)` to discover columns; they differ per cell.

**Structural first.** `GROUP BY` / `COUNT(*)` / `DISTINCT` cost nothing. Get the shape before going semantic.

**Discover then narrow.** Broad `vec_ops` -> find themes -> pre-filter next query with findings.

Push constraints into the pre-filter (2nd arg), not `WHERE` after `vec_ops`.

**Pivot on mode shift.** Theme -> quantify with `GROUP BY`. ID -> exact retrieval with `JOIN` + `ORDER BY position`.

**Cross-cell** when needed. Different cells have different columns and date ranges.

Column names vary: `created_at` (`claude_code`), `timestamp`, `file_date` (other cells). Always `PRAGMA` first.

# PRESETS

Use presets when possible. `@name` as the query. `@orient` discovers all presets per cell.

Positional args: `@story session=abc123`, `@digest days=14`.

# EXTREMELY IMPORTANT: ALWAYS START WITH @orient FOR THE REQUESTED CELL

**`query="@orient"`, `cell="cell_name"`**

Returns cell schema, views, hubs, presets. Do this BEFORE any other queries.

# Final

**USE YOUR JUDGEMENT TO BEST ANSWER THE USERS QUERY. ENSURE TO QUALIFY STATEMENTS WHEN THERE IS NO EXPLICIT EVIDENCE FOR YOUR CLAIMS**
