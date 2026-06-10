# Hacker News Cell Instructions

Public HN stories and comments via Algolia HN Search API. No credentials.
Coverage is bounded by queries and date range set at init.

```bash
flex init --module hn --hn-queries "sqlite,embeddings" --hn-since 30d \
  --hn-max-stories 200 --hn-max-comments-per-story 100 \
  --hn-max-pages 5 --hn-hits-per-page 20
```

Minimal smoke init (one story, no comments):

```bash
flex init --module hn --hn-queries sqlite --hn-since 1d \
  --hn-max-stories 1 --hn-max-comments-per-story 0 \
  --hn-max-pages 1 --hn-hits-per-page 1
```

Start every task with orient:

```text
cell="hn" query="@orient"
```

Returns shape, views with columns, graph hubs, communities, presets, and
these instructions. Use it as the live manual — do not guess column names.

## What This Cell Is For

- what the HN audience says about a technology, tool, or company
- top stories by score and comment volume
- comment-thread sentiment, counterarguments, and expert dissent
- recurring complaints, praise, and skepticism patterns
- semantic search over HN opinion and technical debate
- how a topic landed on HN over time

## Core Surfaces

`chunks` — unified retrieval surface. One row per story or comment.

```text
id, content, created_at, type, source_id, position, title,
thread_url, hn_url, thread_score, thread_comments,
author, score, story_id, parent_id,
centrality, is_hub, is_bridge, community_id
```

`type` is `story` or `comment`. `score` is the HN points for that row.
`thread_score` is the parent story's score. `story_id` maps a comment to
its parent story. `hn_url` is the direct `news.ycombinator.com/item?id=...` link.

`threads` — one row per HN story.

```text
source_id, title, author, score, num_comments,
url, hn_url, file_date, chunk_count,
centrality, is_hub, is_bridge, community_id
```

`url` is the external link (NULL for Ask HN / self-posts).

## Search Modes

**Structural** (free; no embeddings):

```sql
-- Top stories by score
SELECT title, score, num_comments, hn_url
FROM threads
ORDER BY score DESC LIMIT 20;

-- Top by comment volume
SELECT title, score, num_comments, hn_url
FROM threads
ORDER BY num_comments DESC LIMIT 20;
```

**Keyword** — exact terms, project names, URLs, error text:

```sql
SELECT k.rank, k.snippet, c.type, c.author, c.score, c.hn_url
FROM keyword('"context window"') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC LIMIT 15;
```

Scoped to comments (pre-filter prevents pool starvation):

```sql
SELECT k.rank, k.snippet, c.author, c.score, c.hn_url
FROM keyword('local first',
  'SELECT id FROM chunks WHERE type = ''comment''') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC LIMIT 15;
```

**Semantic** — themes, sentiment, fuzzy concepts. Scope in the second arg:

```sql
SELECT v.id, v.score, c.type, c.author, c.score AS hn_score,
       c.hn_url, substr(c.content, 1, 500) AS content
FROM vec_ops(
  'similar:what do users complain about with AI coding assistants diverse',
  'SELECT id FROM chunks WHERE type = ''comment'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 15;
```

Suppress dominant cluster to surface edge opinions:

```sql
SELECT v.id, v.score, c.author, substr(c.content, 1, 400) AS content
FROM vec_ops(
  'similar:criticisms of large language models diverse suppress:hallucination suppress:copyright',
  'SELECT id FROM chunks WHERE type = ''comment'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 15;
```

**Hybrid** (keyword intersection + semantic scoring):

```sql
SELECT k.rank, v.score, c.type, c.author, c.hn_url,
       substr(c.content, 1, 400) AS content
FROM keyword('sqlite') k
JOIN vec_ops('similar:SQLite performance benchmarks and production use cases') v ON k.id = v.id
JOIN chunks c ON c.id = k.id
ORDER BY v.score DESC LIMIT 10;
```

## Thread Drilldown

Navigate from a story to its comment chunks via `source_id`:

```sql
SELECT position, type, author, score,
       substr(content, 1, 600) AS content
FROM chunks
WHERE source_id = 'hn_44497045'
ORDER BY position;
```

Top-scoring comments in a thread:

```sql
SELECT author, score, substr(content, 1, 400) AS content
FROM chunks
WHERE source_id = 'hn_44497045' AND type = 'comment'
ORDER BY score DESC LIMIT 10;
```

Use `parent_id` to reconstruct reply chains. `story_id` pivots from any
comment back to its parent story.

## Graph Navigation

```sql
-- Hub stories
SELECT title, score, num_comments, hn_url, centrality
FROM threads WHERE is_hub = 1
ORDER BY centrality DESC LIMIT 10;
```

Semantic search restricted to hubs:

```sql
SELECT v.id, v.score, c.title, c.centrality,
       substr(c.content, 1, 400) AS content
FROM vec_ops(
  'similar:how AI agents handle long-running tasks and tool use',
  'SELECT id FROM chunks WHERE is_hub = 1'
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 10;
```

Use `is_bridge = 1` to find discussion spanning topic boundaries.

## Preset Bias

- `@orient` — live schema and preset list
- `@me days=N` — content from authors in `_meta.authors`
  (set with `--hn-authors user1,user2` at init)
- `@bridges` — cross-community connector threads
- `@genealogy concept=...` — topic lineage through time and hubs
- `@health` — pipeline and embedding coverage check

Refresh runs via `flex.modules.hn.compile.refresh` with `lifecycle='refresh'`.
The Algolia API is stateless; each refresh re-queries by term and date range.

## Reporting

- cell: `hn`
- `source_id` for thread-level findings
- `hn_url` for direct links to items
- `type`, `author`, `score`, vector score or keyword rank for each row cited
- compact excerpt; note if content is clipped
- state the init query scope so the reader knows topic and date coverage
