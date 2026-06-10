# Reddit Cell Instructions

Subreddit-scoped public archive via Arctic Shift. No Reddit authentication.
A cell covers only the subreddits named at install or refresh time.

```bash
flex init --module reddit --subreddits ClaudeCode,LocalLLaMA --since 30d
```

Start every task with orient:

```text
cell="reddit" query="@orient"
```

Returns shape, views with columns, graph hubs, communities, presets, and
these instructions. Use it as the live manual — do not guess column names.

## What This Cell Is For

- what users say about a product, tool, or topic across named subreddits
- complaint patterns, praise, friction points, and recurring themes
- top posts by score; comment threads by engagement
- per-subreddit distribution and coverage
- semantic search over community opinion and experience

## Core Surfaces

`chunks` — unified retrieval surface. One row per post or comment.

```text
id, content, created_at, type, source_id, position, title,
subreddit, thread_url, thread_score, thread_comments,
author, score, parent_id, depth, permalink,
centrality, is_hub, is_bridge, community_id
```

`score` is the Reddit vote score for that row. `thread_score` is the parent
thread's score. `depth` is comment nesting depth (0 = top-level, NULL = post).

`threads` — one row per Reddit thread.

```text
source_id, title, subreddit, author, score, num_comments,
url, file_date, chunk_count, centrality, is_hub, is_bridge, community_id
```

`all_chunks` / `all_threads` bypass the surface filter for audits.
`_raw_chunks` / `_raw_sources` are the unprocessed base tables.

## Search Modes

**Structural** (free; no embeddings):

```sql
-- Per-subreddit thread count
SELECT subreddit, COUNT(*) AS threads
FROM threads
GROUP BY subreddit ORDER BY threads DESC;

-- Top threads by score
SELECT title, subreddit, score, num_comments, url
FROM threads
ORDER BY score DESC LIMIT 20;
```

**Keyword** — exact terms, names, error text:

```sql
SELECT k.rank, k.snippet, c.subreddit, c.type, c.author, c.score, c.permalink
FROM keyword('"context window"') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC LIMIT 15;
```

Scoped to one subreddit (pre-filter prevents pool starvation):

```sql
SELECT k.rank, k.snippet, c.author, c.score, c.permalink
FROM keyword('rate limit',
  'SELECT id FROM chunks WHERE subreddit = ''ClaudeAI''') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC LIMIT 15;
```

**Semantic** — complaints, themes, sentiment. Scope in the second arg:

```sql
SELECT v.id, v.score, c.subreddit, c.type, c.author,
       c.score AS reddit_score, c.permalink,
       substr(c.content, 1, 500) AS content
FROM vec_ops(
  'similar:what do users complain about with Claude Code diverse',
  'SELECT id FROM chunks WHERE subreddit = ''ClaudeCode'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 15;
```

Suppress the dominant theme to surface edges:

```sql
SELECT v.id, v.score, c.subreddit, c.author,
       substr(c.content, 1, 400) AS content
FROM vec_ops(
  'similar:problems with AI coding tools diverse suppress:hallucination suppress:cost',
  'SELECT id FROM chunks WHERE type = ''comment'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 15;
```

**Hybrid** (keyword intersection + semantic scoring):

```sql
SELECT k.rank, v.score, c.subreddit, c.author, c.permalink,
       substr(c.content, 1, 400) AS content
FROM keyword('memory') k
JOIN vec_ops('similar:agent memory and context persistence across sessions') v ON k.id = v.id
JOIN chunks c ON c.id = k.id
ORDER BY v.score DESC LIMIT 10;
```

## Thread Drilldown

Navigate from a `source_id` to its comment chunks:

```sql
SELECT position, type, author, score, depth,
       substr(content, 1, 600) AS content
FROM chunks
WHERE source_id = 'ClaudeCode_1r24g2i'
ORDER BY position;
```

Use `parent_id` to reconstruct reply chains. Top-scoring comments:

```sql
SELECT author, score, depth, substr(content, 1, 400) AS content
FROM chunks
WHERE source_id = 'ClaudeCode_1r24g2i' AND type = 'comment'
ORDER BY score DESC LIMIT 10;
```

## Graph Navigation

```sql
-- Hub threads
SELECT title, subreddit, score, num_comments, url, centrality
FROM threads WHERE is_hub = 1
ORDER BY centrality DESC LIMIT 10;
```

Semantic search restricted to hubs:

```sql
SELECT v.id, v.score, c.title, c.centrality,
       substr(c.content, 1, 400) AS content
FROM vec_ops(
  'similar:how developers integrate AI agents into existing codebases',
  'SELECT id FROM chunks WHERE is_hub = 1'
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 10;
```

Use `is_bridge = 1` to find cross-cluster discussion.

## Preset Bias

- `@orient` — live schema and preset list
- `@me days=N` — content from authors in `_meta.authors`
- `@bridges` — cross-community connector threads
- `@genealogy concept=...` — topic lineage through time and hubs
- `@health` — pipeline and embedding coverage check

Refresh runs via `flex.modules.reddit.compile.refresh` with
`lifecycle='refresh'`. Pass `--subreddits` and `--since` to control scope.

## Reporting

- cell: `reddit`
- `source_id` + `subreddit` for thread-level findings
- `permalink` for chunk-level findings
- `type`, `author`, `score`, vector score or keyword rank for each row cited
- compact excerpt; note if content is clipped
- state the subreddit scope so the reader knows coverage limits
