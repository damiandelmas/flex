# GitHub Cell Instructions

This cell indexes GitHub issues and comments from configured repositories. Each
source is one GitHub issue; each chunk is the issue body (position 0) or an
individual comment (position 1..N).

Always start with:

```text
cell="github" query="@orient"
```

`@orient` returns the live schema, presets, recent activity, and coverage
notes. Call it once per task before any other query.

## What This Cell Is For

Use the GitHub cell when the question is about issue trackers:

- open or closed issues and their discussion threads
- bug reports, feature requests, and support questions across repos
- comment authors, reaction counts, and engagement patterns
- finding unanswered questions worth replying to
- semantic search over problem descriptions, workarounds, and decisions

## Core Surfaces

`issues` is the source-level surface. One row per GitHub issue with aggregated
stats and graph intelligence. Use it for structural queries: counts by repo,
open vs closed breakdown, top-scored issues, label filtering.

`posts` is the chunk-level surface. One row per issue body or comment with
author, `item_type` (`'issue'` or `'comment'`), reaction score, `issue_state`,
and graph columns. Use it for semantic search and thread navigation.

Key columns in `posts`: `id`, `content`, `timestamp`, `source_id`, `position`,
`title`, `repo`, `issue_url`, `issue_score`, `issue_comments`, `issue_state`,
`issue_labels`, `item_type`, `author`, `score`, `issue_number`, `url`,
`centrality`, `is_hub`, `community_id`.

Key columns in `issues`: `source_id`, `title`, `repo`, `author`, `score`,
`num_comments`, `url`, `state`, `labels`, `issue_number`, `file_date`,
`chunk_count`, `centrality`, `is_hub`, `community_id`.

## Authentication and Rate Limits

Auth is optional for public repositories. Without credentials, the GitHub REST
API allows 60 requests per hour per originating IP. With `GITHUB_TOKEN` or
`gh auth login`, the limit is 5,000 requests per hour.

Auth resolution order:

1. `GITHUB_TOKEN` environment variable
2. `gh auth token` (gh CLI)
3. Unauthenticated public API requests (60 req/h — warn the user when
   exhaustion is likely; structural queries against the local cell are free)

GitHub search endpoints use a separate, tighter quota. The module keeps GitHub
search disabled unless `--github-queries` is explicitly set at init time.

## Choosing Search Mode

**Structural first.** `GROUP BY` / `COUNT(*)` / `DISTINCT` cost nothing.

```sql
SELECT repo, state, COUNT(*) AS n
FROM issues
GROUP BY repo, state
ORDER BY n DESC;
```

**Exact keyword** for issue numbers, error messages, usernames, quoted phrases.

```sql
SELECT k.id, k.rank, k.snippet, p.repo, p.issue_number, p.author, p.issue_state
FROM keyword('"rate limit"', 'SELECT id FROM posts') k
JOIN posts p ON k.id = p.id
ORDER BY k.rank DESC
LIMIT 15;
```

**Semantic** for capability, problem, or symptom descriptions. Pre-filter with
`posts` or a structural sub-select to avoid pool starvation.

```sql
SELECT v.id, v.score, p.repo, p.issue_number, p.author,
       p.item_type, p.issue_state,
       substr(p.content, 1, 500) AS excerpt
FROM vec_ops(
  'similar:users hitting memory problems on long sessions diverse',
  'SELECT id FROM posts WHERE issue_state = ''open'''
) v
JOIN posts p ON v.id = p.id
ORDER BY v.score DESC
LIMIT 15;
```

**Suppress dominant theme** to surface edge discussions:

```sql
SELECT v.id, v.score, p.repo, p.issue_number,
       substr(p.content, 1, 400) AS excerpt
FROM vec_ops(
  'similar:authentication failures and token expiry suppress:password reset suppress:OAuth flow diverse',
  'SELECT id FROM posts'
) v
JOIN posts p ON v.id = p.id
ORDER BY v.score DESC
LIMIT 12;
```

**Hybrid** (exact term present, ranked by semantic relevance):

```sql
SELECT k.id, k.rank, v.score, p.repo, p.issue_number, p.item_type,
       substr(p.content, 1, 400) AS excerpt
FROM keyword('"out of memory"') k
JOIN vec_ops('similar:memory leak during file processing large uploads') v ON k.id = v.id
JOIN posts p ON k.id = p.id
ORDER BY v.score DESC
LIMIT 10;
```

## Preset Bias

Use presets when they fit. Do not write custom SQL for covered cases.

- `@orient` — live schema, presets, coverage
- `@open-issues` — open issues by reaction score, optional `days=N repo=owner/name`
- `@reply-targets` — open issues with few comments that look like questions, optional `days=N`

```text
@open-issues days=14 repo=anthropics/claude-code
@reply-targets days=7
```

## Issue Thread Drilldown

After finding a target issue, retrieve the full thread ordered by position.

```sql
SELECT position, item_type, author, score,
       datetime(timestamp, 'unixepoch') AS created_at,
       substr(content, 1, 1200) AS content
FROM posts
WHERE issue_number = 42
  AND repo = 'owner/repo'
ORDER BY position;
```

For the issue body only (position 0):

```sql
SELECT title, author, score, num_comments, labels, state, url
FROM issues
WHERE issue_number = 42
  AND repo = 'owner/repo';
```

## Bounded Refresh

Keep scheduled refreshes bounded. Leave `max_issues` and
`max_comments_per_issue` metadata in place. Use `--github-static` for
manual-only refresh. Initial install example:

```bash
flex init --module github \
  --github-repos owner/repo \
  --github-since 30d \
  --github-max-issues 200 \
  --github-max-comments 50
```

## Reporting Results

Include enough context for stable follow-up:

- cell name: `github`
- `repo`, `issue_number`, `item_type`, `author`
- `issue_state`, `score`, timestamp
- vector score or keyword rank when retrieved semantically
- a short excerpt; note when content is truncated
- if a chunk is clipped, report the `source_id` and suggest thread drilldown
