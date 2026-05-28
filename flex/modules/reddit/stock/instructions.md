# Reddit Cell

Reddit cells are subreddit-scoped public archive cells. They do not index all
of Reddit. A cell is created only from the subreddits explicitly passed at
install or refresh time.

Install example:

```bash
flex init --module reddit --subreddits ClaudeCode,LocalLLaMA --since 30d
```

Use `@orient` first. The primary query surfaces are:

- `chunks`: filtered post/comment chunks, with `type`, `subreddit`, `author`,
  score fields, thread metadata, and graph columns.
- `threads`: filtered source-level thread view.
- `all_chunks` and `all_threads`: unfiltered audit views.
- `@me`: content from authors listed in `_meta.authors`.

The default views hide low-signal rows through `_meta` scope keys. Raw rows
remain in `_raw_chunks` and `_raw_sources`, and the `all_*` views bypass the
surface filter.

Useful starting queries:

```sql
SELECT subreddit, COUNT(*) AS threads
FROM threads
GROUP BY subreddit
ORDER BY threads DESC;
```

```sql
SELECT created_at, subreddit, type, author, score, title, content
FROM chunks
WHERE subreddit = 'ClaudeCode'
ORDER BY created_at DESC
LIMIT 20;
```
