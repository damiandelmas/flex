# Hacker News Cell

This cell indexes public Hacker News stories and comments through the Algolia HN Search API. It does not use credentials, cookies, or a private API key.

Start with `@orient`. The primary view is `chunks`, where `type` is `story` or `comment`. Use `threads` when you want one row per HN story.

Useful first queries:

```sql
SELECT title, score, num_comments, url
FROM threads
ORDER BY score DESC
LIMIT 20;
```

```sql
SELECT c.created_at, c.type, c.author, c.score, c.title, c.content
FROM chunks c
WHERE c.content LIKE '%sqlite%'
ORDER BY c.created_at DESC
LIMIT 20;
```

```sql
SELECT k.rank, c.type, c.author, c.title, k.snippet
FROM keyword('local first search') k
JOIN chunks c ON c.id = k.id
LIMIT 20;
```

`@me days=30` returns rows authored by usernames stored in `_meta.authors`. Set those during init with `--hn-authors user1,user2`.

For a tiny no-auth smoke init, use:

```bash
flex init --module hn --hn-queries sqlite --hn-since 1d --hn-max-stories 1 --hn-max-comments-per-story 0 --hn-max-pages 1 --hn-hits-per-page 1
```

The registered cell refreshes through `flex.modules.hn.compile.refresh` with `lifecycle='refresh'`.
