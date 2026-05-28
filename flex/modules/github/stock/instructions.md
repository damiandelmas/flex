# GitHub Cell

The GitHub module indexes public GitHub issues and comments into a Flex cell.
Use it for project support queues, ecosystem questions, bug reports, feature
requests, and reply-target discovery.

Start with:

```sql
@orient
```

Primary views:

- `posts`: one row per issue body or comment chunk.
- `issues`: one row per GitHub issue source.

Useful presets:

- `@open-issues days=30`
- `@open-issues days=30 repo=owner/name`
- `@reply-targets days=14`

Install with a small first pull:

```bash
flex init --module github \
  --github-repos owner/repo \
  --github-since 7d \
  --github-max-issues 10 \
  --github-max-comments 10
```

GitHub authentication is optional for public repositories. Without auth, the
GitHub REST API primary rate limit is 60 requests per hour per originating IP.
With `GITHUB_TOKEN` or `gh auth login`, authenticated requests normally use the
user token budget of 5,000 requests per hour. GitHub search endpoints use a
separate, tighter search bucket, so this module keeps search disabled during
`flex init --module github` unless `--github-queries` is set.

Auth resolution order:

1. `GITHUB_TOKEN`
2. `gh auth token`
3. unauthenticated public API requests

Keep scheduled refreshes bounded by leaving `max_issues` and
`max_comments_per_issue` metadata in place. Use `--github-static` when you want
manual refresh only.

