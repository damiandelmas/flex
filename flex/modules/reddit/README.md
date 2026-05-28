# Flex Reddit Module

Reddit is a public source-cell module for selected subreddits. It is not an
all-Reddit crawler.

```bash
flex init --module reddit --subreddits ClaudeCode,LocalLLaMA --since 30d
```

The install path creates a `reddit` cell, pulls a bounded window from the
configured subreddits through Arctic Shift, registers the cell for refresh, and
installs the module views, presets, and `@orient` instructions.

## Scope

- `--subreddits` is required.
- `--since` bounds the initial backfill window, defaulting to `30d`.
- Refreshes continue from per-subreddit cursors in `_meta.sub_cursors`.
- The registry lifecycle is `refresh` with
  `flex.modules.reddit.compile.refresh`.

## Query Surface

- `chunks`: filtered post/comment chunks.
- `threads`: filtered thread-level sources.
- `all_chunks`: unfiltered post/comment chunks.
- `all_threads`: unfiltered threads.
- `@me`: authored content from `_meta.authors`.

Start with:

```bash
flex core search --cell reddit "@orient"
```
