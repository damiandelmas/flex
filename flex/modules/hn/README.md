# Flex Hacker News Module

The HN module creates a public no-auth Flex cell from Hacker News stories and comments via the Algolia HN Search API.

```bash
flex init --module hn
```

Small smoke run:

```bash
flex init --module hn --hn-queries sqlite --hn-since 1d --hn-max-stories 1 --hn-max-comments-per-story 0 --hn-max-pages 1 --hn-hits-per-page 1
```

The cell registers as `hn`, installs the `chunks` and `threads` views, installs HN presets, and exposes module instructions through `@orient`.

```bash
flex core search --cell hn "@orient"
flex core search --cell hn "SELECT title, score FROM threads ORDER BY score DESC LIMIT 20"
```
