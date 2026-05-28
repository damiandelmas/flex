# Dev.to Flex Module

Dev.to is a public no-auth source module for Forem articles and comments.
It creates a `devto` cell from configured tags and optional public author
usernames.

## Install

```bash
flex core module install /path/to/devto --name devto
flex core init --module devto --tags python,ai --since 7d --limit 3 --comment-limit 0
```

When the module is packaged with Flex, the first command is not needed:

```bash
flex core init --module devto --tags python,ai --since 7d --limit 3 --comment-limit 0
```

## Scope

The module scope is the configured tag list plus optional author usernames.
No Dev.to account, token, or secret is used.

Useful flags:

```text
--tags python,ai,mcp        Dev.to tags to query
--authors username          Public author usernames to include
--since 7d                  Pull articles published in the last N days
--limit 3                   Max articles per tag or author
--comment-limit 0           Skip comments for a tiny first cell
--no-comments               Skip comments
--devto-graph               Build graph after indexing
```

The selected scope is stored in `_meta.scope`, `_meta.tags`, `_meta.authors`,
`_meta.tag_limit`, `_meta.comment_limit`, and `_meta.include_comments`.

## Query

Start with:

```bash
flex core search --cell devto "@orient"
flex core search --cell devto "@scope"
flex core search --cell devto "@tags"
```

The primary view is `chunks`. Its `type` column is `article` or `comment`.
Use `tags`, `author`, `article_url`, `score`, `reading_time`, and
`created_at` for ordinary narrowing.
