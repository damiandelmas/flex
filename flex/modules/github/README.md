# Flex GitHub Module

Public source module for GitHub Issues. It compiles issues and comments into a
SQLite Flex cell with `posts`, `issues`, `@open-issues`, `@reply-targets`, and
the general `@orient` surface.

```bash
flex init --module github --github-repos owner/repo
flex core search --cell github "@orient"
flex core search --cell github "@open-issues days=30"
```

Authentication is optional for public repositories. Set `GITHUB_TOKEN` or run
`gh auth login` for higher API limits; otherwise the module uses unauthenticated
public REST requests with small default install limits.
