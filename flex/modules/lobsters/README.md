# Flex Lobsters Module

Indexes public Lobsters stories and comments into a Flex cell. The module uses
the public JSON endpoints on `lobste.rs`; no API key or account is required.

## Install

From a checkout or released module folder:

```bash
flex module install /path/to/lobsters
flex init --module lobsters --lobsters-limit 5
```

For active development, use:

```bash
flex module install /path/to/lobsters --editable --force
```

## Query

```bash
flex core search --cell lobsters "@orient"
flex core search --cell lobsters "@recent"
flex core search --cell lobsters "@top"
flex core search --cell lobsters "@tag tag=python"
```

The primary view is `chunks`. It exposes `type` (`story` or `comment`), thread
metadata, author, score, depth, parent id, tags, and graph columns when graph
enrichment has been built. `threads` is the story-level surface.

## Refresh

```bash
flex refresh --cells lobsters
flex refresh --cells lobsters --dry-run
python -m flex.modules.lobsters.compile.refresh --cell lobsters --since 7d --limit 5
```

Registry installs use `lifecycle='refresh'` and
`refresh_module='flex.modules.lobsters.compile.refresh'`.
