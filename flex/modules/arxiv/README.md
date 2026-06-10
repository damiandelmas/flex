# Flex arXiv Module

arXiv is a public no-auth source-cell module for research papers pulled from
the arXiv API. Each source is a paper; each chunk is a section, abstract, or
structural piece extracted from it. Metadata (categories, DOI, journal_ref,
comment) and raw arXiv IDs are preserved non-destructively.

```bash
flex init --module arxiv --arxiv-query "all:retrieval augmented generation"
```

The install path creates an `arxiv` cell, runs a small seed pull, installs the
module views, presets, and `@orient` instructions, and registers the cell for
refresh with `flex.modules.arxiv.compile.refresh`.

## Scope

- `--arxiv-query` takes comma-separated arXiv API search queries; the default
  is the public seed query `all:retrieval augmented generation`.
- `--arxiv-ids` pulls exact papers by comma-separated arXiv ID.
- `--arxiv-max-papers` caps papers per query (default: 25).
- `--arxiv-with-source` downloads LaTeX source during initial ingest for
  full-section parsing.
- No authentication required. Every arXiv API request waits at least 3
  seconds; defaults are deliberately small.
- `SEMANTIC_SCHOLAR_API_KEY` is optional and only used by expansion tools.

## Appending Queries

The worker's `--append` flag adds to an existing cell instead of replacing it.
Appended queries merge into the stored query list in `_meta`, so scheduled
refresh widens its scope rather than silently ignoring them:

```bash
python -m flex.modules.arxiv.compile.worker \
    --queries "all:mixture of experts" --cell arxiv --append
```

## Refresh

Refresh reads the stored queries and `last_pull_ts` cursor from cell `_meta`,
pulls only new papers since then, and ingests idempotently (dedup by base
arXiv ID, `INSERT OR IGNORE` on chunks). The graph rebuilds automatically once
enough new sources accumulate.

```bash
python -m flex.refresh --cells arxiv --dry-run
```

## Query Surface

- `chunks`: unified retrieval surface (sections with paper metadata and graph
  columns).
- `papers`: arXiv alias for `chunks` with `section_type`.
- `sources`: one row per paper (title, authors, categories, citation count,
  graph columns).
- `keyword()` for exact terms and quoted phrases; `vec_ops()` for conceptual
  search with SQL pre-filters.
- Presets: `@orient`, `@landscape`, `@bridges`, `@genealogy`, `@health`.

Start with:

```bash
flex core search --cell arxiv "@orient"
flex core search --cell arxiv "@landscape"
```
