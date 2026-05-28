# arXiv Cell

Start with `@orient`. Use `chunks` or `papers` for section-level search and `sources` for one row per paper. `chunks` is the universal primary view; `papers` keeps the same rows with arXiv-oriented naming.

This module is public and requires no authentication for normal arXiv API pulls. Defaults are deliberately small: initial public install seeds a tiny query set, worker and refresh commands cap papers per query, and every arXiv API request waits at least 3 seconds before another request.

Use `SEMANTIC_SCHOLAR_API_KEY` only for optional expansion commands that call Semantic Scholar. Without the key, the arXiv cell still installs, refreshes, and queries through the normal arXiv API surface.

Useful columns:

- `papers.paper_title`, `papers.content`, `papers.section_type`, `papers.primary_category`, `papers.abs_url`
- `sources.title`, `sources.authors`, `sources.primary_category`, `sources.section_count`, `sources.has_latex`

Useful presets:

- `@landscape` summarizes paper count, section count, categories, and date range.

For a safe refresh probe, run:

```bash
python -m flex.refresh --cells arxiv --dry-run
```
