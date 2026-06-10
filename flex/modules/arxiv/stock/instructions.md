# arXiv Cell Instructions

This cell indexes arXiv papers as a semantic research landscape. Each source is
a paper; each row is a section, abstract, or structural chunk extracted from it.

The cell is self-contained. Start here:

```text
cell="arxiv" query="@orient"
```

`@orient` returns the schema, presets, graph entry points, hub papers, and
communities. Use it as the live manual before any other query.

## What This Cell Is For

- semantic search over paper sections, abstracts, and findings
- category-scoped or date-scoped literature surveys
- identifying hub papers and cross-community bridges
- tracing a concept's lineage across published work
- recovering full section text by `arxiv_id` or title

This cell covers the installed corpus only. It does not query the live arXiv
API at retrieval time.

## Core Surfaces

`chunks` — unified retrieval surface. Columns: `paper_title`, `authors`,
`abs_url`, `arxiv_id`, `section_heading`, `heading_command`, `primary_category`,
`published`, `centrality`, `is_hub`, `is_bridge`, `community_id`.

`papers` — arXiv alias for `chunks`; adds `section_type` in place of `type`.
Use when `section_type` matters.

`sources` — one row per paper. Columns: `title`, `authors`, `abs_url`,
`citation_count`, `section_count`, `primary_category`, `published`, `has_latex`,
plus graph columns.

## Choosing Search Mode

Structural first when you know category, date, arxiv_id, or authors:

```sql
SELECT source_id, title, primary_category, published, section_count
FROM sources
WHERE primary_category LIKE 'cs.IR%'
ORDER BY published DESC LIMIT 20;
```

`keyword()` for exact terms, model names, dataset names, quoted phrases:

```sql
SELECT k.id, k.rank, k.snippet, c.paper_title, c.arxiv_id, c.section_heading
FROM keyword('"chain of thought"') k
JOIN chunks c ON c.id = k.id
ORDER BY k.rank DESC LIMIT 10;
```

`vec_ops()` for conceptual search. Push scope into the pre-filter — not a
`WHERE` after vec_ops. Sparse post-filters starve the 500-candidate pool.

```sql
SELECT v.id, v.score, c.paper_title, c.arxiv_id, c.primary_category,
       substr(c.content, 1, 1400) AS excerpt
FROM vec_ops(
  'similar:retrieval augmentation for long context diverse',
  'SELECT id FROM chunks WHERE primary_category LIKE ''cs.IR%'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 15;
```

## Search Mode Examples

Recency-biased semantic search:

```sql
SELECT v.id, v.score, c.paper_title, c.arxiv_id, c.published,
       substr(c.content, 1, 1200) AS excerpt
FROM vec_ops(
  'similar:tool use language models agentic planning diverse decay:60',
  'SELECT id FROM chunks WHERE published >= date(''now'', ''-90 days'')'
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 12;
```

Suppress dominant signal to surface edges:

```sql
SELECT v.id, v.score, c.paper_title, c.arxiv_id, substr(c.content, 1, 1000) AS excerpt
FROM vec_ops(
  'similar:multimodal reasoning diverse suppress:image captioning suppress:visual question answering',
  'SELECT id FROM chunks WHERE primary_category LIKE ''cs.CV%'''
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 10;
```

Hybrid: keyword gate + semantic ranking:

```sql
SELECT k.id, k.rank, v.score, c.paper_title, c.arxiv_id, substr(c.content, 1, 800) AS excerpt
FROM keyword('"mixture of experts"') k
JOIN vec_ops('similar:sparse gating routing experts scaling') v ON k.id = v.id
JOIN chunks c ON c.id = k.id
ORDER BY v.score DESC LIMIT 10;
```

Concept trajectory:

```sql
SELECT v.id, v.score, c.paper_title, c.published, substr(c.content, 1, 800) AS excerpt
FROM vec_ops(
  'similar:attention from:local window attention to:global sparse attention'
) v
JOIN chunks c ON c.id = v.id
ORDER BY v.score DESC LIMIT 10;
```

Category inventory (structural, free):

```sql
SELECT primary_category, COUNT(*) AS papers,
       MIN(published) AS earliest, MAX(published) AS latest
FROM sources WHERE primary_category != ''
GROUP BY primary_category ORDER BY papers DESC;
```

## Paper Navigation

Sections of one paper:

```sql
SELECT position, section_heading, section_type, substr(content, 1, 1600) AS content
FROM papers WHERE arxiv_id = '2004.12832' ORDER BY position;
```

Hub papers as entry points for unfamiliar areas:

```sql
SELECT source_id, title, primary_category, centrality, community_id
FROM sources WHERE is_hub = 1 ORDER BY centrality DESC LIMIT 10;
```

## Module Operational Notes

No authentication required for normal arXiv API pulls. Defaults are deliberately
small: initial install seeds a small query set; worker and refresh commands cap
papers per query; every arXiv API request waits at least 3 seconds.

`SEMANTIC_SCHOLAR_API_KEY` is optional. Used only by expansion commands that
call Semantic Scholar. Without the key the arXiv cell installs, refreshes, and
queries normally.

Safe refresh probe (no writes):

```bash
python -m flex.refresh --cells arxiv --dry-run
```

## Preset Bias

- `@orient` — live schema, hubs, communities, presets
- `@landscape` — paper count, section count, categories, date range
- `@bridges` — cross-community connector papers
- `@genealogy concept=...` — concept lineage through time and hubs
- `@health` — pipeline health check and embedding coverage

Use raw SQL when a preset is too broad or when you need a precise pre-filter.

## Reporting Results

Include per result: `arxiv_id`, `paper_title`, `section_heading`,
`primary_category`, `published`, `abs_url`, vector score or keyword rank, and a
compact excerpt. When the corpus may not cover an area, say so. If a section is
clipped, note the `arxiv_id` and `section_heading` for re-query.
