# Dev.to Cell Instructions

This cell contains public Dev.to articles and comments pulled through the
Forem API. It does not require credentials.

The configured scope is the durable contract. Read it first with `@scope` or
from `_meta.scope`; it records tags, authors, recency, article limits, comment
limits, and whether comments were fetched.

Use `chunks` as the primary query surface. `type = 'article'` selects article
bodies. `type = 'comment'` selects comments. Tag data is stored as JSON text in
`tags`, so presets use `json_each(tags)` where possible and simple `LIKE`
filters as a fallback.

Useful starting points:

```sql
@orient
@scope
@tags
@recent
@by-tag tag='python'
```

Prefer bounded queries. Filter by `created_at`, `type`, `author`, or tag before
running broad `keyword()` or `vec_ops()` searches.
