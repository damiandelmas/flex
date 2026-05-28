# Lobsters Cell Instructions

This cell contains public Lobsters stories and comments. It requires no credentials.

Start with `@orient`. Use `chunks` for story/comment retrieval and `threads`
for one-row-per-story inspection. Useful columns include `type`, `author`,
`score`, `depth`, `parent_id`, `thread_url`, `story_url`, `thread_tags`,
`centrality`, `is_hub`, `is_bridge`, and `community_id`.

Common moves:

- `@recent` for latest stories and comments.
- `@top` for high-score stories and comments.
- `@tag tag=python` for story threads tagged with a Lobsters tag.
- `keyword('sqlite')` against `chunks` for full-text lookup.
- `vec_ops('similar:rust async runtime')` joined to `chunks` for semantic lookup.
