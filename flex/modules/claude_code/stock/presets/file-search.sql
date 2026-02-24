-- @name: file-search
-- @description: BM25 search over file bodies (Write/Edit/Read content). Faster than LIKE — use this for file content search.
-- @params: query (required)

SELECT
    m.target_file,
    m.session_id,
    m.tool_name,
    m.created_at,
    snippet(content_fts, 0, '>>>', '<<<', '...', 20) AS match
FROM content_fts
JOIN _raw_content rc ON content_fts.rowid = rc.rowid
JOIN _edges_raw_content erc ON erc.content_hash = rc.hash
JOIN messages m ON m.id = erc.chunk_id
WHERE content_fts MATCH :query
  AND m.target_file IS NOT NULL
ORDER BY bm25(content_fts)
LIMIT 50
