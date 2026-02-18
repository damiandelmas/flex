-- @name: keyword-hybrid
-- @description: BM25 keyword match — use as SQL pre-filter for vec_ops or standalone
-- @params: query (required)

SELECT c.id, c.content, es.source_id, c.timestamp
FROM chunks_fts
JOIN _raw_chunks c ON chunks_fts.rowid = c.rowid
JOIN _edges_source es ON c.id = es.chunk_id
WHERE chunks_fts MATCH :query
ORDER BY bm25(chunks_fts)
LIMIT 200
