-- @name: feedback
-- @description: What the human thought about a topic — opinions, reactions, critiques. Pre-filters to user prompts with opinion-length content (20-2000 chars).
-- @params: concept (required), limit (default: 20)
-- @multi: true

-- @query: opinions
SELECT v.id, ROUND(v.score, 4) as score,
    SUBSTR(m.content, 1, 300) as preview,
    m.session_id, m.timestamp
FROM vec_ops('_raw_chunks', :concept, 'diverse recent:7',
    'SELECT id FROM messages
     WHERE type = ''user_prompt''
     AND length(content) BETWEEN 20 AND 2000') v
JOIN messages m ON v.id = m.id
ORDER BY v.score DESC
LIMIT :limit

-- @query: sessions
SELECT DISTINCT s.session_id, s.title,
    s.started_at, s.message_count,
    ROUND(s.centrality, 4) as centrality
FROM vec_ops('_raw_chunks', :concept, 'diverse',
    'SELECT id FROM messages
     WHERE type = ''user_prompt''
     AND length(content) BETWEEN 20 AND 2000') v
JOIN messages m ON v.id = m.id
JOIN sessions s ON m.session_id = s.session_id
ORDER BY s.centrality DESC
LIMIT 10
