-- @name: decisions
-- @description: What the human chose or directed — selections, directions, instructions to act. Filters to user prompts containing directive language.
-- @params: concept (required), limit (default: 20)
-- @multi: true

-- @query: directives
SELECT v.id, ROUND(v.score, 4) as score,
    SUBSTR(m.content, 1, 300) as preview,
    m.session_id, m.timestamp
FROM vec_ops('_raw_chunks', :concept, 'diverse recent:7',
    'SELECT id FROM messages
     WHERE type = ''user_prompt''
     AND length(content) BETWEEN 10 AND 3000
     AND (content LIKE ''%lets %'' OR content LIKE ''%let''''s%''
       OR content LIKE ''%create%'' OR content LIKE ''%use this%''
       OR content LIKE ''%iterate%'' OR content LIKE ''%spawn%''
       OR content LIKE ''%go with%'' OR content LIKE ''%mix %''
       OR content LIKE ''%blend%'' OR content LIKE ''%make %''
       OR content LIKE ''%change%'' OR content LIKE ''%keep %'')') v
JOIN messages m ON v.id = m.id
ORDER BY v.score DESC
LIMIT :limit

-- @query: files_acted_on
SELECT m.target_file, COUNT(*) as touches,
    COUNT(DISTINCT m.session_id) as sessions,
    MAX(m.timestamp) as last_touch
FROM vec_ops('_raw_chunks', :concept, 'diverse') v
JOIN messages m ON v.id = m.id
WHERE m.target_file IS NOT NULL
GROUP BY m.target_file
ORDER BY touches DESC
LIMIT 15
