-- @name: full
-- @description: Recover the best full body for a message/chunk id, climbing from clipped output rows to their source tool body when possible
-- @params: id (required)

WITH target AS (
    SELECT *
    FROM messages
    WHERE id = :id OR id LIKE '%' || :id || '%'
    ORDER BY CASE WHEN id = :id THEN 0 ELSE 1 END, length(id)
    LIMIT 1
),
chunk_marker AS (
    SELECT
        CASE
            WHEN content LIKE 'Chunk ID:%' AND instr(content, char(10)) > 0
            THEN trim(substr(
                content,
                length('Chunk ID: ') + 1,
                instr(content, char(10)) - length('Chunk ID: ') - 1
            ))
            ELSE NULL
        END AS marker
    FROM target
),
sibling AS (
    SELECT m.*
    FROM messages m
    JOIN target t ON m.session_id = t.session_id
    JOIN chunk_marker cm ON cm.marker IS NOT NULL
    WHERE m.id != t.id
      AND m.file_body LIKE 'Chunk ID: ' || cm.marker || '%'
    ORDER BY abs(COALESCE(m.position, 0) - COALESCE(t.position, 0))
    LIMIT 1
),
candidates AS (
    SELECT
        0 AS priority,
        t.id AS source_id,
        t.session_id,
        t.position,
        t.tool_name,
        t.target_file,
        t.type,
        'messages.file_body' AS source_column,
        t.content AS source_content,
        t.file_body AS source_body
    FROM target t
    WHERE t.file_body IS NOT NULL

    UNION ALL

    SELECT
        1 AS priority,
        s.id AS source_id,
        s.session_id,
        s.position,
        s.tool_name,
        s.target_file,
        s.type,
        'messages.file_body:sibling_chunk_marker' AS source_column,
        s.content AS source_content,
        s.file_body AS source_body
    FROM sibling s
    WHERE s.file_body IS NOT NULL

    UNION ALL

    SELECT
        2 AS priority,
        t.id AS source_id,
        t.session_id,
        t.position,
        t.tool_name,
        t.target_file,
        t.type,
        'messages.content' AS source_column,
        t.content AS source_content,
        t.content AS source_body
    FROM target t
),
picked AS (
    SELECT *
    FROM candidates
    ORDER BY priority
    LIMIT 1
),
normalized AS (
    SELECT
        *,
        CASE
            WHEN source_body IS NULL THEN NULL
            WHEN json_valid(source_body) THEN COALESCE(
                json_extract(source_body, '$.content'),
                json_extract(source_body, '$.output'),
                json_extract(source_body, '$.stdout'),
                json_extract(source_body, '$.text'),
                source_body
            )
            WHEN instr(source_body, 'Output:' || char(10)) > 0
            THEN substr(source_body, instr(source_body, 'Output:' || char(10)) + length('Output:' || char(10)))
            ELSE source_body
        END AS body,
        CASE
            WHEN source_body LIKE 'Chunk ID:%' AND instr(source_body, 'Output:' || char(10)) > 0 THEN 1
            ELSE 0
        END AS stripped_wrapper
    FROM picked
)
SELECT
    :id AS requested_id,
    source_id AS resolved_id,
    session_id,
    position,
    tool_name,
    target_file,
    type,
    source_column,
    CASE WHEN priority < 2 THEN 1 ELSE 0 END AS full_available,
    stripped_wrapper,
    length(source_content) AS content_len,
    length(source_body) AS source_body_len,
    length(body) AS body_len,
    body
FROM normalized;
