-- @name: message-index
-- @description: Ordered index of user/assistant turns for exact message selection. Whole session by default; window with limit/tail, set preview width with chars. Set compact=1 for a cheaper payload (message_id/created_at dropped, role abbreviated) — reconstruct message_id as {session}_{message_number}.
-- @params: session (required), chars (default: 60), limit (default: 1000), tail (default: 1), role (default: all), compact (default: 0)

WITH t AS (
    SELECT
        position,
        type,
        id,
        created_at,
        trim(replace(replace(replace(content, char(10), ' '), char(13), ' '), char(9), ' ')) AS flat
    FROM messages
    WHERE session_id = :session
      AND type IN ('user_prompt', 'assistant')
      AND (
            :role = 'all'
         OR (:role = 'user'      AND type = 'user_prompt')
         OR (:role = 'assistant' AND type = 'assistant')
      )
),
n AS (SELECT COUNT(*) AS total FROM t),
w AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (ORDER BY position DESC) AS from_end,
        ROW_NUMBER() OVER (ORDER BY position ASC)  AS from_start
    FROM t
)
SELECT
    w.position                                   AS message_number,
    CASE
        WHEN :compact = 1 THEN CASE w.type WHEN 'user_prompt' THEN 'u' ELSE 'a' END
        ELSE CASE w.type WHEN 'user_prompt' THEN 'user' ELSE 'assistant' END
    END                                           AS role,
    substr(w.flat, 1, :chars)                    AS opening,
    length(w.flat)                               AS full_chars,
    CASE WHEN :compact = 1 THEN NULL ELSE w.id END AS message_id,
    CASE WHEN :compact = 1 THEN NULL ELSE w.created_at END AS created_at,
    n.total                                      AS total_turns,
    max(0, n.total - :limit)                     AS omitted
FROM w, n
WHERE (:tail = 1 AND w.from_end   <= :limit)
   OR (:tail = 0 AND w.from_start <= :limit)
ORDER BY w.position;
