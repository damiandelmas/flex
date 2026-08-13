-- @name: hydrate
-- @description: Recover a complete compact Ledger lineage plus the newest exact evidence that fits the budget.
-- @params: after (default: __hybrid__), limit (default: 8), max_chars (default: 6000)

WITH hydrated AS (
    SELECT
        s.source_order AS self_order,
        s.selection_reason,
        c.target_cell_id,
        c.target_cell_name,
        c.parent_object_id AS session_id,
        c.target_object_id,
        c.target_object_type,
        c.source_order,
        c.created_at,
        c.native_type,
        a.annotation_id,
        a.wing,
        a.hall,
        a.room,
        a.weight,
        a.note,
        c.content,
        printf(
            '%012d:%012d:%s',
            COALESCE(s.source_order, 0),
            COALESCE(c.source_order, 0),
            a.annotation_id
        ) AS hydration_cursor,
        length(COALESCE(a.note, ''))
          + length(COALESCE(c.content, ''))
          + 512 AS hydration_chars
    FROM self() s
    JOIN _flex_self_content c
      ON c.selection_id = s.selection_id
    JOIN annotations a
      ON a.target_cell_id = c.target_cell_id
     AND a.target_chunk_id = c.target_object_id
), scored AS (
    SELECT
        hydrated.*,
        ROW_NUMBER() OVER (
            ORDER BY hydration_cursor DESC
        ) AS recent_row,
        SUM(hydration_chars) OVER (
            ORDER BY hydration_cursor DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS recent_chars
    FROM hydrated
), cursor_eligible AS (
    SELECT
        scored.*,
        ROW_NUMBER() OVER (
            ORDER BY hydration_cursor
        ) AS cursor_row,
        COUNT(*) OVER () AS cursor_landmarks,
        SUM(hydration_chars) OVER (
            ORDER BY hydration_cursor
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cursor_chars
    FROM scored
    WHERE COALESCE(:after, '__hybrid__') <> '__hybrid__'
      AND hydration_cursor > CASE
          WHEN COALESCE(:after, '__start__') = '__start__' THEN ''
          ELSE :after
      END
), cursor_page AS (
    SELECT *
    FROM cursor_eligible
    WHERE cursor_row <= MIN(MAX(CAST(COALESCE(:limit, 8) AS INTEGER), 1), 100)
      AND (
          cursor_chars <= MIN(
              MAX(CAST(COALESCE(:max_chars, 6000) AS INTEGER), 4096),
              200000
          )
          OR cursor_row = 1
      )
), selected AS (
    SELECT
        scored.*,
        'hybrid' AS delivery_mode,
        CASE
            WHEN recent_row <= MIN(MAX(CAST(COALESCE(:limit, 8) AS INTEGER), 1), 100)
             AND recent_chars <= MIN(
                 MAX(CAST(COALESCE(:max_chars, 6000) AS INTEGER), 4096),
                 200000
             )
            THEN 1 ELSE 0
        END AS evidence_expanded
    FROM scored
    WHERE COALESCE(:after, '__hybrid__') = '__hybrid__'

    UNION ALL

    SELECT
        cursor_page.self_order,
        cursor_page.selection_reason,
        cursor_page.target_cell_id,
        cursor_page.target_cell_name,
        cursor_page.session_id,
        cursor_page.target_object_id,
        cursor_page.target_object_type,
        cursor_page.source_order,
        cursor_page.created_at,
        cursor_page.native_type,
        cursor_page.annotation_id,
        cursor_page.wing,
        cursor_page.hall,
        cursor_page.room,
        cursor_page.weight,
        cursor_page.note,
        cursor_page.content,
        cursor_page.hydration_cursor,
        cursor_page.hydration_chars,
        cursor_page.recent_row,
        cursor_page.recent_chars,
        'cursor' AS delivery_mode,
        1 AS evidence_expanded
    FROM cursor_page
), selected_stats AS (
    SELECT
        COUNT(*) AS selected_landmarks,
        SUM(evidence_expanded) AS expanded_landmarks,
        SUM(CASE WHEN evidence_expanded = 0 THEN 1 ELSE 0 END) AS compact_landmarks,
        SUM(CASE WHEN evidence_expanded = 1 THEN hydration_chars ELSE 0 END)
            AS expanded_chars,
        MAX(hydration_cursor) AS next_cursor
    FROM selected
), ordered AS (
    SELECT
        selected.*,
        ROW_NUMBER() OVER (ORDER BY hydration_cursor) AS output_row
    FROM selected
)
SELECT
    ordered.target_cell_id,
    ordered.target_cell_name,
    ordered.session_id,
    ordered.target_object_id,
    ordered.source_order,
    ordered.wing,
    ordered.hall,
    ordered.room,
    ordered.weight,
    substr(replace(COALESCE(ordered.note, ''), char(10), ' '), 1, 100)
        AS note_preview,
    CASE WHEN ordered.evidence_expanded = 1 THEN ordered.note END AS note,
    CASE WHEN ordered.evidence_expanded = 1 THEN ordered.content END AS content,
    CASE WHEN ordered.evidence_expanded = 1 THEN 'full' ELSE 'compact' END
        AS evidence_state,
    '@full id=' || ordered.target_object_id AS recover,
    CASE WHEN ordered.output_row = selected_stats.selected_landmarks
         THEN ordered.delivery_mode END AS delivery_mode,
    CASE WHEN ordered.output_row = selected_stats.selected_landmarks
         THEN CASE WHEN ordered.delivery_mode = 'hybrid' THEN 1 ELSE 0 END
    END AS lineage_complete,
    CASE WHEN ordered.output_row = selected_stats.selected_landmarks
         THEN CASE WHEN ordered.delivery_mode = 'cursor'
              THEN MAX(
                  (SELECT COUNT(*) FROM cursor_eligible)
                    - selected_stats.selected_landmarks,
                  0
              )
              ELSE selected_stats.compact_landmarks
         END
    END AS remaining_landmarks,
    CASE WHEN ordered.output_row = selected_stats.selected_landmarks
         THEN CASE WHEN ordered.delivery_mode = 'cursor'
                    AND (SELECT COUNT(*) FROM cursor_eligible)
                        > selected_stats.selected_landmarks
                   THEN 1 ELSE 0 END
    END AS has_more,
    CASE WHEN ordered.output_row = selected_stats.selected_landmarks THEN
        CASE
            WHEN ordered.delivery_mode = 'cursor'
             AND (SELECT COUNT(*) FROM cursor_eligible)
                 > selected_stats.selected_landmarks
            THEN '@hydrate after=' || selected_stats.next_cursor
              || ' limit=' || MIN(MAX(CAST(COALESCE(:limit, 8) AS INTEGER), 1), 100)
              || ' max_chars=' || MIN(
                    MAX(CAST(COALESCE(:max_chars, 6000) AS INTEGER), 4096),
                    200000
                 )
            WHEN ordered.delivery_mode = 'hybrid'
             AND selected_stats.compact_landmarks > 0
            THEN '@hydrate after=__start__'
              || ' limit=' || MIN(MAX(CAST(COALESCE(:limit, 8) AS INTEGER), 1), 100)
              || ' max_chars=' || MIN(
                    MAX(CAST(COALESCE(:max_chars, 6000) AS INTEGER), 4096),
                    200000
                 )
        END
    END AS next
FROM ordered
CROSS JOIN selected_stats
ORDER BY ordered.hydration_cursor;
