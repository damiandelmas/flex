-- @name: self-orient
-- @description: Content-free query contract for the current session-seeded Ledger scope.
-- @multi: true

-- @query: seed
SELECT
    s.target_cell_id,
    s.target_cell_name,
    s.target_object_id,
    s.target_object_type,
    s.source_order,
    s.selection_reason,
    COUNT(DISTINCT c.target_object_id) AS addressable_objects,
    COUNT(DISTINCT a.annotation_id) AS ledger_landmarks
FROM self() s
LEFT JOIN _flex_self_content c
  ON c.selection_id = s.selection_id
LEFT JOIN annotations a
  ON a.target_cell_id = c.target_cell_id
 AND a.target_chunk_id = c.target_object_id
GROUP BY s.selection_id
ORDER BY s.source_order;

-- @query: topology
SELECT
    c.target_cell_id,
    c.target_cell_name,
    c.target_object_type,
    COUNT(DISTINCT c.parent_object_id) AS parent_objects,
    COUNT(DISTINCT c.target_object_id) AS addressable_objects,
    COUNT(DISTINCT a.annotation_id) AS ledger_landmarks,
    MIN(c.source_order) AS first_source_order,
    MAX(c.source_order) AS last_source_order
FROM _flex_self_content c
LEFT JOIN annotations a
  ON a.target_cell_id = c.target_cell_id
 AND a.target_chunk_id = c.target_object_id
GROUP BY c.target_cell_id, c.target_cell_name, c.target_object_type
ORDER BY c.target_cell_name, c.target_object_type;

-- @query: materialized_cells
SELECT
    target_cell_id,
    target_cell_name,
    cell_alias,
    COUNT(*) AS selected_objects,
    GROUP_CONCAT(DISTINCT target_object_type) AS selected_types
FROM _flex_self_objects
GROUP BY target_cell_id, target_cell_name, cell_alias
ORDER BY target_cell_name;

-- @query: query_contract
SELECT 'hydrate current seed' AS purpose,
       '@hydrate; compact lineage is complete in one call, and its terminal next query enters exhaustive full-evidence cursor mode' AS sql
UNION ALL
SELECT 'navigate current self',
       '@index'
UNION ALL
SELECT 'navigate one landmark',
       '@index seed=:annotation_id depth=1 limit=40'
UNION ALL
SELECT 'global Ledger contract',
       '@orient global'
UNION ALL
SELECT 'expand self through SQL',
       'SELECT * FROM self(''SELECT target_cell_id, target_object_id, target_object_type, source_order, selection_reason FROM ...'')'
UNION ALL
SELECT 'hydrate an expanded self',
       'Join self(...) to _flex_self_content and annotations on exact target_cell_id + target_object_id.'
UNION ALL
SELECT 'index an expanded self',
       'Join self(''SELECT ...'') to _ledger_index_cards; the derived cards and relations use the same selected roots.';

-- @query: mutation_contract
SELECT 'add' AS operation,
       'INSERT INTO annotations(annotation_id,note,target_cell_id,target_chunk_id,wing,hall,room,weight,author_provider,author_session_id,author_source) VALUES(ledger_annotation_id(:target_cell_id,:target_chunk_id),:note,:target_cell_id,:target_chunk_id,:wing,:hall,:room,:weight,ledger_author_provider(),ledger_author_session_id(),ledger_author_source()) RETURNING annotation_id,target_cell_id,target_chunk_id' AS sql
UNION ALL
SELECT 'revise',
       'UPDATE annotations SET note=:note,wing=:wing,hall=:hall,room=:room,weight=:weight,author_provider=ledger_author_provider(),author_session_id=ledger_author_session_id(),author_source=ledger_author_source() WHERE target_cell_id=:target_cell_id AND target_chunk_id=:target_chunk_id RETURNING annotation_id'
UNION ALL
SELECT 'remove',
       'DELETE FROM annotations WHERE target_cell_id=:target_cell_id AND target_chunk_id=:target_chunk_id RETURNING annotation_id,target_cell_id,target_chunk_id';

-- @query: limitations
SELECT 'persistence' AS boundary,
       'Self membership exists only for this query connection.' AS detail
UNION ALL
SELECT 'identity',
       'Cells and objects retain provider-native durable identities.'
UNION ALL
SELECT 'semantic',
       'Self materialization does not provide cross-cell vector dispatch.'
UNION ALL
SELECT 'index',
       '@index is a query-local navigation projection; it stores no independent summaries or authority.'
UNION ALL
SELECT 'authority',
       'Attached target cells remain read-only; the primary Ledger annotations relation accepts its declared INSERT, UPDATE, and DELETE contract.';
