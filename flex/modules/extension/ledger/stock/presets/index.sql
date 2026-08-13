-- @name: index
-- @description: Navigate the derived Ledger map for the current or SQL-composed self.
-- @params: seed (default: __root__), depth (default: 1), limit (default: 40)
-- @multi: true

-- @query: identity
SELECT
    'Flex Ledger continuity index' AS representation,
    root.address AS root_address,
    'query-local' AS publication_signature,
    'annotations and provider cells remain authoritative; summaries are authored notes and structural aggregates' AS boundary,
    COUNT(DISTINCT s.selection_id) AS selected_roots,
    root.structural_child_count
FROM self() s
JOIN _ledger_index_cards root ON root.tree_depth=0
GROUP BY root.address;

-- @query: map
SELECT
    address,parent_address,position,label,index_kind,captured_role,
    CASE WHEN length(summary)>360
         THEN substr(summary,1,357) || '...'
         ELSE summary END AS summary,
    length(summary) AS summary_chars,
    native_source_address,target_cell_id,target_cell_name,target_chunk_id,
    annotation_id,revision,source_order,wing,hall,room,weight,target_readable,
    structural_child_count,outgoing_relationship_count
FROM _ledger_index_cards
WHERE COALESCE(:seed,'__root__')='__root__'
  AND captured_role<>'annotation-revision'
ORDER BY tree_depth,parent_address,position,address;

-- @query: node
WITH target AS (
    SELECT address
    FROM _ledger_index_cards
    WHERE COALESCE(:seed,'__root__')<>'__root__'
      AND (
          address=:seed
          OR (captured_role<>'annotation-revision' AND annotation_id=:seed)
          OR (captured_role<>'annotation-revision' AND target_chunk_id=:seed)
          OR (captured_role<>'annotation-revision' AND native_source_address=:seed)
      )
)
SELECT
    c.address,c.parent_address,c.position,c.label,c.index_kind,c.captured_role,
    c.summary,c.native_source_address,c.target_cell_id,c.target_cell_name,
    c.target_chunk_id,c.annotation_id,c.revision,c.source_order,c.created_at,
    c.author_provider,c.author_session_id,c.wing,c.hall,c.room,c.weight,
    c.target_readable,c.structural_child_count,c.outgoing_relationship_count
FROM _ledger_index_cards c
JOIN target USING(address)
ORDER BY c.tree_depth,c.position,c.address;

-- @query: lineage
WITH RECURSIVE target(address) AS (
    SELECT address
    FROM _ledger_index_cards
    WHERE COALESCE(:seed,'__root__')<>'__root__'
      AND (
          address=:seed
          OR (captured_role<>'annotation-revision' AND annotation_id=:seed)
          OR (captured_role<>'annotation-revision' AND target_chunk_id=:seed)
          OR (captured_role<>'annotation-revision' AND native_source_address=:seed)
      )
), ancestors(
    address,parent_address,position,label,index_kind,captured_role,summary,distance
) AS (
    SELECT
        parent.address,parent.parent_address,parent.position,parent.label,
        parent.index_kind,parent.captured_role,parent.summary,1
    FROM _ledger_index_cards child
    JOIN target ON child.address=target.address
    JOIN _ledger_index_cards parent ON parent.address=child.parent_address

    UNION ALL

    SELECT
        parent.address,parent.parent_address,parent.position,parent.label,
        parent.index_kind,parent.captured_role,parent.summary,
        ancestors.distance+1
    FROM ancestors
    JOIN _ledger_index_cards parent ON parent.address=ancestors.parent_address
    WHERE ancestors.distance<63
)
SELECT
    address,parent_address,position,label,index_kind,captured_role,
    CASE WHEN length(summary)>360
         THEN substr(summary,1,357) || '...'
         ELSE summary END AS summary,
    distance
FROM ancestors
ORDER BY distance DESC,address;

-- @query: neighborhood
WITH RECURSIVE target(address) AS (
    SELECT address
    FROM _ledger_index_cards
    WHERE COALESCE(:seed,'__root__')<>'__root__'
      AND (
          address=:seed
          OR (captured_role<>'annotation-revision' AND annotation_id=:seed)
          OR (captured_role<>'annotation-revision' AND target_chunk_id=:seed)
          OR (captured_role<>'annotation-revision' AND native_source_address=:seed)
      )
), walk(
    address,parent_address,position,label,index_kind,captured_role,summary,
    native_source_address,target_readable,distance,route
) AS (
    SELECT
        child.address,child.parent_address,child.position,child.label,
        child.index_kind,child.captured_role,child.summary,
        child.native_source_address,child.target_readable,1,
        '/' || printf('%08d',child.position)
    FROM _ledger_index_cards child
    JOIN target ON child.parent_address=target.address
    WHERE CAST(COALESCE(:depth,1) AS INTEGER)>0

    UNION ALL

    SELECT
        child.address,child.parent_address,child.position,child.label,
        child.index_kind,child.captured_role,child.summary,
        child.native_source_address,child.target_readable,walk.distance+1,
        walk.route || '/' || printf('%08d',child.position)
    FROM walk
    JOIN _ledger_index_cards child ON child.parent_address=walk.address
    WHERE walk.distance<MIN(MAX(CAST(COALESCE(:depth,1) AS INTEGER),0),12)
), ranked AS (
    SELECT
        walk.*,
        COUNT(*) OVER () AS total_rows,
        ROW_NUMBER() OVER (ORDER BY route,address) AS row_number
    FROM walk
)
SELECT
    address,parent_address,position,label,index_kind,captured_role,
    CASE WHEN length(summary)>360
         THEN substr(summary,1,357) || '...'
         ELSE summary END AS summary,
    length(summary) AS summary_chars,
    native_source_address,target_readable,distance,total_rows,
    MAX(total_rows-MIN(MAX(CAST(COALESCE(:limit,40) AS INTEGER),1),200),0)
        AS omitted_rows
FROM ranked
WHERE row_number<=MIN(MAX(CAST(COALESCE(:limit,40) AS INTEGER),1),200)
ORDER BY route,address;

-- @query: relations
WITH target(address) AS (
    SELECT address
    FROM _ledger_index_cards
    WHERE COALESCE(:seed,'__root__')<>'__root__'
      AND (
          address=:seed
          OR (captured_role<>'annotation-revision' AND annotation_id=:seed)
          OR (captured_role<>'annotation-revision' AND target_chunk_id=:seed)
          OR (captured_role<>'annotation-revision' AND native_source_address=:seed)
      )
), ranked AS (
    SELECT
        r.*,
        COUNT(*) OVER () AS total_relations,
        ROW_NUMBER() OVER (
            ORDER BY r.position,r.predicate,r.object_address
        ) AS relation_number
    FROM _ledger_index_relations r
    JOIN target ON r.subject_address=target.address
)
SELECT
    subject_address,predicate,object_address,position,evidence_anchor,
    evidence_basis,target_cell_id,target_chunk_id,total_relations,
    MAX(total_relations-MIN(MAX(CAST(COALESCE(:limit,40) AS INTEGER),1),200),0)
        AS omitted_relations
FROM ranked
WHERE relation_number<=MIN(MAX(CAST(COALESCE(:limit,40) AS INTEGER),1),200)
ORDER BY position,predicate,object_address;
