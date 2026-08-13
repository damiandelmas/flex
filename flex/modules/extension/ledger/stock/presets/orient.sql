-- @name: orient
-- @description: Relational orientation for the Ledger annotation cell.
-- @multi: true

-- @query: about
SELECT key, value
FROM _meta
WHERE key IN (
    'cell_type', 'description', 'schema', 'ledger_schema_version',
    'ledger_presets_version', 'embed', 'lifecycle'
)
ORDER BY key;

-- @query: capabilities
SELECT 'sql' AS capability, 'available' AS status,
       'Compose directly against annotations and annotation_history.' AS detail
UNION ALL
SELECT 'keyword',
       CASE WHEN EXISTS (
           SELECT 1 FROM sqlite_master
           WHERE type = 'table' AND name = 'chunks_fts'
       ) THEN 'available' ELSE 'unavailable' END,
       'Use keyword(term) as a table source and join k.id to annotations.annotation_id.'
UNION ALL
SELECT 'semantic', 'unavailable',
       'Ledger does not embed commentary; use keyword() and relational SQL.'
UNION ALL
SELECT 'revisions', 'available',
       'annotation_history contains prior snapshots and the current annotation.'
UNION ALL
SELECT 'index', 'available',
       '@index derives a compact map, node, lineage, neighborhood, and relations over a materialized self.'
UNION ALL
SELECT 'mutations', 'available',
       'INSERT, UPDATE, and DELETE the annotations relation directly; Ledger preserves revisions and FTS.';

-- @query: shape
SELECT
    COUNT(*) AS annotations,
    (SELECT COUNT(*) FROM annotation_revisions) AS prior_revisions,
    COUNT(DISTINCT target_cell_id) AS target_cells,
    COUNT(DISTINCT wing) AS wings,
    COUNT(DISTINCT room) AS rooms,
    MIN(datetime(updated_at, 'unixepoch', 'localtime')) AS first_update,
    MAX(datetime(updated_at, 'unixepoch', 'localtime')) AS last_update,
    ROUND(AVG(length(note)), 1) AS avg_note_chars,
    MIN(length(note)) AS min_note_chars,
    MAX(length(note)) AS max_note_chars
FROM annotations;

-- @query: target_cells
SELECT
    target_cell_id,
    COUNT(*) AS annotations,
    COUNT(DISTINCT wing) AS wings,
    COUNT(DISTINCT room) AS rooms,
    ROUND(AVG(weight), 2) AS avg_weight,
    datetime(MAX(updated_at), 'unixepoch', 'localtime') AS latest
FROM annotations
GROUP BY target_cell_id
ORDER BY annotations DESC, target_cell_id;

-- @query: halls
SELECT
    hall,
    COUNT(*) AS annotations,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
    ROUND(AVG(weight), 2) AS avg_weight,
    ROUND(AVG(length(note)), 1) AS avg_note_chars,
    datetime(MAX(updated_at), 'unixepoch', 'localtime') AS latest
FROM annotations
GROUP BY hall
ORDER BY annotations DESC, hall;

-- @query: wings
SELECT
    COALESCE(wing, '(none)') AS wing,
    COUNT(*) AS annotations,
    COUNT(DISTINCT room) AS rooms,
    COUNT(DISTINCT hall) AS halls,
    ROUND(AVG(weight), 2) AS avg_weight,
    datetime(MAX(updated_at), 'unixepoch', 'localtime') AS latest
FROM annotations
GROUP BY wing
ORDER BY annotations DESC, wing
LIMIT 25;

-- @query: calibration
WITH populations AS (
    SELECT
        COUNT(*) AS annotations,
        COUNT(DISTINCT room) AS rooms
    FROM annotations
), measures AS (
    SELECT
        'weight' AS metric,
        CAST(weight AS TEXT) AS bucket,
        COUNT(*) AS observed,
        (SELECT annotations FROM populations) AS population
    FROM annotations
    GROUP BY weight

    UNION ALL

    SELECT
        'placement', 'wing missing', COUNT(*),
        (SELECT annotations FROM populations)
    FROM annotations
    WHERE wing IS NULL

    UNION ALL

    SELECT
        'placement', 'room missing', COUNT(*),
        (SELECT annotations FROM populations)
    FROM annotations
    WHERE room IS NULL

    UNION ALL

    SELECT
        'note_length', 'over 1000 chars', COUNT(*),
        (SELECT annotations FROM populations)
    FROM annotations
    WHERE length(note) > 1000

    UNION ALL

    SELECT
        'room_density', 'rooms with 10+ annotations', COUNT(*),
        (SELECT rooms FROM populations)
    FROM (
        SELECT room
        FROM annotations
        WHERE room IS NOT NULL
        GROUP BY room
        HAVING COUNT(*) >= 10
    )
)
SELECT
    metric,
    bucket,
    observed,
    population,
    ROUND(100.0 * observed / NULLIF(population, 0), 1) AS pct
FROM measures
ORDER BY metric, bucket;

-- @query: query_contract
SELECT 'structured filtering' AS purpose,
       'SELECT * FROM annotations WHERE wing = :wing AND weight >= :weight ORDER BY updated_at DESC' AS sql
UNION ALL
SELECT 'keyword plus SQL',
       'SELECT a.*, k.rank FROM keyword(:term) k JOIN annotations a ON a.annotation_id = k.id ORDER BY k.rank DESC'
UNION ALL
SELECT 'version history',
       'SELECT * FROM annotation_history WHERE annotation_id = :annotation_id ORDER BY revision'
UNION ALL
SELECT 'exact target coordinate',
       'SELECT target_cell_id, target_chunk_id FROM annotations WHERE annotation_id = :annotation_id'
UNION ALL
SELECT 'navigate current self',
       '@index'
UNION ALL
SELECT 'continuity recovery',
       '@hydrate returns complete compact lineage plus budgeted newest full evidence; its terminal row carries the receipt and executable next query for optional older bodies';

-- @query: mutation_contract
SELECT 'add' AS operation,
       'INSERT INTO annotations(annotation_id,note,target_cell_id,target_chunk_id,wing,hall,room,weight,author_provider,author_session_id,author_source) VALUES(ledger_annotation_id(:target_cell_id,:target_chunk_id),:note,:target_cell_id,:target_chunk_id,:wing,:hall,:room,:weight,ledger_author_provider(),ledger_author_session_id(),ledger_author_source()) RETURNING annotation_id,target_cell_id,target_chunk_id' AS sql
UNION ALL
SELECT 'revise',
       'UPDATE annotations SET note=:note,wing=:wing,hall=:hall,room=:room,weight=:weight,author_provider=ledger_author_provider(),author_session_id=ledger_author_session_id(),author_source=ledger_author_source() WHERE target_cell_id=:target_cell_id AND target_chunk_id=:target_chunk_id RETURNING annotation_id'
UNION ALL
SELECT 'remove',
       'DELETE FROM annotations WHERE target_cell_id=:target_cell_id AND target_chunk_id=:target_chunk_id RETURNING annotation_id,target_cell_id,target_chunk_id';

-- @query: operations
SELECT
    operation,
    COUNT(*) AS operations,
    datetime(MIN(timestamp), 'unixepoch', 'localtime') AS first_at,
    datetime(MAX(timestamp), 'unixepoch', 'localtime') AS last_at
FROM _ops
GROUP BY operation
ORDER BY operations DESC, operation;

-- @query: presets
SELECT name, description, params FROM _presets ORDER BY name;
