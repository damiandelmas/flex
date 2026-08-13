-- @name: files
-- @description: File body sub-chunks — searchable document content from Write operations. Each row is a section/function/block extracted from a written file.
-- id: format {session_id}_{line_num}:fb:{position}. vec_ops JOIN: JOIN files f ON v.id = f.id
-- file: absolute path of the written file.
-- session_id: the Claude Code session that wrote this file.
-- title: section heading (markdown) or function/class name (python). Empty for whole-file chunks.
-- content: actual file section content — what gets embedded and searched.
-- ext: file extension (md, py, sql, etc). Use for scoping: WHERE ext = 'py'
-- Pre-filter for vec_ops: 'SELECT id FROM files' or 'SELECT id FROM files WHERE ext = ''md'''

DROP VIEW IF EXISTS files;
CREATE VIEW files AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch', 'localtime') AS created_at,
    s.source_id AS session_id,
    fb.target_file AS file,
    fb.title,
    fb.position AS chunk_position,
    CASE
        WHEN fb.target_file LIKE '%.%'
        THEN LOWER(SUBSTR(fb.target_file, LENGTH(RTRIM(fb.target_file, REPLACE(REPLACE(fb.target_file, '/', ''), '.', ''))) + 1))
        ELSE ''
    END AS ext
FROM _raw_chunks r
JOIN _types_file_body fb ON r.id = fb.chunk_id
LEFT JOIN _edges_source s ON r.id = s.chunk_id
;
