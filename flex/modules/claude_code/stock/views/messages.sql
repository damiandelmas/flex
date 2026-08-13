-- @name: messages
-- @description: Chunk-level surface for claude_code cells. Tool ops, message type, delegation edges. Session title/message_count as breadcrumbs.
-- id: unique per message. Format: {session_id}_{line_num}. vec_ops JOIN: JOIN chunks c ON v.id = c.id (or JOIN messages m for message-specific columns)
-- session_id: the Claude Code session UUID. Same as sessions.session_id.
-- child_session_id: non-NULL on Task chunks — follow to sessions for the spawned agent.
-- file_uuids: JSON array of SOMA file UUIDs. COALESCE(json_extract(file_uuids, '$[0]'), target_file) for rename-safe dedup.
-- target_file: path of file operated on. NULL for non-tool messages. Use WHERE target_file LIKE '%name%' to find all sessions that touched a file.
-- content: tool call signature only (e.g. "Write /path/to/file"). NOT the file body.
-- file_body: actual file content for Write/Edit/Read/Bash chunks. NULL for non-file messages.
--            For BM25 search across file bodies use @file-search preset (faster than LIKE).
--            For inline inspection: WHERE file_body LIKE '%pattern%' AND tool_name IN ('Write', 'Edit')
-- exclude_paths: _meta key (JSON array of LIKE patterns) filters target_file. Chunks with NULL target_file pass through.

DROP VIEW IF EXISTS messages;
CREATE VIEW messages AS
SELECT
    r.id,
    r.content,
    r.timestamp,
    datetime(r.timestamp, 'unixepoch', 'localtime') AS created_at,
    s.source_id AS session_id,
    s.position AS position,
    src.project,
    src.title,
    src.message_count,
    t.tool_name,
    t.target_file,
    t.success,
    t.cwd,
    tp.type,
    cr.child_session_id,
    cr.agent_type,
    cr.file_uuids,
    (
        -- One chunk may carry several bodies, labelled by _edges_raw_content.role:
        --   input = tool call args (JSON) | output = the payload | status = *_end exit info
        -- file_body is the OUTPUT. NULL means no output was captured (see tool_input).
        SELECT rc.content
        FROM _edges_raw_content erc
        JOIN _raw_content rc ON rc.hash = erc.content_hash
        WHERE erc.chunk_id = r.id
          AND (
                erc.role = 'output'
             OR erc.role IS NULL
             OR erc.role NOT IN ('input','status')
          )
        ORDER BY
            CASE
                WHEN erc.role = 'output' THEN 0
                WHEN erc.role IS NULL THEN 1
                WHEN erc.role NOT IN ('input','status','backup') THEN 2
                WHEN erc.role = 'backup' THEN 3
                ELSE 4
            END,
            erc.ordinal DESC,
            erc.content_hash
        LIMIT 1
    ) AS file_body,
    tp.branch_id,
    (
        SELECT rc.content
        FROM _edges_raw_content erc
        JOIN _raw_content rc ON rc.hash = erc.content_hash
        WHERE erc.chunk_id = r.id AND erc.role = 'input'
        ORDER BY erc.ordinal, erc.content_hash
        LIMIT 1
    ) AS tool_input,
    (
        SELECT rc.content
        FROM _edges_raw_content erc
        JOIN _raw_content rc ON rc.hash = erc.content_hash
        WHERE erc.chunk_id = r.id AND erc.role = 'status'
        ORDER BY erc.ordinal DESC, erc.content_hash
        LIMIT 1
    ) AS exit_status
FROM _raw_chunks r
LEFT JOIN _edges_source s ON r.id = s.chunk_id
LEFT JOIN _raw_sources src ON s.source_id = src.source_id
LEFT JOIN _edges_tool_ops t ON r.id = t.chunk_id
LEFT JOIN _types_message tp ON r.id = tp.chunk_id
LEFT JOIN _enrich_chunk_rollup cr ON r.id = cr.chunk_id
WHERE NOT EXISTS (
    SELECT 1 FROM _meta m, json_each(m.value) j
    WHERE m.key = 'exclude_paths'
      AND t.target_file LIKE '%' || j.value || '%'
);
