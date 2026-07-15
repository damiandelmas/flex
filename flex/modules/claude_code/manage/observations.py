"""Indexed one-row projection for coding-agent file observations."""

from __future__ import annotations

import os
import sqlite3


DDL = """
CREATE TABLE IF NOT EXISTS _enrich_observations (
    chunk_id TEXT PRIMARY KEY,
    session_id TEXT,
    position INTEGER,
    timestamp INTEGER,
    tool_name TEXT,
    target_file TEXT,
    normalized_path TEXT,
    path_basename TEXT,
    file_uuid TEXT,
    operation_type TEXT,
    cwd TEXT,
    message_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_observations_session_position
    ON _enrich_observations(session_id, position DESC);
CREATE INDEX IF NOT EXISTS idx_observations_path
    ON _enrich_observations(normalized_path);
CREATE INDEX IF NOT EXISTS idx_observations_basename
    ON _enrich_observations(path_basename);
CREATE INDEX IF NOT EXISTS idx_observations_uuid
    ON _enrich_observations(file_uuid);
CREATE INDEX IF NOT EXISTS idx_observations_time
    ON _enrich_observations(timestamp);
CREATE INDEX IF NOT EXISTS idx_soft_ops_chunk
    ON _edges_soft_ops(chunk_id);
"""


def ensure_observation_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)


def _normalize_path(path: str | None, cwd: str | None) -> tuple[str | None, str | None]:
    if not path:
        return None, None
    value = os.path.expanduser(str(path).strip())
    if not os.path.isabs(value) and cwd:
        value = os.path.join(cwd, value)
    value = os.path.normpath(value).replace(os.sep, '/')
    return value, value.rsplit('/', 1)[-1]


def upsert_observation(conn: sqlite3.Connection, chunk_id: str) -> bool:
    """Re-derive one observation after all late tool/SOMA edges are present."""
    has_identity = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_edges_file_identity'"
    ).fetchone() is not None
    identity_expr = (
        "(SELECT file_uuid FROM _edges_file_identity fi "
        " WHERE fi.chunk_id=r.id ORDER BY fi.file_uuid LIMIT 1)"
        if has_identity else "NULL"
    )
    row = conn.execute(
        f"""
        SELECT r.id, es.source_id, es.position, r.timestamp,
               t.tool_name, t.target_file, t.cwd, tp.type,
               (SELECT file_path FROM _edges_soft_ops so
                WHERE so.chunk_id=r.id ORDER BY so.id LIMIT 1) AS soft_path,
               {identity_expr} AS file_uuid
        FROM _raw_chunks r
        LEFT JOIN _edges_source es ON es.chunk_id=r.id
        LEFT JOIN _edges_tool_ops t ON t.chunk_id=r.id
        LEFT JOIN _types_message tp ON tp.chunk_id=r.id
        WHERE r.id=?
        LIMIT 1
        """,
        (chunk_id,),
    ).fetchone()
    if not row:
        return False
    (_id, session_id, position, timestamp, tool_name, target_file, cwd,
     message_type, soft_path, file_uuid) = row
    path = target_file or soft_path
    normalized, basename = _normalize_path(path, cwd)
    if not any((tool_name, normalized, file_uuid)):
        conn.execute("DELETE FROM _enrich_observations WHERE chunk_id=?", (chunk_id,))
        return False
    operation = {
        'Write': 'mutation', 'Edit': 'mutation', 'MultiEdit': 'mutation',
        'Read': 'read', 'Bash': 'stdout_observation',
    }.get(tool_name, 'target_file' if target_file else 'observation')
    conn.execute(
        """
        INSERT INTO _enrich_observations
          (chunk_id,session_id,position,timestamp,tool_name,target_file,
           normalized_path,path_basename,file_uuid,operation_type,cwd,message_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(chunk_id) DO UPDATE SET
          session_id=excluded.session_id, position=excluded.position,
          timestamp=excluded.timestamp, tool_name=excluded.tool_name,
          target_file=excluded.target_file, normalized_path=excluded.normalized_path,
          path_basename=excluded.path_basename, file_uuid=excluded.file_uuid,
          operation_type=excluded.operation_type, cwd=excluded.cwd,
          message_type=excluded.message_type
        """,
        (chunk_id, session_id, position, timestamp, tool_name, target_file,
         normalized, basename, file_uuid, operation, cwd, message_type),
    )
    return True


def rebuild_observations(conn: sqlite3.Connection) -> int:
    """Idempotently backfill the projection from canonical edge tables."""
    ensure_observation_schema(conn)
    has_identity = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_edges_file_identity'"
    ).fetchone() is not None
    unions = ["SELECT chunk_id FROM _edges_tool_ops",
              "SELECT chunk_id FROM _edges_soft_ops"]
    if has_identity:
        unions.append("SELECT chunk_id FROM _edges_file_identity")
    candidates = " UNION ".join(unions)
    identity_expr = (
        "(SELECT file_uuid FROM _edges_file_identity fi "
        " WHERE fi.chunk_id=r.id ORDER BY fi.file_uuid LIMIT 1)"
        if has_identity else "NULL"
    )
    conn.create_function(
        "flex_normalize_path", 2,
        lambda path, cwd: _normalize_path(path, cwd)[0],
        deterministic=True,
    )
    conn.create_function(
        "flex_path_basename", 2,
        lambda path, cwd: _normalize_path(path, cwd)[1],
        deterministic=True,
    )
    conn.execute("DELETE FROM _enrich_observations")
    conn.execute(f"""
        WITH candidates AS ({candidates}), projected AS (
            SELECT r.id AS chunk_id, es.source_id AS session_id,
                   es.position, r.timestamp, t.tool_name, t.target_file,
                   t.cwd, tp.type AS message_type,
                   COALESCE(t.target_file,
                     (SELECT file_path FROM _edges_soft_ops so
                      WHERE so.chunk_id=r.id ORDER BY so.id LIMIT 1)) AS observed_path,
                   {identity_expr} AS file_uuid
            FROM candidates c
            JOIN _raw_chunks r ON r.id=c.chunk_id
            LEFT JOIN _edges_source es ON es.chunk_id=r.id
            LEFT JOIN _edges_tool_ops t ON t.chunk_id=r.id
            LEFT JOIN _types_message tp ON tp.chunk_id=r.id
        )
        INSERT INTO _enrich_observations
          (chunk_id,session_id,position,timestamp,tool_name,target_file,
           normalized_path,path_basename,file_uuid,operation_type,cwd,message_type)
        SELECT chunk_id,session_id,position,timestamp,tool_name,target_file,
               flex_normalize_path(observed_path,cwd),
               flex_path_basename(observed_path,cwd), file_uuid,
               CASE
                 WHEN tool_name IN ('Write','Edit','MultiEdit') THEN 'mutation'
                 WHEN tool_name='Read' THEN 'read'
                 WHEN tool_name='Bash' THEN 'stdout_observation'
                 WHEN target_file IS NOT NULL THEN 'target_file'
                 ELSE 'observation'
               END,
               cwd,message_type
        FROM projected
        WHERE tool_name IS NOT NULL OR observed_path IS NOT NULL OR file_uuid IS NOT NULL
    """)
    written = conn.execute("SELECT COUNT(*) FROM _enrich_observations").fetchone()[0]
    conn.commit()
    return written
