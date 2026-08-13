-- Ledger is its own commentary cell. Target cells are never altered.

CREATE TABLE IF NOT EXISTS _raw_chunks (
    id          TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    embedding   BLOB,
    timestamp   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS _meta (
    key         TEXT PRIMARY KEY,
    value       TEXT
);

CREATE TABLE IF NOT EXISTS _presets (
    name        TEXT PRIMARY KEY,
    description TEXT,
    params      TEXT DEFAULT '',
    sql         TEXT,
    source      TEXT DEFAULT 'stock'
);

CREATE TABLE IF NOT EXISTS _types_annotation (
    chunk_id    TEXT PRIMARY KEY REFERENCES _raw_chunks(id),
    wing        TEXT,
    hall        TEXT NOT NULL DEFAULT 'discoveries'
                CHECK (hall IN ('facts','events','discoveries','preferences','advice')),
    room        TEXT,
    weight      INTEGER NOT NULL DEFAULT 3 CHECK (weight BETWEEN 1 AND 5),
    author_provider   TEXT,
    author_session_id TEXT,
    author_source     TEXT,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS _edges_annotation_target (
    chunk_id        TEXT PRIMARY KEY REFERENCES _raw_chunks(id),
    target_cell_id  TEXT NOT NULL,
    target_chunk_id TEXT NOT NULL,
    UNIQUE (target_cell_id, target_chunk_id)
);

CREATE INDEX IF NOT EXISTS annotation_target_cell
    ON _edges_annotation_target(target_cell_id);
CREATE INDEX IF NOT EXISTS annotation_target_chunk
    ON _edges_annotation_target(target_chunk_id);
CREATE INDEX IF NOT EXISTS annotation_wing ON _types_annotation(wing);
CREATE INDEX IF NOT EXISTS annotation_hall ON _types_annotation(hall);
CREATE INDEX IF NOT EXISTS annotation_room ON _types_annotation(room);
CREATE INDEX IF NOT EXISTS annotation_updated ON _types_annotation(updated_at);

CREATE TABLE IF NOT EXISTS annotation_revisions (
    annotation_id     TEXT NOT NULL,
    revision          INTEGER NOT NULL,
    target_cell_id    TEXT NOT NULL,
    target_chunk_id   TEXT NOT NULL,
    note              TEXT NOT NULL,
    wing              TEXT,
    hall              TEXT NOT NULL,
    room              TEXT,
    weight            INTEGER NOT NULL,
    author_provider   TEXT,
    author_session_id TEXT,
    author_source     TEXT,
    created_at        INTEGER NOT NULL,
    preserved_at      INTEGER NOT NULL,
    operation         TEXT NOT NULL CHECK (operation IN ('superseded','removed')),
    PRIMARY KEY (annotation_id, revision)
);

CREATE INDEX IF NOT EXISTS annotation_revision_target
    ON annotation_revisions(target_cell_id, target_chunk_id);
CREATE INDEX IF NOT EXISTS annotation_revision_created
    ON annotation_revisions(created_at);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content)
    VALUES('delete', OLD.rowid, OLD.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_au
AFTER UPDATE OF content ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content)
    VALUES('delete', OLD.rowid, OLD.content);
    INSERT INTO chunks_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
END;

CREATE VIEW IF NOT EXISTS annotations AS
SELECT
    c.id AS annotation_id,
    c.content AS note,
    e.target_cell_id,
    e.target_chunk_id,
    t.wing,
    t.hall,
    t.room,
    t.weight,
    t.author_provider,
    t.author_session_id,
    t.author_source,
    c.timestamp AS created_at,
    t.updated_at,
    COALESCE((
        SELECT MAX(r.revision) + 1
        FROM annotation_revisions r
        WHERE r.annotation_id = c.id
    ), 1) AS current_revision
FROM _raw_chunks c
JOIN _types_annotation t ON t.chunk_id = c.id
JOIN _edges_annotation_target e ON e.chunk_id = c.id;

CREATE VIEW IF NOT EXISTS annotation_history AS
SELECT
    annotation_id,
    revision,
    target_cell_id,
    target_chunk_id,
    note,
    wing,
    hall,
    room,
    weight,
    author_provider,
    author_session_id,
    author_source,
    created_at,
    preserved_at,
    operation,
    0 AS is_current
FROM annotation_revisions
UNION ALL
SELECT
    annotation_id,
    current_revision AS revision,
    target_cell_id,
    target_chunk_id,
    note,
    wing,
    hall,
    room,
    weight,
    author_provider,
    author_session_id,
    author_source,
    updated_at AS created_at,
    NULL AS preserved_at,
    'current' AS operation,
    1 AS is_current
FROM annotations;

-- `annotations` is the authored SQL contract. The physical envelope remains
-- normalized, but every writer receives the same revision and FTS behavior.
CREATE TRIGGER IF NOT EXISTS annotations_insert
INSTEAD OF INSERT ON annotations BEGIN
    SELECT CASE
        WHEN NEW.target_cell_id IS NULL OR NEW.target_chunk_id IS NULL
        THEN RAISE(ABORT, 'annotation target identity is required')
        WHEN NEW.note IS NULL
        THEN RAISE(ABORT, 'annotation note is required')
    END;
    INSERT INTO _raw_chunks(id, content, timestamp)
    VALUES (
        COALESCE(
            NEW.annotation_id,
            ledger_annotation_id(NEW.target_cell_id, NEW.target_chunk_id)
        ),
        NEW.note,
        COALESCE(NEW.created_at, CAST(strftime('%s','now') AS INTEGER))
    );
    INSERT INTO _types_annotation(
        chunk_id, wing, hall, room, weight,
        author_provider, author_session_id, author_source, updated_at
    ) VALUES (
        COALESCE(
            NEW.annotation_id,
            ledger_annotation_id(NEW.target_cell_id, NEW.target_chunk_id)
        ),
        NEW.wing,
        COALESCE(NEW.hall, 'discoveries'),
        NEW.room,
        COALESCE(NEW.weight, 3),
        NEW.author_provider,
        NEW.author_session_id,
        NEW.author_source,
        COALESCE(NEW.updated_at, CAST(strftime('%s','now') AS INTEGER))
    );
    INSERT INTO _edges_annotation_target(
        chunk_id, target_cell_id, target_chunk_id
    ) VALUES (
        COALESCE(
            NEW.annotation_id,
            ledger_annotation_id(NEW.target_cell_id, NEW.target_chunk_id)
        ),
        NEW.target_cell_id,
        NEW.target_chunk_id
    );
END;

CREATE TRIGGER IF NOT EXISTS annotations_update
INSTEAD OF UPDATE ON annotations
WHEN NEW.note IS NOT OLD.note
  OR NEW.wing IS NOT OLD.wing
  OR NEW.hall IS NOT OLD.hall
  OR NEW.room IS NOT OLD.room
  OR NEW.weight IS NOT OLD.weight
  OR NEW.author_provider IS NOT OLD.author_provider
  OR NEW.author_session_id IS NOT OLD.author_session_id
  OR NEW.author_source IS NOT OLD.author_source
BEGIN
    SELECT CASE
        WHEN NEW.annotation_id IS NOT OLD.annotation_id
          OR NEW.target_cell_id IS NOT OLD.target_cell_id
          OR NEW.target_chunk_id IS NOT OLD.target_chunk_id
        THEN RAISE(ABORT, 'annotation identity and target are immutable')
        WHEN NEW.note IS NULL
        THEN RAISE(ABORT, 'annotation note is required')
    END;
    INSERT INTO annotation_revisions(
        annotation_id, revision, target_cell_id, target_chunk_id, note,
        wing, hall, room, weight, author_provider, author_session_id,
        author_source, created_at, preserved_at, operation
    ) VALUES (
        OLD.annotation_id, OLD.current_revision,
        OLD.target_cell_id, OLD.target_chunk_id, OLD.note,
        OLD.wing, OLD.hall, OLD.room, OLD.weight,
        OLD.author_provider, OLD.author_session_id, OLD.author_source,
        OLD.updated_at, CAST(strftime('%s','now') AS INTEGER), 'superseded'
    );
    UPDATE _raw_chunks
       SET content = NEW.note
     WHERE id = OLD.annotation_id
       AND content IS NOT NEW.note;
    UPDATE _types_annotation
       SET wing = NEW.wing,
           hall = COALESCE(NEW.hall, 'discoveries'),
           room = NEW.room,
           weight = COALESCE(NEW.weight, 3),
           author_provider = NEW.author_provider,
           author_session_id = NEW.author_session_id,
           author_source = NEW.author_source,
           updated_at = CASE
               WHEN NEW.updated_at IS OLD.updated_at
               THEN CAST(strftime('%s','now') AS INTEGER)
               ELSE NEW.updated_at
           END
     WHERE chunk_id = OLD.annotation_id;
END;

CREATE TRIGGER IF NOT EXISTS annotations_delete
INSTEAD OF DELETE ON annotations BEGIN
    INSERT INTO annotation_revisions(
        annotation_id, revision, target_cell_id, target_chunk_id, note,
        wing, hall, room, weight, author_provider, author_session_id,
        author_source, created_at, preserved_at, operation
    ) VALUES (
        OLD.annotation_id, OLD.current_revision,
        OLD.target_cell_id, OLD.target_chunk_id, OLD.note,
        OLD.wing, OLD.hall, OLD.room, OLD.weight,
        OLD.author_provider, OLD.author_session_id, OLD.author_source,
        OLD.updated_at, CAST(strftime('%s','now') AS INTEGER), 'removed'
    );
    DELETE FROM _edges_annotation_target WHERE chunk_id = OLD.annotation_id;
    DELETE FROM _types_annotation WHERE chunk_id = OLD.annotation_id;
    DELETE FROM _raw_chunks WHERE id = OLD.annotation_id;
END;
