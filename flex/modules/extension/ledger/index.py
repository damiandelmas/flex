"""Query-local Index projection for the Ledger continuity surface."""

from __future__ import annotations

import json
import re
import sqlite3

from flex.retrieve.flexpresets import _strip_sql


CARDS = "_ledger_index_cards"
RELATIONS = "_ledger_index_relations"
STATE = "_ledger_index_state"
_MARKERS = (CARDS, RELATIONS)


def _references_index(sql: str) -> bool:
    """Return true only for relation references, not prose in SQL literals."""
    skeleton = _strip_sql(sql)
    return any(
        re.search(rf"(?<![A-Za-z0-9_]){re.escape(marker)}(?![A-Za-z0-9_])", skeleton)
        for marker in _MARKERS
    )


def _relation_exists(db: sqlite3.Connection, schema: str, name: str) -> bool:
    return db.execute(
        f"SELECT 1 FROM {schema}.sqlite_master "
        "WHERE name=? AND type IN ('table','view') LIMIT 1",
        (name,),
    ).fetchone() is not None


def _scope_signature(db: sqlite3.Connection) -> str:
    rows = db.execute(
        "SELECT target_cell_id,target_object_id,target_object_type,"
        "source_order,selection_reason FROM _flex_self_objects "
        "ORDER BY selection_id"
    ).fetchall()
    return json.dumps([tuple(row) for row in rows], separators=(",", ":"))


def _install_cards(db: sqlite3.Connection, signature: str) -> None:
    db.executescript(f"""
        DROP TABLE IF EXISTS temp.{STATE};
        DROP TABLE IF EXISTS temp.{RELATIONS};
        DROP TABLE IF EXISTS temp.{CARDS};
        DROP TABLE IF EXISTS temp._ledger_index_membership;

        CREATE TEMP TABLE _ledger_index_membership AS
        SELECT DISTINCT
            c.selection_id,
            c.target_cell_id,
            c.target_cell_name,
            c.parent_object_id,
            c.target_object_id,
            c.source_order,
            c.native_type,
            CASE WHEN c.content IS NULL THEN 0 ELSE 1 END AS target_readable
        FROM _flex_self_content c
        JOIN annotations a
          ON a.target_cell_id=c.target_cell_id
         AND a.target_chunk_id=c.target_object_id

        UNION ALL

        SELECT
            s.selection_id,
            s.target_cell_id,
            s.target_cell_name,
            NULL,
            s.target_object_id,
            s.source_order,
            NULL,
            0
        FROM _flex_self_objects s
        JOIN annotations a
          ON s.target_object_type='object'
         AND a.target_cell_id=s.target_cell_id
         AND a.target_chunk_id=s.target_object_id
        WHERE NOT EXISTS (
            SELECT 1 FROM _flex_self_content c
            WHERE c.selection_id=s.selection_id
              AND c.target_cell_id=s.target_cell_id
              AND c.target_object_id=s.target_object_id
        );

        CREATE TEMP TABLE {CARDS} (
            address TEXT PRIMARY KEY,
            parent_address TEXT,
            position INTEGER,
            label TEXT NOT NULL,
            index_kind TEXT NOT NULL,
            captured_role TEXT NOT NULL,
            summary TEXT,
            native_source_address TEXT,
            target_cell_id TEXT,
            target_cell_name TEXT,
            target_chunk_id TEXT,
            annotation_id TEXT,
            revision INTEGER,
            source_order INTEGER,
            created_at,
            author_provider TEXT,
            author_session_id TEXT,
            wing TEXT,
            hall TEXT,
            room TEXT,
            weight INTEGER,
            target_readable INTEGER NOT NULL DEFAULT 1,
            tree_depth INTEGER NOT NULL,
            structural_child_count INTEGER NOT NULL DEFAULT 0,
            outgoing_relationship_count INTEGER NOT NULL DEFAULT 0
        );

        INSERT INTO {CARDS}(
            address,parent_address,position,label,index_kind,captured_role,
            summary,native_source_address,target_cell_id,target_cell_name,
            target_chunk_id,source_order,target_readable,tree_depth
        )
        SELECT
            'self://' || r.seed_cell_id || '/' || r.seed_session_id,
            NULL,
            0,
            r.seed_cell_name || ' current self',
            'container',
            'self',
            printf(
                '%d selected root(s), %d addressable object(s), %d Ledger landmark(s).',
                (SELECT COUNT(*) FROM _flex_self_objects),
                (SELECT COUNT(*) FROM _flex_self_content),
                (SELECT COUNT(DISTINCT annotation_id) FROM annotations a
                 JOIN _ledger_index_membership m
                   ON m.target_cell_id=a.target_cell_id
                  AND m.target_object_id=a.target_chunk_id)
            ),
            r.seed_cell_name || ':' || r.seed_session_id,
            r.seed_cell_id,
            r.seed_cell_name,
            r.seed_session_id,
            0,
            1,
            0
        FROM _flex_runtime r;

        INSERT OR IGNORE INTO {CARDS}(
            address,parent_address,position,label,index_kind,captured_role,
            summary,native_source_address,target_cell_id,target_cell_name,
            target_chunk_id,source_order,target_readable,tree_depth
        )
        SELECT
            CASE s.target_object_type
              WHEN 'session' THEN 'session://'
              ELSE 'object://'
            END || s.target_cell_id || '/' || s.target_object_id,
            (SELECT address FROM {CARDS} WHERE tree_depth=0),
            s.source_order,
            s.target_cell_name || ' ' || s.target_object_type || ' ' || s.target_object_id,
            'container',
            s.target_object_type,
            printf(
                '%d addressable object(s), %d Ledger landmark(s); selected by %s.',
                COUNT(DISTINCT c.target_object_id),
                COUNT(DISTINCT a.annotation_id),
                s.selection_reason
            ),
            s.target_cell_name || ':' || s.target_object_id,
            s.target_cell_id,
            s.target_cell_name,
            s.target_object_id,
            s.source_order,
            CASE WHEN COUNT(c.target_object_id)>0 THEN 1 ELSE 0 END,
            1
        FROM _flex_self_objects s
        LEFT JOIN _flex_self_content c ON c.selection_id=s.selection_id
        LEFT JOIN annotations a
          ON a.target_cell_id=c.target_cell_id
         AND a.target_chunk_id=c.target_object_id
        GROUP BY s.selection_id;

        INSERT OR IGNORE INTO {CARDS}(
            address,parent_address,position,label,index_kind,captured_role,
            summary,native_source_address,target_cell_id,target_cell_name,
            target_chunk_id,annotation_id,revision,source_order,created_at,
            author_provider,author_session_id,wing,hall,room,weight,
            target_readable,tree_depth
        )
        WITH ranked AS (
            SELECT
                m.*,
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.annotation_id
                    ORDER BY m.selection_id,m.source_order
                ) AS chosen
            FROM _ledger_index_membership m
            JOIN annotations a
              ON a.target_cell_id=m.target_cell_id
             AND a.target_chunk_id=m.target_object_id
        )
        SELECT
            'ledger://annotation/' || annotation_id,
            CASE (SELECT target_object_type FROM _flex_self_objects s
                  WHERE s.selection_id=ranked.selection_id)
              WHEN 'session' THEN 'session://'
              ELSE 'object://'
            END || target_cell_id || '/' ||
                (SELECT target_object_id FROM _flex_self_objects s
                 WHERE s.selection_id=ranked.selection_id),
            source_order,
            COALESCE(NULLIF(room,''),NULLIF(wing,''),hall) || ' · ' ||
                substr(replace(note,char(10),' '),1,80),
            CASE WHEN EXISTS (
                SELECT 1 FROM annotation_revisions r
                WHERE r.annotation_id=ranked.annotation_id
            ) THEN 'container' ELSE 'leaf' END,
            'annotation',
            note,
            target_cell_name || ':' || target_chunk_id,
            target_cell_id,
            target_cell_name,
            target_chunk_id,
            annotation_id,
            current_revision,
            source_order,
            updated_at,
            author_provider,
            author_session_id,
            wing,hall,room,weight,
            target_readable,
            2
        FROM ranked
        WHERE chosen=1;

        INSERT OR IGNORE INTO {CARDS}(
            address,parent_address,position,label,index_kind,captured_role,
            summary,native_source_address,target_cell_id,target_cell_name,
            target_chunk_id,annotation_id,revision,source_order,created_at,
            author_provider,author_session_id,wing,hall,room,weight,
            target_readable,tree_depth
        )
        SELECT
            'ledger://annotation/' || h.annotation_id || '/revision/' || h.revision,
            'ledger://annotation/' || h.annotation_id,
            h.revision,
            'revision ' || h.revision || ' · ' || h.operation,
            'leaf',
            'annotation-revision',
            h.note,
            parent.native_source_address,
            h.target_cell_id,
            parent.target_cell_name,
            h.target_chunk_id,
            h.annotation_id,
            h.revision,
            parent.source_order,
            h.created_at,
            h.author_provider,
            h.author_session_id,
            h.wing,h.hall,h.room,h.weight,
            parent.target_readable,
            3
        FROM annotation_history h
        JOIN {CARDS} parent
          ON parent.annotation_id=h.annotation_id
         AND parent.captured_role='annotation'
        WHERE h.is_current=0;

        CREATE TEMP TABLE {RELATIONS} (
            subject_address TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object_address TEXT NOT NULL,
            position INTEGER,
            evidence_anchor TEXT,
            evidence_basis TEXT NOT NULL,
            target_cell_id TEXT,
            target_chunk_id TEXT
        );

        INSERT INTO {RELATIONS}
        SELECT parent_address,'contains',address,position,label,'derived',
               target_cell_id,target_chunk_id
        FROM {CARDS}
        WHERE parent_address IS NOT NULL;

        INSERT INTO {RELATIONS}
        SELECT address,'annotates',
               'flex://object/' || target_cell_id || '/' || target_chunk_id,
               source_order,target_chunk_id,'exact',target_cell_id,target_chunk_id
        FROM {CARDS}
        WHERE captured_role='annotation';

        INSERT INTO {RELATIONS}
        SELECT address,'version_of',parent_address,revision,label,'exact',
               target_cell_id,target_chunk_id
        FROM {CARDS}
        WHERE captured_role='annotation-revision';

        INSERT INTO {RELATIONS}
        SELECT newer.address,'supersedes',older.address,newer.revision,
               newer.label,'exact',newer.target_cell_id,newer.target_chunk_id
        FROM {CARDS} newer
        JOIN {CARDS} older
          ON older.annotation_id=newer.annotation_id
         AND older.captured_role='annotation-revision'
         AND older.revision=newer.revision-1
        WHERE newer.captured_role='annotation-revision';

        INSERT INTO {RELATIONS}
        SELECT current.address,'supersedes',prior.address,current.revision,
               current.label,'exact',current.target_cell_id,current.target_chunk_id
        FROM {CARDS} current
        JOIN {CARDS} prior
          ON prior.annotation_id=current.annotation_id
         AND prior.captured_role='annotation-revision'
         AND prior.revision=current.revision-1
        WHERE current.captured_role='annotation';

        UPDATE {CARDS}
           SET structural_child_count=(
                   SELECT COUNT(*) FROM {CARDS} child
                   WHERE child.parent_address={CARDS}.address
               ),
               outgoing_relationship_count=(
                   SELECT COUNT(*) FROM {RELATIONS} relation
                   WHERE relation.subject_address={CARDS}.address
               );

    """)
    db.execute(f"CREATE TEMP TABLE {STATE}(signature TEXT NOT NULL)")
    db.execute(f"INSERT INTO {STATE} VALUES (?)", (signature,))


def materialize_ledger_index(db: sqlite3.Connection, sql: str) -> str:
    """Install the derived Ledger Index only when its relations are referenced."""
    if not _references_index(sql):
        return sql
    if not _relation_exists(db, "main", "annotations"):
        return json.dumps({"error": "Ledger Index requires the Ledger cell as primary"})
    if not _relation_exists(db, "temp", "_flex_self_objects"):
        return json.dumps({"error": "Ledger Index requires a materialized self() scope"})
    try:
        signature = _scope_signature(db)
        prior = None
        if _relation_exists(db, "temp", STATE):
            prior = db.execute(f"SELECT signature FROM {STATE}").fetchone()
        if not _relation_exists(db, "temp", CARDS) or not prior or prior[0] != signature:
            _install_cards(db, signature)
    except sqlite3.DatabaseError as exc:
        return json.dumps({"error": f"Ledger Index materialization failed: {exc}"})
    return sql
