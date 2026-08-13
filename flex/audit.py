"""Build small, additive evidence audits over registered Flex cells.

An audit cell stores authored observations, not copies of the material it audits.
Every observation is a retrievable chunk and every evidence edge preserves the
target cell and target object address needed to recover the primary source.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Iterable

from flex.registry import get_cell_metadata
from flex.sdk import create, ingest, register, source


AUDIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL,
    corpus_manifest TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_components (
    component_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    scope TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _types_audit_observation (
    chunk_id TEXT PRIMARY KEY REFERENCES _raw_chunks(id) ON DELETE CASCADE,
    snapshot_id TEXT NOT NULL REFERENCES audit_snapshots(snapshot_id),
    component_id TEXT NOT NULL REFERENCES audit_components(component_id),
    observation_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('observed', 'supported', 'contradicted')),
    transfer_status TEXT NOT NULL CHECK (
        transfer_status IN ('extract', 'adapt', 'reject', 'investigate')
    ),
    flex_target TEXT,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _edges_audit_evidence (
    chunk_id TEXT NOT NULL REFERENCES _raw_chunks(id) ON DELETE CASCADE,
    target_cell_id TEXT NOT NULL,
    target_cell_name TEXT NOT NULL,
    target_object_id TEXT NOT NULL,
    evidence_role TEXT NOT NULL CHECK (
        evidence_role IN ('implements', 'proves', 'context', 'rejects')
    ),
    note TEXT,
    PRIMARY KEY (chunk_id, target_cell_id, target_object_id, evidence_role)
);

CREATE INDEX IF NOT EXISTS idx_audit_observation_component
ON _types_audit_observation(component_id, recorded_at);
"""

_AUDIT_ORIENT_SQL = """
SELECT 'about' AS query, json_object(
  'authority', 'Authored observations are the audit authority; evidence links recover immutable source objects from registered target cells.',
  'lifecycle', 'static',
  'composition', 'ATTACH each target cell query-locally and join _edges_audit_evidence on exact target_object_id.'
) AS results
UNION ALL
SELECT 'shape', json_object(
  'snapshots', (SELECT COUNT(*) FROM audit_snapshots),
  'components', (SELECT COUNT(*) FROM audit_components),
  'observations', (SELECT COUNT(*) FROM _types_audit_observation),
  'evidence_links', (SELECT COUNT(*) FROM _edges_audit_evidence)
)
UNION ALL
SELECT 'query_surface', json_object(
  'observations', 'SELECT component_id, observation_id, status, transfer_status, flex_target FROM _types_audit_observation ORDER BY recorded_at',
  'evidence', 'JOIN _edges_audit_evidence to the attached target cell chunks relation on target_object_id'
)
"""


@dataclass(frozen=True)
class Evidence:
    target_cell_name: str
    target_object_id: str
    role: str
    note: str | None = None


@dataclass(frozen=True)
class Observation:
    observation_id: str
    component_id: str
    component_label: str
    scope: str
    content: str
    status: str
    transfer_status: str
    flex_target: str | None
    evidence: tuple[Evidence, ...]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _target_cell(evidence: Evidence) -> dict:
    metadata = get_cell_metadata(evidence.target_cell_name)
    if not metadata:
        raise ValueError(f"Evidence cell is not registered: {evidence.target_cell_name}")
    path = Path(metadata["path"])
    if not path.exists():
        raise ValueError(f"Evidence cell database is missing: {path}")
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as target:
        exists = target.execute(
            "SELECT 1 FROM chunks WHERE id=? LIMIT 1", (evidence.target_object_id,)
        ).fetchone()
    if not exists:
        raise ValueError(
            f"Evidence object is absent from {evidence.target_cell_name}: "
            f"{evidence.target_object_id}"
        )
    return metadata


def build_audit(
    *,
    name: str,
    description: str,
    snapshot_id: str,
    purpose: str,
    corpus_manifest: str,
    observations: Iterable[Observation],
) -> str:
    """Append one frozen snapshot of source-linked observations to an audit cell.

    Existing snapshots and observations are retained. Reusing an observation ID
    is intentionally idempotent; changing an observation requires a new ID and
    a new snapshot, preserving the earlier claim and its evidence.
    """
    observations = tuple(observations)
    if not observations:
        raise ValueError("An audit snapshot requires at least one observation")

    recorded_at = _now()
    db = create(name, description, cell_type="audit", schema=AUDIT_SCHEMA)
    db.execute(
        "INSERT OR IGNORE INTO audit_snapshots(snapshot_id,purpose,corpus_manifest,recorded_at) "
        "VALUES (?,?,?,?)",
        (snapshot_id, purpose, corpus_manifest, recorded_at),
    )
    source(db, snapshot_id, f"Audit snapshot {snapshot_id}")

    chunks: list[dict] = []
    edges: list[tuple[str, dict, Evidence]] = []
    for observation in observations:
        if not observation.evidence:
            raise ValueError(f"Observation needs evidence: {observation.observation_id}")
        db.execute(
            "INSERT OR IGNORE INTO audit_components(component_id,label,scope) VALUES (?,?,?)",
            (observation.component_id, observation.component_label, observation.scope),
        )
        chunks.append(
            {
                "id": observation.observation_id,
                "content": observation.content,
                "snapshot_id": snapshot_id,
                "component_id": observation.component_id,
                "observation_id": observation.observation_id,
                "status": observation.status,
                "transfer_status": observation.transfer_status,
                "flex_target": observation.flex_target,
                "recorded_at": recorded_at,
            }
        )
        for evidence in observation.evidence:
            edges.append((observation.observation_id, _target_cell(evidence), evidence))

    ingest(db, snapshot_id, chunks, types="_types_audit_observation")
    for chunk_id, target, evidence in edges:
        db.execute(
            "INSERT OR IGNORE INTO _edges_audit_evidence "
            "(chunk_id,target_cell_id,target_cell_name,target_object_id,evidence_role,note) "
            "VALUES (?,?,?,?,?,?)",
            (
                chunk_id,
                target["id"],
                evidence.target_cell_name,
                evidence.target_object_id,
                evidence.role,
                evidence.note,
            ),
        )
    db.commit()
    cell_id = register(
        db,
        name=name,
        description=description,
        cell_type="audit",
        lifecycle="static",
    )
    db.execute(
        "INSERT OR REPLACE INTO _presets(name,description,params,sql,source) VALUES (?,?,?,?,?)",
        (
            "orient",
            "Audit identity, immutable evidence contract, and SQL query surface.",
            "",
            _AUDIT_ORIENT_SQL,
            "flex.audit",
        ),
    )
    db.commit()
    db.close()
    return cell_id
