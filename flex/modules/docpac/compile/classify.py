"""
Doc-pac semantic classification layer.

Extracted from the init.py monolith so the worker and the shared compile
pipeline can drive the same typing. Pure: parsed entries in → entries stamped
with doc_type/temporal/_meta out. No DB, no embedding.

Merge priority (highest first): frontmatter doc_type (applied by the caller) >
_flex_types.json sidecar (here) > FOLDER_MAP path inference (docpac.py). This
file owns the sidecar tier and the compound-prefix temporal derivation.
"""
import json
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path


# Compound doc_type prefix → temporal. A sidecar/frontmatter may supply a
# compound doc_type (`change.code`); temporal is derived from its prefix.
# Keys are the canonical categories (docpac.py CATEGORY_TEMPORAL): compound
# doc_types are `category.subtype`, so the prefix is a category, not a subtype.
# 'design' kept as a legacy alias for pre-coordinate flat doc_types.
TEMPORAL_MAP = {
    'change':   'past',
    'current':  'present',
    'intended': 'future',
    'external': 'exogenous',
    'design':   'future',   # legacy flat doc_type (pre category×subtype)
}

_FILE_DATE_HEALTH_DDL = """
CREATE TABLE IF NOT EXISTS _health_defects (
    kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    observed_value TEXT,
    detail TEXT,
    PRIMARY KEY (kind, subject_id)
)
"""


def validate_file_date(value) -> str | None:
    """Return an accepted raw calendar token, or NULL for malformed/future data."""
    if value is None or str(value).strip() == '':
        return None
    raw = str(value).strip()
    parsed = None
    try:
        if re.fullmatch(r'\d{6}', raw):
            parsed = date(2000 + int(raw[:2]), int(raw[2:4]), int(raw[4:6]))
        elif re.fullmatch(r'\d{6}-\d{4}', raw):
            parsed = datetime(2000 + int(raw[:2]), int(raw[2:4]), int(raw[4:6]),
                              int(raw[7:9]), int(raw[9:11])).date()
        elif re.fullmatch(r'20\d{6}', raw):
            parsed = datetime.strptime(raw, '%Y%m%d').date()
        elif re.match(r'^20\d{2}-\d{2}-\d{2}', raw):
            parsed = date.fromisoformat(raw[:10])
    except ValueError:
        return None
    if parsed is None or parsed > date.today() + timedelta(days=1):
        return None
    return raw


def record_file_date_health(conn: sqlite3.Connection, source_id: str, raw_value) -> str | None:
    """Normalize the public value and keep a queryable defect for rejection."""
    conn.execute(_FILE_DATE_HEALTH_DDL)
    normalized = validate_file_date(raw_value)
    if raw_value not in (None, '') and normalized is None:
        conn.execute(
            "INSERT OR REPLACE INTO _health_defects VALUES (?,?,?,?)",
            ('invalid_file_date', source_id, str(raw_value),
             'rejected: not a valid non-future supported calendar token'),
        )
    else:
        conn.execute(
            "DELETE FROM _health_defects WHERE kind='invalid_file_date' AND subject_id=?",
            (source_id,),
        )
    return normalized


def heal_invalid_file_dates(conn: sqlite3.Connection) -> int:
    """Repair existing cells in place without touching valid calendar tokens."""
    conn.execute(_FILE_DATE_HEALTH_DDL)
    rows = conn.execute(
        "SELECT source_id,file_date FROM _raw_sources WHERE file_date IS NOT NULL AND file_date<>''"
    ).fetchall()
    bad = 0
    for source_id, raw in rows:
        if validate_file_date(raw) is None:
            record_file_date_health(conn, source_id, raw)
            conn.execute("UPDATE _raw_sources SET file_date=NULL WHERE source_id=?", (source_id,))
            bad += 1
    conn.commit()
    return bad


def derive_temporal(doc_type, fallback):
    """Derive temporal from compound doc_type prefix (change.code → past)."""
    if doc_type:
        prefix = doc_type.split('.')[0]
        return TEMPORAL_MAP.get(prefix, fallback)
    return fallback


def load_sidecar(corpus_root):
    """Read `_flex_types.json` at the corpus root, splitting uuid-keyed (new)
    from path-keyed (legacy) entries.

    Returns (uuid_keyed, path_keyed) dicts — both empty when no sidecar exists.
    """
    types_file = Path(corpus_root) / '_flex_types.json'
    uuid_keyed = {}
    path_keyed = {}
    if types_file.exists():
        raw = json.loads(types_file.read_text())
        for k, v in raw.items():
            # uuid-keyed (new) vs path-keyed (legacy)
            if len(k) == 36 and k.count('-') == 4:
                uuid_keyed[k] = v
            else:
                path_keyed[k] = v
    return uuid_keyed, path_keyed


def apply_sidecar_overrides(entries, corpus_root, get_file_uuid,
                            uuid_keyed, path_keyed):
    """Stamp `entry.doc_type` / `entry.temporal` / `entry._meta` from the
    `_flex_types.json` sidecar.

    For each non-skip entry: resolve its file_uuid, look it up uuid-keyed first,
    fall back to the relative path. A sidecar doc_type overrides the folder-map
    inference and re-derives temporal from its prefix; frontmatter (applied by
    the caller, later) still wins over this. Every entry gets a `_meta` dict
    carrying at least its file_uuid.

    Returns the count of entries matched by the sidecar.
    """
    corpus_root = Path(corpus_root)
    overridden = 0
    for entry in entries:
        if entry.skip:
            continue
        # Try uuid-keyed first, fall back to relative path
        file_uuid = get_file_uuid(entry.path)
        t = uuid_keyed.get(file_uuid)
        if t is None:
            try:
                rel = str(Path(entry.path).relative_to(corpus_root))
            except ValueError:
                rel = entry.path
            t = path_keyed.get(rel)
        if t:
            doc_type = t.get('doc_type')
            if doc_type:
                entry.doc_type = doc_type
                entry.temporal = derive_temporal(doc_type, entry.temporal)
            entry._meta = {
                'file_uuid':  file_uuid,
                'confidence': t.get('confidence', 1.0),
                'validity':   t.get('validity',   1.0),
                'maturity':   t.get('maturity',   1.0),
                'summary':    t.get('summary'),
                'keywords':   t.get('keywords'),
                'type':       t.get('type'),
            }
            overridden += 1
        else:
            entry._meta = {'file_uuid': file_uuid}
    return overridden
