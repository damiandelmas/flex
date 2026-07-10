"""One-off migration: re-type a docpac cell's doc_type to the category.subtype
coordinate (option C).

Re-derives doc_type per source through the NEW pipeline (folder coordinate +
_flex_types.json sidecar + frontmatter override), then UPDATEs _raw_sources and
_types_docpac in place — NO re-embed, embeddings/graph untouched — and reinstalls
the curated views (which split doc_type → category/subtype).

Run with PYTHONPATH=<worktree> so it uses the new grammar. Idempotent: running
twice produces the same coordinate.

    python -m flex.modules.docpac.manage.retype_coordinate --cell PATH --corpus DIR [--dry-run]
"""
import argparse
import sqlite3
import sys
from collections import Counter
from pathlib import Path

from flex.modules.docpac.compile.docpac import parse_docpac_file
from flex.modules.docpac.compile.classify import load_sidecar, apply_sidecar_overrides
from flex.modules.docpac.compile.context_config import load_context_config
from flex.modules.docpac.compile.init import _make_file_uuid_resolver
from flex.modules.markdown.compile.profile import docpac_profile
from flex.compile.markdown import extract_frontmatter

_VIEWS = Path(__file__).resolve().parent.parent / 'stock' / 'views'


def retype(cell_path: str, corpus_root: str, dry_run: bool = False) -> dict:
    cfg = load_context_config(corpus_root)
    uuid_keyed, path_keyed = load_sidecar(corpus_root)
    get_uuid, _ = _make_file_uuid_resolver(
        'deterministic', needs_soma_identity=bool(uuid_keyed))
    prof = docpac_profile()

    conn = sqlite3.connect(cell_path)
    conn.execute("PRAGMA busy_timeout=30000")
    before = Counter(r[0] for r in conn.execute(
        "SELECT doc_type FROM _raw_sources"))
    srcs = conn.execute(
        "SELECT source_id, source_path FROM _raw_sources").fetchall()

    changed = missing = 0
    after = Counter()
    for source_id, source_path in srcs:
        if not source_path or not Path(source_path).exists():
            missing += 1
            cur = conn.execute(
                "SELECT doc_type FROM _raw_sources WHERE source_id=?",
                (source_id,)).fetchone()
            after[cur[0] if cur else None] += 1
            continue
        entry = parse_docpac_file(source_path, corpus_root, config=cfg)
        apply_sidecar_overrides([entry], corpus_root, get_uuid, uuid_keyed, path_keyed)
        try:
            fm, _ = extract_frontmatter(Path(source_path).read_text(encoding='utf-8'))
        except Exception:
            fm = {}
        src = prof.classify_source(entry, fm)
        new_dt, new_temp = src['doc_type'], src['temporal']
        after[new_dt] += 1
        if not dry_run:
            conn.execute(
                "UPDATE _raw_sources SET doc_type=?, temporal=? WHERE source_id=?",
                (new_dt, new_temp, source_id))
            conn.execute(
                "UPDATE _types_docpac SET doc_type=?, temporal=? WHERE chunk_id IN "
                "(SELECT chunk_id FROM _edges_source WHERE source_id=?)",
                (new_dt, new_temp, source_id))
        changed += 1

    if not dry_run:
        conn.commit()
        from flex.views import install_views
        install_views(conn, _VIEWS)
        conn.commit()
    conn.close()
    return {'sources': len(srcs), 'retyped': changed, 'missing_file': missing,
            'before': dict(before), 'after': dict(after)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cell', required=True)
    ap.add_argument('--corpus', required=True)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    r = retype(a.cell, a.corpus, dry_run=a.dry_run)
    print(f"sources={r['sources']} retyped={r['retyped']} missing_file={r['missing_file']}")
    print("before:", dict(sorted(r['before'].items(), key=lambda x: -x[1])))
    print("after: ", dict(sorted(r['after'].items(), key=lambda x: -x[1])))


if __name__ == '__main__':
    main()
