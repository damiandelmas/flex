"""
Self-declaration loader for docpac context folders.

`load_context_config(corpus_root)` resolves the effective grammar a corpus
ingests with: the canonical `docpac-v1` spine (stock data, mirrors the docpac.py
constants), optionally overlaid by a per-node `context.json` `schema` block.

The override may only RE-PROJECT folders onto docpac-v1's vocabulary — it can
never introduce a new doc_type/temporal. That cap is the cross-cell coherence
guarantee (Flex queries the spine, not the paths).
"""
import json
from pathlib import Path

_STOCK = Path(__file__).resolve().parent.parent / 'stock' / 'profiles' / 'docpac-v1.json'


def _load_docpac_v1() -> dict:
    return json.loads(_STOCK.read_text())


def _tuples(m: dict) -> dict:
    return {k: tuple(v) for k, v in m.items()}


def canonical_vocab(base: dict):
    """The allowed (categories, subtypes) sets, derived from docpac-v1 itself.
    None entries (plan-subfolder references carry category=None) are skipped."""
    categories, subtypes = set(), set()
    for src in (base['folder_map'], base['plan_subfolder_map']):
        for v in src.values():
            if v[0] is not None:
                categories.add(v[0])
            if v[1] is not None:
                subtypes.add(v[1])
    return categories, subtypes


def load_context_config(corpus_root) -> dict:
    """Resolve the grammar for a corpus.

    Returns a dict: folder_map (tuple values), plan_subfolder_map, skip_folders
    (set), indicators (set), split_level, type_table. With no `context.json` the
    result is docpac-v1 verbatim.

    Raises ValueError if a context.json override introduces a non-canonical
    doc_type/temporal or an unknown inherit target.
    """
    base = _load_docpac_v1()
    folder_map = _tuples(base['folder_map'])
    plan_map = _tuples(base['plan_subfolder_map'])
    skip = set(base['skip_folders'])
    indicators = set(base['indicators'])
    split_level = base.get('split_level', 1)  # lossless default: split at every heading
    type_table = base.get('type_table', '_types_docpac')
    # chunking block (profile-as-data): declarative split STRATEGY per coordinate/
    # folder. Optional — absent → {} (callers fall back to split_level). Shape:
    #   {"default": {...}, "by_subtype": {sub: {...}}, "by_folder": {path: {...}}}
    chunking = dict(base.get('chunking') or {})

    ctx = Path(corpus_root) / 'context.json'
    if ctx.exists():
        try:
            decl = json.loads(ctx.read_text())
        except (ValueError, OSError):
            decl = {}
        schema = decl.get('schema', {}) or {}

        inherit = schema.get('inherit', 'docpac-v1')
        if inherit not in ('docpac-v1', None):
            raise ValueError(f"context.json: unknown inherit target '{inherit}'")

        categories, subtypes = canonical_vocab(base)
        for folder, val in (schema.get('map') or {}).items():
            if not isinstance(val, (list, tuple)) or len(val) < 2:
                raise ValueError(
                    f"context.json: folder '{folder}' -> {val!r} must be a "
                    f"[category, subtype] pair.")
            cat, sub = val[0], val[1]
            if (cat is not None and cat not in categories) or \
               (sub is not None and sub not in subtypes):
                raise ValueError(
                    f"context.json: folder '{folder}' -> ({cat}, {sub}) introduces "
                    f"a non-canonical value. Overrides may only re-project onto "
                    f"docpac-v1 (categories={sorted(categories)}, "
                    f"subtypes={sorted(subtypes)}).")
            folder_map[folder] = (cat, sub)

        for f in (schema.get('skip_folders') or []):
            skip.add(f)
        for ind in (schema.get('indicators') or []):
            indicators.add(ind)
        if 'split_level' in schema:
            split_level = schema['split_level']
        # chunking override: context.json may set default + extend by_subtype/by_folder
        sc = schema.get('chunking') or {}
        if 'default' in sc:
            chunking['default'] = sc['default']
        for tier in ('by_subtype', 'by_folder'):
            if sc.get(tier):
                chunking[tier] = {**(chunking.get(tier) or {}), **sc[tier]}

    # Per-flavor split default (fs consolidation — sibling of Seam-3). An obsidian
    # vault preserves its full-note chunk model when it rides the docpac engine:
    # route it through `whole` (one node per note) so the migration is
    # (source_path, position, content)-parity with the frozen full_note baseline,
    # not a re-chunk. Detection = an explicit context.json `schema.flavor`, else the
    # canonical `.obsidian/` marker. Only fills the default when the corpus hasn't
    # declared its own chunking default (explicit config always wins). Non-obsidian
    # prose is untouched → keeps heading@split_level (docpac's per-heading grain).
    flavor = 'prose'
    if ctx.exists() and isinstance(schema, dict) and schema.get('flavor'):
        flavor = schema['flavor']
    elif (Path(corpus_root) / '.obsidian').is_dir():
        flavor = 'obsidian'
    if flavor == 'obsidian' and 'default' not in chunking:
        # full_note + the note-stem prefix so a migrated vault byte-matches its
        # instant baseline (`{stem}\n{body}` — the note NAME stays in-content).
        chunking['default'] = {'split': 'whole', 'title_prefix': True}

    # validate every declared split against the known dispatch set (coherence cap:
    # a typo is a build-time error, not a silent wrong split)
    from flex.compile.chunk_config import VALID_SPLITS
    rules = [chunking.get('default')] + list((chunking.get('by_subtype') or {}).values()) \
        + list((chunking.get('by_folder') or {}).values())
    for rule in rules:
        s = (rule or {}).get('split')
        if s and s not in VALID_SPLITS:
            raise ValueError(f"chunking: unknown split '{s}' "
                             f"(valid: {sorted(VALID_SPLITS)})")

    return {
        'folder_map': folder_map,
        'plan_subfolder_map': plan_map,
        'skip_folders': skip,
        'indicators': indicators,
        'split_level': split_level,
        'type_table': type_table,
        'chunking': chunking,
        'flavor': flavor,
    }
