"""Wikilink extraction and resolution for markdown files."""

import re
import unicodedata
from dataclasses import dataclass, field

WIKILINK_RE = re.compile(r'\[\[([^\]|#]+?)(?:[|#][^\]]*)?\]\]')


def is_template_target(target: str) -> bool:
    """True if target contains template syntax (Templater/Handlebars)."""
    return '<%' in target or '{{' in target


def extract_raw_wikilinks(body: str) -> list[str]:
    """Return list of raw link targets from [[...]] syntax.

    Deduplicates within a single file.
    Filters out template syntax targets.
    """
    seen = set()
    for match in WIKILINK_RE.finditer(body):
        target = match.group(1).strip()
        if target and not is_template_target(target):
            seen.add(target)
    return list(seen)


# ─── Resolution (114b) ───────────────────────────────────────────────────────

def _normalize(s: str) -> str:
    """NFKD + casefold for resolution comparisons."""
    return unicodedata.normalize('NFKD', s).casefold()


@dataclass
class ResolutionMaps:
    path_set: set              # all relative paths (with .md extension)
    suffix_map: dict           # trailing sub-paths → full path
    basename_map: dict         # lowercase basename (no ext) → path
    alias_map: dict            # normalized alias → path
    title_map: dict            # normalized title (stem) → path


def build_resolution_maps(entries, aliases_by_path: dict = None) -> ResolutionMaps:
    """Build lookup maps from a full vault scan.

    Args:
        entries: list of VaultEntry (or anything with .rel_path, .stem)
        aliases_by_path: dict of {rel_path: [alias1, alias2, ...]}
    """
    aliases_by_path = aliases_by_path or {}
    path_set = set()
    suffix_map = {}
    basename_map = {}
    alias_map = {}
    title_map = {}

    for entry in entries:
        rel = entry.rel_path
        path_set.add(rel)

        # Basename map (no extension, lowercased)
        bn = _normalize(entry.stem)
        basename_map.setdefault(bn, rel)

        # Title map (same as basename for files without explicit title)
        title_map.setdefault(bn, rel)

        # Suffix map — ALL trailing sub-paths
        parts = rel.split('/')
        for i in range(1, len(parts)):
            suffix = '/'.join(parts[i:]).lower()
            suffix_map.setdefault(suffix, rel)

        # Alias map
        for alias in aliases_by_path.get(rel, []):
            alias_map.setdefault(_normalize(alias), rel)

    return ResolutionMaps(
        path_set=path_set,
        suffix_map=suffix_map,
        basename_map=basename_map,
        alias_map=alias_map,
        title_map=title_map,
    )


def resolve_wikilink(target: str, maps: ResolutionMaps, from_path: str) -> str | None:
    """Resolve a raw wikilink target to a file path.

    Five-step pipeline: exact → suffix → basename → alias → title.
    Skips self-links. Returns None if unresolved.
    """
    # Step 1: Exact path match
    target_with_ext = target + '.md' if not target.endswith('.md') else target
    if target_with_ext in maps.path_set and target_with_ext != from_path:
        return target_with_ext

    # Step 2: Suffix/partial-path match (only when target contains /)
    if '/' in target:
        suffix_key = target_with_ext.lower()
        match = maps.suffix_map.get(suffix_key)
        if match and match != from_path:
            return match

    # Step 3: Basename match
    bn = _normalize(target.split('/')[-1])
    # Strip .md if present for basename lookup
    if bn.endswith('.md'):
        bn = bn[:-3]
    match = maps.basename_map.get(bn)
    if match and match != from_path:
        return match

    # Step 4: Alias match
    norm = _normalize(target)
    match = maps.alias_map.get(norm)
    if match and match != from_path:
        return match

    # Step 5: Title match
    match = maps.title_map.get(norm)
    if match and match != from_path:
        return match

    return None


def resolve_all_wikilinks(db, entries, aliases_by_path: dict = None) -> tuple[int, int]:
    """Resolve all raw wikilinks and write edge tables.

    Reads from _edges_wikilink_raw, writes to _edges_wikilink (resolved)
    and _edges_wikilink_unresolved (ghost nodes). Drops _edges_wikilink_raw
    after consumption.

    Returns (resolved_count, unresolved_count).
    """
    # Create target tables
    db.execute("""CREATE TABLE IF NOT EXISTS _edges_wikilink (
        chunk_id TEXT NOT NULL,
        from_path TEXT NOT NULL,
        to_path TEXT NOT NULL,
        PRIMARY KEY (from_path, to_path, chunk_id)
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_wikilink_to ON _edges_wikilink(to_path)")

    db.execute("""CREATE TABLE IF NOT EXISTS _edges_wikilink_unresolved (
        from_path TEXT NOT NULL,
        raw_target TEXT NOT NULL,
        PRIMARY KEY (from_path, raw_target)
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_unresolved_target ON _edges_wikilink_unresolved(raw_target)")

    # Build resolution maps
    maps = build_resolution_maps(entries, aliases_by_path)

    # Read raw wikilinks
    rows = db.execute("SELECT source_id, raw_target FROM _edges_wikilink_raw").fetchall()
    if not rows:
        return 0, 0

    resolved_count = 0
    unresolved_count = 0

    # Get chunk_ids per source for the chunk_id column
    source_chunks = {}
    for source_id, raw_target in rows:
        if source_id not in source_chunks:
            chunk_ids = [r[0] for r in db.execute(
                "SELECT chunk_id FROM _edges_source WHERE source_id = ? ORDER BY rowid LIMIT 1",
                (source_id,)
            ).fetchall()]
            source_chunks[source_id] = chunk_ids[0] if chunk_ids else source_id

    for source_id, raw_target in rows:
        resolved = resolve_wikilink(raw_target, maps, source_id)
        chunk_id = source_chunks.get(source_id, source_id)

        if resolved:
            db.execute(
                "INSERT OR IGNORE INTO _edges_wikilink (chunk_id, from_path, to_path) VALUES (?, ?, ?)",
                (chunk_id, source_id, resolved)
            )
            resolved_count += 1
        else:
            db.execute(
                "INSERT OR IGNORE INTO _edges_wikilink_unresolved (from_path, raw_target) VALUES (?, ?)",
                (source_id, raw_target)
            )
            unresolved_count += 1

    # Drop raw table — consumed
    db.execute("DROP TABLE IF EXISTS _edges_wikilink_raw")
    db.commit()

    return resolved_count, unresolved_count
