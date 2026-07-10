"""`.flexpresets.json` file-read + cascade + `_presets` INSERT — the COMPILE half of
Phase B (issue 260704: cell-shipped presets that survive regen).

Ownership seam: the SCHEMA + VALIDATION are interface-owned
(`flex.retrieve.flexpresets.validate_flexpresets`); this module owns only the
file-read/cascade discovery (same shape as `chunk_config.nearest_flexchunk`), the
`_presets.source` provenance stamping, and the INSERT — called from the docpac build
path AFTER stock install, so a cell preset with a fresh (non-stock) name always lands
last and survives a rebuild that wipes+reinstalls stock. Absent file = byte-identical
to today.
"""
from __future__ import annotations

from pathlib import Path

from flex.retrieve.flexpresets import validate_flexpresets, FLEXPRESETS_FILENAME


def _cascade_files(root: str | Path) -> list[tuple[Path, str]]:
    """`.flexpresets.json` files from `root` walking UP to the fs root, NEAREST-FIRST.
    Same cascade shape as `.flexchunk.json`; nearest wins on a name collision."""
    out: list[tuple[Path, str]] = []
    d = Path(root).resolve()
    while True:
        fp = d / FLEXPRESETS_FILENAME
        if fp.is_file():
            try:
                out.append((fp, fp.read_text()))
            except Exception:
                pass  # unreadable file → treated as absent (fail-safe)
        if d.parent == d:
            break
        d = d.parent
    return out


def _ensure_source_column(conn) -> bool:
    """Additive/back-compat: ensure `_presets.source` exists and stamp existing rows
    (installed BEFORE cell presets = stock) as 'stock', so @orient can label
    provenance. Returns False if the cell has no `_presets` table yet."""
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if "_presets" not in tables:
        return False
    cols = {r[1] for r in conn.execute("PRAGMA table_info(_presets)")}
    if "source" not in cols:
        conn.execute("ALTER TABLE _presets ADD COLUMN source TEXT")
    conn.execute("UPDATE _presets SET source='stock' WHERE source IS NULL")
    return True


def install_flexpresets(conn, root: str | Path, warn=None) -> dict:
    """Read the cascaded `.flexpresets.json` from `root`, validate each file via the
    interface-owned validator, and INSERT OR REPLACE the survivors into `_presets`
    (source='cell'). MUST run AFTER stock install (both docpac build entry points).
    Never raises on a bad file/entry — validation fails safe (skips + warns), so a
    malformed `.flexpresets.json` can never corrupt the cell. Returns
    {installed, skipped, warnings}."""
    warn = warn or (lambda m: None)
    if not _ensure_source_column(conn):
        return {"installed": 0, "skipped": 0, "warnings": ["no _presets table"]}

    files = _cascade_files(root)
    if not files:
        return {"installed": 0, "skipped": 0, "warnings": []}

    stock_names = {r[0] for r in conn.execute("SELECT name FROM _presets")}
    installed = skipped = 0
    warnings: list[str] = []
    taken: set[str] = set()  # cross-file nearest-wins (files are nearest-first)
    for fp, text in files:
        res = validate_flexpresets(text, stock_names=stock_names)
        for name, reason in res.skipped:
            skipped += 1
            msg = f"[flexpresets] {fp}: skipped '{name}': {reason}"
            warnings.append(msg)
            warn(msg)
        for row in res.valid:
            if row["name"] in taken:
                continue  # a nearer .flexpresets.json already defined this name
            taken.add(row["name"])
            conn.execute(
                "INSERT OR REPLACE INTO _presets (name, description, params, sql, source) "
                "VALUES (?, ?, ?, ?, ?)",
                (row["name"], row["description"], row["params"], row["sql"], row["source"]))
            installed += 1
    return {"installed": installed, "skipped": skipped, "warnings": warnings}
