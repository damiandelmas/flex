"""Seed database-backed SQL programs from packaged defaults.

The database is the runtime authority.  Files are compile-time defaults only;
this module never scans skills or chooses among runtime filesystem sources.
Existing rows are preserved so SQL edits remain authoritative.
"""
import hashlib
import sqlite3
import sys
from pathlib import Path

from flex.retrieve.presets import PresetLoader
from flex.registry import resolve_cell, list_cells
from flex.modules.specs import module_spec_for, normalize_cell_type, stock_subdirs

# Preset source directories
PRESET_ROOT = Path(__file__).resolve().parent.parent / "retrieve" / "presets"
GENERAL_DIR = PRESET_ROOT / "general"

# Module-specific preset directories (keyed by cell_type from registry)
MODULE_ROOT = Path(__file__).resolve().parent.parent / "modules"
PUBLIC_PRESET_GROUPS = {
    "claude_code": ("claude_code", "soma"),
}

# Exact prior stock programs that may be upgraded in place. User-authored SQL
# remains authoritative; only a byte-identical known stock revision qualifies.
LEGACY_STOCK_SHA256 = {
    "orient": {
        "839536bc5b67ec71b7a39ec43bd41eae848b3cb838c64bf21877cf178eeeea17",
    },
}


def _stock_preset_dir(module_name: str) -> Path | None:
    path = MODULE_ROOT / module_name / "stock" / "presets"
    if path.exists() and any(path.glob("*.sql")):
        return path
    return None


def _preset_dirs_for(cell_type: str | None) -> list[Path]:
    """Return preset directories for a cell type. General + module-specific."""
    dirs = [GENERAL_DIR]
    if module_spec_for(cell_type):
        spec_dirs = stock_subdirs(cell_type, "presets_from", "presets")
        dirs.extend(spec_dirs)
    else:
        normalized = normalize_cell_type(cell_type)
        module_names = PUBLIC_PRESET_GROUPS.get(
            normalized, (normalized,) if normalized else ()
        )
        for module_name in module_names:
            path = _stock_preset_dir(module_name)
            if path:
                dirs.append(path)
    return dirs


def _expected_presets(cell_type: str | None) -> dict[str, str]:
    """Resolve the final stock name→SQL contract in installation order."""
    expected: dict[str, str] = {}
    for directory in _preset_dirs_for(cell_type):
        for path in sorted(directory.glob("*.sql")):
            text = path.read_text()
            name = PresetLoader._parse(text, path.stem)["name"]
            expected[name] = text
    return expected


def _packaged_variants(cell_type: str | None) -> dict[str, set[str]]:
    """Return every packaged default that may legitimately be superseded."""
    variants: dict[str, set[str]] = {}
    for directory in _preset_dirs_for(cell_type):
        for path in sorted(directory.glob("*.sql")):
            text = path.read_text(encoding="utf-8")
            name = PresetLoader._parse(text, path.stem)["name"]
            variants.setdefault(name, set()).add(text)
    return variants


RETIRED_DDL = (
    "CREATE TABLE IF NOT EXISTS _presets_retired ("
    "name TEXT PRIMARY KEY, reason TEXT, retired_at TEXT DEFAULT (datetime('now')))"
)


def _retired(conn: sqlite3.Connection) -> set[str]:
    """Stock preset names this database has retired; seeding never re-adds them.

    Retire a preset by deleting its _presets row and inserting its name here,
    for example when its tables exist but hold nothing for this database.
    """
    conn.execute(RETIRED_DDL)
    return {row[0] for row in conn.execute("SELECT name FROM _presets_retired")}


def _runs_here(conn: sqlite3.Connection, parsed: dict) -> bool:
    """Whether every query of a preset compiles against this database.

    A stock group can carry presets for tables a given database lacks (chat
    cells share the coding-agent group but have no tool or delegation edges).
    Seeding those would install programs that always fail, and re-seed them
    after every refresh even when someone deleted them. EXPLAIN prepares each
    statement without running it; parameters bind as NULL.
    """
    import re
    for query in parsed.get("queries") or []:
        sql = (query.get("sql") or "").strip().rstrip(";")
        if not sql:
            continue
        names = set(re.findall(r"(?<![:\w]):([A-Za-z_]\w*)", sql))
        try:
            conn.execute("EXPLAIN " + sql, {name: None for name in names}).fetchall()
        except sqlite3.Error as exc:
            # keyword() and vec_ops() are registered by the engine at query
            # time, not stored in the file; their absence here says nothing.
            message = str(exc)
            if "no such function" in message or re.search(
                r"no such table: (?:\w+\.)?(?:keyword|vec_ops)\b", message
            ):
                continue
            return False
    return True


def ensure_cell_presets(
    conn: sqlite3.Connection,
    cell_type: str | None,
    corpus_root: str | Path | None = None,
) -> bool:
    """Add missing packaged defaults without replacing SQL-owned rows."""
    del corpus_root
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _presets ("
        "name TEXT PRIMARY KEY, description TEXT, params TEXT DEFAULT '', "
        "sql TEXT, source TEXT)"
    )
    before = conn.total_changes
    variants = _packaged_variants(cell_type)
    retired = _retired(conn)
    for name, sql in _expected_presets(cell_type).items():
        if name in retired:
            continue
        parsed = PresetLoader._parse(sql, name)
        existing = conn.execute(
            "SELECT sql FROM _presets WHERE name=?", (name,)
        ).fetchone()
        values = (parsed.get("description", ""), parsed.get("params", ""), sql, name)
        if existing is None:
            if not _runs_here(conn, parsed):
                continue
            conn.execute(
                "INSERT INTO _presets(name,description,params,sql) VALUES (?,?,?,?)",
                (name, *values[:3]),
            )
        elif existing[0] != sql and (
            existing[0] in variants.get(name, set())
            or hashlib.sha256(existing[0].encode("utf-8")).hexdigest()
            in LEGACY_STOCK_SHA256.get(name, set())
        ):
            conn.execute(
                "UPDATE _presets SET description=?,params=?,sql=? WHERE name=?",
                values,
            )
    changed = conn.total_changes != before
    if changed:
        conn.commit()
    return changed


def install_cell(cell_name: str, preset_dirs: list[Path] = None):
    """Seed missing SQL programs into one registered cell."""
    db_path = resolve_cell(cell_name)
    if db_path is None or not db_path.exists():
        print(f"  {cell_name}: SKIP (not found)")
        return

    if preset_dirs is None:
        # Detect cell_type from registry
        cell_type = None
        for cell in list_cells():
            if cell['name'] == cell_name:
                cell_type = cell.get('cell_type')
                break
        preset_dirs = _preset_dirs_for(cell_type)

    try:
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _presets ("
            "name TEXT PRIMARY KEY, description TEXT, params TEXT DEFAULT '', "
            "sql TEXT, source TEXT)"
        )
        retired = _retired(conn)
        for directory in preset_dirs:
            for path in sorted(Path(directory).glob("*.sql")):
                text = path.read_text(encoding="utf-8")
                parsed = PresetLoader._parse(text, path.stem)
                if parsed["name"] in retired or not _runs_here(conn, parsed):
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO _presets"
                    "(name,description,params,sql) VALUES (?,?,?,?)",
                    (parsed["name"], parsed["description"], parsed.get("params", ""), text),
                )
        conn.commit()
        print(f"  {cell_name}: database query catalog ready")

        conn.close()
    except sqlite3.OperationalError as e:
        print(f"  {cell_name}: LOCKED ({e}) — retry after stopping flex-worker")


def install_all():
    """Seed missing default SQL programs into registered cells."""
    print("Checking database query catalogs...")
    cells = list_cells()
    if not cells:
        print("  No cells registered. Run 'flex init' first.")
        return
    for cell in cells:
        install_cell(cell['name'])
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for name in sys.argv[1:]:
            install_cell(name)
    else:
        install_all()
