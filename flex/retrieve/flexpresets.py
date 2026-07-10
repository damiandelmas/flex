"""`.flexpresets.json` — cell-shipped presets that survive regen (issue 260704).

Contract owner: flex-engine:interface. This module owns the SCHEMA + VALIDATION for a
cell's `.flexpresets.json`; the file-read/cascade + `_presets` INSERT live in the compile
build path (compile owns that primitive). A build path calls `validate_flexpresets(...)`,
then inserts the returned `.valid` rows alongside stock presets.

Why survive regen: regen rebuilds `_presets` from module stock, wiping anything custom.
A cell that ships `.flexpresets.json` gets its domain presets (prose: @summaries/@changelogs;
code: late-bind @callers/@impact via _symbols) reinstalled on every (re)build.

Validation is fail-closed PER ENTRY — a bad entry is skipped with a reason, the rest install.

SELECT-only is enforced in two layers (defense in depth):
  1. install-time (here): a STATIC, late-bind-tolerant check — the SQL is shape-checked
     without executing it or requiring its referenced tables to exist (critical: a code
     cell's @callers references `_symbols`, built later). We reject anything that isn't a
     pure SELECT/WITH read.
  2. runtime: every preset still runs through the existing read-only SQLite authorizer
     (flex/retrieve/keyword.py::_SELECT_ONLY) at execution — the ultimate guard. The static
     check just refuses to store an obviously-mutating preset in the first place.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

FLEXPRESETS_FILENAME = ".flexpresets.json"
SUPPORTED_VERSION = 1

# Preset name == the @name invocation token.
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")

# Statements that mutate or escape the read sandbox. Scanned as whole words against the
# comment/string-stripped skeleton, so a column named `updated_at` or a LIKE '%update%'
# body never trips them.
_FORBIDDEN = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE",
    "ATTACH", "DETACH", "PRAGMA", "REINDEX", "VACUUM", "TRIGGER", "ANALYZE",
)
_FORBIDDEN_RE = re.compile(r"\b(" + "|".join(_FORBIDDEN) + r")\b", re.IGNORECASE)
_STARTS_READ_RE = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE)


@dataclass
class ValidatedFlexpresets:
    valid: list[dict] = field(default_factory=list)          # rows: {name, description, params, sql, source='cell'}
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (name_or_'<file>', reason)

    @property
    def ok(self) -> bool:
        return not self.skipped


def _strip_sql(sql: str) -> str:
    """Remove `--` line comments, `/* */` blocks, and '...' string literals (''-escaped),
    leaving a skeleton safe to scan for statement shape + forbidden keywords."""
    out = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            j = sql.find("\n", i)
            i = n if j == -1 else j
        elif c == "/" and i + 1 < n and sql[i + 1] == "*":
            j = sql.find("*/", i + 2)
            i = n if j == -1 else j + 2
            out.append(" ")
        elif c == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            out.append(" '' ")  # collapse literal to an inert placeholder
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _sql_is_read_only(sql: str) -> tuple[bool, str]:
    """Static, late-bind-tolerant SELECT-only check. Does NOT execute the SQL or require
    its tables to exist. Returns (ok, reason)."""
    skeleton = _strip_sql(sql)
    statements = [s.strip() for s in skeleton.split(";") if s.strip()]
    if not statements:
        return False, "empty sql"
    for stmt in statements:
        if not _STARTS_READ_RE.match(stmt):
            return False, "sql must be a SELECT/WITH read (multi-statement blocks: each @query must be SELECT/WITH)"
        m = _FORBIDDEN_RE.search(stmt)
        if m:
            return False, f"forbidden keyword '{m.group(1).upper()}' — presets are read-only"
    return True, ""


def validate_flexpresets(raw, *, stock_names, filename: str = FLEXPRESETS_FILENAME) -> ValidatedFlexpresets:
    """Validate a parsed `.flexpresets.json` payload.

    raw          : the parsed JSON (dict) — or a str, parsed here as a convenience.
    stock_names  : iterable of reserved stock preset names for this cell type (never shadowed).
    Returns ValidatedFlexpresets(valid rows ready for INSERT, skipped (name, reason)).
    A malformed file fails SAFE: returns empty .valid (never raises), so a bad file can
    never corrupt the cell — the build simply installs stock only.
    """
    result = ValidatedFlexpresets()
    stock = {str(n).lower() for n in stock_names}

    if isinstance(raw, str):
        import json
        try:
            raw = json.loads(raw)
        except Exception as e:
            result.skipped.append((filename, f"invalid JSON: {e}"))
            return result

    if not isinstance(raw, dict):
        result.skipped.append((filename, "top-level must be a JSON object"))
        return result

    version = raw.get("version")
    if version != SUPPORTED_VERSION:
        result.skipped.append((filename, f"unsupported version {version!r} (expected {SUPPORTED_VERSION}); file skipped"))
        return result

    entries = raw.get("presets")
    if not isinstance(entries, list):
        result.skipped.append((filename, "'presets' must be a list"))
        return result

    seen: set[str] = set()
    for idx, e in enumerate(entries):
        label = (e.get("name") if isinstance(e, dict) else None) or f"<entry {idx}>"
        if not isinstance(e, dict):
            result.skipped.append((label, "entry must be an object"))
            continue

        name = e.get("name")
        if not isinstance(name, str) or not _NAME_RE.match(name):
            result.skipped.append((label, "name must match ^[a-z0-9][a-z0-9_-]*$"))
            continue
        if name.lower() in stock:
            result.skipped.append((name, "shadows a stock preset name (reserved) — rename"))
            continue
        if name.lower() in seen:
            result.skipped.append((name, "duplicate name within this file"))
            continue

        desc = e.get("description")
        if not isinstance(desc, str) or not desc.strip():
            result.skipped.append((name, "description is required"))
            continue

        sql = e.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            result.skipped.append((name, "sql is required"))
            continue
        ok, reason = _sql_is_read_only(sql)
        if not ok:
            result.skipped.append((name, reason))
            continue

        params = e.get("params", "")
        if not isinstance(params, str):
            result.skipped.append((name, "params must be a string (space/comma-separated names)"))
            continue

        seen.add(name.lower())
        result.valid.append({
            "name": name,
            "description": desc.strip(),
            "params": params,
            "sql": sql,
            "source": "cell",
        })

    return result
