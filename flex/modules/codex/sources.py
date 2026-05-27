"""Small Codex source resolver.

This only answers: which Codex homes should refresh scan?
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class CodexSource:
    codex_home: Path
    sessions_dir: Path
    state_db: Path
    source_kind: str
    source_field: str
    source_order: int

    @property
    def usable(self) -> bool:
        return self.sessions_dir.is_dir()


def flex_config_path() -> Path:
    return Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")) / "config.json"


def _default_sessions_dir() -> Path:
    return Path.home() / ".codex" / "sessions"


def _read_meta(conn, key: str) -> str | None:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row and row[0] else None


def _entries_from_json(raw: str | None) -> list:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else [value]


def _config_entries() -> list:
    path = flex_config_path()
    if not path.exists():
        return []
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    codex = config.get("codex") if isinstance(config, dict) else {}
    if not isinstance(codex, dict):
        return []
    entries = codex.get("extra_sources") or []
    return entries if isinstance(entries, list) else [entries]


def _source_from_entry(entry, *, source_kind: str, source_field: str, source_order: int) -> CodexSource | None:
    if isinstance(entry, str):
        codex_home = Path(entry).expanduser()
        return CodexSource(
            codex_home=codex_home,
            sessions_dir=codex_home / "sessions",
            state_db=codex_home / "state_5.sqlite",
            source_kind=source_kind,
            source_field=source_field,
            source_order=source_order,
        )
    if not isinstance(entry, dict):
        return None

    codex_home = Path(entry["codex_home"]).expanduser() if entry.get("codex_home") else None
    sessions_dir = Path(entry["sessions_dir"]).expanduser() if entry.get("sessions_dir") else None
    if codex_home is None:
        if sessions_dir is None or sessions_dir.name != "sessions":
            return None
        codex_home = sessions_dir.parent
    if sessions_dir is None:
        sessions_dir = codex_home / "sessions"
    state_db = Path(entry["state_db"]).expanduser() if entry.get("state_db") else codex_home / "state_5.sqlite"
    return CodexSource(
        codex_home=codex_home,
        sessions_dir=sessions_dir,
        state_db=state_db,
        source_kind=str(entry.get("source_kind") or source_kind),
        source_field=source_field,
        source_order=source_order,
    )


_AURA_CODEX_HOME_FIELDS = {
    "omx_box_codex_home": "aura-omx-box",
    "codex_box_codex_home": "aura-codex-box",
    "omx_package_codex_home": "aura-omx-package",
    "codex_package_codex_home": "aura-codex-package",
}
_AURA_CODEX_HOME_FIELD_ORDER = {
    field: idx for idx, field in enumerate(_AURA_CODEX_HOME_FIELDS)
}


def _walk_json(value, prefix: str = ""):
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            yield path, child
            yield from _walk_json(child, path)
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            yield from _walk_json(child, f"{prefix}[{idx}]")


def _codex_home_from_transcript(path: str) -> Path | None:
    transcript = Path(path).expanduser()
    for parent in transcript.parents:
        if parent.name == "sessions":
            return parent.parent
    return None


def _aura_row_entries(row: dict, source_field: str) -> Iterable[tuple[Path, str, str]]:
    codex_home_entries: list[tuple[int, Path, str, str]] = []
    evidence_entries: list[tuple[Path, str, str]] = []
    for path, value in _walk_json(row):
        if not isinstance(value, str) or not value:
            continue
        field = path.rsplit(".", 1)[-1]
        if field in _AURA_CODEX_HOME_FIELDS:
            codex_home_entries.append(
                (
                    _AURA_CODEX_HOME_FIELD_ORDER[field],
                    Path(value).expanduser(),
                    _AURA_CODEX_HOME_FIELDS[field],
                    f"{source_field}.{path}",
                )
            )
        elif field == "native_state_ref":
            codex_home = Path(value).expanduser()
            if (codex_home / "sessions").is_dir():
                evidence_entries.append((codex_home, "aura-native-state", f"{source_field}.{path}"))
        elif field == "transcript_path":
            codex_home = _codex_home_from_transcript(value)
            if codex_home is not None:
                evidence_entries.append((codex_home, "aura-transcript", f"{source_field}.{path}"))

    for _priority, codex_home, source_kind, entry_field in sorted(codex_home_entries):
        yield codex_home, source_kind, entry_field
    yield from evidence_entries


def _aura_entries(registry_path: Path, ledger_path: Path) -> Iterable[tuple[Path, str, str]]:
    if registry_path.exists():
        try:
            data = json.loads(registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = {}
        rows = data.values() if isinstance(data, dict) else data if isinstance(data, list) else []
        for row in rows:
            if isinstance(row, dict):
                yield from _aura_row_entries(row, "aura-registry")

    if ledger_path.exists():
        try:
            lines = ledger_path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            lines = []
        for line in lines:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield from _aura_row_entries(row, "aura-ledger")


def resolve_sources(
    conn,
    *,
    aura_registry: Path | None = None,
    aura_ledger: Path | None = None,
) -> list[CodexSource]:
    sources: list[CodexSource] = []
    order = 0

    primary_meta = _read_meta(conn, "codex_source_path")
    primary = Path(primary_meta).expanduser() if primary_meta else _default_sessions_dir()
    sources.append(
        CodexSource(
            codex_home=primary.parent if primary.name == "sessions" else primary,
            sessions_dir=primary,
            state_db=(primary.parent if primary.name == "sessions" else primary) / "state_5.sqlite",
            source_kind="global",
            source_field="_meta.codex_source_path",
            source_order=order,
        )
    )
    order += 1

    for entry in _entries_from_json(_read_meta(conn, "codex_extra_sources")):
        source = _source_from_entry(
            entry,
            source_kind="explicit",
            source_field="_meta.codex_extra_sources",
            source_order=order,
        )
        if source:
            sources.append(source)
        order += 1

    for entry in _config_entries():
        source = _source_from_entry(
            entry,
            source_kind="config",
            source_field="config.codex.extra_sources",
            source_order=order,
        )
        if source:
            sources.append(source)
        order += 1

    registry_path = aura_registry or Path.home() / ".aura" / "registry" / "seats.json"
    ledger_path = aura_ledger or Path.home() / ".aura" / "registry" / "session-ledger.jsonl"
    for codex_home, kind, source_field in _aura_entries(registry_path, ledger_path):
        if not codex_home.exists():
            continue
        sources.append(
            CodexSource(
                codex_home=codex_home,
                sessions_dir=codex_home / "sessions",
                state_db=codex_home / "state_5.sqlite",
                source_kind=kind,
                source_field=source_field,
                source_order=order,
            )
        )
        order += 1

    resolved: list[CodexSource] = []
    seen: set[str] = set()
    for source in sources:
        key = str(source.codex_home)
        if key in seen:
            continue
        seen.add(key)
        resolved.append(source)
    return resolved


def combined_signature(sources: list[CodexSource]) -> str:
    payload = []
    for source in sources:
        total = 0
        count = 0
        max_mtime = 0
        if source.sessions_dir.is_dir():
            for path in source.sessions_dir.rglob("rollout-*.jsonl"):
                try:
                    st = path.stat()
                except OSError:
                    continue
                total += st.st_size
                count += 1
                max_mtime = max(max_mtime, st.st_mtime_ns)
        payload.append(
            {
                "home": str(source.codex_home),
                "sessions": str(source.sessions_dir),
                "state": str(source.state_db),
                "kind": source.source_kind,
                "usable": source.usable,
                "count": count,
                "total": total,
                "max_mtime": max_mtime,
            }
        )
    return hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
