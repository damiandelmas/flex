"""Temporary read-only composition of registered Flex cells.

Meta is deliberately small: a leading ``ATTACH`` prelude names registered
cells, this module resolves and attaches them read-only, and the existing Flex
query executor handles the remaining SQL.  Attached cells retain their own
schema, identity, retrieval surfaces, and lifecycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from typing import Iterable

from flex import registry


_ALIAS_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_QUERY_START_RE = re.compile(
    r"(?:ATTACH|SELECT|WITH|PRAGMA|EXPLAIN)\b|@",
    re.IGNORECASE,
)
_RESERVED_ALIASES = frozenset({"main", "temp"})


@dataclass(frozen=True)
class Attachment:
    cell_name: str
    alias: str
    path: Path


@dataclass(frozen=True)
class MaterializedCell:
    """A registered cell as it appears on the current SQLite connection."""

    cell_id: str
    cell_name: str
    alias: str
    path: Path


class _MalformedAttach(ValueError):
    pass


def _skip_trivia(sql: str, offset: int) -> int:
    """Skip SQL whitespace and comments without interpreting their contents."""
    length = len(sql)
    while offset < length:
        if sql[offset].isspace():
            offset += 1
            continue
        if sql.startswith("--", offset):
            newline = sql.find("\n", offset + 2)
            return length if newline < 0 else _skip_trivia(sql, newline + 1)
        if sql.startswith("/*", offset):
            end = sql.find("*/", offset + 2)
            return length if end < 0 else _skip_trivia(sql, end + 2)
        break
    return offset


def _keyword_at(sql: str, offset: int, keyword: str) -> int | None:
    end = offset + len(keyword)
    if sql[offset:end].upper() != keyword:
        return None
    if end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
        return None
    return end


def _quoted_value(sql: str, offset: int) -> tuple[str, int]:
    if offset >= len(sql) or sql[offset] not in {"'", '"'}:
        raise _MalformedAttach("expected a quoted registered cell name")
    quote = sql[offset]
    offset += 1
    value: list[str] = []
    while offset < len(sql):
        char = sql[offset]
        if char == quote:
            if offset + 1 < len(sql) and sql[offset + 1] == quote:
                value.append(quote)
                offset += 2
                continue
            if not value:
                raise _MalformedAttach("registered cell name cannot be empty")
            return "".join(value), offset + 1
        value.append(char)
        offset += 1
    raise _MalformedAttach("unterminated registered cell name")


def _parse_attach(sql: str, offset: int) -> tuple[tuple[str, str] | None, int]:
    """Parse one ATTACH statement at *offset* or report that none starts there."""
    after_attach = _keyword_at(sql, offset, "ATTACH")
    if after_attach is None:
        return None, offset

    offset = _skip_trivia(sql, after_attach)
    after_database = _keyword_at(sql, offset, "DATABASE")
    if after_database is not None:
        offset = _skip_trivia(sql, after_database)

    cell_name, offset = _quoted_value(sql, offset)
    offset = _skip_trivia(sql, offset)
    after_as = _keyword_at(sql, offset, "AS")
    if after_as is None:
        raise _MalformedAttach("expected AS after registered cell name")

    offset = _skip_trivia(sql, after_as)
    alias_match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", sql[offset:])
    if alias_match is None:
        raise _MalformedAttach("expected an ASCII SQL alias")
    alias = alias_match.group(0)
    offset += len(alias)

    after_alias = _skip_trivia(sql, offset)
    if after_alias < len(sql) and sql[after_alias] == ";":
        offset = after_alias + 1
    else:
        offset = after_alias
        if offset < len(sql) and _QUERY_START_RE.match(sql, offset) is None:
            raise _MalformedAttach("expected ';' or a query after SQL alias")
    return (cell_name, alias), offset


def _parse_prelude(sql: str) -> tuple[list[tuple[str, str]], str, str | None]:
    offset = _skip_trivia(sql, 0)
    parsed: list[tuple[str, str]] = []
    try:
        while offset < len(sql):
            item, next_offset = _parse_attach(sql, offset)
            if item is None:
                break
            parsed.append(item)
            offset = _skip_trivia(sql, next_offset)
    except _MalformedAttach as exc:
        return [], sql, f"Invalid ATTACH prelude: {exc}"

    if not parsed:
        return [], sql, None
    return parsed, sql[offset:].strip(), None


def _resolve_attachments(
    requested: list[tuple[str, str]],
    *,
    explicit_cells: Iterable[str],
    available_cells: Iterable[str],
    existing_aliases: Iterable[str],
) -> tuple[list[Attachment], str | None]:
    allowed = set(explicit_cells)
    available = sorted(set(available_cells))
    used_aliases = {alias.casefold() for alias in existing_aliases}
    used_aliases.update(_RESERVED_ALIASES)
    resolved: list[Attachment] = []

    for cell_name, alias in requested:
        alias_key = alias.casefold()
        if _ALIAS_RE.fullmatch(alias) is None:
            return [], f"Invalid ATTACH alias: '{alias}'"
        if alias_key in used_aliases:
            return [], f"Duplicate or reserved ATTACH alias: '{alias}'"
        used_aliases.add(alias_key)

        if allowed and cell_name not in allowed:
            return [], (
                f"Cell not allowed by --cell: '{cell_name}'. "
                f"Allowed: {sorted(allowed)}"
            )
        metadata = registry.get_cell_metadata(cell_name)
        if not metadata or not metadata.get("active", 1):
            return [], (
                f"Unknown or inactive cell: '{cell_name}'. "
                f"Available: {available}"
            )
        path = registry.resolve_cell(cell_name)
        if path is None:
            return [], f"Unknown cell: '{cell_name}'. Available: {available}"
        path = Path(path)
        if not path.exists():
            return [], f"Cell path not found on disk: {path}"
        resolved.append(Attachment(cell_name, alias, path))
    return resolved, None


def attach_registered_cells(
    db: sqlite3.Connection,
    sql: str,
    *,
    explicit_cells: Iterable[str] = (),
    available_cells: Iterable[str] = (),
) -> tuple[str, str | None]:
    """Attach a leading registered-cell prelude and return remaining SQL.

    Every request is parsed and resolved before any database is attached.  If
    SQLite rejects an attachment after validation, attachments created by this
    call are detached before the error is returned.
    """
    requested, remaining, error = _parse_prelude(sql)
    if error or not requested:
        return sql if error else remaining, error

    existing_aliases = [row[1] for row in db.execute("PRAGMA database_list")]
    attachments, error = _resolve_attachments(
        requested,
        explicit_cells=explicit_cells,
        available_cells=available_cells,
        existing_aliases=existing_aliases,
    )
    if error:
        return sql, error

    attached: list[str] = []
    try:
        for item in attachments:
            uri = f"{item.path.resolve().as_uri()}?mode=ro"
            db.execute(f'ATTACH DATABASE ? AS "{item.alias}"', (uri,))
            attached.append(item.alias)
    except sqlite3.DatabaseError as exc:
        for alias in reversed(attached):
            try:
                db.execute(f'DETACH DATABASE "{alias}"')
            except sqlite3.DatabaseError:
                pass
        return sql, f"ATTACH failed for '{item.cell_name}': {exc}"

    return remaining, None


def _derived_alias(cell_name: str, used: set[str]) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", cell_name)
    if not base or not re.match(r"[A-Za-z_]", base):
        base = f"cell_{base}"
    alias = base
    suffix = 2
    while alias.casefold() in used or alias.casefold() in _RESERVED_ALIASES:
        alias = f"{base}_{suffix}"
        suffix += 1
    used.add(alias.casefold())
    return alias


def attach_cell_ids(
    db: sqlite3.Connection,
    cell_ids: Iterable[str],
    *,
    explicit_cells: Iterable[str] = (),
) -> tuple[dict[str, MaterializedCell], str | None]:
    """Materialize registered cell identities as read-only schemas.

    Existing schemas, including ``main``, are reused by path identity. Every
    requested identity is validated before a new attachment is created, and a
    SQLite failure detaches every schema created by this call.
    """
    requested_ids = list(dict.fromkeys(str(value) for value in cell_ids if value))
    if not requested_ids:
        return {}, None

    cells_by_id = {
        str(item.get("id")): item
        for item in registry.list_cells()
        if item.get("id")
    }
    allowed = set(explicit_cells)
    database_rows = db.execute("PRAGMA database_list").fetchall()
    existing_by_path: dict[Path, str] = {}
    used_aliases = {str(row[1]).casefold() for row in database_rows}
    used_aliases.update(_RESERVED_ALIASES)
    for row in database_rows:
        if row[2]:
            try:
                existing_by_path[Path(row[2]).resolve()] = str(row[1])
            except OSError:
                continue

    resolved: dict[str, MaterializedCell] = {}
    pending: list[MaterializedCell] = []
    for cell_id in requested_ids:
        metadata = cells_by_id.get(cell_id)
        if not metadata or not metadata.get("active", 1):
            return {}, f"Unknown or inactive cell identity: '{cell_id}'"
        cell_name = str(metadata["name"])
        if allowed and cell_name not in allowed:
            return {}, (
                f"Cell not allowed by --cell: '{cell_name}'. "
                f"Allowed: {sorted(allowed)}"
            )
        path = registry.resolve_cell(cell_name)
        if path is None:
            return {}, f"Unknown cell: '{cell_name}'"
        path = Path(path)
        if not path.exists():
            return {}, f"Cell path not found on disk: {path}"
        resolved_path = path.resolve()
        alias = existing_by_path.get(resolved_path)
        if alias is None:
            alias = _derived_alias(cell_name, used_aliases)
            pending.append(MaterializedCell(cell_id, cell_name, alias, resolved_path))
            existing_by_path[resolved_path] = alias
        resolved[cell_id] = MaterializedCell(
            cell_id, cell_name, alias, resolved_path
        )

    attached: list[str] = []
    try:
        for item in pending:
            uri = f"{item.path.as_uri()}?mode=ro"
            db.execute(f'ATTACH DATABASE ? AS "{item.alias}"', (uri,))
            attached.append(item.alias)
    except sqlite3.DatabaseError as exc:
        for alias in reversed(attached):
            try:
                db.execute(f'DETACH DATABASE "{alias}"')
            except sqlite3.DatabaseError:
                pass
        return {}, f"ATTACH failed for '{item.cell_name}': {exc}"

    return resolved, None
