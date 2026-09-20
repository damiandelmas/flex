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
import uuid

from flex import registry


_ALIAS_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_QUERY_START_RE = re.compile(
    r"(?:ATTACH|SELECT|WITH|PRAGMA|EXPLAIN)\b|@",
    re.IGNORECASE,
)
_RESERVED_ALIASES = frozenset({"main", "temp"})
RETRIEVAL_WORLD_MEMBERS = "_flex_retrieval_world_members"
RETRIEVAL_WORLD_OBJECTS = "_flex_retrieval_world_objects"
RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS = "_flex_retrieval_world_default_exclusions"
_WORLD_OBJECT_NAMESPACE = uuid.UUID("6e8dd3b8-a72f-5e22-92d2-cc11e96ad43f")
_RETRIEVAL_COLUMNS = {
    "_raw_sources": frozenset({"source_id"}),
    "_raw_chunks": frozenset({"id", "content", "embedding"}),
}


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


@dataclass(frozen=True)
class RetrievalWorldMember:
    """One trusted native retrieval member attached to the current query."""

    ordinal: int
    world_name: str
    cell_id: str
    cell_name: str
    schema_alias: str
    path: Path


class _MalformedAttach(ValueError):
    pass


def flex_world_id(cell_id: str, native_id: str) -> str:
    """Return the stable world coordinate for one cell-native object."""
    cell_id = str(cell_id or "")
    native_id = str(native_id or "")
    if not cell_id or not native_id:
        raise ValueError("world object identity requires cell_id and native_id")
    return str(uuid.uuid5(_WORLD_OBJECT_NAMESPACE, cell_id + "\0" + native_id))


def has_retrieval_world(db: sqlite3.Connection) -> bool:
    """Return whether this connection carries a trusted retrieval declaration."""
    names = {
        str(row[0])
        for row in db.execute(
            "SELECT name FROM sqlite_temp_master WHERE type='table' "
            "AND name IN (?, ?)",
            (RETRIEVAL_WORLD_MEMBERS, RETRIEVAL_WORLD_OBJECTS),
        ).fetchall()
    }
    return names == {RETRIEVAL_WORLD_MEMBERS, RETRIEVAL_WORLD_OBJECTS}


def retrieval_world_members(
    db: sqlite3.Connection,
) -> tuple[RetrievalWorldMember, ...]:
    """Read the validated ordered declaration installed on *db*."""
    if not has_retrieval_world(db):
        return ()
    rows = db.execute(
        f"SELECT ordinal,world_name,cell_id,cell_name,schema_alias "
        f"FROM temp.{RETRIEVAL_WORLD_MEMBERS} ORDER BY ordinal"
    ).fetchall()
    paths = {
        str(row[1]): Path(str(row[2])).resolve()
        for row in db.execute("PRAGMA database_list").fetchall()
        if row[2]
    }
    members: list[RetrievalWorldMember] = []
    for row in rows:
        alias = str(row[4])
        path = paths.get(alias)
        if path is None:
            raise RuntimeError(
                f"retrieval world member is no longer attached: {row[3]}"
            )
        members.append(
            RetrievalWorldMember(
                ordinal=int(row[0]),
                world_name=str(row[1]),
                cell_id=str(row[2]),
                cell_name=str(row[3]),
                schema_alias=alias,
                path=path,
            )
        )
    return tuple(members)


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


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _retrieval_profile_issues(path: Path) -> list[str]:
    """Return generic native-retrieval contract failures for one cell."""
    uri = f"{path.resolve().as_uri()}?mode=ro"
    source = sqlite3.connect(uri, uri=True)
    try:
        relations = {
            str(row[0]): (str(row[1]), str(row[2] or ""))
            for row in source.execute(
                "SELECT name,type,sql FROM sqlite_master "
                "WHERE type IN ('table','view')"
            ).fetchall()
        }
        issues: list[str] = []
        required = set(_RETRIEVAL_COLUMNS) | {"chunks_fts"}
        missing = sorted(required - relations.keys())
        if missing:
            issues.append("missing relations: " + ", ".join(missing))
        for relation, expected in _RETRIEVAL_COLUMNS.items():
            if relation not in relations:
                continue
            columns = {
                str(row[1])
                for row in source.execute(
                    f"PRAGMA table_info({_quote_identifier(relation)})"
                ).fetchall()
            }
            absent = sorted(expected - columns)
            if absent:
                issues.append(
                    f"{relation} missing columns: " + ", ".join(absent)
                )
        if "chunks_fts" in relations:
            relation_type, create_sql = relations["chunks_fts"]
            if (
                relation_type != "table"
                or re.search(
                    r"\bCREATE\s+VIRTUAL\s+TABLE\b.*\bUSING\s+fts5\s*\(",
                    create_sql,
                    re.IGNORECASE | re.DOTALL,
                ) is None
            ):
                issues.append("chunks_fts is not an FTS5 virtual table")
        return issues
    finally:
        source.close()


def install_retrieval_world(
    db: sqlite3.Connection,
    *,
    world_name: str,
    members: Iterable[tuple[str, str]],
) -> tuple[RetrievalWorldMember, ...]:
    """Install one trusted, query-local native retrieval declaration.

    ``members`` is supplied by trusted product code, never parsed from caller
    SQL.  All Registry rows, paths, aliases, and common retrieval primitives
    are validated before the first ATTACH.  The resulting TEMP tables are the
    sole dispatch marker understood by world-aware materializers; unrelated
    attached databases never become retrieval members.

    This function creates an empty object-coordinate table.  The product world
    inserts its source/chunk rows after installing its own canonical views.
    """
    world_name = str(world_name or "").strip()
    if not world_name:
        raise ValueError("retrieval world name cannot be blank")
    requested = [(str(name), str(alias)) for name, alias in members]
    if not requested:
        raise ValueError("retrieval world requires at least one member")
    if len({name for name, _ in requested}) != len(requested):
        raise ValueError("retrieval world member names must be unique")
    if len({alias.casefold() for _, alias in requested}) != len(requested):
        raise ValueError("retrieval world aliases must be unique")

    temp_names = {
        str(row[0]) for row in db.execute(
            "SELECT name FROM sqlite_temp_master WHERE name IN (?, ?)",
            (RETRIEVAL_WORLD_MEMBERS, RETRIEVAL_WORLD_OBJECTS),
        ).fetchall()
    }
    if temp_names:
        raise RuntimeError("a retrieval world is already installed")

    database_rows = db.execute("PRAGMA database_list").fetchall()
    used_aliases = {str(row[1]).casefold() for row in database_rows}
    used_aliases.update(_RESERVED_ALIASES)
    resolved: list[RetrievalWorldMember] = []
    seen_ids: set[str] = set()
    for ordinal, (cell_name, alias) in enumerate(requested):
        if not cell_name:
            raise ValueError("retrieval world member name cannot be blank")
        if _ALIAS_RE.fullmatch(alias) is None:
            raise ValueError(f"invalid retrieval world alias: {alias!r}")
        if alias.casefold() in used_aliases:
            raise ValueError(f"duplicate or reserved retrieval world alias: {alias!r}")
        used_aliases.add(alias.casefold())

        metadata = registry.get_cell_metadata(cell_name)
        if not metadata or not metadata.get("active", 1):
            raise RuntimeError(
                f"retrieval world member is unknown or inactive: {cell_name}"
            )
        cell_id = str(metadata.get("id") or "").strip()
        if not cell_id:
            raise RuntimeError(
                f"retrieval world member has no durable cell ID: {cell_name}"
            )
        if cell_id in seen_ids:
            raise RuntimeError(
                f"retrieval world member identity is duplicated: {cell_id}"
            )
        seen_ids.add(cell_id)
        path = registry.resolve_cell(cell_name)
        if path is None:
            raise RuntimeError(f"retrieval world member is unresolved: {cell_name}")
        path = Path(path).resolve()
        registered_path = metadata.get("path")
        if (
            registered_path
            and Path(str(registered_path)).resolve() != path
        ):
            raise RuntimeError(
                "retrieval world member Registry path changed during preflight: "
                f"{cell_name}"
            )
        if not path.is_file():
            raise RuntimeError(f"retrieval world member path is missing: {path}")
        profile_issues = _retrieval_profile_issues(path)
        if profile_issues:
            raise RuntimeError(
                f"retrieval world member {cell_name!r} lacks common profile: "
                + "; ".join(profile_issues)
            )
        resolved.append(
            RetrievalWorldMember(
                ordinal=ordinal,
                world_name=world_name,
                cell_id=cell_id,
                cell_name=cell_name,
                schema_alias=alias,
                path=path,
            )
        )

    attached: list[str] = []
    try:
        for member in resolved:
            uri = f"{member.path.as_uri()}?mode=ro"
            db.execute(
                f"ATTACH DATABASE ? AS {_quote_identifier(member.schema_alias)}",
                (uri,),
            )
            attached.append(member.schema_alias)

        db.execute(
            f"CREATE TEMP TABLE {RETRIEVAL_WORLD_MEMBERS}("
            "ordinal INTEGER PRIMARY KEY, world_name TEXT NOT NULL, "
            "cell_id TEXT NOT NULL UNIQUE, cell_name TEXT NOT NULL UNIQUE, "
            "schema_alias TEXT NOT NULL UNIQUE)"
        )
        db.executemany(
            f"INSERT INTO {RETRIEVAL_WORLD_MEMBERS} VALUES(?,?,?,?,?)",
            [
                (
                    item.ordinal,
                    item.world_name,
                    item.cell_id,
                    item.cell_name,
                    item.schema_alias,
                )
                for item in resolved
            ],
        )
        db.execute(
            f"CREATE TEMP TABLE {RETRIEVAL_WORLD_OBJECTS}("
            "object_kind TEXT NOT NULL CHECK(object_kind IN ('source','chunk')), "
            "id TEXT NOT NULL, cell_id TEXT NOT NULL, native_id TEXT NOT NULL, "
            "PRIMARY KEY(object_kind,id), "
            "UNIQUE(object_kind,cell_id,native_id), "
            f"FOREIGN KEY(cell_id) REFERENCES {RETRIEVAL_WORLD_MEMBERS}(cell_id))"
        )
        db.execute(
            f"CREATE INDEX temp.idx_flex_world_objects_native "
            f"ON {RETRIEVAL_WORLD_OBJECTS}(cell_id,native_id,object_kind)"
        )
        db.execute(
            f"CREATE TEMP TABLE {RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS}("
            "object_kind TEXT NOT NULL CHECK(object_kind IN ('source','chunk')), "
            "id TEXT NOT NULL, PRIMARY KEY(object_kind,id), "
            f"FOREIGN KEY(object_kind,id) REFERENCES {RETRIEVAL_WORLD_OBJECTS}"
            "(object_kind,id))"
        )
        db.create_function("flex_world_id", 2, flex_world_id, deterministic=True)
    except Exception:
        try:
            db.execute(
                f"DROP TABLE IF EXISTS temp.{RETRIEVAL_WORLD_DEFAULT_EXCLUSIONS}"
            )
            db.execute(f"DROP TABLE IF EXISTS temp.{RETRIEVAL_WORLD_OBJECTS}")
            db.execute(f"DROP TABLE IF EXISTS temp.{RETRIEVAL_WORLD_MEMBERS}")
        except sqlite3.DatabaseError:
            pass
        for alias in reversed(attached):
            try:
                db.execute(f"DETACH DATABASE {_quote_identifier(alias)}")
            except sqlite3.DatabaseError:
                pass
        raise

    return tuple(resolved)
