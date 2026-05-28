"""
Flex SDK — public interface for creating cells.

    # Quick: index anything in one line
    from flex.sdk import index
    index("my-docs", ["text one", "text two", "text three"])

    # Structured: cell with types, trees, graph
    from flex.sdk import create, source, ingest, link, embed, graph, register
    db = create("slack", "Slack workspace messages", schema=TYPES_DDL)
    source(db, "general", "#general")
    ingest(db, "general", messages, types="_types_slack")
    link(db, reply_id, parent_id, "reply")
    embed(db)
    graph(db)
    register(db, "slack", "Slack workspace messages")

Under the hood this calls the same machinery that powers every flex cell:
core.py, embed.py, meditate.py, views.py, registry.py. The caller never
imports from flex.modules.* or writes DDL.
"""

import hashlib
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from flex.core import open_cell, set_meta, get_meta, log_op, validate_cell
from flex.registry import CELLS_DIR, register_cell as _register_cell, resolve_cell
from flex.views import regenerate_views, install_views

# Track cell metadata outside the connection (sqlite3.Connection doesn't allow
# arbitrary attributes). Keyed by id(db).
_cell_meta: dict[int, dict] = {}


def _coerce_timestamp(value: Optional[object], *, default_now: bool = True) -> int:
    """Normalize supported timestamp inputs to Unix epoch seconds."""
    if value is None or value == "":
        if default_now:
            return int(time.time())
        raise ValueError("timestamp is required")

    if isinstance(value, bool):
        raise ValueError(f"Invalid timestamp: {value!r}")

    if isinstance(value, (int, float)):
        return int(value)

    if isinstance(value, datetime):
        return int(value.timestamp())

    text = str(value).strip()
    if not text:
        if default_now:
            return int(time.time())
        raise ValueError("timestamp is required")

    try:
        return int(float(text))
    except ValueError:
        pass

    iso_text = text.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(iso_text).timestamp())
    except ValueError as e:
        raise ValueError(f"Invalid timestamp: {value!r}") from e

# ─── Schema ───────────────────────────────────────────────────────────────────

_BASE_SCHEMA = """\
CREATE TABLE IF NOT EXISTS _raw_chunks (
    id          TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    embedding   BLOB,
    timestamp   INTEGER,
    created_at  INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS _raw_sources (
    source_id   TEXT PRIMARY KEY,
    title       TEXT,
    embedding   BLOB,
    timestamp   INTEGER,
    created_at  INTEGER DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id    TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    PRIMARY KEY (chunk_id)
);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

CREATE TABLE IF NOT EXISTS _edges_tree (
    id          TEXT NOT NULL,
    parent_id   TEXT,
    branch_at   TEXT,
    relation    TEXT NOT NULL,
    depth       INTEGER DEFAULT 0,
    PRIMARY KEY (id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_tree_parent   ON _edges_tree(parent_id);
CREATE INDEX IF NOT EXISTS idx_tree_relation ON _edges_tree(relation);

CREATE TABLE IF NOT EXISTS _enrich_source_graph (
    source_id       TEXT PRIMARY KEY,
    centrality      REAL DEFAULT 0,
    community_id    INTEGER,
    community_label TEXT,
    is_hub          INTEGER DEFAULT 0,
    is_bridge       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS _presets (
    name        TEXT PRIMARY KEY,
    description TEXT,
    params      TEXT DEFAULT '',
    sql         TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);

-- FTS sync triggers (match production cells)
CREATE TRIGGER IF NOT EXISTS raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', OLD.rowid, OLD.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_au AFTER UPDATE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', OLD.rowid, OLD.content);
    INSERT INTO chunks_fts(rowid, content) VALUES (NEW.rowid, NEW.content);
END;
"""


# ─── Quick Path ──────────────────────────────────────────────────────────────

def index(
    name: str,
    content: 'list[str] | Path',
    description: Optional[str] = None,
) -> sqlite3.Connection:
    """Index anything in one line. Text list or folder path → queryable cell.

    No schema decisions, no types, no graph. Embed + FTS + views + presets.
    Works 100% of the time.

    Args:
        name: Cell name (used for MCP discovery).
        content: List of strings, or Path to a folder of files.
        description: Optional. Defaults to "{name} — {n} chunks".

    Returns:
        Open sqlite3.Connection. Cell is registered and MCP-queryable.
    """
    if isinstance(content, Path):
        chunks = _walk_and_chunk(content)
    else:
        chunks = [{"content": s} for s in content if s and s.strip()]

    if not chunks:
        raise ValueError("No content to index")

    desc = description or f"{name} — {len(chunks)} chunks"
    db = create(name, desc)

    # Auto-source: one per file, or one default for string lists
    if isinstance(content, Path):
        by_file: dict[str, list] = {}
        for c in chunks:
            f = c.pop('_source', 'default')
            by_file.setdefault(f, []).append(c)
        for filepath, file_chunks in by_file.items():
            source(db, filepath, Path(filepath).name if filepath != 'default' else name)
            ingest(db, filepath, file_chunks)
    else:
        source(db, "default", name)
        ingest(db, "default", chunks)

    try:
        embed(db)
    except RuntimeError as e:
        if 'model not found' in str(e).lower() or 'embedding model' in str(e).lower():
            import sys
            print(
                f"[flex] Embedding model not found. Run 'flex init' first to download it,\n"
                f"       or set NOMIC_API_KEY for cloud embedding.\n"
                f"       Cell created at {_cell_meta.get(id(db), {}).get('path', '?')} with {len(chunks)} chunks (unembedded).",
                file=sys.stderr
            )
        else:
            raise
    # No graph — zero failure modes.
    register(db, name, desc)
    return db


def _walk_and_chunk(folder: Path) -> list[dict]:
    """Walk a folder, chunk files by language. Returns list of chunk dicts."""
    chunks = []
    try:
        from flex.compile.chunkers import chunk_file_body
    except ImportError:
        chunk_file_body = None

    for f in sorted(folder.rglob('*')):
        if not f.is_file() or f.name.startswith('.'):
            continue
        if f.suffix not in ('.md', '.txt', '.py', '.js', '.ts', '.jsx', '.tsx', '.rs', '.go', '.yaml', '.yml', '.json', '.csv'):
            continue
        try:
            text = f.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            continue
        if not text.strip():
            continue

        rel = str(f.relative_to(folder))

        if chunk_file_body and f.suffix in ('.md', '.py', '.js', '.ts', '.jsx', '.tsx'):
            parts = chunk_file_body(text, str(f))
            for part in parts:
                part['_source'] = rel
                if 'content' not in part:
                    part['content'] = part.get('body', text)
                chunks.append(part)
        else:
            chunks.append({"content": text, "_source": rel})

    return chunks


# ─── Structured Path ─────────────────────────────────────────────────────────

def create(
    name: str,
    description: str,
    cell_type: Optional[str] = None,
    schema: Optional[str] = None,
    db_path: Optional[str | Path] = None,
) -> sqlite3.Connection:
    """Create a new cell with the universal schema.

    Args:
        name: Cell name (used for registry and MCP discovery).
        description: Human-readable description.
        cell_type: Type identifier (e.g. 'slack', 'jira'). Defaults to name.
        schema: Additional DDL for module-specific tables (e.g. _types_slack).
                Appended after base schema. Use CREATE TABLE IF NOT EXISTS.
        db_path: Explicit path. Defaults to ~/.flex/cells/{uuid}.db.

    Returns:
        Open sqlite3.Connection to the new cell.
    """
    import uuid

    if db_path is None:
        # Reuse existing cell path if registered — prevents orphan .db files
        existing = resolve_cell(name)
        if existing and existing.exists():
            db_path = existing
        else:
            CELLS_DIR.mkdir(parents=True, exist_ok=True)
            db_path = CELLS_DIR / f"{uuid.uuid4()}.db"

    db = open_cell(str(db_path))
    db.executescript(_BASE_SCHEMA)

    if schema:
        db.executescript(schema)

    set_meta(db, 'description', description)
    set_meta(db, 'cell_type', cell_type or name)
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'embedding_model', 'nomic-embed-text-v1.5')
    set_meta(db, 'embedding_dim', '128')

    _cell_meta[id(db)] = {'path': str(db_path), 'name': name}

    db.commit()
    return db


def source(
    db: sqlite3.Connection,
    source_id: str,
    title: str,
    timestamp: Optional[int] = None,
) -> None:
    """Register a source (document, thread, channel, person, etc.).

    Args:
        db: Cell connection from create().
        source_id: Unique identifier. Human-readable recommended.
        title: Display name.
        timestamp: Unix epoch. Defaults to now.
    """
    ts = _coerce_timestamp(timestamp)
    db.execute(
        "INSERT OR IGNORE INTO _raw_sources (source_id, title, timestamp) VALUES (?, ?, ?)",
        (source_id, title, ts)
    )


def ingest(
    db: sqlite3.Connection,
    source_id: str,
    chunks: list[dict],
    types: Optional[str] = None,
) -> int:
    """Ingest chunks into a cell.

    Args:
        db: Cell connection from create().
        source_id: Which source these chunks belong to.
        chunks: List of dicts. Required key: 'content'. Optional keys:
                'id' (auto-generated if missing), 'timestamp',
                and any keys matching columns in the types table.
        types: Types table name (e.g. '_types_slack'). If provided,
               chunk dict keys matching its columns are auto-inserted.

    Returns:
        Number of chunks inserted.
    """
    # Discover types table columns if provided
    type_cols = set()
    if types:
        import re as _re
        if not _re.match(r'^_types_[a-zA-Z0-9_]+$', types):
            raise ValueError(f"Invalid types table name: {types!r}. Must match _types_[a-z0-9_]+")
        try:
            cols = db.execute(f"PRAGMA table_info([{types}])").fetchall()
            type_cols = {c[1] for c in cols} - {'chunk_id'}
        except sqlite3.OperationalError:
            pass

    count = 0
    for i, chunk in enumerate(chunks):
        content = chunk.get('content', '')
        if not content:
            continue

        chunk_id = chunk.get('id') or _make_chunk_id(source_id, i, content)
        ts = _coerce_timestamp(chunk.get('timestamp'))

        db.execute(
            "INSERT OR IGNORE INTO _raw_chunks (id, content, timestamp) VALUES (?, ?, ?)",
            (chunk_id, content, ts)
        )
        db.execute(
            "INSERT OR IGNORE INTO _edges_source (chunk_id, source_id) VALUES (?, ?)",
            (chunk_id, source_id)
        )

        if types and type_cols:
            # Only use keys that are confirmed column names from PRAGMA
            matched = {k: v for k, v in chunk.items() if k in type_cols}
            if matched:
                safe_cols = [f'[{c}]' for c in matched.keys()]  # bracket-quote each
                cols_str = ', '.join(['[chunk_id]'] + safe_cols)
                placeholders = ', '.join(['?'] * (1 + len(matched)))
                db.execute(
                    f"INSERT OR IGNORE INTO [{types}] ({cols_str}) VALUES ({placeholders})",
                    [chunk_id] + list(matched.values())
                )

        count += 1

    db.commit()
    return count


def link(
    db: sqlite3.Connection,
    child_id: str,
    parent_id: str,
    relation: str = 'reply',
    depth: int = 0,
    branch_at: Optional[str] = None,
) -> None:
    """Link a child to a parent in _edges_tree.

    Args:
        child_id: The child chunk or source ID.
        parent_id: The parent chunk or source ID.
        relation: Relationship type ('reply', 'spawn', 'fork', 'subsection').
        depth: Distance from root.
        branch_at: Chunk where branching occurred (optional).
    """
    db.execute(
        "INSERT OR IGNORE INTO _edges_tree (id, parent_id, branch_at, relation, depth) "
        "VALUES (?, ?, ?, ?, ?)",
        (child_id, parent_id, branch_at, relation, depth)
    )


def embed(db: sqlite3.Connection, batch_size: int = 64, enrich_fn=None) -> int:
    """Embed all chunks missing embeddings, then mean-pool sources.

    Args:
        enrich_fn: Optional callable(str) -> str that transforms content
            before embedding without modifying stored content.
            Example: keyword_enrich.enrich_content for YAKE prefix.

    Returns number of chunks embedded.
    """
    from flex.compile.embed import embed_new
    return embed_new(db, batch_size=batch_size, enrich_fn=enrich_fn)


def graph(
    db: sqlite3.Connection,
    threshold: float = 0.55,
    center: bool = True,
    min_sources: int = 50,
) -> bool:
    """Build similarity graph over source embeddings.

    Computes communities, hubs, bridges, centrality. Writes to
    _enrich_source_graph. Self-protecting: skips if too few sources,
    wipes degenerate graphs (community/source ratio > 0.6).

    Returns True if a quality graph was built.
    """
    source_count = db.execute(
        "SELECT COUNT(*) FROM _raw_sources WHERE embedding IS NOT NULL"
    ).fetchone()[0]

    if source_count < min_sources:
        return False

    try:
        from flex.manage.meditate import build_similarity_graph, compute_scores, persist

        G, edges = build_similarity_graph(
            db, table='_raw_sources', id_col='source_id',
            threshold=threshold, center=center,
        )
        scores = compute_scores(G)

        # Quality gate: detect degenerate graphs
        communities = scores.get('communities', [])
        n_communities = len(communities)
        n_nodes = len(scores.get('centralities', {}))
        if n_nodes > 0 and n_communities > 0 and n_communities / n_nodes > 0.6:
            import sys
            print(f"[flex-sdk] graph degenerate ({n_communities} communities / {n_nodes} sources), skipping", file=sys.stderr)
            db.execute("DELETE FROM _enrich_source_graph")
            db.commit()
            return False

        persist(db, scores, table='_enrich_source_graph', id_col='source_id')
        db.commit()
        return True
    except Exception as e:
        import sys
        print(f"[flex-sdk] graph build failed: {e}", file=sys.stderr)
        return False


def register(
    db: sqlite3.Connection,
    name: Optional[str] = None,
    description: Optional[str] = None,
    cell_type: Optional[str] = None,
    views_dir: Optional[Path] = None,
    presets_dirs: Optional[list[Path]] = None,
    lifecycle: Optional[str] = None,
    refresh_interval: Optional[int] = None,
    refresh_script: Optional[str] = None,
    watch_path: Optional[str] = None,
    watch_pattern: Optional[str] = None,
) -> str:
    """Register cell: validate, generate views, install presets, activate for MCP.

    This is the last call. After register(), the cell is queryable via MCP
    and flex search.

    Args:
        db: Cell connection from create().
        name: Cell name. Defaults to create() value.
        description: Cell description. Defaults to create() value.
        cell_type: Cell type. Defaults to create() value.
        views_dir: Path to curated .sql view files. Auto-generates if None.
        presets_dirs: Extra preset directories beyond the general set.
        lifecycle: 'static' (default) | 'refresh' | 'watch'.
        refresh_interval: Seconds between refreshes (lifecycle='refresh' only).
        refresh_script: Path to script that refreshes this cell.
        watch_path: Directory to monitor (lifecycle='watch' only).
        watch_pattern: Glob pattern within watch_path.

    Returns:
        Cell UUID from registry.
    """
    meta = _cell_meta.get(id(db), {})
    name = name or meta.get('name', 'unnamed')
    db_path = meta.get('path')
    existing_path = resolve_cell(name)
    is_existing = bool(existing_path and existing_path.exists())

    if lifecycle == 'refresh' and refresh_interval is None:
        raise ValueError("refresh_interval is required when lifecycle='refresh'")

    # Validate
    try:
        validate_cell(db)
    except ValueError as e:
        import sys
        print(f"[flex-sdk] validation warning: {e}", file=sys.stderr)

    # Views
    if views_dir and views_dir.exists():
        install_views(db, views_dir)
    regenerate_views(db)

    # Presets
    _GENERAL_PRESETS = Path(__file__).resolve().parent / "retrieve" / "presets" / "general"
    all_preset_dirs = [_GENERAL_PRESETS]
    if presets_dirs:
        all_preset_dirs.extend(presets_dirs)

    from flex.retrieve.presets import install_presets
    db.execute("""CREATE TABLE IF NOT EXISTS _presets (
        name TEXT PRIMARY KEY, description TEXT,
        params TEXT DEFAULT '', sql TEXT
    )""")
    for pd in all_preset_dirs:
        if pd.exists():
            install_presets(db, pd)

    # Meta
    if description:
        set_meta(db, 'description', description)
    if cell_type:
        set_meta(db, 'cell_type', cell_type)

    now = datetime.now(timezone.utc).isoformat()
    set_meta(db, 'registered_at', now)

    # Provenance
    chunk_count = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    source_count = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    log_op(db, 'sdk_register', '_raw_chunks',
           params={'name': name, 'chunks': chunk_count, 'sources': source_count},
           rows_affected=chunk_count, source='flex/sdk.py')

    db.commit()

    # Register in registry.db (with lifecycle if specified)
    cell_id = _register_cell(
        name=name,
        path=db_path or ':memory:',
        cell_type=cell_type or get_meta(db, 'cell_type'),
        description=description or get_meta(db, 'description'),
        lifecycle=lifecycle if lifecycle is not None else ('static' if not is_existing else None),
        refresh_interval=refresh_interval,
        refresh_script=refresh_script,
        watch_path=watch_path,
        watch_pattern=watch_pattern,
    )

    return cell_id


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_chunk_id(source_id: str, position: int, content: str) -> str:
    """Generate a deterministic chunk ID from source + position + content hash."""
    content_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
    return f"{source_id}:{position}:{content_hash}"


def _register_extra_commands(sub):
    import sys
    idx_p = sub.add_parser("index", help="Index a folder into a cell")
    idx_p.add_argument("path", help="Path to folder")
    idx_p.add_argument("--name", default=None)
    idx_p.add_argument("--description", default=None)
    idx_p.add_argument("--exclude", action="append", default=[],
                       help="Exclude patterns (repeatable)")

    def cmd_index(args):
        from pathlib import Path
        path = Path(args.path).resolve()
        if not path.exists():
            print(f"Error: {path} does not exist", file=sys.stderr)
            sys.exit(1)
        name = args.name or path.name.replace('-', '_').replace(' ', '_').lower()

        # Route to markdown module for folders with .md files
        if path.is_dir() and list(path.rglob('*.md')):
            try:
                from flex.modules.markdown.compile.init import compile_vault
                cell_type = 'obsidian' if (path / '.obsidian').is_dir() else 'markdown'
                desc = args.description or f"{name} — {cell_type} vault"
                compile_vault(path, name=name, cell_type=cell_type,
                              description=desc, exclude=args.exclude or None)
                return
            except ImportError:
                pass  # markdown module not available, fall through to sdk.index

        desc = args.description or f"{name} — indexed from {path}"
        print(f"Indexing {path} as '{name}'...")
        db = index(name, path, description=desc)
        chunks = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        sources = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
        db.close()
        print(f"  {chunks} chunks from {sources} sources")
        print(f"  Query: flex search --cell {name} \"@orient\"")

    idx_p.set_defaults(func=cmd_index)


try:
    from flex.registry import register_hook
    register_hook("register_extra_commands", _register_extra_commands)
except ImportError:
    pass
