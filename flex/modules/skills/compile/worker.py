"""
Skills cell compiler — indexes the Claude Code ecosystem.

Source = GitHub repo (one tool/project).
Chunk = catalog entry, full README, README section span, skill artifact.
Tree = three-scale hierarchy: catalog → readme → section spans via _edges_tree.

Non-destructive ingestion:
  - Raw headings preserved in _types_skills.section_heading
  - Canonical section_type mapping at VIEW level only
  - Full README stored in _raw_content (same as arxiv LaTeX)
  - GitHub owner/repo is universal dedup key across awesome lists

Public surface:
    cell name = tools
    implementation package = flex.modules.skills

Entry point:
    python -m flex.modules.skills.compile.worker \
        --cell tools \
        --awesome hesreallyhim/awesome-claude-code \
        --graph
"""

import argparse
import hashlib
import json
import os
import sys
import time
import uuid as uuid_lib
from pathlib import Path
from datetime import datetime, timezone

from flex.core import open_cell, set_meta, validate_cell, log_op


# ═════════════════════════════════════════════════════
# SCHEMA DDL
# ═════════════════════════════════════════════════════

SCHEMA_DDL = """
-- RAW LAYER
CREATE TABLE IF NOT EXISTS _raw_chunks (
    id TEXT PRIMARY KEY,
    content TEXT,
    embedding BLOB,
    timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS _raw_sources (
    source_id TEXT PRIMARY KEY,
    title TEXT,
    source TEXT,
    file_date TEXT,
    author TEXT,
    score INTEGER DEFAULT 0,
    num_comments INTEGER DEFAULT 0,
    url TEXT,
    embedding BLOB
);

CREATE TABLE IF NOT EXISTS _raw_content (
    content_hash TEXT PRIMARY KEY,
    content TEXT
);

-- EDGE LAYER
CREATE TABLE IF NOT EXISTS _edges_source (
    chunk_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_type TEXT DEFAULT 'github',
    position INTEGER
);
CREATE INDEX IF NOT EXISTS idx_es_chunk ON _edges_source(chunk_id);
CREATE INDEX IF NOT EXISTS idx_es_source ON _edges_source(source_id);

CREATE TABLE IF NOT EXISTS _edges_raw_content (
    source_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_type TEXT DEFAULT 'markdown'
);
CREATE INDEX IF NOT EXISTS idx_erc_source ON _edges_raw_content(source_id);

CREATE TABLE IF NOT EXISTS _edges_tree (
    id TEXT NOT NULL,
    parent_id TEXT,
    branch_at TEXT,
    relation TEXT DEFAULT 'child',
    depth INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_et_id ON _edges_tree(id);
CREATE INDEX IF NOT EXISTS idx_et_parent ON _edges_tree(parent_id);

-- TYPES LAYER
CREATE TABLE IF NOT EXISTS _types_skills (
    chunk_id TEXT PRIMARY KEY,
    chunk_type TEXT,
    github_owner TEXT,
    github_repo TEXT,
    tool_name TEXT,
    tool_url TEXT,
    stars INTEGER,
    language TEXT,
    license TEXT,
    topics TEXT,
    last_commit TEXT,
    open_issues INTEGER,
    category TEXT,
    subcategory TEXT,
    tool_type TEXT,
    source_registry TEXT,
    section_heading TEXT,
    heading_command TEXT,
    heading_depth INTEGER,
    quality_score REAL,
    emoji_badges TEXT,
    install_command TEXT,
    skill_name TEXT,
    skill_description TEXT,
    allowed_tools TEXT,
    disallowed_tools TEXT,
    skill_model TEXT,
    permission_mode TEXT,
    user_invocable INTEGER,
    argument_hint TEXT,
    skill_context TEXT,
    max_turns INTEGER,
    preloaded_skills TEXT,
    artifact_path TEXT,
    github_id INTEGER
);

-- SOMA IDENTITY EDGES (compatible with flex/modules/soma/tables.sql)
CREATE TABLE IF NOT EXISTS _edges_repo_identity (
    chunk_id TEXT NOT NULL,
    repo_root TEXT NOT NULL,
    is_tracked INTEGER DEFAULT 1,
    UNIQUE(chunk_id, repo_root)
);
CREATE INDEX IF NOT EXISTS idx_eri_chunk ON _edges_repo_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eri_root ON _edges_repo_identity(repo_root);

CREATE TABLE IF NOT EXISTS _edges_content_identity (
    chunk_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    blob_hash TEXT,
    old_blob_hash TEXT,
    UNIQUE(chunk_id, content_hash)
);
CREATE INDEX IF NOT EXISTS idx_eci_chunk ON _edges_content_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eci_hash ON _edges_content_identity(content_hash);
CREATE INDEX IF NOT EXISTS idx_eci_blob ON _edges_content_identity(blob_hash);

CREATE TABLE IF NOT EXISTS _edges_url_identity (
    chunk_id TEXT NOT NULL,
    url_uuid TEXT NOT NULL,
    UNIQUE(chunk_id, url_uuid)
);
CREATE INDEX IF NOT EXISTS idx_eui_chunk ON _edges_url_identity(chunk_id);
CREATE INDEX IF NOT EXISTS idx_eui_uuid ON _edges_url_identity(url_uuid);

-- ENRICHMENT LAYER
CREATE TABLE IF NOT EXISTS _enrich_source_graph (
    source_id TEXT PRIMARY KEY,
    centrality REAL,
    is_hub INTEGER DEFAULT 0,
    is_bridge INTEGER DEFAULT 0,
    community_id INTEGER
);

-- PRESETS
CREATE TABLE IF NOT EXISTS _presets (
    name TEXT PRIMARY KEY,
    description TEXT,
    params TEXT DEFAULT '',
    sql TEXT
);

-- METADATA + FTS
CREATE TABLE IF NOT EXISTS _meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    content,
    content='_raw_chunks',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS raw_chunks_ai AFTER INSERT ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_ad AFTER DELETE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
END;
CREATE TRIGGER IF NOT EXISTS raw_chunks_au AFTER UPDATE ON _raw_chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, content) VALUES('delete', old.rowid, old.content);
    INSERT INTO chunks_fts(rowid, content) VALUES (new.rowid, new.content);
END;
"""


# ═════════════════════════════════════════════════════
# Source ID helpers
# ═════════════════════════════════════════════════════

def _source_id_from_entry(entry) -> str:
    """Derive source_id from an AwesomeEntry. GitHub owner/repo or URL hash."""
    if entry.github_owner and entry.github_repo:
        return f"{entry.github_owner}/{entry.github_repo}"
    return hashlib.sha256(entry.url.encode()).hexdigest()[:16]


# ═════════════════════════════════════════════════════
# Tree edge builder (same stack algorithm as latex_parser)
# ═════════════════════════════════════════════════════

def build_tree_edges(sections, readme_chunk_id: str) -> list[tuple]:
    """Build _edges_tree rows from parsed markdown sections.

    Args:
        sections: list of (title, content, position, depth) tuples
            from markdown.split_sections(return_depth=True)
        readme_chunk_id: parent chunk id (e.g., 'owner/repo:1')

    Returns:
        List of (id, parent_id, branch_at, relation, depth) tuples.
    """
    edges = []
    # Start with readme chunk as root of this subtree
    stack = [(1, readme_chunk_id)]  # depth 1 = readme chunk

    for title, content, position, heading_depth in sections:
        span_id = f"{readme_chunk_id}:{position + 1}"  # 1-indexed spans
        tree_depth = heading_depth  # h1=1, h2=2, h3=3, etc.

        # Pop until we find a shallower ancestor
        while stack and stack[-1][0] >= tree_depth:
            stack.pop()

        parent_id = stack[-1][1] if stack else readme_chunk_id
        edges.append((span_id, parent_id, parent_id, 'child', tree_depth))
        stack.append((tree_depth, span_id))

    return edges


# ═════════════════════════════════════════════════════
# SOMA identity edge helpers
# ═════════════════════════════════════════════════════

def _insert_identity_edges(db, chunk_id: str, content: str,
                           source_id: str | None = None,
                           github_id: int | None = None,
                           blob_hash: str | None = None,
                           file_path: str | None = None):
    """Insert SOMA-compatible identity edges for a chunk.

    Args:
        db: database connection
        chunk_id: the chunk being identified
        content: chunk text content (for SHA-256 content_hash)
        source_id: 'owner/repo' string (for URL identity)
        github_id: GitHub's immutable repo integer ID (for repo identity)
        blob_hash: GitHub blob SHA-1 from Contents API
        file_path: file path within repo (for URL identity)
    """
    # Content identity — always
    content_hash = hashlib.sha256(content.encode()).hexdigest()
    db.execute("""
        INSERT OR IGNORE INTO _edges_content_identity
        (chunk_id, content_hash, blob_hash)
        VALUES (?, ?, ?)
    """, (chunk_id, content_hash, blob_hash))

    # Repo identity — when github_id available
    if github_id is not None:
        db.execute("""
            INSERT OR IGNORE INTO _edges_repo_identity
            (chunk_id, repo_root, is_tracked)
            VALUES (?, ?, 1)
        """, (chunk_id, f"github:{github_id}"))

    # URL identity — deterministic UUIDv5 from canonical GitHub URL
    if source_id and '/' in source_id:
        owner, repo = source_id.split('/', 1)
        if file_path:
            canonical_url = f"https://github.com/{owner}/{repo}/blob/HEAD/{file_path}"
        else:
            canonical_url = f"https://github.com/{owner}/{repo}"
        url_uuid = str(uuid_lib.uuid5(uuid_lib.NAMESPACE_URL, canonical_url))
        db.execute("""
            INSERT OR IGNORE INTO _edges_url_identity
            (chunk_id, url_uuid)
            VALUES (?, ?)
        """, (chunk_id, url_uuid))


# ═════════════════════════════════════════════════════
# Ingest: catalog entries
# ═════════════════════════════════════════════════════

def ingest_catalog(entries, db, registry_name: str = '') -> tuple[int, int]:
    """Ingest catalog entries from awesome list parsing.

    Returns (sources_created, chunks_created).
    """
    sources_created = 0
    chunks_created = 0

    for entry in entries:
        # Skip entries without a GitHub URL — these are section headings
        # or non-GitHub links that slipped through the parser
        if not entry.github_owner or not entry.github_repo:
            continue

        source_id = _source_id_from_entry(entry)
        chunk_id = f"{source_id}:0"

        # Check if source already exists (dedup)
        existing = db.execute(
            "SELECT source_id FROM _raw_sources WHERE source_id = ?",
            (source_id,)
        ).fetchone()

        if existing:
            # Append to source_registry if new registry
            if registry_name:
                db.execute("""
                    UPDATE _types_skills
                    SET source_registry = source_registry || ',' || ?
                    WHERE chunk_id = ? AND source_registry NOT LIKE '%' || ? || '%'
                """, (registry_name, chunk_id, registry_name))
            continue

        # Create source
        now_ts = int(time.time())
        db.execute("""
            INSERT OR IGNORE INTO _raw_sources
            (source_id, title, source, file_date, author,
             score, num_comments, url, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (
            source_id,
            entry.name,
            'github' if entry.github_owner else 'web',
            '',
            entry.github_owner or '',
            0,  # stars — filled by enrich
            0,  # num_comments — filled later
            entry.url,
        ))
        sources_created += 1

        # Create catalog chunk
        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, entry.description or entry.name, now_ts))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'github', 0)
        """, (chunk_id, source_id))

        # Tree edge: catalog entry is root (depth 0)
        db.execute("""
            INSERT OR IGNORE INTO _edges_tree
            (id, parent_id, branch_at, relation, depth)
            VALUES (?, NULL, NULL, 'root', 0)
        """, (chunk_id,))

        # Types
        db.execute("""
            INSERT OR IGNORE INTO _types_skills
            (chunk_id, chunk_type, github_owner, github_repo,
             tool_name, tool_url, category, subcategory,
             emoji_badges, source_registry, heading_depth)
            VALUES (?, 'catalog', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chunk_id,
            entry.github_owner, entry.github_repo,
            entry.name, entry.url,
            entry.category, entry.subcategory,
            entry.emoji_badges,
            registry_name,
            entry.heading_depth,
        ))

        # Identity edges — content_hash only (no github_id before enrich)
        _insert_identity_edges(db, chunk_id,
                               content=entry.description or entry.name,
                               source_id=source_id)

        chunks_created += 1

    db.commit()
    return sources_created, chunks_created


# ═════════════════════════════════════════════════════
# Ingest: README + span chunks
# ═════════════════════════════════════════════════════

def ingest_readme(source_id: str, readme_content: str, db,
                  blob_hash: str | None = None) -> tuple[int, int]:
    """Ingest a README: store raw, create full chunk, split into span chunks.

    Args:
        source_id: owner/repo string
        readme_content: raw README markdown
        db: database connection
        blob_hash: GitHub blob SHA-1 for the README file

    Returns (readme_chunks, span_chunks).
    """
    from flex.compile.markdown import split_sections

    if not readme_content or not readme_content.strip():
        return 0, 0

    readme_chunk_id = f"{source_id}:1"
    now_ts = int(time.time())

    # Store pristine README in _raw_content
    content_hash = hashlib.sha256(readme_content.encode()).hexdigest()[:16]
    db.execute(
        "INSERT OR IGNORE INTO _raw_content (content_hash, content) VALUES (?, ?)",
        (content_hash, readme_content))
    db.execute(
        "INSERT OR IGNORE INTO _edges_raw_content (source_id, content_hash, content_type) VALUES (?, ?, 'markdown')",
        (source_id, content_hash))

    # Full README as chunk :1
    db.execute("""
        INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
        VALUES (?, ?, NULL, ?)
    """, (readme_chunk_id, readme_content, now_ts))

    db.execute("""
        INSERT OR IGNORE INTO _edges_source
        (chunk_id, source_id, source_type, position)
        VALUES (?, ?, 'github', 1)
    """, (readme_chunk_id, source_id))

    # Get tool_name from catalog chunk
    tool_name_row = db.execute(
        "SELECT tool_name FROM _types_skills WHERE chunk_id = ?",
        (f"{source_id}:0",)
    ).fetchone()
    tool_name = tool_name_row[0] if tool_name_row else source_id

    # Parse github owner/repo from source_id
    parts = source_id.split('/')
    gh_owner = parts[0] if len(parts) == 2 else None
    gh_repo = parts[1] if len(parts) == 2 else None

    db.execute("""
        INSERT OR IGNORE INTO _types_skills
        (chunk_id, chunk_type, github_owner, github_repo, tool_name)
        VALUES (?, 'readme', ?, ?, ?)
    """, (readme_chunk_id, gh_owner, gh_repo, tool_name))

    # Identity edges for README chunk — content_hash + blob_hash + URL
    _insert_identity_edges(db, readme_chunk_id,
                           content=readme_content,
                           source_id=source_id,
                           blob_hash=blob_hash,
                           file_path='README.md')

    # Tree edge: readme is child of catalog root
    catalog_chunk_id = f"{source_id}:0"
    db.execute("""
        INSERT OR IGNORE INTO _edges_tree
        (id, parent_id, branch_at, relation, depth)
        VALUES (?, ?, ?, 'child', 1)
    """, (readme_chunk_id, catalog_chunk_id, catalog_chunk_id))

    readme_chunks = 1

    # Split into span chunks with depth
    sections = split_sections(readme_content, level=1, return_depth=True)
    if not sections:
        sections = split_sections(readme_content, level=2, return_depth=True)

    # Build tree edges for spans
    tree_edges = build_tree_edges(sections, readme_chunk_id)

    span_chunks = 0
    for title, content, position, heading_depth in sections:
        span_id = f"{readme_chunk_id}:{position + 1}"  # 1-indexed

        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (span_id, content, now_ts))

        # Position encoding: 100 + span index
        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'github', ?)
        """, (span_id, source_id, 100 + position + 1))

        heading_cmd = f"h{heading_depth}" if heading_depth > 0 else 'h2'
        db.execute("""
            INSERT OR IGNORE INTO _types_skills
            (chunk_id, chunk_type, github_owner, github_repo, tool_name,
             section_heading, heading_command, heading_depth)
            VALUES (?, 'readme_span', ?, ?, ?, ?, ?, ?)
        """, (span_id, gh_owner, gh_repo, tool_name,
              title, heading_cmd, heading_depth))

        # Identity edges for span — content_hash only
        _insert_identity_edges(db, span_id, content=content,
                               source_id=source_id)

        span_chunks += 1

    # Insert tree edges
    for edge in tree_edges:
        db.execute("""
            INSERT OR IGNORE INTO _edges_tree
            (id, parent_id, branch_at, relation, depth)
            VALUES (?, ?, ?, ?, ?)
        """, edge)

    # Update source num_comments with total chunk count
    total = 1 + span_chunks
    db.execute(
        "UPDATE _raw_sources SET num_comments = num_comments + ? WHERE source_id = ?",
        (total, source_id))

    db.commit()
    return readme_chunks, span_chunks


# ═════════════════════════════════════════════════════
# Ingest: skill artifacts
# ═════════════════════════════════════════════════════

def ingest_skill_artifacts(source_id: str, artifacts, db) -> int:
    """Ingest skill artifacts (SKILL.md, agents, hooks) into the cell.

    Returns number of chunks created.
    """
    if not artifacts:
        return 0

    now_ts = int(time.time())
    catalog_chunk_id = f"{source_id}:0"

    # Get tool_name from catalog
    tool_name_row = db.execute(
        "SELECT tool_name FROM _types_skills WHERE chunk_id = ?",
        (catalog_chunk_id,)
    ).fetchone()
    tool_name = tool_name_row[0] if tool_name_row else source_id

    parts = source_id.split('/')
    gh_owner = parts[0] if len(parts) == 2 else None
    gh_repo = parts[1] if len(parts) == 2 else None

    # Find next available position
    max_pos = db.execute("""
        SELECT COALESCE(MAX(position), 1) FROM _edges_source
        WHERE source_id = ? AND position < 100
    """, (source_id,)).fetchone()[0]
    next_pos = max_pos + 1

    chunks_created = 0
    for artifact in artifacts:
        chunk_id = f"{source_id}:{next_pos}"

        # Store raw content
        content_hash = hashlib.sha256(artifact.content.encode()).hexdigest()[:16]
        db.execute(
            "INSERT OR IGNORE INTO _raw_content (content_hash, content) VALUES (?, ?)",
            (content_hash, artifact.content))

        # Create chunk
        db.execute("""
            INSERT OR IGNORE INTO _raw_chunks (id, content, embedding, timestamp)
            VALUES (?, ?, NULL, ?)
        """, (chunk_id, artifact.content, now_ts))

        db.execute("""
            INSERT OR IGNORE INTO _edges_source
            (chunk_id, source_id, source_type, position)
            VALUES (?, ?, 'github', ?)
        """, (chunk_id, source_id, next_pos))

        # Tree edge from catalog root
        db.execute("""
            INSERT OR IGNORE INTO _edges_tree
            (id, parent_id, branch_at, relation, depth)
            VALUES (?, ?, ?, 'child', 1)
        """, (chunk_id, catalog_chunk_id, catalog_chunk_id))

        # Parse frontmatter fields
        fm = artifact.frontmatter or {}

        # Map frontmatter to _types_skills columns
        # Sanitize: any list/dict value → comma-joined or JSON string
        def _str(v, default=''):
            if v is None: return default
            if isinstance(v, list): return ','.join(str(x) for x in v)
            if isinstance(v, dict): return json.dumps(v)
            return str(v)

        skill_name = _str(fm.get('name'))
        skill_desc = _str(fm.get('description'))
        allowed = _str(fm.get('allowed-tools') or fm.get('tools'))
        disallowed = _str(fm.get('disallowedTools'))
        model = _str(fm.get('model'))
        perm = _str(fm.get('permissionMode'))
        invocable = 1 if fm.get('user-invocable', False) else 0
        arg_hint = _str(fm.get('argument-hint'))
        context = _str(fm.get('context'))
        max_turns = fm.get('maxTurns')
        if isinstance(max_turns, (list, dict)):
            max_turns = None
        skills_list = _str(fm.get('skills'))

        db.execute("""
            INSERT OR IGNORE INTO _types_skills
            (chunk_id, chunk_type, github_owner, github_repo, tool_name,
             skill_name, skill_description, allowed_tools, disallowed_tools,
             skill_model, permission_mode, user_invocable, argument_hint,
             skill_context, max_turns, preloaded_skills, artifact_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chunk_id, artifact.artifact_type,
            gh_owner, gh_repo, tool_name,
            skill_name, skill_desc, allowed, disallowed,
            model, perm, invocable, arg_hint,
            context, max_turns, skills_list, artifact.path,
        ))

        # Identity edges — content_hash + blob_hash + URL with file_path
        _insert_identity_edges(db, chunk_id,
                               content=artifact.content,
                               source_id=source_id,
                               blob_hash=artifact.blob_hash,
                               file_path=artifact.path)

        chunks_created += 1
        next_pos += 1

    db.commit()
    return chunks_created


from flex.compile.embed import embed_new  # noqa: F401 — shared pipeline


# ═════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════

DEFAULT_AWESOME_REPOS = [
    'hesreallyhim/awesome-claude-code',
    'ComposioHQ/awesome-claude-skills',
    'punkpeye/awesome-mcp-servers',
    'travisvn/awesome-claude-skills',
    'BehiSecc/awesome-claude-skills',
    'agarrharr/awesome-cli-apps',
    'VoltAgent/awesome-claude-code-subagents',
]


def main():
    parser = argparse.ArgumentParser(
        description='Index AI development tools into a Flex tools cell')
    parser.add_argument('--cell', default='tools',
                        help='Cell name or path (default: tools)')
    parser.add_argument('--awesome', default=None,
                        help='Comma-separated awesome-list repos to parse')
    parser.add_argument('--enrich', action='store_true',
                        help='Run GitHub API enrichment after catalog ingest')
    parser.add_argument('--readme', action='store_true',
                        help='Fetch + split READMEs into span chunks')
    parser.add_argument('--skills', action='store_true',
                        help='Discover + fetch Claude Code skill artifacts')
    parser.add_argument('--graph', action='store_true',
                        help='Build similarity graph after ingest')
    parser.add_argument('--append', action='store_true',
                        help='Append to existing cell')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show stats without indexing')
    parser.add_argument('--description', default=None,
                        help='Cell description')
    args = parser.parse_args()

    from flex.modules.skills.compile.awesome_parser import parse_awesome_list

    awesome_repos = (args.awesome.split(',') if args.awesome
                     else DEFAULT_AWESOME_REPOS)

    # Parse awesome lists
    all_entries = []
    for repo in awesome_repos:
        repo = repo.strip()
        print(f"\n{'=' * 50}")
        print(f"Parsing: {repo}")
        print(f"{'=' * 50}")
        entries = parse_awesome_list(repo)
        all_entries.extend([(repo, e) for e in entries])

    print(f"\nTotal entries: {len(all_entries)}")

    if args.dry_run:
        # Count unique sources
        seen = set()
        for repo_name, entry in all_entries:
            sid = _source_id_from_entry(entry)
            seen.add(sid)
        print(f"Unique sources: {len(seen)}")
        github_count = sum(1 for _, e in all_entries if e.github_owner)
        print(f"GitHub entries: {github_count}")
        return

    # Resolve / create cell
    cell_path = args.cell
    if not cell_path.endswith('.db'):
        from flex.registry import CELLS_DIR
        CELLS_DIR.mkdir(parents=True, exist_ok=True)
        cell_path = str(CELLS_DIR / f"{args.cell}.db")

    if not args.append and os.path.exists(cell_path):
        os.remove(cell_path)

    db = open_cell(cell_path)
    if not args.append:
        db.executescript(SCHEMA_DDL)

    t0 = time.time()

    # Ingest catalog entries (grouped by registry)
    total_sources = 0
    total_chunks = 0
    for repo_name in awesome_repos:
        repo_name = repo_name.strip()
        repo_entries = [e for r, e in all_entries if r == repo_name]
        # Use short name for registry (last path segment)
        registry_name = repo_name.split('/')[-1] if '/' in repo_name else repo_name
        s, c = ingest_catalog(repo_entries, db, registry_name=registry_name)
        total_sources += s
        total_chunks += c
        print(f"  {repo_name}: +{s} sources, +{c} chunks")

    print(f"\nCatalog: {total_sources} sources, {total_chunks} chunks")

    # GitHub API enrichment
    if args.enrich:
        print("\nEnriching via GitHub API...")
        from flex.modules.skills.compile.github_api import get_repo_metadata
        token = os.environ.get('GITHUB_TOKEN')
        if not token:
            print("  WARNING: GITHUB_TOKEN not set, rate limit is 60 req/hr",
                  file=sys.stderr)

        unenriched = db.execute("""
            SELECT DISTINCT t.github_owner, t.github_repo, es.source_id
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            WHERE t.chunk_type = 'catalog'
            AND t.stars IS NULL
            AND t.github_owner IS NOT NULL
        """).fetchall()

        enriched = 0
        for i, (owner, repo, source_id) in enumerate(unenriched):
            meta = get_repo_metadata(owner, repo, token)
            if meta:
                db.execute("""
                    UPDATE _types_skills SET
                        stars = ?, language = ?, license = ?, topics = ?,
                        last_commit = ?, open_issues = ?, github_id = ?
                    WHERE chunk_id = ?
                """, (meta['stars'], meta['language'], meta['license'],
                      meta['topics'], meta['last_commit'], meta['open_issues'],
                      meta.get('github_id'),
                      f"{source_id}:0"))
                db.execute("UPDATE _raw_sources SET score = ? WHERE source_id = ?",
                           (meta['stars'], source_id))
                # Override description if GitHub has one
                if meta['description']:
                    db.execute("UPDATE _raw_chunks SET content = ? WHERE id = ?",
                               (meta['description'], f"{source_id}:0"))
                enriched += 1

            if (i + 1) % 50 == 0:
                print(f"  Enriched {i+1}/{len(unenriched)}")
                db.commit()

            time.sleep(0.1)

        # Backfill repo identity for ALL chunks sharing the same source_id
        # Catalog chunk has github_id; propagate to readme, span, skill chunks via source
        db.execute("""
            INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked)
            SELECT es2.chunk_id, 'github:' || t.github_id, 1
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            JOIN _edges_source es2 ON es.source_id = es2.source_id
            WHERE t.github_id IS NOT NULL
            AND t.chunk_type = 'catalog'
            AND es2.chunk_id NOT IN (SELECT chunk_id FROM _edges_repo_identity)
        """)

        db.commit()
        print(f"  Enriched: {enriched}/{len(unenriched)} repos")

    # README fetch + split
    if args.readme:
        print("\nFetching READMEs...")
        from flex.modules.skills.compile.github_api import get_readme
        token = os.environ.get('GITHUB_TOKEN')

        sources_needing_readme = db.execute("""
            SELECT DISTINCT t.github_owner, t.github_repo, es.source_id
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            WHERE t.chunk_type = 'catalog'
            AND t.github_owner IS NOT NULL
            AND es.source_id NOT IN (
                SELECT es2.source_id FROM _types_skills t2
                JOIN _edges_source es2 ON t2.chunk_id = es2.chunk_id
                WHERE t2.chunk_type = 'readme'
            )
        """).fetchall()

        readme_total = 0
        span_total = 0
        for i, (owner, repo, source_id) in enumerate(sources_needing_readme):
            result = get_readme(owner, repo, token)
            if result:
                readme_content, readme_blob_hash = result
                r, s = ingest_readme(source_id, readme_content, db,
                                     blob_hash=readme_blob_hash)
                readme_total += r
                span_total += s

            if (i + 1) % 50 == 0:
                print(f"  Fetched {i+1}/{len(sources_needing_readme)}")

            time.sleep(0.1)

        total_chunks += readme_total + span_total
        print(f"  READMEs: {readme_total}, Spans: {span_total}")

    # Skill artifact discovery
    if args.skills:
        print("\nDiscovering skill artifacts...")
        from flex.modules.skills.compile.github_api import discover_skill_artifacts
        token = os.environ.get('GITHUB_TOKEN')

        sources_to_check = db.execute("""
            SELECT DISTINCT t.github_owner, t.github_repo, es.source_id
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            WHERE t.chunk_type = 'catalog'
            AND t.github_owner IS NOT NULL
            AND es.source_id NOT IN (
                SELECT es2.source_id FROM _types_skills t2
                JOIN _edges_source es2 ON t2.chunk_id = es2.chunk_id
                WHERE t2.chunk_type IN ('skill', 'agent', 'hook', 'command')
            )
        """).fetchall()

        skill_total = 0
        for i, (owner, repo, source_id) in enumerate(sources_to_check):
            artifacts = discover_skill_artifacts(owner, repo, token)
            if artifacts:
                n = ingest_skill_artifacts(source_id, artifacts, db)
                skill_total += n

            if (i + 1) % 50 == 0:
                print(f"  Checked {i+1}/{len(sources_to_check)}")

        total_chunks += skill_total
        print(f"  Skill artifacts: {skill_total}")

    # Backfill repo identity for any chunks still missing it
    try:
        db.execute("""
            INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked)
            SELECT es2.chunk_id, 'github:' || t.github_id, 1
            FROM _types_skills t
            JOIN _edges_source es ON t.chunk_id = es.chunk_id
            JOIN _edges_source es2 ON es.source_id = es2.source_id
            WHERE t.github_id IS NOT NULL
            AND t.chunk_type = 'catalog'
            AND es2.chunk_id NOT IN (SELECT chunk_id FROM _edges_repo_identity)
        """)
        db.commit()
    except Exception:
        pass  # Identity tables may not exist in older cells

    validate_cell(db)

    # Embed
    print("\nEmbedding...")
    embedded = embed_new(db)
    print(f"Embedded: {embedded} chunks")

    # Log
    log_op(db, 'skills_ingest', '_raw_chunks',
           params={'sources': total_sources, 'chunks': total_chunks,
                   'embedded': embedded, 'awesome_repos': awesome_repos},
           rows_affected=total_chunks,
           source='skills/compile/worker.py')
    db.commit()

    # Graph
    if args.graph:
        import subprocess
        print("\nBuilding similarity graph...")
        subprocess.run([sys.executable, '-m', 'flex.manage.meditate',
                        '--cell', cell_path], check=True)

    # Views
    views_dir = Path(__file__).parent.parent / 'stock' / 'views'
    if views_dir.exists():
        from flex.views import install_views
        install_views(db, views_dir)
    from flex.views import regenerate_views
    regenerate_views(db)

    # Presets
    from flex.retrieve.presets import install_presets
    preset_dir = Path(__file__).resolve().parent.parent.parent.parent / 'retrieve' / 'presets' / 'general'
    if preset_dir.exists():
        install_presets(db, preset_dir)
    platform_preset_dir = Path(__file__).parent.parent / 'stock' / 'presets'
    if platform_preset_dir.exists():
        install_presets(db, platform_preset_dir)

    # Metadata
    set_meta(db, 'cell_type', 'tools')
    set_meta(db, 'substrate', 'skills')
    set_meta(db, 'surface', 'tools')
    set_meta(db, 'implementation_module', 'flex.modules.skills')
    set_meta(db, 'description',
             args.description or 'AI development tools catalog')
    set_meta(db, 'created_at', datetime.now(timezone.utc).isoformat())
    max_ts = db.execute("SELECT MAX(timestamp) FROM _raw_chunks").fetchone()[0] or 0
    set_meta(db, 'last_pull_ts', str(max_ts))
    set_meta(db, 'last_pull_at', datetime.now(timezone.utc).isoformat())
    set_meta(db, 'awesome_repos', json.dumps(awesome_repos))

    # Register
    from flex.registry import register_cell
    cell_name = args.cell if not args.cell.endswith('.db') else Path(args.cell).stem
    register_cell(
        name=cell_name, path=cell_path, cell_type='tools',
        description=args.description or 'AI development tools catalog',
        lifecycle='refresh',
        refresh_interval=6 * 60 * 60,
        refresh_module='flex.modules.skills.compile.refresh',
    )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — {cell_path}")
    db.close()


if __name__ == '__main__':
    main()
