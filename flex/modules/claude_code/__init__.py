"""Claude Code module — public substrate for coding-agent cells.

Exports:
    ENRICHMENT_STUBS   DDL for the `_enrich_*` / `_types_*` tables the
                      curated views LEFT JOIN against. Stubs let views
                      resolve before first enrichment pass. Other
                      coding-agent modules may reuse this substrate.
    run_enrichment    public install-time enrichment pipeline
                      (formerly install._run_enrichment_quiet).
"""

# Single source of truth for coding-agent enrichment stub tables.
ENRICHMENT_STUBS: list[str] = [
    """CREATE TABLE IF NOT EXISTS _enrich_source_graph (
        source_id TEXT PRIMARY KEY, centrality REAL, is_hub INTEGER DEFAULT 0,
        is_bridge INTEGER DEFAULT 0, community_id INTEGER, community_label TEXT)""",
    """CREATE TABLE IF NOT EXISTS _types_source_warmup (
        source_id TEXT PRIMARY KEY, is_warmup_only INTEGER DEFAULT 0)""",
    """CREATE TABLE IF NOT EXISTS _enrich_session_summary (
        source_id TEXT PRIMARY KEY, fingerprint_index TEXT)""",
    """CREATE TABLE IF NOT EXISTS _enrich_repo_identity (
        repo_root TEXT PRIMARY KEY, repo_path TEXT, project TEXT, git_remote TEXT)""",
    """CREATE TABLE IF NOT EXISTS _enrich_file_graph (
        source_id TEXT PRIMARY KEY, file_community_id INTEGER, file_centrality REAL,
        file_is_hub INTEGER DEFAULT 0, shared_file_count INTEGER)""",
    """CREATE TABLE IF NOT EXISTS _enrich_delegation_graph (
        source_id TEXT PRIMARY KEY, agents_spawned INTEGER,
        is_orchestrator INTEGER DEFAULT 0, delegation_depth INTEGER,
        parent_session TEXT)""",
    """CREATE TABLE IF NOT EXISTS _ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER DEFAULT (strftime('%s','now')),
        operation TEXT, target TEXT, sql TEXT, params TEXT,
        rows_affected INTEGER, source TEXT)""",
    """CREATE TABLE IF NOT EXISTS _views (
        name TEXT PRIMARY KEY, sql TEXT NOT NULL,
        description TEXT, created_at INTEGER)""",
]


def __getattr__(name):
    """Lazy import of run_enrichment — avoids heavy module load at package init."""
    if name == "run_enrichment":
        from flex.modules.claude_code.enrichment import run_enrichment as _fn
        return _fn
    raise AttributeError(name)
