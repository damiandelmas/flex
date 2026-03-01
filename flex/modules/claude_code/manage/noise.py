"""Claude Code noise filtering — session eligibility and graph filters.

These are specific to claude-code cells where:
- Warmup sessions (message_count < 5): /mcp reconnects, aborts, empty spawns
- Sessions with < 20 chunks carry minimal content (graph exclusion only)

Agent sessions (source_id LIKE 'agent-%') are NOT excluded — they are
sub-sessions linked to parents via the delegation graph. The warmup
heuristic catches junk agents (empty spawns). Substantive agent work
(audits, pipeline runs, research) gets fingerprints and graph signal.
Query-time filtering via delegation_depth or parent_session is available.

Doc-pac cells do NOT use these filters — 3-8 chunk sources are normal there.
"""

# Minimum chunk count for a session to enter the similarity graph
MIN_CHUNKS = 20

# Warmup detection: sessions with fewer than this many JSONL messages are warmup.
# Catches /mcp reconnects (0-1 msgs), aborted sessions, empty agent spawns.
# Reactive: worker evaluates per-session at sync time, not in batch.
WARMUP_MESSAGE_THRESHOLD = 5

# Orchestrator detection threshold (agents spawned)
ORCHESTRATOR_THRESHOLD = 5

# File graph: skip files touched by too many sessions (noise like .gitignore)
MAX_SESSIONS_PER_FILE = 200

# Infrastructure paths that appear across virtually all sessions.
# These carry no project signal — they pollute project attribution votes,
# file co-edit graphs, and source embedding pooling.
INFRA_PATH_PATTERNS = [
    '/.nexus/',        # nexus knowledge injection (reads every session)
    '/.claude/hooks/', # Claude Code hook scripts
]

# Repo-level equivalents for _enrich_repo_identity.repo_path comparisons.
# No trailing slash — repo paths are directory roots.
INFRA_REPO_PATH_PATTERNS = [
    '/.nexus',
    '/.claude',
]


def infra_repo_exclude_sql(col='eri.repo_path'):
    """SQL AND-fragment to exclude infrastructure repo paths from attribution queries.

    Usage — append to WHERE clauses joining _enrich_repo_identity:
        WHERE eri.project IS NOT NULL
          AND {infra_repo_exclude_sql()}
    """
    clauses = [f"{col} NOT LIKE '%{p}%'" for p in INFRA_REPO_PATH_PATTERNS]
    return ' AND '.join(clauses)


def infra_file_exclude_sql(col='t.target_file'):
    """SQL AND-fragment to exclude infrastructure file paths from tool_op queries.

    Usage — append to any WHERE clause joining _edges_tool_ops:
        WHERE rs.git_root IS NULL
          AND {infra_file_exclude_sql()}
    """
    clauses = [f"{col} NOT LIKE '%{p}%'" for p in INFRA_PATH_PATTERNS]
    return ' AND '.join(clauses)


def session_filter_sql():
    """WHERE clause for eligible sessions (summary, profile enrichments).

    Returns SQL that selects source_ids from _raw_sources.
    Filters: no warmups (message_count < WARMUP_MESSAGE_THRESHOLD).
    Agent sessions included — they are sub-sessions, not noise.
    """
    return """
        SELECT source_id FROM _raw_sources
        WHERE source_id NOT IN (
            SELECT source_id FROM _types_source_warmup WHERE is_warmup_only = 1
        )
    """


def graph_filter_sql():
    """WHERE fragment for build_similarity_graph().

    Pass as: build_similarity_graph(db, where=graph_filter_sql())
    Filters: min chunks (content threshold), no warmups.
    Agent sessions included — substantive agents enter the graph.
    """
    return """source_id IN (
        SELECT source_id FROM _edges_source
        GROUP BY source_id HAVING COUNT(*) >= {min_chunks}
    ) AND source_id NOT IN (
        SELECT source_id FROM _types_source_warmup WHERE is_warmup_only = 1
    )""".format(min_chunks=MIN_CHUNKS)
