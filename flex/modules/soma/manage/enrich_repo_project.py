"""
Repo Project Enrichment — SOMA-first project attribution.

Builds _enrich_repo_identity lookup table from live SOMA registry,
then backfills _raw_sources.project + git_root via a prioritized stack:

  1. SOMA repo_root hash       content-addressed, survives everything
  2. primary_cwd + git         Claude Code's own spawn point, any folder structure
  3. target_file git vote      old sessions with no cwd, git-based only
  4. git_root → SOMA upgrade   promote any resolved git_root to SOMA-level name
  5. agent delegation          agents inherit project from spawning parent

No path convention assumptions. Git itself defines project boundaries.
User-specific naming lives in _enrich_repo_identity (via SOMA alias).

Requires: _edges_repo_identity (populated by compile/enrich.py + worker.py)
Optional: SOMA RepoIdentity (~/.soma/repo-identity.db)
"""

import sys
import time
from collections import defaultdict
from pathlib import Path

FLEX_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(FLEX_ROOT))

from flex.core import open_cell
from flex.registry import resolve_cell

CLAUDE_CODE_DB = resolve_cell('claude_code')

CREATE_TABLE = """
CREATE TABLE _enrich_repo_identity (
    repo_root  TEXT PRIMARY KEY,
    repo_path  TEXT,
    project    TEXT,
    git_remote TEXT
)
"""


from flex.modules.soma.lib.git import git_root_from_path as _git_root_from_path
from flex.modules.soma.lib.git import project_from_git_root as _project_from_git_root


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — SOMA repo_root hash
# ─────────────────────────────────────────────────────────────────────────────

def build_repo_map(db) -> dict:
    """
    Resolve all distinct repo_root hashes in the cell via SOMA RepoIdentity.
    Returns {repo_root: {repo_path, project, git_remote}}
    SOMA alias takes priority over path-derived name.
    """
    try:
        from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
        ri = RepoIdentity()
    except ImportError:
        print("  SOMA not available — install soma to resolve repo_root hashes")
        return {}

    rows = db.execute("""
        SELECT DISTINCT repo_root
        FROM _edges_repo_identity
        WHERE repo_root IS NOT NULL AND repo_root != ''
    """).fetchall()

    print(f"  {len(rows)} distinct repo_roots in cell")

    resolved = {}
    unresolved = []

    for (repo_root,) in rows:
        repo = ri.get_by_root_commit(repo_root)
        if repo:
            project = repo.alias or _project_from_git_root(repo.path)
            resolved[repo_root] = {
                'repo_path': repo.path,
                'project': project,
                'git_remote': repo.remote_url or '',
            }
        else:
            unresolved.append(repo_root[:12])

    print(f"  Resolved: {len(resolved)}, Unresolved: {len(unresolved)}")
    if unresolved:
        print(f"  Unresolved (not in SOMA registry): {', '.join(unresolved)}")
        print(f"  Tip: run `soma scan ~/projects` to register missing repos")

    return resolved


def persist_lookup(db, repo_map: dict):
    """Write _enrich_repo_identity lookup table."""
    db.execute("DROP TABLE IF EXISTS _enrich_repo_identity")
    db.execute(CREATE_TABLE)

    for repo_root, info in repo_map.items():
        db.execute(
            """INSERT INTO _enrich_repo_identity (repo_root, repo_path, project, git_remote)
               VALUES (?, ?, ?, ?)""",
            (repo_root, info['repo_path'], info['project'], info['git_remote'])
        )

    print(f"  Written {len(repo_map)} rows to _enrich_repo_identity")


def backfill_from_soma_hash(db) -> int:
    """
    Primary: update project + git_root from SOMA repo_root hash.
    Joins _edges_repo_identity → _enrich_repo_identity. Most reliable signal.
    """
    result = db.execute("""
        UPDATE _raw_sources
        SET
            project = (
                SELECT eri.project
                FROM _edges_source es
                JOIN _edges_repo_identity edri ON es.chunk_id = edri.chunk_id
                JOIN _enrich_repo_identity eri ON edri.repo_root = eri.repo_root
                WHERE es.source_id = _raw_sources.source_id
                  AND eri.project IS NOT NULL AND eri.project != ''
                GROUP BY edri.repo_root
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ),
            git_root = (
                SELECT eri.repo_path
                FROM _edges_source es
                JOIN _edges_repo_identity edri ON es.chunk_id = edri.chunk_id
                JOIN _enrich_repo_identity eri ON edri.repo_root = eri.repo_root
                WHERE es.source_id = _raw_sources.source_id
                  AND eri.repo_path IS NOT NULL AND eri.repo_path != ''
                GROUP BY edri.repo_root
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM _edges_source es
            JOIN _edges_repo_identity edri ON es.chunk_id = edri.chunk_id
            JOIN _enrich_repo_identity eri ON edri.repo_root = eri.repo_root
            WHERE es.source_id = _raw_sources.source_id
              AND eri.project IS NOT NULL AND eri.project != ''
        )
    """)
    return result.rowcount


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — primary_cwd (Claude Code's own convention)
# ─────────────────────────────────────────────────────────────────────────────

def backfill_from_primary_cwd(db, soma_map: dict) -> int:
    """
    Fallback: primary_cwd → git → project.

    Claude Code records where it was spawned. That IS the project context.
    No path convention assumptions — git itself defines the repo boundary.

      git -C cwd show-toplevel → git_root
      soma_map.get(git_root)   → SOMA-level name (if known)
      _project_from_git_root() → default convention (main→parent, worktrees rule)
      _project_from_git_root(cwd) → if no git at all
    """
    rows = db.execute("""
        SELECT source_id, primary_cwd
        FROM _raw_sources
        WHERE git_root IS NULL
          AND primary_cwd IS NOT NULL
          AND primary_cwd != ''
    """).fetchall()

    if not rows:
        return 0

    updated = 0
    for source_id, cwd in rows:
        git_root = _git_root_from_path(cwd)

        if git_root:
            project = soma_map.get(git_root) or _project_from_git_root(git_root)
        else:
            # Not a git repo — use cwd directly, no git_root
            project = _project_from_git_root(cwd)
            git_root = None

        db.execute(
            "UPDATE _raw_sources SET project = ?, git_root = ? WHERE source_id = ? AND git_root IS NULL",
            (project, git_root, source_id)
        )
        if db.execute("SELECT changes()").fetchone()[0]:
            updated += 1

    return updated


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — target_file git vote (old sessions, no cwd)
# ─────────────────────────────────────────────────────────────────────────────

def backfill_from_target_files(db, soma_map: dict) -> int:
    """
    Fallback for old sessions with no primary_cwd.

    Vote on git roots across target_file paths. Git defines the boundary —
    no path convention parsing. Most common git root wins.
    """
    rows = db.execute("""
        SELECT es.source_id, t.target_file, COUNT(*) as n
        FROM _raw_sources rs
        JOIN _edges_source es ON rs.source_id = es.source_id
        JOIN _edges_tool_ops t ON es.chunk_id = t.chunk_id
        WHERE rs.git_root IS NULL
          AND (rs.primary_cwd IS NULL OR rs.primary_cwd = '')
          AND t.target_file IS NOT NULL AND t.target_file != ''
        GROUP BY es.source_id, t.target_file
        ORDER BY es.source_id, n DESC
    """).fetchall()

    if not rows:
        return 0

    # Vote on git_root per session
    session_root_votes: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for source_id, target_file, n in rows:
        git_root = _git_root_from_path(target_file)
        if git_root:
            session_root_votes[source_id][git_root] += n

    updated = 0
    for source_id, root_votes in session_root_votes.items():
        git_root = max(root_votes, key=lambda k: root_votes[k])
        project = soma_map.get(git_root) or _project_from_git_root(git_root)

        db.execute(
            "UPDATE _raw_sources SET project = ?, git_root = ? WHERE source_id = ? AND git_root IS NULL",
            (project, git_root, source_id)
        )
        if db.execute("SELECT changes()").fetchone()[0]:
            updated += 1

    return updated


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — promote git_root → SOMA name
# ─────────────────────────────────────────────────────────────────────────────

def upgrade_from_git_root(db) -> int:
    """
    For sessions that have git_root but no SOMA repo_root entry,
    join git_root to _enrich_repo_identity.repo_path to get SOMA project name.

    Fixes path-derived names that were wrong (e.g. fleet → soma for worktrees).
    Pure SQL — no subprocess.
    """
    result = db.execute("""
        UPDATE _raw_sources
        SET project = (
            SELECT eri.project
            FROM _enrich_repo_identity eri
            WHERE eri.repo_path = _raw_sources.git_root
            LIMIT 1
        )
        WHERE git_root IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM _edges_source es
              JOIN _edges_repo_identity edri ON es.chunk_id = edri.chunk_id
              WHERE es.source_id = _raw_sources.source_id
          )
          AND EXISTS (
              SELECT 1 FROM _enrich_repo_identity eri
              WHERE eri.repo_path = _raw_sources.git_root
          )
    """)
    return result.rowcount


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — agent delegation
# ─────────────────────────────────────────────────────────────────────────────

def backfill_from_delegations(db) -> int:
    """
    Agents are ephemeral — no cwd, no files. Their project IS their parent's.
    Walk _edges_delegations to inherit project + git_root from spawning session.
    """
    result = db.execute("""
        UPDATE _raw_sources
        SET
            project = (
                SELECT rs_parent.project
                FROM _edges_delegations d
                JOIN _raw_sources rs_parent
                  ON COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36)) = rs_parent.source_id
                WHERE d.child_session_id = _raw_sources.source_id
                  AND rs_parent.project IS NOT NULL AND rs_parent.project != ''
                LIMIT 1
            ),
            git_root = (
                SELECT rs_parent.git_root
                FROM _edges_delegations d
                JOIN _raw_sources rs_parent
                  ON COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36)) = rs_parent.source_id
                WHERE d.child_session_id = _raw_sources.source_id
                  AND rs_parent.git_root IS NOT NULL
                LIMIT 1
            )
        WHERE _raw_sources.git_root IS NULL
          AND EXISTS (
              SELECT 1 FROM _edges_delegations d
              WHERE d.child_session_id = _raw_sources.source_id
          )
    """)
    return result.rowcount


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Repo Project Enrichment (SOMA-first)")
    print("=" * 60)

    t_start = time.time()
    db = open_cell(str(CLAUDE_CODE_DB))
    print(f"\nOpened: {CLAUDE_CODE_DB}")

    tables = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    if '_edges_repo_identity' not in tables:
        print("\n_edges_repo_identity not found. Run compile pipeline first.")
        db.close()
        return

    # 1. Build SOMA lookup table
    print("\nResolving repo_root hashes via SOMA...")
    repo_map = build_repo_map(db)

    if not repo_map:
        print("Nothing resolved. Exiting.")
        db.close()
        return

    print("\nWriting _enrich_repo_identity...")
    persist_lookup(db, repo_map)
    db.commit()

    # soma_map: git_root path → project (for fast lookup in fallbacks)
    soma_map = {info['repo_path']: info['project'] for info in repo_map.values()}

    # 2. SOMA hash — primary
    print("\n[1] SOMA repo_root hash...")
    n = backfill_from_soma_hash(db)
    db.commit()
    print(f"    {n} sessions")

    # 3. primary_cwd → git
    print("\n[2] primary_cwd + git...")
    n = backfill_from_primary_cwd(db, soma_map)
    db.commit()
    print(f"    {n} sessions")

    # 4. target_file git vote (old sessions with no cwd)
    print("\n[3] target_file git vote (no cwd)...")
    n = backfill_from_target_files(db, soma_map)
    db.commit()
    print(f"    {n} sessions")

    # 5. Upgrade any git_root to SOMA name
    print("\n[4] Upgrade git_root → SOMA name...")
    n = upgrade_from_git_root(db)
    db.commit()
    print(f"    {n} sessions upgraded")

    # 6. Agent delegation
    print("\n[5] Agent delegation...")
    n = backfill_from_delegations(db)
    db.commit()
    print(f"    {n} sessions")

    # Summary
    total = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    resolved = db.execute("SELECT COUNT(*) FROM _raw_sources WHERE git_root IS NOT NULL").fetchone()[0]
    null_project = db.execute("SELECT COUNT(*) FROM _raw_sources WHERE project IS NULL").fetchone()[0]
    print(f"\nResolution: {resolved}/{total} sessions have git_root")
    print(f"Unresolvable: {total - resolved} (no file signal, no parent)")
    if null_project:
        print(f"NULL project: {null_project}")

    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed:.1f}s")
    db.close()


if __name__ == "__main__":
    main()
