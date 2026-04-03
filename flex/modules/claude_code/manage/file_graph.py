"""Claude Code file co-edit graph — bipartite projection from shared file_uuids.

Two sessions are connected if they touched the same file. Weight = shared files.
Only meaningful for cells with _edges_file_identity (claude-code cells).
Doc-pac cells have no file operations — this module doesn't apply.
"""

from collections import defaultdict

import networkx as nx

from flex.modules.claude_code.manage.noise import MAX_SESSIONS_PER_FILE


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_file_graph (
    source_id TEXT PRIMARY KEY,
    file_community_id INTEGER,
    file_centrality REAL,
    file_is_hub INTEGER DEFAULT 0,
    shared_file_count INTEGER
)
"""


def build_file_graph(db):
    """Build session-session graph from shared file_uuids.

    Returns (networkx.Graph, dict[source_id -> set of file_uuids]).
    """
    rows = db.execute("""
        SELECT DISTINCT es.source_id, fi.file_uuid
        FROM _edges_file_identity fi
        JOIN _edges_source es ON fi.chunk_id = es.chunk_id
        WHERE fi.file_uuid IS NOT NULL AND fi.file_uuid != ''
    """).fetchall()

    file_to_sessions = defaultdict(set)
    session_files = defaultdict(set)
    for r in rows:
        file_to_sessions[r['file_uuid']].add(r['source_id'])
        session_files[r['source_id']].add(r['file_uuid'])

    print(f"  {len(file_to_sessions)} unique files, {len(session_files)} sessions with file ops")

    # Skip files touched by too many sessions (noise)
    skipped_files = 0
    for file_uuid in list(file_to_sessions.keys()):
        if len(file_to_sessions[file_uuid]) > MAX_SESSIONS_PER_FILE:
            del file_to_sessions[file_uuid]
            skipped_files += 1
    if skipped_files:
        print(f"  Skipped {skipped_files} files touched by >{MAX_SESSIONS_PER_FILE} sessions")

    # Bipartite projection
    G = nx.Graph()
    for sid in session_files:
        G.add_node(sid)

    edge_weights = defaultdict(int)
    for file_uuid, sessions in file_to_sessions.items():
        sessions = list(sessions)
        for i in range(len(sessions)):
            for j in range(i + 1, len(sessions)):
                pair = tuple(sorted([sessions[i], sessions[j]]))
                edge_weights[pair] += 1

    for (s1, s2), weight in edge_weights.items():
        G.add_edge(s1, s2, weight=weight)

    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G, session_files


def analyze_file_graph(G):
    """Louvain + PageRank + hub detection on file co-edit graph.

    Returns (partition dict, pagerank dict, hubs set).
    """
    from networkx.algorithms import community as nx_community

    if G.number_of_nodes() == 0:
        return {}, {}, set()

    try:
        partition_sets = nx_community.louvain_communities(G, weight='weight', seed=42)
        partition = {}
        for i, members in enumerate(partition_sets):
            for m in members:
                partition[m] = i
    except (nx.NetworkXError, ValueError, ZeroDivisionError):
        partition = {n: 0 for n in G.nodes()}

    try:
        pr = nx.pagerank(G, weight='weight')
    except (nx.NetworkXError, nx.PowerIterationFailedConvergence, ZeroDivisionError):
        pr = {n: 0.0 for n in G.nodes()}

    # Hubs: top 10% by PageRank within each community
    comm_scores = defaultdict(list)
    for node, comm_id in partition.items():
        comm_scores[comm_id].append((node, pr.get(node, 0)))

    hubs = set()
    for comm_id, scores in comm_scores.items():
        scores.sort(key=lambda x: x[1], reverse=True)
        n_hubs = max(1, len(scores) // 10)
        for node, _ in scores[:n_hubs]:
            hubs.add(node)

    return partition, pr, hubs
