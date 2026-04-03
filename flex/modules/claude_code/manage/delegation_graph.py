"""Claude Code delegation graph — directed parent->child agent graph.

Only meaningful for cells with _edges_delegations (claude-code cells).
Doc-pac cells have no agent spawning — this module doesn't apply.
"""

from collections import deque

import networkx as nx

from flex.modules.claude_code.manage.noise import ORCHESTRATOR_THRESHOLD


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS _enrich_delegation_graph (
    source_id TEXT PRIMARY KEY,
    agents_spawned INTEGER,
    is_orchestrator INTEGER DEFAULT 0,
    delegation_depth INTEGER,
    parent_session TEXT
)
"""


def build_delegation_graph(db):
    """Build directed graph from delegation edges.

    Returns networkx.DiGraph.
    """
    rows = db.execute("""
        SELECT DISTINCT
            COALESCE(d.parent_source_id, substr(d.chunk_id, 1, 36)) as parent,
            d.child_session_id as child
        FROM _edges_delegations d
        WHERE d.child_session_id IS NOT NULL
    """).fetchall()

    G = nx.DiGraph()
    for r in rows:
        G.add_edge(r['parent'], r['child'])

    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def compute_delegation_metrics(G):
    """Compute per-node metrics on delegation graph.

    Returns dict[node_id -> {agents_spawned, is_orchestrator, delegation_depth, parent_session}].
    """
    if G.number_of_nodes() == 0:
        return {}

    # BFS from roots for depth
    roots = [n for n in G.nodes() if G.in_degree(n) == 0]
    depth = {}
    queue = deque([(r, 0) for r in roots])
    while queue:
        node, d = queue.popleft()
        if node in depth:
            continue
        depth[node] = d
        for child in G.successors(node):
            queue.append((child, d + 1))

    # Handle cycles
    for node in G.nodes():
        if node not in depth:
            depth[node] = -1

    metrics = {}
    for node in G.nodes():
        out = G.out_degree(node)
        parents = list(G.predecessors(node))
        metrics[node] = {
            'agents_spawned': out,
            'is_orchestrator': 1 if out > ORCHESTRATOR_THRESHOLD else 0,
            'delegation_depth': depth.get(node, 0),
            'parent_session': parents[0] if parents else None,
        }

    return metrics
