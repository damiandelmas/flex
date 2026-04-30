"""Tree graph primitive + claude_code delegation wrapper.

The generic primitive (``build_tree_graph``) reads a parent→child edge table
and computes per-node tree metrics: depth from roots, out-degree
(children spawned), orchestrator flag, and the first parent. Works on any
table with parent/child columns — claude_code delegations, markdown section
hierarchy, reply trees, etc.

The claude_code wrapper (``build_delegation_graph``) calls the primitive with
delegation-specific defaults to preserve the pre-refactor signature.
"""

from collections import deque

import networkx as nx

from flex.modules.claude_code.manage.noise import ORCHESTRATOR_THRESHOLD


CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {output_table} (
    source_id TEXT PRIMARY KEY,
    agents_spawned INTEGER,
    is_orchestrator INTEGER DEFAULT 0,
    delegation_depth INTEGER,
    parent_session TEXT
)
"""


# Legacy constant preserved for callers that still import it.
CREATE_TABLE = CREATE_TABLE_TEMPLATE.format(output_table='_enrich_delegation_graph')


def build_tree_graph(
    db,
    *,
    edge_table: str = '_edges_delegations',
    parent_col: str = 'parent_source_id',
    child_col: str = 'child_session_id',
    relation_filter: str | None = None,
    output_table: str = '_enrich_delegation_graph',
) -> nx.DiGraph:
    """Directed tree graph built from a parent/child edge table.

    Args:
        edge_table: edge table (e.g. ``_edges_tree``, ``_edges_delegations``)
        parent_col: column naming the parent node
        child_col: column naming the child node
        relation_filter: optional WHERE clause fragment, e.g.
            ``"relation = 'subsection'"``. Combined with the non-null child
            guard via AND.
        output_table: informational only — caller owns CREATE / writes.

    Returns:
        ``nx.DiGraph`` (may contain multiple roots and cycles if the input
        isn't strictly a tree — downstream metrics handle that).
    """
    where = f"{child_col} IS NOT NULL"
    if relation_filter:
        where = f"({where}) AND ({relation_filter})"

    # For delegations, the legacy fallback uses substr(chunk_id, 1, 36) to
    # derive a parent when parent_col is NULL. We replicate that only for the
    # default delegation shape — generic callers get a straight COALESCE.
    if edge_table == '_edges_delegations' and parent_col == 'parent_source_id':
        parent_expr = f"COALESCE({parent_col}, substr(chunk_id, 1, 36))"
    else:
        parent_expr = parent_col

    rows = db.execute(f"""
        SELECT DISTINCT
            {parent_expr} AS parent,
            {child_col} AS child
        FROM {edge_table}
        WHERE {where}
    """).fetchall()

    G = nx.DiGraph()
    for r in rows:
        if r['parent'] is None:
            continue
        G.add_edge(r['parent'], r['child'])

    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def build_delegation_graph(db) -> nx.DiGraph:
    """Back-compat wrapper — claude_code delegation graph."""
    return build_tree_graph(
        db,
        edge_table='_edges_delegations',
        parent_col='parent_source_id',
        child_col='child_session_id',
        output_table='_enrich_delegation_graph',
    )


def compute_delegation_metrics(G):
    """Compute per-node metrics on any directed tree graph.

    Returns ``dict[node_id -> {agents_spawned, is_orchestrator,
    delegation_depth, parent_session}]``.
    """
    if G.number_of_nodes() == 0:
        return {}

    # BFS from roots for depth
    roots = [n for n in G.nodes() if G.in_degree(n) == 0]
    depth: dict = {}
    queue = deque([(r, 0) for r in roots])
    while queue:
        node, d = queue.popleft()
        if node in depth:
            continue
        depth[node] = d
        for child in G.successors(node):
            queue.append((child, d + 1))

    # Handle cycles / disconnected nodes
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
