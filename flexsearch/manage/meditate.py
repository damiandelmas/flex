"""
Flexsearch Meditate — offline graph intelligence.

Produces columns that SQL consumes. networkx computes things SQL can't:
PageRank, Louvain communities, hub/bridge identification.

Output is INSERT INTO _enrich_* tables. Once persisted as columns,
SQL composes them freely. networkx is the producer, SQL is the consumer.

Four functions:
- build_similarity_graph() → embeddings → networkx graph
- compute_scores()         → louvain, pagerank, hubs, bridges
- persist()                → INSERT INTO _enrich_*, regenerate views
- run_sandbox()            → execute arbitrary networkx script
"""

import numpy as np
import sqlite3
from typing import Optional


def build_similarity_graph(db: sqlite3.Connection, table: str = '_raw_sources',
                           id_col: str = 'id', embedding_col: str = 'embedding',
                           threshold: float = 0.5, top_k: int = None):
    """
    Build similarity graph from embeddings via matrix multiply.

    Args:
        db: SQLite connection
        table: Table with embeddings
        id_col: Column with item identifiers
        embedding_col: Column with embedding blobs
        threshold: Minimum cosine similarity for edge creation
        top_k: If set, keep only top K neighbors per node

    Returns:
        (NetworkX graph, edge_count) or (None, 0)
    """
    import networkx as nx

    # Load embeddings
    rows = db.execute(
        f"SELECT [{id_col}], [{embedding_col}] FROM [{table}] "
        f"WHERE [{embedding_col}] IS NOT NULL"
    ).fetchall()

    if not rows:
        return None, 0

    item_ids = []
    embeddings = []
    for row in rows:
        item_ids.append(row[0])
        embeddings.append(np.frombuffer(row[1], dtype=np.float32))

    # Matrix multiply for all-pairs cosine similarity
    emb_matrix = np.vstack(embeddings)
    norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
    emb_matrix = emb_matrix / (norms + 1e-10)
    sim_matrix = emb_matrix @ emb_matrix.T

    # Build graph
    G = nx.Graph()
    for item_id in item_ids:
        G.add_node(item_id)

    n = len(item_ids)
    edge_count = 0

    if top_k:
        for i in range(n):
            sims = sim_matrix[i]
            top_indices = np.argsort(sims)[::-1][1:top_k + 1]
            for j in top_indices:
                if sims[j] > 0.1:
                    G.add_edge(item_ids[i], item_ids[j],
                               weight=float(sims[j]))
                    edge_count += 1
    else:
        for i in range(n):
            for j in range(i + 1, n):
                if sim_matrix[i, j] >= threshold:
                    G.add_edge(item_ids[i], item_ids[j],
                               weight=float(sim_matrix[i, j]))
                    edge_count += 1

    print(f"Graph: {G.number_of_nodes()} nodes, {edge_count} edges")
    return G, edge_count


def compute_scores(G) -> dict:
    """
    Run graph algorithms on a networkx graph.

    Returns:
        {
            'communities': [{'id': int, 'members': [str]}],
            'centralities': {node_id: float},
            'hubs': [node_id],
            'bridges': [node_id],
        }
    """
    import networkx as nx
    from networkx.algorithms import community as nx_community

    if G is None or G.number_of_nodes() == 0:
        return {'communities': [], 'centralities': {}, 'hubs': [], 'bridges': []}

    # Louvain community detection
    communities = []
    try:
        partition = nx_community.louvain_communities(G, seed=42)
        for i, members in enumerate(partition):
            communities.append({
                'id': i,
                'members': list(members),
                'size': len(members)
            })
    except (nx.NetworkXError, ValueError, ZeroDivisionError):
        pass

    # PageRank centrality
    try:
        centralities = nx.pagerank(G, weight='weight')
    except (nx.NetworkXError, nx.PowerIterationFailedConvergence, ZeroDivisionError):
        centralities = {}

    # Hub identification (top 10% by centrality)
    hubs = []
    if centralities:
        sorted_nodes = sorted(centralities.items(), key=lambda x: x[1], reverse=True)
        hub_threshold = max(1, len(sorted_nodes) // 10)
        hubs = [node for node, _ in sorted_nodes[:hub_threshold]]

    # Bridge identification (high betweenness centrality)
    bridges = []
    try:
        betweenness = nx.betweenness_centrality(G, weight='weight')
        if betweenness:
            sorted_bridges = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
            bridge_threshold = max(1, len(sorted_bridges) // 20)
            bridges = [node for node, _ in sorted_bridges[:bridge_threshold]]
    except (nx.NetworkXError, ZeroDivisionError):
        pass

    return {
        'communities': communities,
        'centralities': centralities,
        'hubs': hubs,
        'bridges': bridges,
    }


def persist(db: sqlite3.Connection, scores: dict,
            table: str = '_enrich_source_graph',
            id_col: str = None):
    """
    Write graph scores to enrichment table.

    Creates table if needed. Wipes and rewrites (enrichments are mutable).
    Caller should run regenerate_views() after.

    Args:
        db: SQLite connection
        scores: Output from compute_scores()
        table: Target enrichment table
        id_col: Column for the node identifier. Auto-detected from table
                name if None: 'source_id' if 'source' in table, else 'chunk_id'.
    """
    if id_col is None:
        id_col = 'source_id' if 'source' in table else 'chunk_id'

    # Create table if not exists
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS [{table}] (
            [{id_col}] TEXT PRIMARY KEY,
            centrality REAL,
            is_hub INTEGER DEFAULT 0,
            is_bridge INTEGER DEFAULT 0,
            community_id INTEGER
        )
    """)

    # Wipe existing (enrichments are always safe to wipe)
    db.execute(f"DELETE FROM [{table}]")

    # Build community membership map
    community_map = {}
    for comm in scores.get('communities', []):
        for member in comm.get('members', []):
            community_map[member] = comm['id']

    hub_set = set(scores.get('hubs', []))
    bridge_set = set(scores.get('bridges', []))
    centralities = scores.get('centralities', {})

    # Collect all node IDs
    all_ids = set(centralities.keys())
    for comm in scores.get('communities', []):
        all_ids.update(comm.get('members', []))

    # Insert
    for node_id in all_ids:
        db.execute(
            f"INSERT OR REPLACE INTO [{table}] "
            f"([{id_col}], centrality, is_hub, is_bridge, community_id) "
            f"VALUES (?, ?, ?, ?, ?)",
            (
                node_id,
                centralities.get(node_id, 0.0),
                1 if node_id in hub_set else 0,
                1 if node_id in bridge_set else 0,
                community_map.get(node_id),
            )
        )

    db.commit()
    print(f"Persisted {len(all_ids)} graph scores to {table}")

    # Regenerate views to pick up new/changed enrichment
    from flexsearch.core import regenerate_views
    regenerate_views(db)


def run_sandbox(db: sqlite3.Connection, G, script: str) -> dict:
    """
    Execute arbitrary networkx script in a sandboxed environment.

    The script has access to: graph (G), db, numpy, networkx.
    It writes results to the `result` dict.

    Args:
        db: SQLite connection
        G: NetworkX graph
        script: Python code string

    Returns:
        The `result` dict from the sandbox
    """
    import json
    import networkx as nx
    from networkx.algorithms import community as nx_community
    from collections import Counter

    safe_globals = {
        'np': np, 'nx': nx,
        'nx_community': nx_community,
        'Counter': Counter,
        'graph': G, 'db': db,
        'result': {},
        '__builtins__': {
            k: v for k, v in __builtins__.items()
            if k in {
                'print', 'len', 'range', 'enumerate', 'str', 'int', 'float',
                'list', 'dict', 'set', 'tuple', 'bool', 'None', 'True', 'False',
                'isinstance', 'sorted', 'min', 'max', 'sum', 'round',
                'zip', 'map', 'filter', 'any', 'all', 'abs', 'hasattr',
                'getattr', 'setattr', 'ValueError', 'TypeError', 'KeyError',
                'Exception',
            }
        } if isinstance(__builtins__, dict) else {},
    }

    try:
        exec(compile(script, '<meditate>', 'exec'), safe_globals)
        result = safe_globals.get('result', {})
        return json.loads(json.dumps(
            result,
            default=lambda x: int(x) if hasattr(x, 'item') else str(x)
        ))
    except (SyntaxError, NameError, TypeError, ValueError, KeyError,
            AttributeError, IndexError, ZeroDivisionError, RuntimeError,
            ArithmeticError, LookupError, StopIteration, ImportError) as e:
        return {'error': str(e), 'type': type(e).__name__}
