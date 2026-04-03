"""
Flex Meditate — offline graph intelligence.

Produces columns that SQL consumes. networkx computes things SQL can't:
PageRank, Louvain communities, hub/bridge identification.

Output is INSERT INTO _enrich_* tables. Once persisted as columns,
SQL composes them freely. networkx is the producer, SQL is the consumer.

Similarity search backends (tried in order):
1. faiss-gpu  — GPU-accelerated ANN (NVIDIA GPU required)
2. faiss-cpu  — CPU ANN (fast, no GPU needed)
3. numpy      — batched brute-force (no dependencies, memory-safe)

Four functions:
- build_similarity_graph() → embeddings → networkx graph
- compute_scores()         → louvain, pagerank, hubs, bridges
- persist()                → INSERT INTO _enrich_*, regenerate views
- run_sandbox()            → execute arbitrary networkx script
"""

import numpy as np
import sqlite3
from typing import Optional


# ─── Similarity search backends ─────────────────────────────────

def _try_faiss():
    """Try to load FAISS. Returns (faiss_module, 'gpu'|'cpu') or (None, None)."""
    try:
        import faiss
        # Try GPU first
        try:
            if faiss.get_num_gpus() > 0:
                return faiss, 'gpu'
        except AttributeError:
            pass  # faiss-cpu doesn't have get_num_gpus
        return faiss, 'cpu'
    except ImportError:
        return None, None


def _search_faiss(emb_matrix, top_k, threshold, faiss, backend):
    """Use FAISS for approximate nearest neighbor search.

    Returns list of (i, j, similarity) tuples for edges above threshold.
    """
    n, dim = emb_matrix.shape
    # Inner product on L2-normalized vectors = cosine similarity
    index = faiss.IndexFlatIP(dim)

    if backend == 'gpu':
        res = faiss.StandardGpuResources()
        index = faiss.index_cpu_to_gpu(res, 0, index)

    index.add(emb_matrix.astype(np.float32))
    # Search for top_k+1 (first result is self)
    similarities, indices = index.search(emb_matrix.astype(np.float32), top_k + 1)

    edges = []
    for i in range(n):
        for rank in range(top_k + 1):
            j = indices[i, rank]
            sim = similarities[i, rank]
            if j == i or j < 0:
                continue
            if sim >= threshold:
                edges.append((i, j, float(sim)))
    return edges


def _search_batched_numpy(emb_matrix, top_k, threshold, batch_size=2000):
    """Batched brute-force similarity search. Memory-safe for large matrices.

    Instead of materializing an N×N matrix (114K×114K = 49GB), processes
    in row-batches of `batch_size`. Peak memory: N × batch_size float32.
    """
    n = emb_matrix.shape[0]
    edges = []

    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        # (batch_size × dim) @ (dim × N) = (batch_size × N)
        sim_block = emb_matrix[start:end] @ emb_matrix.T

        for local_i in range(end - start):
            global_i = start + local_i
            row = sim_block[local_i]
            row[global_i] = -1  # exclude self

            if top_k:
                top_indices = np.argpartition(row, -top_k)[-top_k:]
                for j in top_indices:
                    if row[j] >= threshold:
                        edges.append((global_i, int(j), float(row[j])))
            else:
                above = np.where(row >= threshold)[0]
                for j in above:
                    if j > global_i:  # upper triangle only
                        edges.append((global_i, int(j), float(row[j])))

        if start % (batch_size * 5) == 0 and start > 0:
            print(f"  similarity: {start}/{n} rows processed...")

    return edges


# ─── Main graph builder ─────────────────────────────────────────

def build_similarity_graph(db: sqlite3.Connection, table: str = '_raw_sources',
                           id_col: str = 'id', embedding_col: str = 'embedding',
                           threshold: float = 0.5, top_k: int = None,
                           where: str = None, center: bool = False):
    """
    Build similarity graph from embeddings.

    Uses FAISS (GPU → CPU) when available, falls back to batched numpy.
    For large corpora (>10K sources), defaults to top_k=20 neighbors
    instead of all-pairs to keep memory and graph density bounded.

    Args:
        db: SQLite connection
        table: Table with embeddings
        id_col: Column with item identifiers
        embedding_col: Column with embedding blobs
        threshold: Minimum cosine similarity for edge creation
        top_k: Keep only top K neighbors per node. Auto-set for large corpora.
        where: Optional SQL WHERE fragment to filter rows
        center: If True, subtract corpus mean before similarity computation.
                Removes shared embedding direction, making pairwise similarity
                reflect topical differences rather than shared vocabulary.

    Returns:
        (NetworkX graph, edge_count) or (None, 0)
    """
    import networkx as nx

    # Load embeddings
    query = (
        f"SELECT [{id_col}], [{embedding_col}] FROM [{table}] "
        f"WHERE [{embedding_col}] IS NOT NULL"
    )
    if where:
        query += f" AND ({where})"
    rows = db.execute(query).fetchall()

    if not rows:
        return None, 0

    item_ids = []
    embeddings = []
    for row in rows:
        item_ids.append(row[0])
        embeddings.append(np.frombuffer(row[1], dtype=np.float32))

    n = len(item_ids)
    dim = len(embeddings[0])
    print(f"Loaded {n} embeddings ({dim}-dim)")

    # Auto top_k for large corpora — all-pairs is O(n²) memory
    if top_k is None and n > 10_000:
        top_k = 20
        print(f"  auto top_k={top_k} (corpus > 10K)")

    # Build and normalize embedding matrix
    emb_matrix = np.vstack(embeddings)
    norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
    emb_matrix = emb_matrix / (norms + 1e-10)

    # Mean centering: subtract corpus mean, re-normalize.
    # What remains is what makes each item *different* from the average.
    if center:
        corpus_mean = emb_matrix.mean(axis=0)
        emb_matrix = emb_matrix - corpus_mean
        norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1  # avoid div by zero
        emb_matrix = emb_matrix / norms

    # Select backend
    faiss, faiss_backend = _try_faiss()
    if faiss is not None:
        print(f"  backend: faiss ({faiss_backend})")
        k = top_k or min(n - 1, 50)  # default 50 neighbors for threshold mode
        edges = _search_faiss(emb_matrix, k, threshold, faiss, faiss_backend)
    else:
        print(f"  backend: numpy (batched)")
        edges = _search_batched_numpy(emb_matrix, top_k, threshold)

    # Build graph
    G = nx.Graph()
    for item_id in item_ids:
        G.add_node(item_id)

    # Deduplicate edges (both backends may produce (i,j) and (j,i))
    seen = set()
    edge_count = 0
    for i, j, sim in edges:
        key = (min(i, j), max(i, j))
        if key not in seen:
            seen.add(key)
            G.add_edge(item_ids[i], item_ids[j], weight=sim)
            edge_count += 1

    print(f"Graph: {G.number_of_nodes()} nodes, {edge_count} edges")
    return G, edge_count


def _try_networkit():
    """Try to load NetworKit. Returns module or None."""
    try:
        import networkit as nk
        return nk
    except ImportError:
        return None


def _nx_to_nk(G):
    """Convert networkx graph to networkit graph. Returns (nk_graph, node_list).

    node_list maps networkit integer IDs back to original string IDs.
    """
    nk = _try_networkit()
    node_list = list(G.nodes())
    node_idx = {n: i for i, n in enumerate(node_list)}
    nk_g = nk.Graph(len(node_list), weighted=True)
    for u, v, d in G.edges(data=True):
        nk_g.addEdge(node_idx[u], node_idx[v], d.get('weight', 1.0))
    return nk_g, node_list


def _compute_scores_networkit(G) -> dict:
    """Graph algorithms via NetworKit (C++ backend, 10-100x faster)."""
    import networkit as nk

    nk_g, node_list = _nx_to_nk(G)
    n = nk_g.numberOfNodes()
    print(f"  scoring: networkit ({n} nodes, {nk_g.numberOfEdges()} edges)")

    # Louvain community detection
    communities = []
    try:
        plm = nk.community.PLM(nk_g, refine=True)
        plm.run()
        partition = plm.getPartition()
        community_map_raw = {}
        for node in range(n):
            cid = partition.subsetOf(node)
            community_map_raw.setdefault(cid, []).append(node)
        for i, (cid, members) in enumerate(
                sorted(community_map_raw.items(), key=lambda x: -len(x[1]))):
            communities.append({
                'id': i,
                'members': [node_list[m] for m in members],
                'size': len(members),
            })
    except Exception:
        pass

    # PageRank (normalized to sum to 1.0 — NetworKit doesn't guarantee this
    # on disconnected graphs, unlike networkx)
    centralities = {}
    try:
        pr = nk.centrality.PageRank(nk_g)
        pr.run()
        scores = pr.scores()
        total = sum(scores)
        if total > 0:
            centralities = {node_list[i]: scores[i] / total for i in range(n)}
        else:
            centralities = {node_list[i]: scores[i] for i in range(n)}
    except Exception:
        pass

    # Hubs (top 10% by PageRank)
    hubs = []
    if centralities:
        sorted_nodes = sorted(centralities.items(), key=lambda x: x[1], reverse=True)
        hub_threshold = max(1, len(sorted_nodes) // 10)
        hubs = [node for node, _ in sorted_nodes[:hub_threshold]]

    # Bridges (betweenness) — skip for large graphs, too expensive even in C++
    bridges = []
    if n <= 50_000:
        try:
            if n > 500:
                k = min(100, n // 5)
                bc = nk.centrality.EstimateBetweenness(nk_g, k, normalized=True)
            else:
                bc = nk.centrality.Betweenness(nk_g, normalized=True)
            bc.run()
            scores = bc.scores()
            betweenness = {node_list[i]: scores[i] for i in range(n)}
            sorted_bridges = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
            bridge_threshold = max(1, len(sorted_bridges) // 20)
            bridges = [node for node, _ in sorted_bridges[:bridge_threshold]]
        except Exception:
            pass
    else:
        print(f"  skipping betweenness (n={n} > 50K)")

    return {
        'communities': communities,
        'centralities': centralities,
        'hubs': hubs,
        'bridges': bridges,
    }


def _compute_scores_networkx(G) -> dict:
    """Graph algorithms via NetworkX (pure Python fallback)."""
    import networkx as nx
    from networkx.algorithms import community as nx_community

    n = G.number_of_nodes()
    print(f"  scoring: networkx ({n} nodes, {G.number_of_edges()} edges)")

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

    # Bridge identification — skip for large graphs
    bridges = []
    if n <= 50_000:
        try:
            if n > 500:
                k = min(100, n // 5)
                betweenness = nx.betweenness_centrality(G, weight='weight', k=k)
            else:
                betweenness = nx.betweenness_centrality(G, weight='weight')
            if betweenness:
                sorted_bridges = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
                bridge_threshold = max(1, len(sorted_bridges) // 20)
                bridges = [node for node, _ in sorted_bridges[:bridge_threshold]]
        except (nx.NetworkXError, ZeroDivisionError):
            pass
    else:
        print(f"  skipping betweenness (n={n} > 50K)")

    return {
        'communities': communities,
        'centralities': centralities,
        'hubs': hubs,
        'bridges': bridges,
    }


def compute_scores(G) -> dict:
    """
    Run graph algorithms. Uses NetworKit (C++) when available,
    falls back to NetworkX (Python).

    Returns:
        {
            'communities': [{'id': int, 'members': [str]}],
            'centralities': {node_id: float},
            'hubs': [node_id],
            'bridges': [node_id],
        }
    """
    if G is None or G.number_of_nodes() == 0:
        return {'communities': [], 'centralities': {}, 'hubs': [], 'bridges': []}

    nk = _try_networkit()
    if nk is not None:
        return _compute_scores_networkit(G)
    return _compute_scores_networkx(G)


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
            community_id INTEGER,
            community_label TEXT
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

    # Log mutation
    from flex.core import log_op
    log_op(db, 'persist_graph_scores', table,
           rows_affected=len(all_ids), source='meditate.py')

    # Regenerate views to pick up new/changed enrichment
    from flex.core import regenerate_views
    regenerate_views(db)


def run_cli(cell_path: str, table: str = '_raw_sources',
            threshold: float = 0.55, center: bool = True,
            where: str = None):
    """Run meditate as a standalone operation on a cell file.

    Used by module workers via subprocess to avoid import coupling.
    """
    import sqlite3
    db = sqlite3.connect(cell_path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")

    G, edge_count = build_similarity_graph(
        db, table=table, id_col='source_id',
        threshold=threshold, center=center, where=where)

    if G is not None:
        scores = compute_scores(G)
        persist(db, scores, table='_enrich_source_graph', id_col='source_id')

    db.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Flex meditate — offline graph intelligence')
    parser.add_argument('--cell', required=True, help='Path to cell .db file')
    parser.add_argument('--table', default='_raw_sources')
    parser.add_argument('--threshold', type=float, default=0.55)
    parser.add_argument('--top-k', type=int, default=None,
                        help='Top K neighbors per node (auto: 20 for >10K sources)')
    parser.add_argument('--no-center', action='store_true')
    parser.add_argument('--where', default=None)
    parser.add_argument('--backend', choices=['faiss', 'numpy'], default=None,
                        help='Force similarity backend (default: auto-detect)')
    args = parser.parse_args()

    run_cli(args.cell, table=args.table, threshold=args.threshold,
            center=not args.no_center, where=args.where)


if __name__ == '__main__':
    main()
