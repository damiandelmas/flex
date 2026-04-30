"""Shared-attribute graph primitive + claude_code file co-edit wrapper.

The generic primitive (``build_shared_attribute_graph``) builds a bipartite
projection: sources connected by a shared attribute value. The claude_code
wrapper (``build_file_graph``) calls it with file_identity defaults.

Any cell can reuse the primitive for shared-author, shared-tag, shared-repo
projections by passing its own edge table + attribute column.
"""

from collections import defaultdict

import networkx as nx

from flex.modules.claude_code.manage.noise import MAX_SESSIONS_PER_FILE


# Template — callers substitute ``output_table`` before CREATE IF NOT EXISTS.
CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {output_table} (
    source_id TEXT PRIMARY KEY,
    file_community_id INTEGER,
    file_centrality REAL,
    file_is_hub INTEGER DEFAULT 0,
    shared_file_count INTEGER
)
"""


# Legacy constant preserved for callers that still import it.
CREATE_TABLE = CREATE_TABLE_TEMPLATE.format(output_table='_enrich_file_graph')


def build_shared_attribute_graph(
    db,
    *,
    edge_table: str = '_edges_file_identity',
    attribute_col: str = 'file_uuid',
    source_edge_table: str = '_edges_source',
    source_col: str = 'source_id',
    output_table: str = '_enrich_file_graph',
    max_per_attribute: int = MAX_SESSIONS_PER_FILE,
):
    """Bipartite projection: sources connected by a shared attribute value.

    Reads ``(chunk_id, attribute_col)`` from ``edge_table``, joins with
    ``source_edge_table`` to get ``(source_col, attribute_col)``, builds a
    bipartite graph, and projects onto source-source edges weighted by the
    count of shared attribute values.

    Args:
        edge_table: table containing ``(chunk_id, <attribute_col>)`` rows
        attribute_col: the attribute to project on
            (e.g. ``file_uuid``, ``author``, ``tag``)
        source_edge_table: chunk → source bridge (usually ``_edges_source``)
        source_col: source identifier column (usually ``source_id``)
        output_table: informational only — caller is responsible for the
            ``CREATE TABLE`` / writes. Accepted so wrappers can keep the
            original signature.
        max_per_attribute: skip attribute values shared by more than N
            sources (noise floor — prevents highly-shared attributes from
            making everything connected).

    Returns:
        ``(nx.Graph, dict[source_id -> set[attribute_value]])``
    """
    rows = db.execute(f"""
        SELECT DISTINCT es.{source_col}, fi.{attribute_col}
        FROM {edge_table} fi
        JOIN {source_edge_table} es ON fi.chunk_id = es.chunk_id
        WHERE fi.{attribute_col} IS NOT NULL AND fi.{attribute_col} != ''
    """).fetchall()

    attr_to_sources: dict = defaultdict(set)
    source_attrs: dict = defaultdict(set)
    for r in rows:
        attr_val = r[attribute_col]
        src_id = r[source_col]
        attr_to_sources[attr_val].add(src_id)
        source_attrs[src_id].add(attr_val)

    print(f"  {len(attr_to_sources)} unique {attribute_col} values, "
          f"{len(source_attrs)} sources with {attribute_col}")

    # Skip attribute values shared by too many sources (noise)
    skipped = 0
    for val in list(attr_to_sources.keys()):
        if len(attr_to_sources[val]) > max_per_attribute:
            del attr_to_sources[val]
            skipped += 1
    if skipped:
        print(f"  Skipped {skipped} {attribute_col} values shared by "
              f">{max_per_attribute} sources")

    # Bipartite projection
    G = nx.Graph()
    for sid in source_attrs:
        G.add_node(sid)

    edge_weights: dict = defaultdict(int)
    for val, sources in attr_to_sources.items():
        sources = list(sources)
        for i in range(len(sources)):
            for j in range(i + 1, len(sources)):
                pair = tuple(sorted([sources[i], sources[j]]))
                edge_weights[pair] += 1

    for (s1, s2), weight in edge_weights.items():
        G.add_edge(s1, s2, weight=weight)

    print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G, source_attrs


def build_file_graph(db):
    """Back-compat wrapper — claude_code file co-edit graph.

    Two sessions are connected if they touched the same file. Weight = shared
    files. Prefer :func:`build_shared_attribute_graph` for new callers.
    """
    return build_shared_attribute_graph(
        db,
        edge_table='_edges_file_identity',
        attribute_col='file_uuid',
        source_edge_table='_edges_source',
        source_col='source_id',
        output_table='_enrich_file_graph',
        max_per_attribute=MAX_SESSIONS_PER_FILE,
    )


def analyze_file_graph(G):
    """Louvain + PageRank + hub detection on any projected shared-attribute graph.

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
    comm_scores: dict = defaultdict(list)
    for node, comm_id in partition.items():
        comm_scores[comm_id].append((node, pr.get(node, 0)))

    hubs = set()
    for comm_id, scores in comm_scores.items():
        scores.sort(key=lambda x: x[1], reverse=True)
        n_hubs = max(1, len(scores) // 10)
        for node, _ in scores[:n_hubs]:
            hubs.add(node)

    return partition, pr, hubs
