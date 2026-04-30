"""Combined wikilink + embedding graph for markdown cells."""

from collections import Counter


def _ensure_markdown_graph_columns(db) -> None:
    """Ensure markdown-only graph columns exist before curated views install."""
    try:
        db.execute("ALTER TABLE _enrich_source_graph ADD COLUMN hub_type TEXT")
    except Exception:
        pass


def build_combined_graph(db, threshold=None):
    """Build graph from wikilink edges + embedding similarity.

    1. Query resolved wikilinks, collapse to source-level (src, dst, 1.0)
    2. Call build_similarity_graph with extra_edges
    3. Compute scores (is_hub, is_bridge, community_id)
    4. Classify hub_type from wikilink subgraph only (directed signal)
    5. Persist node scores to _enrich_source_graph
    6. Build and persist edge provenance to _enrich_graph_provenance

    Returns True if graph was built, False if skipped.
    """
    from flex.manage.meditate import build_similarity_graph, compute_scores, persist
    _ensure_markdown_graph_columns(db)

    # 1. Query resolved wikilinks (source-level)
    try:
        wikilink_rows = db.execute("""
            SELECT DISTINCT w.from_path, w.to_path
            FROM _edges_wikilink w
            WHERE w.from_path != w.to_path
        """).fetchall()
    except Exception:
        wikilink_rows = []

    wikilink_pairs = [(src, dst, 1.0) for src, dst in wikilink_rows]
    print(f"  {len(wikilink_pairs)} wikilink edges (source-level)")

    # 2. Build combined graph
    G, edge_count = build_similarity_graph(
        db, table='_raw_sources', id_col='source_id',
        threshold=threshold or 0.55, center=True,
        extra_edges=wikilink_pairs if wikilink_pairs else None,
    )

    if G is None or G.number_of_nodes() == 0:
        return False

    # 3. Compute scores on combined graph (undirected)
    scores = compute_scores(G)
    if not scores.get('centralities'):
        return False

    # 4. Classify hub types from wikilink subgraph only (directed signal)
    if wikilink_pairs:
        wikilink_in = Counter(dst for _, dst, _ in wikilink_pairs)
        wikilink_out = Counter(src for src, _, _ in wikilink_pairs)

        centralities = scores.get('centralities', {})
        hubs = set(scores.get('hubs', []))

        for source_id in centralities:
            if source_id in hubs:
                in_d = wikilink_in.get(source_id, 0)
                out_d = wikilink_out.get(source_id, 0)
                if in_d == 0 and out_d == 0:
                    # Hub by embedding only — no directional signal
                    pass  # hub_type stays NULL
                elif out_d > in_d * 1.5:
                    scores.setdefault('hub_types', {})[source_id] = 'connector'
                elif in_d > out_d * 1.5:
                    scores.setdefault('hub_types', {})[source_id] = 'authority'
                else:
                    scores.setdefault('hub_types', {})[source_id] = 'authority'

    # 5. Persist node scores
    persist(db, scores, table='_enrich_source_graph', id_col='source_id')

    # Update hub_type column if we classified any
    hub_types = scores.get('hub_types', {})
    if hub_types:
        for source_id, hub_type in hub_types.items():
            db.execute(
                "UPDATE _enrich_source_graph SET hub_type = ? WHERE source_id = ?",
                (hub_type, source_id)
            )

    # 6. Build and persist edge provenance (markdown-specific)
    _persist_edge_provenance(db, G, wikilink_pairs)

    db.commit()
    return True


def _persist_edge_provenance(db, G, wikilink_pairs):
    """Write edge provenance to _enrich_graph_provenance."""
    db.execute("""CREATE TABLE IF NOT EXISTS _enrich_graph_provenance (
        source_a TEXT NOT NULL,
        source_b TEXT NOT NULL,
        edge_type TEXT NOT NULL,
        weight REAL NOT NULL,
        PRIMARY KEY (source_a, source_b)
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_graph_prov_type ON _enrich_graph_provenance(edge_type)")
    db.execute("DELETE FROM _enrich_graph_provenance")

    # Build provenance from graph edges (all are at least semantic)
    edge_provenance = {}
    for u, v, d in G.edges(data=True):
        key = (min(u, v), max(u, v))
        edge_provenance[key] = ('semantic', d.get('weight', 0.0))

    # Overlay wikilink edges
    wikilink_set = {(min(s, d), max(s, d)) for s, d, _ in wikilink_pairs}
    for key in wikilink_set:
        if key in edge_provenance:
            edge_provenance[key] = ('both', max(edge_provenance[key][1], 1.0))
        else:
            edge_provenance[key] = ('wikilink', 1.0)

    # Batch insert
    db.executemany(
        "INSERT OR REPLACE INTO _enrich_graph_provenance (source_a, source_b, edge_type, weight) VALUES (?, ?, ?, ?)",
        [(a, b, etype, w) for (a, b), (etype, w) in edge_provenance.items()]
    )

    counts = Counter(etype for etype, _ in edge_provenance.values())
    print(f"  Edge provenance: {counts.get('semantic', 0)} semantic, "
          f"{counts.get('wikilink', 0)} wikilink, {counts.get('both', 0)} both")
