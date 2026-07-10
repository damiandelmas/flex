"""Doc-pac noise filtering — minimal, because doc-pac sources are clean.

Doc-pac cells are built from folder structures. Every source is a real
document (markdown file, plan, spec). Sources have 3-8 chunks typically.
No warmups, no agent children, no aborted sessions.

Graph threshold differs from claude-code because doc-pac corpora are
topically homogeneous (all docs about one project), so baseline pairwise
similarity is higher. Tested: 0.55 optimal for a single-project docs
corpus (102 sources), vs 0.5 for a multi-project coding-agent corpus
(5,774 sources across 29 projects).
"""

# Graph similarity threshold (higher than claude-code due to corpus homogeneity)
GRAPH_THRESHOLD = 0.55


def graph_filter_sql():
    """WHERE fragment for build_similarity_graph().

    Doc-pac cells need no noise filter — all sources are content.
    Returns None to signal "no filter".
    """
    return None
