"""fs.compile.enrich — uniform additive enrichment passes.

Each pass runs AFTER a docpac cell is compiled and only ADDS rows to
enrichment tables. It must never mutate the invariant projection
(_raw_chunks content/boundaries, _edges_source, _types_docpac,
_raw_sources content). See obsidian.enrich_obsidian.
"""
