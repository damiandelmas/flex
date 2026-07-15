---
name: flex:codegraph
description: Query the code capability of a Flex filesystem cell: symbols, containment, callers, callees, impact, imports, exact text, and optional semantic retrieval.
allowed-tools:
  - Bash
user-invocable: true
---

# flex:codegraph

Code navigation is a capability of a filesystem cell. Begin with:

```bash
flex core search --cell <name> "@orient"
```

Python uses `ast`; JS/TS uses tree-sitter. Definitions are stored in `_symbols`,
calls in `_edges_call`, imports in `_edges_import`, containment in `_edges_tree`,
and their bodies in `chunks WHERE file_kind='code'`.

Prefer the graph presets:

```text
@callers symbol=NAME
@callees symbol=NAME
@impact symbol=NAME
@subtree root=CHUNK_ID
```

Use `keyword()` for exact identifiers, literals, and errors. On ordinary 0.53
filesystem cells, use `vec_ops()` for semantic code recall when `@orient` reports
embeddings enabled. Legacy `codegraph` compatibility cells remain structural and
must not be described as semantic.

Same-named definitions are candidate sets. External calls remain unresolved, and
member calls other than supported `this.m()`/`super.m()` shapes are not certain
edges. Resolve a candidate to its `def_id` and `source_id` before making a claim.
