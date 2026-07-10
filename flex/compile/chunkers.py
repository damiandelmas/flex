"""Language-routed file body chunking.

Splits file content at semantic boundaries by language:
  .md       → split_sections() (## headers)
  .py       → ast.parse() (functions, classes)
  .js/.jsx  → tree-sitter (functions, classes, arrow fns, exports)
  .ts/.tsx  → tree-sitter (functions, classes, interfaces, type aliases)
  else      → whole file as one chunk

Each chunker returns list of {'content': str, 'title': str, 'position': int}.
"""

import ast

from flex.compile.markdown import split_sections

# Skip files that are too small (noise) or too large (memory)
MIN_BODY_SIZE = 50
MAX_BODY_SIZE = 100_000


def chunk_file_body(content: str, file_path: str) -> list[dict]:
    """Route file content to the appropriate chunker by extension."""
    ext = file_path.rsplit('.', 1)[-1].lower() if '.' in file_path else ''

    if ext == 'md':
        return _chunk_markdown(content)
    elif ext == 'py':
        return _chunk_python(content, file_path)
    elif ext in ('js', 'jsx', 'ts', 'tsx'):
        return _chunk_treesitter(content, file_path, ext)
    else:
        return _chunk_whole(content, file_path)


def _chunk_markdown(content: str) -> list[dict]:
    """Split on ## headers via existing split_sections()."""
    sections = split_sections(content, level=2)
    if not sections:
        return [{'content': content, 'title': '', 'position': 0}]
    return [{'content': body, 'title': title, 'position': pos}
            for title, body, pos in sections]


def _chunk_python(source: str, path: str) -> list[dict]:
    """Split at function/class boundaries via stdlib ast."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return _chunk_whole(source, path)

    chunks = []
    lines = source.splitlines()
    pos = 0

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = '\n'.join(lines[node.lineno - 1: node.end_lineno])
            chunks.append({
                'content': body,
                'title': node.name,
                'position': pos,
            })
            pos += 1

    if not chunks:
        return _chunk_whole(source, path)
    return chunks


# ─── Tree-sitter JS/TS chunking ────────────────────────────────

# Node types that represent top-level declarations worth splitting on
_JS_CHUNK_TYPES = {
    'function_declaration', 'class_declaration', 'lexical_declaration',
    'export_statement', 'variable_declaration', 'method_definition',
}
_TS_CHUNK_TYPES = _JS_CHUNK_TYPES | {
    'interface_declaration', 'type_alias_declaration', 'enum_declaration',
}

# Map extension to (grammar loader, chunk types)
_TS_LANGS = {}  # lazy-loaded


def _get_ts_lang(ext: str):
    """Lazy-load tree-sitter language for an extension."""
    if ext not in _TS_LANGS:
        try:
            from tree_sitter import Language, Parser
            if ext in ('js', 'jsx'):
                import tree_sitter_javascript as tsjs
                lang = Language(tsjs.language())
                _TS_LANGS[ext] = (Parser(lang), _JS_CHUNK_TYPES)
            elif ext == 'tsx':
                import tree_sitter_typescript as tsts
                lang = Language(tsts.language_tsx())
                _TS_LANGS[ext] = (Parser(lang), _TS_CHUNK_TYPES)
            else:  # ts
                import tree_sitter_typescript as tsts
                lang = Language(tsts.language_typescript())
                _TS_LANGS[ext] = (Parser(lang), _TS_CHUNK_TYPES)
        except ImportError:
            _TS_LANGS[ext] = (None, set())
    return _TS_LANGS[ext]


def _extract_name(node) -> str:
    """Extract the declaration name from a tree-sitter node."""
    # Direct name child (function_declaration, class_declaration, etc.)
    for child in node.children:
        if child.type == 'identifier':
            return child.text.decode('utf-8', errors='replace')
        if child.type == 'type_identifier':
            return child.text.decode('utf-8', errors='replace')
    # export_statement wraps a declaration — recurse
    if node.type == 'export_statement':
        for child in node.children:
            if child.type in (_JS_CHUNK_TYPES | _TS_CHUNK_TYPES):
                return _extract_name(child)
    # lexical_declaration → first variable_declarator → name
    if node.type in ('lexical_declaration', 'variable_declaration'):
        for child in node.children:
            if child.type == 'variable_declarator':
                for gc in child.children:
                    if gc.type == 'identifier':
                        return gc.text.decode('utf-8', errors='replace')
    return ''


def _chunk_treesitter(content: str, path: str, ext: str) -> list[dict]:
    """Split JS/TS at function/class/interface boundaries via tree-sitter."""
    parser, chunk_types = _get_ts_lang(ext)
    if parser is None:
        return _chunk_whole(content, path)

    try:
        tree = parser.parse(content.encode('utf-8'))
    except Exception:
        return _chunk_whole(content, path)

    chunks = []
    lines = content.splitlines()
    pos = 0

    for node in tree.root_node.children:
        if node.type not in chunk_types:
            continue
        start_row = node.start_point[0]
        end_row = node.end_point[0]
        body = '\n'.join(lines[start_row:end_row + 1])
        if len(body.strip()) < 20:
            continue
        name = _extract_name(node)
        chunks.append({
            'content': body,
            'title': name,
            'position': pos,
        })
        pos += 1

    if not chunks:
        return _chunk_whole(content, path)
    return chunks


def _chunk_whole(content: str, path: str) -> list[dict]:
    """Embed whole file as one chunk."""
    return [{'content': content, 'title': '', 'position': 0}]


# ─── Instant-parity code-containment tree (shared home for instant/install.py
# and the markdown chunk_resolver kernel — both build the SAME node shape:
# {id, content, section_title, position, depth, container_id[, _calls]}) ──────

def _flat_nodes(abs_path: str, text: str) -> list[dict]:
    """Depth-1 flat nodes under root, via chunk_file_body — the shared fallback
    for the code-tree builders below (def-less file, parse failure, no grammar)."""
    from flex.sdk import _make_chunk_id

    nodes = []
    for i, part in enumerate(chunk_file_body(text, abs_path)):
        body = part.get("content") or part.get("body") or text
        nodes.append({"id": _make_chunk_id(abs_path, i, body), "content": body,
                      "section_title": part.get("title", "") or "",
                      "position": part.get("position", i), "depth": 1,
                      "container_id": abs_path})
    return nodes


def _build_code_tree(abs_path: str, text: str) -> list[dict]:
    """Python AST tree: module ⊃ class ⊃ method/function, span-based.

    A node's content is its OWN region (signature + body up to its first nested def);
    nested defs are recursed as children — so a class node does not duplicate its
    methods' text (the code analogue of a heading-section excluding its sub-sections).
    A '(module)' leaf captures the module-level preamble (docstring/imports) before the
    first def. Falls back to flat chunking on a SyntaxError or a def-less file."""
    import ast as _ast
    from flex.sdk import _make_chunk_id

    try:
        tree = _ast.parse(text)
    except SyntaxError:
        return _flat_nodes(abs_path, text)
    lines = text.splitlines()
    DEF = (_ast.FunctionDef, _ast.AsyncFunctionDef, _ast.ClassDef)
    top_defs = [n for n in _ast.iter_child_nodes(tree) if isinstance(n, DEF)]
    if not top_defs:
        return _flat_nodes(abs_path, text)

    nodes: list[dict] = []

    def _own_call_names(node):
        """Bare-function calls (`foo()`) in node's own scope, pruning nested
        def subtrees. Attribute calls (`x.method()`) are not collected."""
        prune = {id(c) for c in _ast.iter_child_nodes(node) if isinstance(c, DEF)}
        names = []

        def _rec(n):
            for child in _ast.iter_child_nodes(n):
                if id(child) in prune:
                    continue
                if isinstance(child, _ast.Call) and isinstance(child.func, _ast.Name):
                    names.append(child.func.id)
                _rec(child)
        _rec(node)
        return names

    def _def_start(n):
        # py3.8+: FunctionDef/ClassDef.lineno is the `def`/`class` line, NOT the
        # `@decorator` lines above it. Start the span at the earliest decorator so
        # decorator text belongs to its owning node instead of an unassigned gap.
        return min([d.lineno for d in getattr(n, 'decorator_list', [])] + [n.lineno])

    def _emit(node, depth, parent_id):
        kids = [c for c in _ast.iter_child_nodes(node) if isinstance(c, DEF)]
        start = _def_start(node) - 1
        end = (_def_start(kids[0]) - 1) if kids else (node.end_lineno or node.lineno)
        own = "\n".join(lines[start:end]).rstrip() or \
            "\n".join(lines[start:(node.end_lineno or node.lineno)])
        cid = _make_chunk_id(abs_path, len(nodes), own)
        nodes.append({"id": cid, "content": own, "section_title": node.name,
                      "position": len(nodes), "depth": depth, "container_id": parent_id,
                      "_calls": _own_call_names(node)})
        for c in kids:
            _emit(c, depth + 1, cid)

    preamble = "\n".join(lines[:_def_start(top_defs[0]) - 1]).strip()
    if preamble:
        nodes.append({"id": _make_chunk_id(abs_path, len(nodes), preamble),
                      "content": preamble, "section_title": "(module)",
                      "position": len(nodes), "depth": 1, "container_id": abs_path})
    for node in top_defs:
        _emit(node, 1, abs_path)
    return nodes


_TS_EXTS = {"ts", "tsx", "js", "jsx", "mjs", "cjs"}


def _ts_language(ext):
    """tree-sitter Language for a JS/TS-family extension (None if grammars unavailable)."""
    try:
        from tree_sitter import Language
        if ext == "ts":
            import tree_sitter_typescript as _t
            return Language(_t.language_typescript())
        if ext == "tsx":
            import tree_sitter_typescript as _t
            return Language(_t.language_tsx())
        import tree_sitter_javascript as _j  # js/jsx/mjs/cjs (the JS grammar handles JSX)
        return Language(_j.language())
    except Exception:
        return None


def _build_code_tree_ts(abs_path: str, text: str, ext: str) -> list[dict]:
    """tree-sitter tree for JS/TS: module ⊃ class ⊃ method/fn ⊃ nested fn, span-based.

    Mirrors the Python `_build_code_tree`. A node's content is its own region (up to
    its first nested def); nested defs recurse as children. Calls are collected per
    def's own scope: bare `foo()` and `this.m()`/`super.m()`. Other member calls
    (`obj.m()`) are not collected. Falls back to flat on a parse failure or a
    def-less file."""
    from flex.sdk import _make_chunk_id
    lang = _ts_language(ext)
    if lang is None:
        return _flat_nodes(abs_path, text)
    try:
        from tree_sitter import Parser
        src = text.encode("utf-8")
        root = Parser(lang).parse(src).root_node
    except Exception:
        return _flat_nodes(abs_path, text)

    DECL = {"function_declaration", "generator_function_declaration", "method_definition",
            "class_declaration", "abstract_class_declaration"}

    def _name(n):
        if n.type in DECL:
            f = n.child_by_field_name("name")
            return f.text.decode("utf-8", "ignore") if f else None
        if n.type == "variable_declarator":           # const foo = () => {} / function(){}
            v = n.child_by_field_name("value")
            if v is not None and v.type in ("arrow_function", "function_expression"):
                f = n.child_by_field_name("name")
                return f.text.decode("utf-8", "ignore") if f else None
        return None

    def _child_defs(n):
        """Nearest nested defs under n (descend through non-def nodes, stop at each def)."""
        out = []

        def rec(m):
            for c in m.children:
                if _name(c) is not None:
                    out.append(c)
                else:
                    rec(c)
        rec(n)
        return out

    def _own_calls(n, kids):
        """bare foo() + this.m()/super.m() in n's own scope (excluding nested-def subtrees)."""
        prune = {id(c) for c in kids}
        names = []

        def rec(m):
            for c in m.children:
                if id(c) in prune:
                    continue
                if c.type == "call_expression":
                    f = c.child_by_field_name("function")
                    if f is not None and f.type == "identifier":
                        names.append(f.text.decode("utf-8", "ignore"))
                    elif f is not None and f.type == "member_expression":
                        o = f.child_by_field_name("object")
                        p = f.child_by_field_name("property")
                        if o is not None and p is not None and o.type in ("this", "super"):
                            names.append(p.text.decode("utf-8", "ignore"))
                rec(c)
        rec(n)
        return names

    def _def_start(n):
        # A decorator (@Component, @Injectable, …) is a SEPARATE sibling node
        # preceding the def in the grammar, so n.start_byte excludes it. Extend
        # the start back over any leading decorator siblings so decorator text
        # belongs to its owning def instead of an unassigned gap.
        s = n.start_byte
        p = n.prev_sibling
        while p is not None and p.type == "decorator":
            s = p.start_byte
            p = p.prev_sibling
        return s

    nodes: list[dict] = []

    def _emit(n, depth, parent_id):
        kids = _child_defs(n)
        start = _def_start(n)
        own_end = _def_start(kids[0]) if kids else n.end_byte
        own = src[start:own_end].decode("utf-8", "ignore").rstrip() or \
            src[start:n.end_byte].decode("utf-8", "ignore")
        cid = _make_chunk_id(abs_path, len(nodes), own)
        nodes.append({"id": cid, "content": own, "section_title": _name(n) or "",
                      "position": len(nodes), "depth": depth, "container_id": parent_id,
                      "_calls": _own_calls(n, kids)})
        for c in kids:
            _emit(c, depth + 1, cid)

    top = _child_defs(root)
    if not top:
        return _flat_nodes(abs_path, text)
    preamble = src[:_def_start(top[0])].decode("utf-8", "ignore").strip()
    if preamble:
        nodes.append({"id": _make_chunk_id(abs_path, len(nodes), preamble),
                      "content": preamble, "section_title": "(module)",
                      "position": len(nodes), "depth": 1, "container_id": abs_path})
    for n in top:
        _emit(n, 1, abs_path)
    return nodes
