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
