"""resolve_chunker — the declarative split-strategy dispatch (profile-as-data S1).

One strategy vocabulary, one dispatch over the EXISTING chunkers, normalized to a
common node shape. Plus the cascade resolver that picks a strategy for a file from
the declared `chunking` block (by_folder > by_subtype > default) and the
`.flexchunk.json` sidecar (nearest-ancestor wins, highest precedence).

PURE: no DB, no embedding. Returns nodes the pipeline ingests. Nothing imports this
yet — it is the proven kernel; wiring into docpac/instant is a later, gated stage.

Strategy vocabulary (the `split` values):
    heading[:level]   split at every heading >= level (return_depth) — the lossless default
    ast               code: module ⊃ class ⊃ method (python ast / tree-sitter route)
    whole             one node, the whole body
    sliding[:window]  fixed-size window (needs breadcrumb ctx; wrapper TODO)
    template[:sections]  declared section vocab (NEW chunker; TODO)

Node shape (normalized): {content, title, position, depth}.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

VALID_SPLITS = {"heading", "ast", "whole", "sliding", "template"}
_FLEXCHUNK = ".flexchunk.json"

# instant's mode names → the unified vocabulary (reconciliation)
_INSTANT_ALIAS = {"nest": "heading", "flat": "ast_or_heading", "whole": "whole"}


def _normalize(parts) -> list[dict]:
    """Fold the 3 chunker return shapes into [{content,title,position,depth}].
    split_sections → (title, body, pos[, depth]) tuples; chunk_file_body →
    {content,title,position} dicts; ChunkEntry → attrs."""
    out = []
    for i, p in enumerate(parts):
        if isinstance(p, tuple):
            title = p[0]; body = p[1]; pos = p[2] if len(p) > 2 else i
            depth = p[3] if len(p) > 3 else 1
        elif isinstance(p, dict):
            body = p.get("content") or p.get("body") or ""
            title = p.get("title") or p.get("section_title") or ""
            pos = p.get("position", i); depth = p.get("depth", 1)
        else:  # ChunkEntry-like object
            body = getattr(p, "content", "") or getattr(p, "body", "")
            title = getattr(p, "section_title", "") or getattr(p, "title", "")
            pos = getattr(p, "position", i); depth = getattr(p, "heading_depth", 1)
        out.append({"content": body, "title": title or "", "position": pos, "depth": depth})
    return out


def split_by(strategy: str, body: str, file_path: str = "", **params) -> list[dict]:
    """Dispatch ONE strategy to its existing chunker, return normalized nodes."""
    from flex.compile.markdown import split_sections, normalize_headers
    from flex.compile.chunkers import (
        chunk_file_body, _chunk_whole, _build_code_tree, _build_code_tree_ts, _TS_EXTS,
    )

    s = strategy
    if s == "heading":
        fp = file_path or ""
        ext = fp.rsplit(".", 1)[-1].lower() if "." in fp else ""
        if ext == "py":
            return _normalize(_build_code_tree(fp or "x.py", body))
        if ext in _TS_EXTS:
            return _normalize(_build_code_tree_ts(fp or f"x.{ext}", body, ext))
        level = int(params.get("level", 1))
        secs = split_sections(normalize_headers(body), level=level, return_depth=True)
        return _normalize(secs or [("", body.strip(), 0, 0)])
    if s == "ast":
        # code: chunk_file_body routes python→ast, js/ts→tree-sitter by extension.
        # On markdown there is no AST — fall back to heading (lossless) rather than
        # mis-routing through the code chunker.
        if (file_path or "").endswith((".md", ".markdown")):
            return split_by("heading", body, file_path, **params)
        return _normalize(chunk_file_body(body, file_path or "x.py"))
    if s == "whole":
        nodes = _normalize(_chunk_whole(body, file_path or "x"))
        # Obsidian full_note byte-parity: the old vault engine prepended the note
        # stem as line 1 (`{stem}\n{body}` — chunker.py) so the note NAME is part
        # of the searchable/embeddable chunk. Preserve it under `title_prefix` so a
        # migrated vault matches its instant baseline (owner: obsidian = byte-parity).
        if params.get("title_prefix") and file_path:
            from pathlib import Path as _P
            stem = _P(file_path).stem
            if stem:
                for n in nodes:
                    n["content"] = f"{stem}\n{n['content']}"
        return nodes
    if s == "sliding":
        # _sliding_window needs breadcrumb context; until a wrapper lands, fall
        # back to heading (lossless) rather than silently mis-splitting.
        return split_by("heading", body, file_path, **params)
    if s == "template":
        sections = params.get("sections") or []
        return _split_template(body, sections)
    raise ValueError(f"unknown split strategy: {strategy!r} (valid: {sorted(VALID_SPLITS)})")


def _split_template(body: str, sections: list[str]) -> list[dict]:
    """NEW (stub): split a body at a declared section-title vocabulary; one node
    per declared section in canonical order. Present/missing signal is a later
    concern (storage TBD). Absent sections → whole body."""
    if not sections:
        return [{"content": body.strip(), "title": "", "position": 0, "depth": 1}]
    lines = body.splitlines()
    idx = {}  # section_title -> line index
    for i, ln in enumerate(lines):
        h = ln.lstrip("#").strip()
        for s in sections:
            if h == s:
                idx[s] = i
    present = [(s, idx[s]) for s in sections if s in idx]
    present.sort(key=lambda x: x[1])
    out = []
    for pos, (s, start) in enumerate(present):
        end = present[pos + 1][1] if pos + 1 < len(present) else len(lines)
        out.append({"content": "\n".join(lines[start:end]).strip(),
                    "title": s, "position": pos, "depth": 1})
    if not out:
        out = [{"content": body.strip(), "title": "", "position": 0, "depth": 1}]
    return out


def nearest_flexchunk(file_path: str, cache: dict | None = None) -> dict | None:
    """Cascade: nearest `.flexchunk.json` walking up from the file's dir. Returns
    its dict (e.g. {'split':'ast'}) or None. Highest precedence in resolution."""
    cache = cache if cache is not None else {}
    d = Path(file_path).parent
    while True:
        if d not in cache:
            cj = d / _FLEXCHUNK
            val = None
            if cj.is_file():
                try:
                    val = json.loads(cj.read_text())
                except Exception:
                    val = None
            cache[d] = val
        if cache[d]:
            return cache[d]
        if d.parent == d:
            return None
        d = d.parent


def _folder_matches(pat: str, folder: str) -> bool:
    """True if `pat` occurs as a contiguous run of whole path segments in `folder`.
    Segment-bounded so 'archive' matches …/archive/… but not …/my-archived-notes."""
    fp = [p for p in Path(folder or "").parts if p not in ("/", "\\", "")]
    pp = [p for p in Path(pat or "").parts if p not in ("/", "\\", "")]
    if not pp:
        return False
    return any(fp[i:i + len(pp)] == pp for i in range(len(fp) - len(pp) + 1))


def resolve_rule(coordinate: tuple | None, folder: str, file_path: str,
                 chunking: dict | None, cache: dict | None = None,
                 default_level: int = 1) -> dict:
    """Pick the {split, params} rule for a file. Precedence:
    nearest .flexchunk.json > by_folder > by_subtype > default > heading@default_level.
    default_level carries the corpus split_level so parity holds when no chunking block."""
    sidecar = nearest_flexchunk(file_path, cache)
    if sidecar and sidecar.get("split"):
        return _alias(sidecar)
    chunking = chunking or {}
    # by_folder: longest matching path prefix. Match on whole path segments so
    # 'archive' matches /vault/archive but NOT /vault/my-archived-notes (a
    # substring test would); pat may itself be multi-segment ('archive/2024').
    by_folder = chunking.get("by_folder") or {}
    best = None
    for pat, rule in by_folder.items():
        if _folder_matches(pat, folder) and (best is None or len(pat) > len(best[0])):
            best = (pat, rule)
    if best:
        return _alias(best[1])
    # by_subtype: the coordinate's subtype
    if coordinate:
        sub = coordinate[1] if len(coordinate) > 1 else None
        by_sub = chunking.get("by_subtype") or {}
        if sub and sub in by_sub:
            return _alias(by_sub[sub])
    if chunking.get("default"):
        return _alias(chunking["default"])
    return {"split": "heading", "level": default_level}


def _alias(rule: dict) -> dict:
    """Map instant's nest/flat/whole onto the unified vocabulary."""
    r = dict(rule)
    s = r.get("split")
    if s == "nest":
        r["split"] = "heading"
    elif s == "flat":
        # instant 'flat' = chunk_file_body (ast route for code, heading for md)
        r["split"] = "ast"
    return r


def resolve_chunker(coordinate, folder, file_path, chunking=None, cache=None,
                    default_level=1):
    """Return a callable body -> normalized nodes, per the resolved rule."""
    rule = resolve_rule(coordinate, folder, file_path, chunking, cache, default_level)
    split = rule.get("split", "heading")
    if split not in VALID_SPLITS:
        raise ValueError(f"unknown split strategy: {split!r}")
    params = {k: v for k, v in rule.items() if k != "split"}
    return lambda body: split_by(split, body, file_path, **params)
