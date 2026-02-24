"""
SOMA identity module — compile-time enrichment.

Stamps stable identity (file UUID, repo root, content hash, URL UUID)
onto chunks at capture time. Ported from Thread lib/enrich.py.

Key changes from Thread:
  - GitRegistry replaced by RepoIdentity
  - except Exception: pass → logged to stderr
  - insert_edges() extracted from worker.py
  - ensure_tables() runs tables.sql DDL
"""

import base64
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Identity Applicability Rules
# ─────────────────────────────────────────────────────────────────────────────

IDENTITY_APPLICABILITY = {
    'file_uuid': {
        'description': 'Stable UUID from SOMA FileIdentity (survives renames)',
        'applicable_tools': ['Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep'],
        'exclude_paths': [r'^/tmp/', r'^/var/tmp/', r'^/dev/'],
        'requires': 'file exists at enrichment time',
    },
    'repo_root': {
        'description': 'Git root commit hash (survives repo moves)',
        'applicable_tools': ['Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep', 'Bash'],
        'exclude_paths': [r'^/tmp/', r'^/var/tmp/'],
        'requires': 'file is inside a git repository',
    },
    'content_hash': {
        'description': 'SHA-256 of file content (content-addressable)',
        'applicable_tools': ['Write', 'Edit', 'MultiEdit'],
        'exclude_paths': [],
        'requires': 'file exists at enrichment time',
    },
    'blob_hash': {
        'description': 'Git SHA-1 blob hash',
        'applicable_tools': ['Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep'],
        'exclude_paths': [r'^/tmp/'],
        'requires': 'file is inside a git repository',
    },
    'is_tracked': {
        'description': 'Boolean: file is tracked by git',
        'applicable_tools': ['Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep'],
        'exclude_paths': [],
        'requires': 'file is inside a git repository',
    },
    'url_uuid': {
        'description': 'Stable URL identity from SOMA URLIdentity',
        'applicable_tools': ['WebFetch'],
        'exclude_paths': [],
        'requires': 'URL present in content',
    },
}

NON_FILE_TOOLS = [
    'user_prompt', 'assistant',
    'Task', 'TaskOutput',
    'WebSearch',
    'TodoWrite',
    'AskUserQuestion',
    'Skill',
]

# SQL fragments for applicability filtering (used by audit.py)
APPLICABLE_FILE_TOOLS = "('Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep')"
APPLICABLE_MUTATION_TOOLS = "('Write', 'Edit', 'MultiEdit')"
APPLICABLE_REPO_TOOLS = "('Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep', 'Bash')"

# Convenience sets for Python-side checks
FILE_TOOLS = {'Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep'}
MUTATION_TOOLS = {'Write', 'Edit', 'MultiEdit'}
REPO_TOOLS = {'Write', 'Edit', 'MultiEdit', 'Read', 'Glob', 'Grep', 'Bash'}


# ─────────────────────────────────────────────────────────────────────────────
# Optional dependencies — SOMA identity subsystems
# ─────────────────────────────────────────────────────────────────────────────

_FILE_IDENTITY = None
_CONTENT_IDENTITY = None
_REPO_IDENTITY = None
_URL_IDENTITY = None
AVAILABLE = False

try:
    from flex.modules.soma.lib.identity.file_identity import FileIdentity
    from flex.modules.soma.lib.identity.content_identity import ContentIdentity
    from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
    from flex.modules.soma.lib.identity.url_identity import URLIdentity
    _FILE_IDENTITY = FileIdentity()
    _CONTENT_IDENTITY = ContentIdentity()
    _REPO_IDENTITY = RepoIdentity()
    _URL_IDENTITY = URLIdentity()
    AVAILABLE = True
except ImportError:
    pass


# ─────────────────────────────────────────────────────────────────────────────
# DDL — ensure tables
# ─────────────────────────────────────────────────────────────────────────────

def ensure_tables(conn: sqlite3.Connection):
    """Create SOMA identity edge tables if they don't exist. Idempotent."""
    sql_path = Path(__file__).parent / 'tables.sql'
    conn.executescript(sql_path.read_text())


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_git_tracked(file_path: str, repo: str) -> bool:
    """Check if a file is tracked by git (not just in a git repo)."""
    if not file_path or not repo:
        return False
    try:
        rel_path = os.path.relpath(file_path, repo)
        result = subprocess.run(
            ["git", "-C", repo, "ls-files", "--error-unmatch", rel_path],
            capture_output=True, text=True, timeout=5
        )
        return result.returncode == 0
    except Exception as e:
        print(f"[soma] is_git_tracked failed for {file_path}: {e}", file=sys.stderr)
        return False


def _get_git_info(file_path: str, cwd: str = "", tool: str = "") -> dict:
    """Get git repo info for a file.

    Returns dict with: repo, blob_hash, old_blob_hash, is_tracked
    """
    result = {"repo": "", "blob_hash": "", "old_blob_hash": "", "is_tracked": False}

    if not file_path and not cwd:
        return result

    check_path = file_path if file_path and os.path.exists(file_path) else cwd
    if not check_path:
        return result

    try:
        if os.path.isdir(check_path):
            repo_dir = check_path
        else:
            repo_dir = os.path.dirname(check_path)

        repo_result = subprocess.run(
            ["git", "-C", repo_dir, "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=5
        )
        if repo_result.returncode != 0:
            return result

        repo = repo_result.stdout.strip()
        result["repo"] = repo

        if file_path and os.path.isfile(file_path):
            blob_result = subprocess.run(
                ["git", "hash-object", file_path],
                capture_output=True, text=True, timeout=5
            )
            if blob_result.returncode == 0:
                result["blob_hash"] = blob_result.stdout.strip()

            result["is_tracked"] = _is_git_tracked(file_path, repo)

            # Blob at HEAD for file-touching tools (only if tracked)
            if tool in ("Read", "Edit", "Write", "MultiEdit", "Glob", "Grep") and result["is_tracked"]:
                rel_path = file_path.replace(repo + "/", "")
                old_result = subprocess.run(
                    ["git", "-C", repo, "rev-parse", f"HEAD:{rel_path}"],
                    capture_output=True, text=True, timeout=5
                )
                if old_result.returncode == 0:
                    old_blob = old_result.stdout.strip()
                    if not old_blob.startswith("fatal"):
                        result["old_blob_hash"] = old_blob
    except Exception as e:
        print(f"[soma] _get_git_info failed for {file_path}: {e}", file=sys.stderr)

    return result


def _get_content_hash(file_path: str, session: str = "", msg: int = 0,
                      blob_hash: str = "") -> Optional[str]:
    """Compute and store content hash (SHA-256) via ContentIdentity."""
    if not file_path or not _CONTENT_IDENTITY:
        return None
    if not os.path.isfile(file_path):
        return None

    try:
        content = Path(file_path).read_bytes()
        content_hash = _CONTENT_IDENTITY.store(content)

        if session:
            _CONTENT_IDENTITY.add_ref(content_hash, "episode", f"{session}:{msg}")
        if blob_hash:
            _CONTENT_IDENTITY.add_ref(content_hash, "blob", blob_hash)

        return content_hash
    except Exception as e:
        print(f"[soma] _get_content_hash failed for {file_path}: {e}", file=sys.stderr)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Public API — enrich
# ─────────────────────────────────────────────────────────────────────────────

def enrich(chunk: dict) -> dict:
    """Stamp identity fields onto a chunk dict.

    Reads: chunk['file'], chunk['cwd'], chunk['tool'], chunk['url'],
           chunk['web_content'], chunk['web_status'], chunk['session'], chunk['msg']
    Sets:  chunk['file_uuid'], chunk['repo_root'], chunk['blob_hash'],
           chunk['old_blob_hash'], chunk['content_hash'], chunk['is_tracked'],
           chunk['url_uuid'], chunk['web_content_hash'], chunk['file_relative'],
           chunk['repo_remote']
    """
    file_path = chunk.get("file", "")
    cwd = chunk.get("cwd", "")
    tool = chunk.get("tool", "")

    # Git info (repo, blob_hash, old_blob_hash, is_tracked)
    git_info = _get_git_info(file_path, cwd, tool)
    for key, value in git_info.items():
        if value or key == "is_tracked":
            chunk[key] = value

    # RepoIdentity (replaces GitRegistry)
    if file_path and _REPO_IDENTITY:
        try:
            resolved = _REPO_IDENTITY.resolve_file(file_path)
            if resolved:
                relative, repo = resolved
                if relative:
                    chunk["file_relative"] = relative
                if repo.root_commit:
                    chunk["repo_root"] = repo.root_commit
                if repo.remote_url:
                    chunk["repo_remote"] = repo.remote_url
        except Exception as e:
            print(f"[soma] repo_identity failed for {file_path}: {e}", file=sys.stderr)

    # FileIdentity UUID (stable across renames/moves)
    if file_path and _FILE_IDENTITY:
        try:
            file_uuid = _FILE_IDENTITY.assign(file_path)
            if file_uuid:
                chunk["file_uuid"] = file_uuid
        except Exception as e:
            print(f"[soma] file_identity failed for {file_path}: {e}", file=sys.stderr)

    # URL identity for WebFetch/WebSearch
    url = chunk.get("url", "")
    web_content = chunk.get("web_content", "")
    if url and _URL_IDENTITY:
        try:
            is_search = tool == "WebSearch"
            url_uuid = _URL_IDENTITY.assign(url, is_search=is_search)
            if url_uuid:
                chunk["url_uuid"] = url_uuid

                # Store web content if present (WebFetch)
                if web_content and tool == "WebFetch":
                    content_hash = _URL_IDENTITY.record_fetch(
                        url_uuid,
                        content=web_content,
                        status_code=chunk.get("web_status", 200),
                        session_id=chunk.get("session", ""),
                        prompt=chunk.get("prompt", "")
                    )
                    if content_hash:
                        chunk["web_content_hash"] = content_hash
                    # Strip raw content — only hash stored in chunk
                    chunk.pop("web_content", None)
        except Exception as e:
            print(f"[soma] url_identity failed for {url}: {e}", file=sys.stderr)

    # Content hash for file-mutating tools
    if file_path and tool in ("Write", "Edit", "MultiEdit", "Read"):
        content_hash = _get_content_hash(
            file_path,
            session=chunk.get("session", ""),
            msg=chunk.get("msg", 0),
            blob_hash=git_info.get("blob_hash", "")
        )
        if content_hash:
            chunk["content_hash"] = content_hash

    return chunk


# ─────────────────────────────────────────────────────────────────────────────
# Public API — insert_edges
# ─────────────────────────────────────────────────────────────────────────────

def insert_edges(conn: sqlite3.Connection, chunk: dict):
    """Write identity fields from chunk dict into SOMA edge tables.

    Extracted from worker.py insert_chunk_atom() lines 194-220.
    """
    cur = conn.cursor()
    chunk_id = chunk['id']

    if chunk.get('file_uuid'):
        cur.execute(
            "INSERT OR IGNORE INTO _edges_file_identity (chunk_id, file_uuid) VALUES (?, ?)",
            (chunk_id, chunk['file_uuid'])
        )

    if chunk.get('repo_root'):
        cur.execute(
            "INSERT OR IGNORE INTO _edges_repo_identity (chunk_id, repo_root, is_tracked) VALUES (?, ?, ?)",
            (chunk_id, chunk['repo_root'], chunk.get('is_tracked'))
        )

    if chunk.get('content_hash'):
        cur.execute(
            "INSERT OR IGNORE INTO _edges_content_identity (chunk_id, content_hash, blob_hash, old_blob_hash) VALUES (?, ?, ?, ?)",
            (chunk_id, chunk['content_hash'], chunk.get('blob_hash'), chunk.get('old_blob_hash'))
        )

    if chunk.get('url_uuid'):
        cur.execute(
            "INSERT OR IGNORE INTO _edges_url_identity (chunk_id, url_uuid) VALUES (?, ?)",
            (chunk_id, chunk['url_uuid'])
        )


# ─────────────────────────────────────────────────────────────────────────────
# Lookup utilities
# ─────────────────────────────────────────────────────────────────────────────

def find_content_by_blob(blob_hash: str) -> Optional[str]:
    """Find content_hash from blob_hash."""
    if not _CONTENT_IDENTITY or not blob_hash:
        return None
    return _CONTENT_IDENTITY.find_by_ref("blob", blob_hash)


def find_episodes_by_content(content_hash: str) -> list[str]:
    """Find all episode references for a content_hash."""
    if not _CONTENT_IDENTITY or not content_hash:
        return []
    refs = _CONTENT_IDENTITY.get_refs(content_hash)
    return [ref_id for ref_type, ref_id in refs if ref_type == "episode"]


# ─────────────────────────────────────────────────────────────────────────────
# Image extraction (tool_result base64 -> content-store)
# ─────────────────────────────────────────────────────────────────────────────

def extract_tool_result_images(tools_used: str, session: str = "", msg: int = 0) -> tuple[str, list]:
    """Extract base64 images from tool_result content, store in content-store."""
    if not tools_used or not _CONTENT_IDENTITY:
        return tools_used, []

    try:
        tools = json.loads(tools_used)
    except (json.JSONDecodeError, TypeError):
        return tools_used, []

    if not isinstance(tools, list):
        return tools_used, []

    image_hashes = []
    modified = False

    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if 'tool_use_id' not in tool:
            continue

        content = tool.get('content')
        if not isinstance(content, list):
            continue

        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get('type') != 'image':
                continue

            source = item.get('source', {})
            if source.get('type') != 'base64':
                continue
            if 'data' not in source:
                continue

            media_type = source.get('media_type', 'image/png')
            try:
                image_bytes = base64.b64decode(source['data'])
            except Exception:
                continue

            try:
                content_hash = _CONTENT_IDENTITY.store(image_bytes, mime_type=media_type)

                if session:
                    _CONTENT_IDENTITY.add_ref(content_hash, "image", f"{session}:{msg}")

                image_hashes.append({
                    "hash": content_hash,
                    "media_type": media_type,
                    "size": len(image_bytes)
                })

                del source['data']
                source['content_hash'] = content_hash
                modified = True

            except Exception as e:
                print(f"[soma] image extraction failed: {e}", file=sys.stderr)

    if modified:
        return json.dumps(tools), image_hashes
    return tools_used, image_hashes
