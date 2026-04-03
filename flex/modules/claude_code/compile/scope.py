"""
Scope — compile-time data filtering for claude_code cells.

Decides what enters a cell at ingest time. Three filtering levels:
  JSONL -> [SCOPE] -> parse -> chunks -> [NOISE] -> graph/enrich -> [SURFACE] -> views

User config: ~/.flex/config.json
  { "scope": {
      "exclude_tools": ["mcpj5_*", "some_other_tool"],
      "exclude_paths": ["/\\.my_infra/", "/\\.custom/hooks/"]
  }}

Glob patterns via fnmatch (tools), regex patterns (paths). Loaded once at import time.
"""

import json
import re
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional


# --- Built-in patterns ---

SKIP_PATH_PATTERNS = [
    r'^/tmp/',
    r'node_modules',
    r'\.git/',
    r'__pycache__',
    r'\.pyc$',
    # Infrastructure paths that carry no project signal
    r'/\.claude/hooks/',
]

SKIP_BASH_PATTERNS = [
    r'^(ls|pwd|which|type|file|stat|echo|cd)( |$)',
    r'^(cat|head|tail) ',  # Should use Read tool
]


# --- User config: tool exclusion ---

_user_exclude_tools: list[str] = []
_user_exclude_paths: list[str] = []

def _load_user_config():
    """Load scope config from ~/.flex/config.json once."""
    global _user_exclude_tools, _user_exclude_paths
    config_path = Path.home() / '.flex' / 'config.json'
    if config_path.exists():
        try:
            config = json.loads(config_path.read_text())
            scope = config.get('scope', {})
            _user_exclude_tools = scope.get('exclude_tools', [])
            _user_exclude_paths = scope.get('exclude_paths', [])
        except (json.JSONDecodeError, OSError):
            pass

_load_user_config()


def get_all_skip_path_patterns() -> list[str]:
    """Return built-in + user-configured path exclusion patterns."""
    return SKIP_PATH_PATTERNS + _user_exclude_paths


def excluded_tool(tool_name: str) -> bool:
    """Check if a tool name matches any user-configured exclusion pattern.

    Uses fnmatch glob patterns from ~/.flex/config.json scope.exclude_tools.
    """
    if not _user_exclude_tools:
        return False
    for pattern in _user_exclude_tools:
        if fnmatch(tool_name, pattern):
            return True
    return False


def should_skip_file(file_path: Optional[str]) -> bool:
    """Check if a file path should be skipped."""
    if not file_path:
        return False
    for pattern in SKIP_PATH_PATTERNS:
        if re.search(pattern, file_path):
            return True
    return False


def should_skip_bash(command: Optional[str]) -> bool:
    """Check if a bash command should be skipped."""
    if not command:
        return False
    for pattern in SKIP_BASH_PATTERNS:
        if re.search(pattern, command):
            return True
    return False


def should_skip_event(event: dict) -> bool:
    """Check if an entire event should be skipped."""
    tool = event.get("tool", "")
    file_path = event.get("file", "")
    command = event.get("command", "")

    if should_skip_file(file_path):
        return True
    if tool == "Bash" and should_skip_bash(command):
        return True
    if excluded_tool(tool):
        return True
    return False
