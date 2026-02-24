"""
Soma Identity Ecosystem

Creates Tier 1 pointers. Domain-specific, consumer-agnostic.

Primitives:
    file_identity    - stable file UUIDs (survives moves/renames)
    content_identity - content-addressed storage (eternal, deduplicated)
    repo_identity    - repo identity by root_commit (survives moves/clones)
    url_identity     - stable URL identifiers (fetch history, drift detection)
"""

# Import submodules for `from soma.identity import file_identity` pattern
from . import file_identity
from . import content_identity
from . import repo_identity
from . import url_identity

# Import classes for convenience
from .file_identity import FileIdentity, FileInfo
from .file_identity import get_instance as get_file_identity

from .content_identity import ContentIdentity, ContentInfo
from .content_identity import get_instance as get_content_identity

from .repo_identity import RepoIdentity, Repo
# Backward compat alias
GitRegistry = RepoIdentity

from .url_identity import URLIdentity, URLInfo, FetchInfo
from .url_identity import get_instance as get_url_identity

__all__ = [
    # Submodules
    "file_identity", "content_identity", "repo_identity", "url_identity",
    # Classes
    "FileIdentity", "FileInfo", "get_file_identity",
    "ContentIdentity", "ContentInfo", "get_content_identity",
    "RepoIdentity", "Repo", "GitRegistry",
    "URLIdentity", "URLInfo", "FetchInfo", "get_url_identity",
]
