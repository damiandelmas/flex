"""
URL Identity - Stable identifiers for URLs

Tracks web resources with fetch history and drift detection.
"""

from .identity import URLIdentity, URLInfo, FetchInfo, get_instance

__all__ = ["URLIdentity", "URLInfo", "FetchInfo", "get_instance"]
