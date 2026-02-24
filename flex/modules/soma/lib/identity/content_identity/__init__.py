"""
Content Identity - Content-addressed storage for SOMA.

Usage:
    from content_identity import ContentIdentity

    ci = ContentIdentity()
    content_hash = ci.store("content here")
    content = ci.retrieve(content_hash)
"""

from .identity import ContentIdentity, ContentInfo, get_instance

__all__ = ["ContentIdentity", "ContentInfo", "get_instance"]
