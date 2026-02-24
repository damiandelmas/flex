"""
Eternity - Backup, versioning, and cloud sync for soma databases.

Everything survives time.
"""

from .eternity import Eternity, CloudProvider, get_instance

__all__ = ["Eternity", "CloudProvider", "get_instance"]
