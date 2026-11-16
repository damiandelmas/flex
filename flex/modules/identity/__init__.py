"""File identity provider selector. FLEX_IDENTITY=sd-store|soma (default soma; sd-store only when the optional sd-seam helper is installed).

sd-store mints inside a cell's bound roots (its _meta selections). SOMA stays
readable and selectable with FLEX_IDENTITY=soma; existing SOMA uuids resolve
through each cell's _identity_bridge (python -m flex.modules.identity.bridge).
"""
import os
import shutil
from pathlib import Path

_sd = None


def _default_provider() -> str:
    """sd-store when its sd-seam binary is installed, else SOMA.

    A host without the binary (the public wheel does not ship it) keeps minting
    through SOMA instead of failing every required-identity write.
    """
    seam = (os.environ.get("FLEX_SD_SEAM") or shutil.which("sd-seam")
            or (Path.home() / ".local" / "bin" / "sd-seam"))
    return "sd-store" if seam and Path(seam).is_file() else "soma"


def get_instance(conn=None):
    """The provider the fs/docpac writers mint through. Read in this one place.

    With `conn` (the cell being written), the sd-store provider binds that cell's
    own roots from its `_meta` selections, so no caller has to know them.
    """
    global _sd
    provider = os.environ.get("FLEX_IDENTITY") or _default_provider()
    if conn is not None:
        # A cell may pin its provider in _meta ('identity': 'soma' | 'sd-store').
        # Code cells whose roots are whole repositories stay on SOMA: sd-store
        # mints by walking each root in full, which is impractical there.
        try:
            row = conn.execute("SELECT value FROM _meta WHERE key='identity'").fetchone()
            if row and row[0] in ("soma", "sd-store"):
                provider = row[0]
        except Exception:
            pass
    if provider == "sd-store":
        if _sd is None:
            from .sdstore import SdStore
            _sd = SdStore(os.environ.get("FLEX_IDENTITY_CELL", "default"),
                          [r for r in os.environ.get("FLEX_IDENTITY_ROOTS", "").split(os.pathsep) if r])
        if conn is not None:
            from .sdstore import selections
            try:
                meta = dict(conn.execute("SELECT key, value FROM _meta").fetchall())
                cell_id = meta.get("cell_id") or _sd.cell_id
                if cell_id != _sd.cell_id:
                    _sd = SdStore(cell_id)
                for r in selections(conn):
                    _sd.bind(r)
            except Exception:
                pass  # no _meta yet: the caller's roots, if any, still apply
        return _sd
    from flex.modules.soma.lib.identity.file_identity import get_instance as soma
    return soma()


def bind(cell_id: str, roots: list[str]):
    """Point the sd-store provider at a cell's roots (compile entry points call this)."""
    global _sd
    from .sdstore import SdStore
    _sd = SdStore(cell_id, roots)
    return _sd
