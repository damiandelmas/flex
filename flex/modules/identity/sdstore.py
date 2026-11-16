"""sd-store identity provider: the surface the fs/docpac writers use.

Drives the `sd-seam` binary (a CLI over Spacedrive's sd-store). One `walk` per
source root, cached as absolute path -> uuid. Stores live under
$FLEX_HOME/identity/sdstore/<cell_id>/, one sd-store source per root.
"""
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path


def _home() -> Path:
    return Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))


def selections(conn: sqlite3.Connection) -> list[str]:
    """Source roots of a cell: _meta `selections` (JSON list), else `source_path`."""
    meta = dict(conn.execute("SELECT key, value FROM _meta").fetchall())
    roots = json.loads(meta.get("selections") or "[]") or [meta.get("source_path")]
    return [str(Path(r).resolve()) for r in roots if r]


class SdStore:
    def __init__(self, cell_id: str = "default", roots: list[str] | None = None):
        self.cell_id = cell_id
        self.roots: list[str] = []
        self.sources = _home() / "identity" / "sdstore" / cell_id
        local = Path.home() / ".local" / "bin" / "sd-seam"  # services' PATH may omit it
        self.seam = (os.environ.get("FLEX_SD_SEAM") or shutil.which("sd-seam")
                     or (str(local) if local.is_file() else "sd-seam"))
        self.cache: dict[str, dict[str, str]] = {}  # root -> {abs path: uuid}
        self.kinds: dict[str, dict[str, str]] = {}  # root -> {abs path: kind}
        for r in roots or []:
            self.bind(r)

    def bind(self, root: str) -> None:
        root = str(Path(root).resolve())
        if root not in self.roots:
            self.roots.append(root)
            self.roots.sort(key=len, reverse=True)  # longest root wins

    def _source(self, root: str) -> str:
        return "r-" + hashlib.sha256(root.encode()).hexdigest()[:12]

    def _run(self, cmd: str, root: str, *args) -> list[dict]:
        self.sources.mkdir(parents=True, exist_ok=True)
        if cmd == "walk" and not (self.sources / self._source(root) / "data.db").exists():
            self._run("init", root)
        p = subprocess.run([self.seam, cmd, str(self.sources), self._source(root), *map(str, args)],
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError(f"sd-seam {cmd}: {p.stderr.strip()}")
        return [json.loads(l) for l in p.stdout.splitlines() if l.strip()]

    def walk(self, root: str) -> dict[str, str]:
        """Walk a root once; returns and caches {absolute path: uuid}."""
        rows = [r for r in self._run("walk", root, root) if "path" in r]
        self.cache[root] = {f"{root}/{r['path']}": r["uuid"] for r in rows}
        self.kinds[root] = {f"{root}/{r['path']}": r["kind"] for r in rows}
        return self.cache[root]

    def _root_of(self, path: str) -> str | None:
        return next((r for r in self.roots if path == r or path.startswith(r + "/")), None)

    def assign_batch(self, paths: list[str]) -> dict[str, str]:
        out: dict[str, str] = {}
        stale: set[str] = set()
        for p in paths:
            root = self._root_of(p)
            if root is None:
                raise ValueError(f"no bound source root contains {p}")
            if root not in self.cache:
                self.walk(root)
            elif p not in self.cache[root] and os.path.lexists(p):
                stale.add(root)  # new since the cached walk
        for root in stale:
            self.walk(root)
        for p in paths:
            u = self.cache[self._root_of(p)].get(p)
            if u:
                out[p] = u
        return out

    def assign(self, path: str) -> str | None:
        root = self._root_of(path)
        if root is not None and root not in self.cache and (self.sources / self._source(root) / "data.db").exists():
            found = self._run("resolve", root, os.path.relpath(path, root))[0]["uuid"]
            if found:
                return found
        return self.assign_batch([path]).get(path)
