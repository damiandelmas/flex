"""Module spec discovery.

Installable modules may expose a ``MODULE`` dict from ``install.py``. Core
callers use these specs to resolve substrate assets without hardcoding every
cell type in CLI or MCP-adjacent code.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import re
import sys
import types
from functools import lru_cache
from pathlib import Path
from typing import Any


MODULES_ROOT = Path(__file__).resolve().parent
EXTERNAL_MODULES_ENV = "FLEX_MODULE_PATH"

_LEGACY_CELL_TYPE_ALIASES = {
    "claude-code": "claude_code",
    "claude_code": "claude_code",
    "obsidian": "markdown",
}


def normalize_cell_type(cell_type: str | None) -> str | None:
    if not cell_type:
        return None
    return _LEGACY_CELL_TYPE_ALIASES.get(cell_type, cell_type.replace("-", "_"))


def flex_home() -> Path:
    """Return the active Flex home directory."""
    return Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))


def user_modules_root() -> Path:
    """Return the user-installed module root."""
    return flex_home() / "modules"


def external_module_roots() -> list[Path]:
    """Return module roots outside the installed flex package.

    ``~/.flex/modules`` is the default user install location. ``FLEX_MODULE_PATH``
    is a colon-separated development override for local labs checkouts.
    """
    roots = [user_modules_root()]
    raw = os.environ.get(EXTERNAL_MODULES_ENV, "")
    for part in raw.split(os.pathsep):
        if part.strip():
            roots.append(Path(part).expanduser())
    return roots


def _load_module_from_path(folder: str, module_file: Path, kind: str):
    """Load an external module file without requiring a flex.* import path."""
    safe_folder = re.sub(r"\W+", "_", folder)
    package_name = f"_flex_external_modules.{safe_folder}"
    module_name = f"{package_name}.{kind}"
    if "_flex_external_modules" not in sys.modules:
        root_pkg = types.ModuleType("_flex_external_modules")
        root_pkg.__path__ = []
        sys.modules["_flex_external_modules"] = root_pkg
    pkg = types.ModuleType(package_name)
    pkg.__path__ = [str(module_file.parent)]
    sys.modules[package_name] = pkg
    promoted_name = f"flex.modules.{safe_folder}"
    if promoted_name not in sys.modules:
        promoted_pkg = types.ModuleType(promoted_name)
        promoted_pkg.__path__ = [str(module_file.parent)]
        sys.modules[promoted_name] = promoted_pkg
    module_root = str(module_file.parent)
    if module_root not in sys.path:
        sys.path.insert(0, module_root)
    spec = importlib.util.spec_from_file_location(module_name, module_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {module_file}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_install_module_from_path(folder: str, install_py: Path):
    """Load an external install.py without requiring a flex.* import path."""
    return _load_module_from_path(folder, install_py, "install")


def _load_plugin_module_from_path(folder: str, plugin_py: Path):
    """Load an external plugin.py without requiring a flex.* import path."""
    return _load_module_from_path(folder, plugin_py, "plugin")


def discover_install_modules() -> dict[str, dict[str, Any]]:
    """Return installable modules from packaged flex plus user module roots."""
    discovered: dict[str, dict[str, Any]] = {}

    for install_py in sorted(MODULES_ROOT.glob("*/install.py")):
        folder = install_py.parent.name
        if folder.startswith("_"):
            continue
        import_path = f"flex.modules.{folder}.install"
        try:
            mod = importlib.import_module(import_path)
        except Exception:
            continue
        if not hasattr(mod, "run"):
            continue
        cli_name = getattr(mod, "CLI_NAME", folder.replace("_", "-"))
        discovered[cli_name] = {
            "import_path": import_path,
            "module": mod,
            "summary": getattr(mod, "MODULE_SUMMARY", ""),
            "folder": folder,
            "root": install_py.parent,
            "source": "packaged",
        }

    for root in external_module_roots():
        if not root.exists():
            continue
        for install_py in sorted(root.glob("*/install.py")):
            folder = install_py.parent.name
            if folder.startswith("_"):
                continue
            try:
                mod = _load_install_module_from_path(folder, install_py)
            except Exception:
                continue
            if not hasattr(mod, "run"):
                continue
            cli_name = getattr(mod, "CLI_NAME", folder.replace("_", "-"))
            discovered[cli_name] = {
                "import_path": None,
                "module": mod,
                "summary": getattr(mod, "MODULE_SUMMARY", ""),
                "folder": folder,
                "root": install_py.parent,
                "source": "external",
            }

    return discovered


def discover_plugin_modules() -> list[Any]:
    """Return plugin.py modules discovered from external module roots."""
    plugins: list[Any] = []
    for root in external_module_roots():
        if not root.exists():
            continue
        for plugin_py in sorted(root.glob("*/plugin.py")):
            folder = plugin_py.parent.name
            if folder.startswith("_"):
                continue
            try:
                plugins.append(_load_plugin_module_from_path(folder, plugin_py))
            except Exception:
                continue
    return plugins


def discover_query_tokens() -> dict[str, str]:
    """Return query-token registrations from external plugin.py files."""
    tokens: dict[str, str] = {}
    for mod in discover_plugin_modules():
        register = getattr(mod, "register_query_tokens", None)
        if not callable(register):
            continue
        try:
            value = register()
        except Exception:
            continue
        if isinstance(value, dict):
            tokens.update({str(k): str(v) for k, v in value.items()})
    return tokens


def discover_query_materializers() -> list[Any]:
    """Return query materializers from external plugin.py files."""
    materializers: list[Any] = []
    for mod in discover_plugin_modules():
        register = getattr(mod, "register_query_materializers", None)
        if not callable(register):
            continue
        try:
            value = register()
        except Exception:
            continue
        if value:
            materializers.extend(list(value))
    return materializers


@lru_cache(maxsize=1)
def discover_module_specs() -> dict[str, dict[str, Any]]:
    """Return install-module specs keyed by cell_type."""
    specs: dict[str, dict[str, Any]] = {}
    for entry in discover_install_modules().values():
        mod = entry["module"]
        spec = getattr(mod, "MODULE", None)
        if not isinstance(spec, dict):
            continue
        cell_type = spec.get("cell_type")
        if cell_type:
            out = dict(spec)
            out.setdefault("_module_root", entry["root"])
            out.setdefault("_module_source", entry["source"])
            specs[str(cell_type)] = out
    return specs


def module_spec_for(cell_type: str | None) -> dict[str, Any] | None:
    if not cell_type:
        return None
    specs = discover_module_specs()
    if cell_type in specs:
        return specs[cell_type]
    normalized = normalize_cell_type(cell_type)
    if normalized and normalized in specs:
        return specs[normalized]
    return None


def asset_modules_for(cell_type: str | None, key: str) -> list[str]:
    """Resolve asset module names from a module spec field.

    ``key`` is usually ``views_from`` or ``presets_from``. Values are module
    directory names, not cell names.
    """
    spec = module_spec_for(cell_type)
    if spec and spec.get(key):
        return [normalize_cell_type(str(v)) or str(v) for v in spec[key]]
    normalized = normalize_cell_type(cell_type)
    return [normalized] if normalized else []


def enrichment_stubs_from(cell_type: str | None) -> str | None:
    spec = module_spec_for(cell_type)
    if spec and spec.get("enrichment_stubs_from"):
        return normalize_cell_type(str(spec["enrichment_stubs_from"]))
    return normalize_cell_type(cell_type)


def stock_subdirs(cell_type: str | None, key: str, subdir: str) -> list[Path]:
    """Return module stock asset directories for ``key`` and ``subdir``."""
    out: list[Path] = []
    for module_name in asset_modules_for(cell_type, key):
        spec = module_spec_for(module_name)
        root = Path(spec["_module_root"]) if spec and spec.get("_module_root") else MODULES_ROOT / module_name
        path = root / "stock" / subdir
        if path.exists() and any(path.glob("*.sql")):
            out.append(path)
    return out
