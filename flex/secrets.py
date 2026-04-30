"""Shared secret resolution for refresh modules and service code."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SecretResult:
    name: str
    value: str | None
    source: str | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return bool(self.value)


def _flex_home() -> Path:
    return Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))


def _ensure_op_token() -> None:
    if os.environ.get("OP_SERVICE_ACCOUNT_TOKEN"):
        return
    token_path = _flex_home() / "sa-token"
    if token_path.exists():
        token = token_path.read_text().strip()
        if token:
            os.environ["OP_SERVICE_ACCOUNT_TOKEN"] = token


def _secret_paths() -> list[Path]:
    home = _flex_home()
    return [home / "secrets", home / "secrets.env"]


def _read_secret_file() -> dict[str, str]:
    values: dict[str, str] = {}
    for path in _secret_paths():
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def _read_op(uri: str) -> SecretResult:
    _ensure_op_token()
    try:
        proc = subprocess.run(
            ["op", "read", uri],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except FileNotFoundError:
        return SecretResult(uri, None, "op", "op CLI not found")
    except Exception as e:
        return SecretResult(uri, None, "op", str(e))
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        return SecretResult(uri, None, "op", detail or f"op exited {proc.returncode}")
    return SecretResult(uri, proc.stdout.rstrip("\n"), "op")


def load_secrets_file(*, overwrite: bool = False) -> dict[str, str]:
    """Load plain values from ~/.flex/secrets into os.environ."""
    loaded = {}
    for key, value in _read_secret_file().items():
        if value.startswith("op://"):
            continue
        if overwrite or key not in os.environ:
            os.environ[key] = value
            loaded[key] = value
    return loaded


def lookup_secret(
    name: str,
    *,
    env: str | None = None,
    op: str | None = None,
    set_env: bool = False,
) -> SecretResult:
    """Resolve a secret from env, ~/.flex/secrets, or an op:// URI."""
    env_name = env or name
    value = os.environ.get(env_name)
    if value:
        if value.startswith("op://"):
            result = _read_op(value)
            if result.ok and set_env:
                os.environ[env_name] = result.value or ""
            return SecretResult(name, result.value, f"env:{env_name}->op", result.error)
        return SecretResult(name, value, f"env:{env_name}")

    file_values = _read_secret_file()
    if env_name in file_values:
        value = file_values[env_name]
        if value.startswith("op://"):
            result = _read_op(value)
            if result.ok and set_env:
                os.environ[env_name] = result.value or ""
            return SecretResult(name, result.value, f"secrets:{env_name}->op", result.error)
        if set_env:
            os.environ[env_name] = value
        return SecretResult(name, value, f"secrets:{env_name}")

    if op:
        result = _read_op(op)
        if result.ok and set_env:
            os.environ[env_name] = result.value or ""
        return SecretResult(name, result.value, "op", result.error)

    return SecretResult(name, None, None, "not found")


def get_secret(
    name: str,
    *,
    env: str | None = None,
    op: str | None = None,
    set_env: bool = False,
) -> str | None:
    """Return a secret value, or None when unavailable."""
    return lookup_secret(name, env=env, op=op, set_env=set_env).value


def _check_one(spec: dict, *, set_env: bool) -> SecretResult:
    return lookup_secret(
        spec.get("name") or spec.get("env"),
        env=spec.get("env"),
        op=spec.get("op"),
        set_env=set_env,
    )


def check_secret_specs(specs: dict | None, *, set_env: bool = False) -> list[str]:
    """Validate a module's REQUIRES_SECRETS declaration.

    Supports either direct specs:
        {"github": {"name": "GITHUB_TOKEN", "env": "GITHUB_TOKEN"}}

    Or alternatives:
        {"x_api": {"any_of": [{"name": "TWITTERAPI_IO_KEY"}, ...]}}
    """
    if not specs:
        return []

    missing = []
    for label, spec in specs.items():
        alternatives = spec.get("any_of")
        if alternatives:
            results = [_check_one(alt, set_env=set_env) for alt in alternatives]
            if any(result.ok for result in results):
                continue
            names = ", ".join(result.name for result in results)
            errors = "; ".join(
                f"{result.name}: {result.error}" for result in results
                if result.error
            )
            missing.append(f"{label} requires one of {names}" + (f" ({errors})" if errors else ""))
            continue

        result = _check_one(spec, set_env=set_env)
        if not result.ok:
            missing.append(f"{label} requires {result.name}" + (f" ({result.error})" if result.error else ""))
    return missing
