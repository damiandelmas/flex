#!/usr/bin/env python3
"""
Flex CLI — flex init + flex search.

pip install getflex
flex init              # storage + model + MCP wiring
flex search --cell claude_code "query"    # raw terminal query
"""

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

FLEX_HOME = Path(os.environ.get("FLEX_HOME", Path.home() / ".flex"))
CLAUDE_DIR = Path.home() / ".claude"
CLAUDE_JSON = Path.home() / ".claude.json"
SYSTEMD_DIR = Path.home() / ".config" / "systemd" / "user"
LAUNCHD_DIR = Path.home() / "Library" / "LaunchAgents"
WORKER_DAEMON_ARGS = ["-m", "flex.daemon", "--no-refresh", "--no-background"]
REFRESH_DAEMON_ARGS = ["-m", "flex.refresh", "--due"]


def _python_command(args: list[str]) -> str:
    return " ".join([sys.executable, *args])


def _safe_write(path: Path, content: str):
    """Write content to path, replacing symlinks with regular files."""
    if path.is_symlink():
        path.unlink()
    path.write_text(content)


# Package data locations (relative to this file)
PKG_ROOT = Path(__file__).parent


def _discover_install_modules() -> dict[str, dict]:
    """Discover packaged and user-installed flex modules.

    Packaged modules live under flex/modules. User modules live under
    ~/.flex/modules or any directory listed in FLEX_MODULE_PATH.
    """
    from flex.modules.specs import discover_install_modules
    return discover_install_modules()


def _git_value(args: list[str], cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def _git_tags_at_head(src: Path) -> list[str]:
    tags = _git_value(["tag", "--points-at", "HEAD"], src)
    if not tags:
        return []
    return [tag for tag in tags.splitlines() if tag]


def _module_tag_for_name(tags: list[str], name: str) -> str | None:
    normalized = name.replace("_", "-").lower()
    matches = [
        tag for tag in tags
        if tag.lower() == normalized or tag.lower().startswith(f"{normalized}-")
    ]
    return matches[0] if len(matches) == 1 else None


def _module_provenance(src: Path, dest: Path, name: str, mode: str) -> dict:
    """Build install provenance for a copied or editable external module."""
    repo_root = _git_value(["rev-parse", "--show-toplevel"], src)
    git_head = _git_value(["rev-parse", "HEAD"], src)
    git_tags = _git_tags_at_head(src)
    git_ref = _module_tag_for_name(git_tags, name)
    git_dirty = None
    source_subdir = None
    if repo_root:
        try:
            source_subdir = str(src.resolve().relative_to(Path(repo_root).resolve()))
        except ValueError:
            source_subdir = None
        dirty = _git_value(["status", "--short"], src)
        git_dirty = bool(dirty)
    return {
        "schema": "flex.module.install.v1",
        "name": name,
        "install_mode": mode,
        "source_path": str(src),
        "source_subdir": source_subdir,
        "installed_path": str(dest),
        "installed_at": datetime.now(timezone.utc).isoformat(),
        "git_repo": repo_root,
        "git_head": git_head,
        "git_ref": git_ref,
        "git_tags": git_tags,
        "git_dirty": git_dirty,
    }


def _write_module_provenance(dest: Path, provenance: dict) -> None:
    meta_path = dest / ".flex-module.json"
    meta_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    try:
        os.chmod(meta_path, 0o600)
    except OSError:
        pass


# ============================================================
# flex init
# ============================================================


def _install_claude_assets(skill_names=None):
    """Copy public Flex skill assets from package ai/ dir to ~/.claude/."""
    _SKILL_ASSETS = {
        "flex": ("skills/flex/", "skills/flex/"),
        "flex:sessions:claudecode": (
            "skills/flex-sessions-claudecode/",
            "skills/flex-sessions-claudecode/",
        ),
        "flex:sessions:codex": (
            "skills/flex-sessions-codex/",
            "skills/flex-sessions-codex/",
        ),
        "flex:sessions:goose": (
            "skills/flex-sessions-goose/",
            "skills/flex-sessions-goose/",
        ),
        "flex:reddit": ("skills/flex-reddit/", "skills/flex-reddit/"),
        "flex:hn": ("skills/flex-hn/", "skills/flex-hn/"),
        "flex:github": ("skills/flex-github/", "skills/flex-github/"),
        "flex:arxiv": ("skills/flex-arxiv/", "skills/flex-arxiv/"),
        "flex:ledger": ("skills/flex-ledger/", "skills/flex-ledger/"),
    }

    skill_mode = os.environ.get("FLEX_SKILL_MODE", "mcp").strip().lower()
    if skill_mode == "none":
        _INSTALL_ASSETS = []
    elif skill_mode in ("", "default", "mcp"):
        requested = tuple(skill_names or ("flex",))
        _INSTALL_ASSETS = [_SKILL_ASSETS[name] for name in requested if name in _SKILL_ASSETS]
    else:
        print(f"  [warn] FLEX_SKILL_MODE={skill_mode!r} is deprecated; installing MCP skills")
        requested = tuple(skill_names or ("flex",))
        _INSTALL_ASSETS = [_SKILL_ASSETS[name] for name in requested if name in _SKILL_ASSETS]

    _claude_src = PKG_ROOT / "ai"
    if not _claude_src.exists():
        return

    def _is_allowed(rel_str):
        """Check if a relative path is in the install set."""
        for entry, _dest in _SKILL_ASSETS.values():
            if entry.endswith("/"):
                if rel_str.startswith(entry) or rel_str == entry.rstrip("/"):
                    return True
            elif rel_str == entry:
                return True
        return False

    for src_asset, dest_asset in _INSTALL_ASSETS:
        if not _is_allowed(src_asset):
            print(f"  [warn] {src_asset} skipped")
            continue

        src_path = _claude_src / src_asset
        if src_path.is_dir():
            # Copy entire directory (skills)
            for src in src_path.rglob("*"):
                if src.is_file():
                    rel = src.relative_to(src_path)
                    dest = CLAUDE_DIR / dest_asset / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.is_symlink():
                        dest.unlink()
                    if src.resolve() != dest.resolve():
                        shutil.copy2(src, dest)
        elif src_path.is_file():
            dest = CLAUDE_DIR / dest_asset
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.is_symlink():
                dest.unlink()
            if src_path.resolve() != dest.resolve():
                shutil.copy2(src_path, dest)


def _install_systemd():
    """Generate and install systemd user units. Returns True if installed."""
    if sys.platform != "linux":
        return False

    # Check systemctl is available and functional before writing unit files
    try:
        result = subprocess.run(
            ["systemctl", "--user", "--no-pager", "status"],
            capture_output=True, timeout=5,
        )
        # Exit codes 0-3 are all "systemctl worked" (3 = no units, still available)
        if result.returncode > 3:
            raise subprocess.CalledProcessError(result.returncode, "systemctl")
    except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return False

    SYSTEMD_DIR.mkdir(parents=True, exist_ok=True)
    python = sys.executable
    worker_cmd = _python_command(WORKER_DAEMON_ARGS)
    refresh_cmd = _python_command(REFRESH_DAEMON_ARGS)

    _SYSTEMD_UNITS = {
        "flex-worker.service": (
            "[Unit]\n"
            "Description=Flex Local Capture Worker\n"
            "After=network.target\n"
            # Backstop so a genuine crash-loop can't respawn forever.
            "StartLimitIntervalSec=300\n"
            "StartLimitBurst=5\n\n"
            "[Service]\n"
            "Type=simple\n"
            f"ExecStart={worker_cmd}\n"
            # Restart=always (not on-failure): a clean SIGTERM — WSL shutdown,
            # an external pkill — must still bring the worker back.
            "Restart=always\n"
            "RestartSec=5\n"
            "Environment=PYTHONUNBUFFERED=1\n"
            "Environment=MALLOC_ARENA_MAX=2\n"
            # memmap the embedding matrix → file-backed, reclaimable pages.
            "Environment=FLEX_VEC_MEMMAP=1\n"
            # 64GB box: the cap is a RUNAWAY BACKSTOP, not a ration (working
            # set ~6G; the graph build peaks ~4.2G). MemorySwapMax=0 is the
            # real guard — a runaway is cgroup-OOM'd cleanly in RAM instead of
            # spilling into swap and thrashing WSL to death (the 2026-06 mode).
            "MemoryMax=24G\n"
            "MemorySwapMax=0\n\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        ),
        "flex-mcp.service": (
            "[Unit]\n"
            "Description=Flex MCP Server\n"
            "After=network.target\n\n"
            "[Service]\n"
            "Type=simple\n"
            f"ExecStart={python} -m flex.serve --http --port 7134\n"
            "Restart=always\n"
            "RestartSec=5\n"
            "Environment=PYTHONUNBUFFERED=1\n"
            # Cap glibc malloc arenas: VectorCache full-matrix rebuilds churn
            # large allocations; unbounded arenas fragment and ratchet RSS.
            "Environment=MALLOC_ARENA_MAX=2\n"
            # memmap the embedding matrix → file-backed, reclaimable pages.
            "Environment=FLEX_VEC_MEMMAP=1\n"
            # 64GB box: the cap is a RUNAWAY BACKSTOP, not a ration (warm set
            # is a few G with memmap). MemorySwapMax=0 forces a runaway to
            # cgroup-OOM cleanly in RAM rather than thrash WSL via swap.
            "MemoryMax=24G\n"
            "MemorySwapMax=0\n"
            # KillMode=mixed: SIGTERM to main only, then SIGKILL the whole
            # cgroup after TimeoutStopSec. Ensures python children get
            # reaped cleanly on restart.
            "KillMode=mixed\n"
            "TimeoutStopSec=10\n"
            # Belt-and-suspenders: hunt for any stragglers left behind by
            # an earlier _start_services_direct() invocation that escaped
            # its caller's process group via start_new_session=True.
            # Leading '-' tells systemd not to fail the service if pkill
            # returns nonzero (no matches = exit 1, which is expected).
            "ExecStopPost=-/usr/bin/pkill -f 'flex\\.mcp_server.*--port 7134'\n\n"
            "[Install]\n"
            "WantedBy=default.target\n"
        ),
        "flex-refresh.service": (
            "[Unit]\n"
            "Description=Flex Registry Refresh Runner\n"
            "After=network.target\n\n"
            "[Service]\n"
            "Type=oneshot\n"
            f"ExecStart={refresh_cmd}\n"
            "Environment=PYTHONUNBUFFERED=1\n"
        ),
        "flex-refresh.timer": (
            "[Unit]\n"
            "Description=Flex Registry Refresh Timer\n\n"
            "[Timer]\n"
            "OnActiveSec=30min\n"
            "OnUnitActiveSec=30min\n"
            "RandomizedDelaySec=2min\n"
            "Unit=flex-refresh.service\n\n"
            "[Install]\n"
            "WantedBy=timers.target\n"
        ),
    }
    for service_name, content in _SYSTEMD_UNITS.items():
        (SYSTEMD_DIR / service_name).write_text(content)

    for cmd, label in [
        (["systemctl", "--user", "daemon-reload"], "daemon-reload"),
        (["systemctl", "--user", "enable", "--now", "flex-worker", "flex-mcp", "flex-refresh.timer"], "enable services"),
    ]:
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=15)
        except subprocess.CalledProcessError as e:
            print(f"  [warn] systemd {label}: {e.stderr.decode().strip()}", file=sys.stderr)
            return False
        except subprocess.TimeoutExpired:
            print(f"  [warn] systemd {label}: timeout", file=sys.stderr)
            return False
    return True


def _launchd_domain() -> str:
    """Return the per-login launchd domain used by Flex user agents."""
    return f"gui/{os.getuid()}"


def _command_error(result) -> str:
    """Render a subprocess diagnostic regardless of capture text mode."""
    value = getattr(result, "stderr", b"") or getattr(result, "stdout", b"")
    return value.decode(errors="replace").strip() if isinstance(value, bytes) else str(value).strip()


def _install_launchd():
    """Generate and install launchd user agents (macOS). Returns True if installed."""
    if sys.platform != "darwin":
        return False

    LAUNCHD_DIR.mkdir(parents=True, exist_ok=True)
    python = sys.executable

    _LAUNCHD_PLISTS = {
        "dev.getflex.worker.plist": f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.getflex.worker</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>-m</string>
        <string>flex.daemon</string>
        <string>--no-refresh</string>
        <string>--no-background</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{FLEX_HOME / "logs" / "worker.log"}</string>
    <key>StandardErrorPath</key>
    <string>{FLEX_HOME / "logs" / "worker.err"}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>
</dict>
</plist>""",
        "dev.getflex.mcp.plist": f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.getflex.mcp</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>-m</string>
        <string>flex.mcp_server</string>
        <string>--http</string>
        <string>--port</string>
        <string>7134</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{FLEX_HOME / "logs" / "mcp.log"}</string>
    <key>StandardErrorPath</key>
    <string>{FLEX_HOME / "logs" / "mcp.err"}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>
</dict>
</plist>""",
        "dev.getflex.refresh.plist": f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.getflex.refresh</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python}</string>
        <string>-m</string>
        <string>flex.refresh</string>
        <string>--due</string>
    </array>
    <key>StartInterval</key>
    <integer>1800</integer>
    <key>RunAtLoad</key>
    <false/>
    <key>StandardOutPath</key>
    <string>{FLEX_HOME / "logs" / "refresh.log"}</string>
    <key>StandardErrorPath</key>
    <string>{FLEX_HOME / "logs" / "refresh.err"}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONUNBUFFERED</key>
        <string>1</string>
    </dict>
</dict>
</plist>""",
    }

    (FLEX_HOME / "logs").mkdir(parents=True, exist_ok=True)

    domain = _launchd_domain()
    installed = True
    for name, content in _LAUNCHD_PLISTS.items():
        plist_path = LAUNCHD_DIR / name
        label = name.replace(".plist", "")
        # Check if already loaded — skip bootstrap if so
        try:
            probe = subprocess.run(
                ["launchctl", "print", f"{domain}/{label}"],
                capture_output=True, timeout=10,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as error:
            print(f"  [warn] launchctl probe {name}: {error}", file=sys.stderr)
            installed = False
            continue
        already_loaded = probe.returncode == 0
        if already_loaded:
            # Update plist in place, kickstart to pick up changes
            plist_path.write_text(content)
            if label != "dev.getflex.refresh":
                try:
                    result = subprocess.run(
                        ["launchctl", "kickstart", "-k", f"{domain}/{label}"],
                        capture_output=True, timeout=10,
                    )
                    if result.returncode != 0:
                        print(f"  [warn] launchctl kickstart {name}: {_command_error(result)}", file=sys.stderr)
                        installed = False
                except (FileNotFoundError, subprocess.TimeoutExpired) as error:
                    print(f"  [warn] launchctl kickstart {name}: {error}", file=sys.stderr)
                    installed = False
        else:
            plist_path.write_text(content)
            try:
                result = subprocess.run(
                    ["launchctl", "bootstrap", domain, str(plist_path)],
                    capture_output=True, timeout=10,
                )
                if result.returncode != 0:
                    print(f"  [warn] launchctl bootstrap {name}: {_command_error(result)}", file=sys.stderr)
                    installed = False
            except (FileNotFoundError, subprocess.TimeoutExpired) as error:
                print(f"  [warn] launchctl bootstrap {name}: {error}", file=sys.stderr)
                installed = False

    return installed


def _is_port_open(port: int) -> bool:
    """Check if a TCP port is accepting connections on localhost."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _is_worker_alive() -> bool:
    """Check whether this Flex home's worker daemon is alive."""
    pid_file = FLEX_HOME / "worker.pid"
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)  # signal 0 = existence check
            return True
        except (ValueError, OSError):
            pid_file.unlink(missing_ok=True)
    # Fallback: scan process table
    try:
        r = subprocess.run(
            ["pgrep", "-f", "flex.daemon"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode != 0:
            return False
        try:
            our_home = FLEX_HOME.resolve()
        except (OSError, RuntimeError):
            our_home = FLEX_HOME
        return any(
            _flex_home_of_pid(int(pid)) == our_home
            for pid in r.stdout.split()
            if pid.isdigit()
        )
    except Exception:
        return False


def _verify_services() -> tuple:
    """Check if worker and MCP are actually running. Returns (worker_ok, mcp_ok)."""
    # MCP: retry port check (service may still be starting)
    mcp_ok = False
    for _ in range(6):  # 3 seconds max
        if _is_port_open(7134):
            mcp_ok = True
            break
        time.sleep(0.5)

    worker_ok = _is_worker_alive()
    return worker_ok, mcp_ok


def _start_services_direct():
    """Start worker + MCP as background processes. Last resort fallback.
    Skipped on Windows — stdio MCP transport handles it per-session."""
    if sys.platform == "win32":
        return
    python = sys.executable
    log_dir = FLEX_HOME / "logs"
    log_dir.mkdir(exist_ok=True)

    def _start(name, cmd, pid_file, log_file):
        # Don't start if already running
        if pid_file.exists():
            try:
                os.kill(int(pid_file.read_text().strip()), 0)
                return  # already running
            except (ValueError, OSError):
                pid_file.unlink(missing_ok=True)

        with open(log_file, "a") as out, open(log_file.with_suffix(".err"), "a") as err:
            proc = subprocess.Popen(cmd, stdout=out, stderr=err, start_new_session=True)
        pid_file.write_text(str(proc.pid))

    if not _is_worker_alive():
        _start("worker",
               [python, *WORKER_DAEMON_ARGS],
               FLEX_HOME / "worker.pid", log_dir / "worker.log")

    if not _is_port_open(7134):
        _start("mcp",
               [python, "-m", "flex.mcp_server", "--http", "--port", "7134"],
               FLEX_HOME / "mcp.pid", log_dir / "mcp.log")


def _flex_home_of_pid(pid: int) -> "Path | None":
    """Best-effort effective FLEX_HOME of a running process, resolved exactly as
    this module resolves its own (the ``FLEX_HOME`` env var, else ``~/.flex``).

    Returns ``None`` when the process's environment can't be read (it exited, or
    it belongs to another user) — callers treat ``None`` as "not ours" and never
    kill it. This is what lets the ``_kill_pid_services`` process-table fallback
    scope to THIS install instead of every flex process on the box.
    """
    home_val = None
    if os.path.isdir("/proc"):  # Linux/WSL
        try:
            raw = Path(f"/proc/{pid}/environ").read_bytes()
        except OSError:
            return None
        for kv in raw.split(b"\x00"):
            if kv.startswith(b"FLEX_HOME="):
                home_val = kv[len(b"FLEX_HOME="):].decode(errors="replace")
                break
    else:  # macOS/BSD: `ps -E` appends the environment to the command column
        try:
            out = subprocess.run(
                ["ps", "-wwE", "-o", "command=", "-p", str(pid)],
                capture_output=True, text=True, timeout=5,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return None
        if out.returncode != 0:
            return None
        for tok in out.stdout.split():
            if tok.startswith("FLEX_HOME="):
                home_val = tok[len("FLEX_HOME="):]
                break
    try:
        return (Path(home_val) if home_val else Path.home() / ".flex").resolve()
    except (OSError, RuntimeError, ValueError):
        return None


def _kill_pid_services():
    """Stop any directly-started services and reap stragglers.

    Three-step cleanup so the next startup binds cleanly:

    1. SIGTERM via PID file (the happy path)
    2. A process-table fallback (``pgrep`` by command-line pattern) for orphans
       adopted by init after the CLI process exited, stale PID files, and
       children that escaped their caller's process group via
       ``start_new_session=True`` — but SIGTERM only those whose effective
       FLEX_HOME matches ours, so a second install / a test / a concurrent
       ``flex init`` is never touched
    3. Poll port 7134 for release before returning, so callers don't
       race a still-closing socket

    Idempotent — safe to call when nothing is running.
    """
    if sys.platform == "win32":
        return
    import signal

    # 1. PID file kill (the happy path)
    for pid_name in ["worker.pid", "mcp.pid"]:
        pid_file = FLEX_HOME / pid_name
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                os.kill(pid, signal.SIGTERM)
            except (ValueError, OSError):
                pass
            pid_file.unlink(missing_ok=True)

    # 2. Process-table fallback — reap escaped children and init-adopted
    # orphans that step 1's PID files no longer point at. SCOPED to THIS
    # FLEX_HOME: a bare `pkill -f` here is system-wide and kills every
    # flex.daemon on the box — any concurrent `flex init`, a test suite, or a
    # second install — which is the production half of the crash-loop bug. So
    # we resolve each candidate's effective FLEX_HOME and SIGTERM only on an
    # exact match; anything we can't attribute to this install is left alone.
    patterns = [
        "flex\\.mcp_server.*--port 7134",
        "flex\\.serve.*--port 7134",
        "flex\\.daemon",
    ]
    try:
        our_home = FLEX_HOME.resolve()
    except (OSError, RuntimeError):
        our_home = FLEX_HOME
    self_pid = os.getpid()
    for pattern in patterns:
        try:
            res = subprocess.run(
                ["pgrep", "-f", pattern],
                capture_output=True, text=True, timeout=5,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        for tok in res.stdout.split():
            try:
                pid = int(tok)
            except ValueError:
                continue
            if pid == self_pid:
                continue
            if _flex_home_of_pid(pid) == our_home:
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass

    # 3. Wait for port 7134 to actually release (up to 5s). Prevents
    # EADDRINUSE on the next start when uvicorn is still unbinding.
    for _ in range(10):
        if not _is_port_open(7134):
            return
        time.sleep(0.5)


def _managed_service_active(service: str) -> bool:
    """Return whether Flex's named service is running under its manager."""
    try:
        if sys.platform == "linux":
            return subprocess.run(
                ["systemctl", "--user", "is-active", service],
                capture_output=True, timeout=5,
            ).returncode == 0
        if sys.platform == "darwin":
            label = {"flex-worker": "dev.getflex.worker", "flex-mcp": "dev.getflex.mcp"}[service]
            return subprocess.run(
                ["launchctl", "print", f"{_launchd_domain()}/{label}"],
                capture_output=True, timeout=5,
            ).returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return False


def _quiesce_services_for_exclusive_work() -> dict:
    """Stop all Flex service owners before an exclusive cell replacement.

    Registry ``active`` remains a discovery/warmup declaration, never a lock.
    The returned receipt records only services this call stopped, so restore is
    exact and does not accidentally start an inactive service.
    """
    worker_live = _is_worker_alive()
    mcp_live = _is_port_open(7134)
    stopped: list[str] = []

    for service in ("flex-worker", "flex-mcp"):
        if not _managed_service_active(service):
            continue
        try:
            if sys.platform == "linux":
                command = ["systemctl", "--user", "stop", service]
            else:
                label = {"flex-worker": "dev.getflex.worker", "flex-mcp": "dev.getflex.mcp"}[service]
                command = ["launchctl", "bootout", f"{_launchd_domain()}/{label}"]
            result = subprocess.run(command, capture_output=True, timeout=20)
            if result.returncode:
                raise RuntimeError(_command_error(result) or "non-zero exit")
            stopped.append(service)
        except (FileNotFoundError, subprocess.TimeoutExpired, RuntimeError) as error:
            _resume_services_after_exclusive_work({"stopped": stopped, "direct": False})
            raise RuntimeError(f"could not pause {service}: {error}") from error

    # After manager-owned units are down, reaping a direct/orphan process can
    # no longer race a supervisor restart.
    _kill_pid_services()
    if _is_worker_alive() or _is_port_open(7134):
        _resume_services_after_exclusive_work({"stopped": stopped, "direct": False})
        raise RuntimeError("could not prove Flex worker and MCP are stopped")

    return {
        "stopped": stopped,
        "direct": (worker_live and "flex-worker" not in stopped)
        or (mcp_live and "flex-mcp" not in stopped),
    }


def _resume_services_after_exclusive_work(receipt: dict) -> None:
    """Restore precisely the service state captured by quiescence."""
    errors: list[str] = []
    for service in receipt.get("stopped", []):
        try:
            if sys.platform == "linux":
                command = ["systemctl", "--user", "start", service]
            else:
                label = {"flex-worker": "dev.getflex.worker", "flex-mcp": "dev.getflex.mcp"}[service]
                command = ["launchctl", "bootstrap", _launchd_domain(), str(LAUNCHD_DIR / f"{label}.plist")]
            result = subprocess.run(command, capture_output=True, timeout=20)
            if result.returncode:
                errors.append(f"{service}: {_command_error(result) or 'non-zero exit'}")
        except (FileNotFoundError, subprocess.TimeoutExpired) as error:
            errors.append(f"{service}: {error}")

    if receipt.get("direct"):
        _start_services_direct()
    if errors:
        raise RuntimeError("could not restore services: " + "; ".join(errors))


def _patch_claude_json():
    """Add MCP server entry to ~/.claude.json.

    Streamable HTTP transport — stateless per-request, supports unlimited
    concurrent Claude Code sessions. One process, warm cache, shared across
    sessions. Services (systemd/launchd) keep it running.
    """
    if CLAUDE_JSON.exists():
        try:
            data = json.loads(CLAUDE_JSON.read_text())
        except (json.JSONDecodeError, ValueError):
            backup = CLAUDE_JSON.with_suffix('.json.bak')
            CLAUDE_JSON.rename(backup)
            print(f"  [warn] Corrupt {CLAUDE_JSON.name} — backed up to {backup.name}", file=sys.stderr)
            data = {}
    else:
        data = {}

    servers = data.setdefault("mcpServers", {})
    entry = {"type": "http", "url": "http://localhost:7134/mcp/"}
    if "flex" not in servers or servers["flex"] != entry:
        servers["flex"] = entry
        _safe_write(CLAUDE_JSON, json.dumps(data, indent=2) + "\n")
        return True
    return False


def _run_enrichment_quiet(conn, progress_cb=None) -> tuple[int, list[str]]:
    """Run enrichment silently. Returns (cluster_count, failed_step_names)."""
    import io
    import contextlib

    try:
        from flex.modules.claude_code.manage.rebuild_all import (
            rebuild_warmup_types, reembed_sources, rebuild_source_graph,
            rebuild_community_labels, rebuild_file_graph, rebuild_delegation_graph,
        )
        from flex.modules.claude_code.manage.enrich_summary import run as run_fingerprints
        from flex.modules.claude_code.manage.enrich_soma_repos import run as _register_soma_repos
        from flex.modules.claude_code.manage.enrich_repo_project import run as run_repo_project
        from flex.views import regenerate_views, install_views
        from flex.manage.install_presets import ensure_cell_presets
    except ImportError:
        return 0, []

    failures: list[str] = []
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        for step, fn in [
            ("warmup types",       lambda: rebuild_warmup_types(conn)),
            ("source pooling",     lambda: reembed_sources(conn)),
            ("source graph",       lambda: rebuild_source_graph(conn)),
            ("file graph",         lambda: rebuild_file_graph(conn)),
            ("delegation graph",   lambda: rebuild_delegation_graph(conn)),
            ("fingerprints",       lambda: run_fingerprints(conn)),
            ("repo registry",      lambda: _register_soma_repos(conn)),
            ("repo attribution",   lambda: run_repo_project(conn)),
            ("community labels",   lambda: rebuild_community_labels(conn)),
        ]:
            if progress_cb:
                progress_cb(step)
            try:
                fn()
            except Exception:
                failures.append(step)

        if progress_cb:
            progress_cb("query catalog")
        try:
            ensure_cell_presets(conn, "claude-code")
        except Exception:
            failures.append("query catalog")

        if progress_cb:
            progress_cb("views")
        try:
            for view_dir in _find_view_dirs('claude_code', 'claude-code'):
                install_views(conn, view_dir)
            regenerate_views(conn)
            conn.commit()
        except Exception:
            failures.append("views")

    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT community_id) FROM _enrich_source_graph"
            " WHERE community_id IS NOT NULL"
        ).fetchone()
        return (row[0] if row else 0), failures
    except Exception:
        return 0, failures




# ── Surface routing ───────────────────────────────────────────────────────
# Present one filesystem compiler with compatibility aliases for the former
# Instant, Markdown/Obsidian, and codegraph installation surfaces.
#
#   filesystem -> mixed filesystem engine (embedded by default)
#   codegraph   -> filesystem, code-only + no-embed compatibility mode
#
# Permanent aliases (old commands keep working):
#   instant             -> filesystem, no-embed (except legacy regen/remove)
#   obsidian / markdown -> filesystem, markdown-only
#   code                -> codegraph compatibility mode
def _resolve_module_surface(module: str, args) -> tuple[str, dict]:
    """Map a user-facing verb/alias to (engine_cli_name, forced_flags).

    engine_cli_name is a real discovered `--module` value; forced flags are
    applied onto `args` before dispatch. A name
    that is neither a surface verb nor an alias passes through untouched.
    """
    if module in ("filesystem", "instant", "obsidian", "markdown"):
        if module == "filesystem":
            return "filesystem", {}
        if module == "instant":
            if getattr(args, "regen", None) or getattr(args, "remove", None):
                return "instant", {}
            return "filesystem", {"no_embed": True}
        return "filesystem", {
            "obsidian": module == "obsidian",
            "_filesystem_file_kinds": ("markdown",),
        }
    if module in ("codegraph", "code"):
        return "filesystem", {
            "no_embed": True,
            "_filesystem_file_kinds": ("code",),
        }
    return module, {}


def cmd_init(args):
    """Initialize flex. Dispatches to the requested module's install hook."""
    import io
    import contextlib
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text

    console = Console()
    _warnings: list[str] = []
    _module = getattr(args, 'module', None)

    # Route legacy surface verbs into the unified filesystem engine and apply
    # their compatibility flags before dispatch.
    if _module:
        _resolved, _forced = _resolve_module_surface(_module, args)
        _module = _resolved
        for _k, _v in _forced.items():
            setattr(args, _k, _v)

    # Stop running worker/MCP before a FULL module install to avoid DB lock
    # contention. Skip for lightweight instant recompiles (--regen/--path/--remove):
    # they write their own no-embed cell and never touch the worker/mcp DBs. On the
    # 60s instant-regen cron, this kill flapped flex-mcp every minute (it never
    # stayed up long enough to finish warming). A regen recompiles the cell; it must
    # not bounce the daemons.
    _regen_op = _module == "instant" and (
        getattr(args, "regen", None)
        or getattr(args, "path", None)
        or getattr(args, "remove", None)
    )
    if _module and not _regen_op:
        _kill_pid_services()

    console.print()

    # 0. Pre-flight: check system deps before doing anything
    _sudo = "" if getattr(os, 'geteuid', lambda: 1)() == 0 else "sudo "
    _missing_sys = [b for b in ("git",) if not shutil.which(b)]
    if _missing_sys:
        console.print("  [yellow]Missing system dependencies — run first:[/yellow]")
        console.print()
        if shutil.which("brew"):
            console.print(f"  [bold]brew install {' '.join(_missing_sys)}[/bold]")
        else:
            console.print(f"  [bold]{_sudo}apt install {' '.join(_missing_sys)}[/bold]")
        console.print()
        console.print("  Then re-run: [bold]flex init[/bold]")
        console.print()
        return

    console.print("  [cyan]Setting up flex[/cyan]...", highlight=False)
    console.print()

    # 1. Storage
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    (FLEX_HOME / "cells").mkdir(exist_ok=True)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        try:
            from flex.modules.soma.lib.identity.file_identity import FileIdentity
            from flex.modules.soma.lib.identity.repo_identity import RepoIdentity
            from flex.modules.soma.lib.identity.url_identity import URLIdentity
            from flex.modules.soma.lib.identity.content_identity import ContentIdentity
            FileIdentity(); RepoIdentity(); URLIdentity(); ContentIdentity()
            from flex.modules.soma.lib.eternity.eternity import Eternity
            Eternity()
        except ImportError:
            pass  # SOMA optional — keep silent
        except Exception as e:
            print(f"[init] SOMA init: {e}", file=sys.stderr)

    console.print("  storage             [green]ok[/green]")

    # 2. Model
    from flex.onnx.fetch import download_model, model_ready
    _model_ok = True
    if not model_ready():
        console.print("  model               downloading")
        try:
            download_model()
        except RuntimeError as e:
            console.print(f"  [yellow]model[/yellow]               [yellow]failed[/yellow]")
            console.print(f"  [dim]Semantic search disabled. SQL and keyword search still work.[/dim]")
            console.print(f"  [dim]Rerun [bold]flex init[/bold] when online to download the embedding model.[/dim]")
            _model_ok = False
            _warnings.append(f"Model download: {e}")
    if _model_ok:
        console.print("  model               [green]ok[/green]")

    # Pass model state down to the module hook
    args._model_ok = _model_ok

    # 3. Dispatch to module hook (or print base panel)
    if not _module:
        _install_claude_assets(("flex", "flex:ledger"))
        console.print()
        panel_content = Text()
        panel_content.append("Flex is ready.\n\n", style="cyan")
        panel_content.append("  flex search     ", style="bold")
        panel_content.append("raw terminal query\n", style="dim")
        panel_content.append("For Claude Code session search:\n", style="dim")
        panel_content.append("  curl -sSL https://getflex.dev/install.sh | bash -s -- claude-code\n", style="bold")
        console.print(Panel(panel_content, padding=(1, 2), highlight=False))
        console.print()
        return

    modules = _discover_install_modules()
    if _module not in modules:
        console.print(f"  [red]Unknown module: {_module}[/red]")
        if modules:
            console.print(f"  Available: {', '.join(sorted(modules.keys()))}")
        else:
            console.print("  No modules installed.")
        console.print()
        sys.exit(1)

    entry = modules[_module]
    mod = entry['module']
    try:
        mod.run(args, console)
    except Exception as e:
        console.print(f"  [red]Install failed: {e}[/red]")
        raise




# ============================================================
# flex hub — cell distribution: view/pull/push/status
# ============================================================

def cmd_hub(args):
    """Dispatch `flex hub <view|pull|push|status>` subcommands.

    `relay` is still dispatchable (undocumented — superseded, retained for
    compatibility with an existing consumer) but deliberately not listed
    here or in any help text; see cmd_hub_relay.
    """
    action = getattr(args, "hub_action", None)
    if action == "view":
        return cmd_hub_view(args)
    if action == "pull":
        return cmd_hub_pull(args)
    if action == "push":
        return cmd_hub_push(args)
    if action == "relay":
        return cmd_hub_relay(args)
    if action == "status":
        return cmd_hub_status(args)
    print("  Expected one of: view, pull, push, status")


def cmd_hub_view(args):
    """List remote cells (register) and local sync status."""
    from flex.hub.manifest import fetch_manifest, diff_manifest
    from flex.registry import list_cells

    remote = fetch_manifest()
    local = list_cells()
    diffs = diff_manifest(remote, local)
    if getattr(args, "json", False):
        import json
        out = {n: {"url": e.url, "size": e.size, "status": diffs.get(n, "new")}
               for n, e in remote.items()}
        print(json.dumps(out, indent=2))
        return
    for name in sorted(remote):
        entry = remote[name]
        status = diffs.get(name, "new")
        print(f"  {name:<20s} {status}")


_R2_CRED_VARS = ("FLEX_R2_ENDPOINT", "FLEX_R2_ACCESS_KEY", "FLEX_R2_SECRET_KEY")


def _guard_r2_credentials():
    """GUARD 3 — private intent with broken credentials must ABORT, not silently
    degrade to the public registry.

    FLEX_R2_BUCKET expresses the intent "use the access-controlled registry." If
    the credentials to reach it are absent, falling back to the public registry
    answers a different question than the one that was asked.

    When every variable is absent there is no registry-intent signal to inspect;
    the env-independent named-cell and provenance guards below cover that case.
    """
    if not os.environ.get("FLEX_R2_BUCKET"):
        return
    absent = [v for v in _R2_CRED_VARS if not os.environ.get(v)]
    if not absent:
        return
    print(
        "error: FLEX_R2_BUCKET is set (access-controlled registry) but these are not: "
        + ", ".join(absent),
        file=sys.stderr,
    )
    print(
        "  refusing to fall back to the public registry — that would answer a "
        "different question than the one you asked.",
        file=sys.stderr,
    )
    sys.exit(2)


def _registry_label(authed: bool) -> str:
    return "PRIVATE (authenticated)" if authed else "PUBLIC (anonymous)"


def _fail_on_missing(missing, remote, authed):
    """GUARD 1 (PRIMARY, env-independent) — a NAMED cell absent from the fetched
    manifest is a loud, named, non-zero failure. Never a silent `continue`.

    The invariant: asked for N, wrote 0, exited 0 — never. A pull that changed
    nothing must never look like a pull that worked, regardless of which registry
    was resolved or why.
    """
    if not missing:
        return
    print(
        f"error: {len(missing)} named cell(s) not in the manifest: "
        + ", ".join(missing),
        file=sys.stderr,
    )
    print(
        f"  resolved registry: {_registry_label(authed)} — "
        f"{len(remote)} cell(s) available",
        file=sys.stderr,
    )
    if not authed:
        print(
            "  hint: cells in an access-controlled registry need "
            + " / ".join(_R2_CRED_VARS)
            + ".",
            file=sys.stderr,
        )
        print(
            "        a systemd EnvironmentFile is NOT loaded by an ssh shell — "
            "source it explicitly.",
            file=sys.stderr,
        )
    sys.exit(1)


def _guard_provenance(cells, local_by_name, authed):
    """GUARD 2 (PROVENANCE) — never silently cross registries.

    A cell installed from the access-controlled registry (origin='private') must
    not be resolved against the public one. Guard 1 catches the common case (the
    private name simply isn't in the public manifest) — but a NAME COLLISION
    defeats it: if a public cell shares a name with a private one, a creds-less
    pull would fetch the public impostor and register it OVER the private cell,
    with an identical source_url, leaving the substitution undetectable.

    Provenance is stamped from the path that ACTUALLY resolved the manifest at
    install time — not inferred from source_url, which cannot carry the signal
    (the manifest composes a public URL even for private cells).

    origin=NULL is UNKNOWN (a legacy row), not a violation — do not enforce. A
    migration must never brick an existing install.
    """
    if authed:
        return
    crossed = [
        n for n in cells
        if (local_by_name.get(n) or {}).get("origin") == "private"
    ]
    if not crossed:
        return
    print(
        f"error: {len(crossed)} cell(s) were installed from an access-controlled "
        "registry, but this context resolved the public one: " + ", ".join(crossed),
        file=sys.stderr,
    )
    print(
        "  refusing to overwrite them from the public registry — a same-named "
        "public cell is not the same cell.",
        file=sys.stderr,
    )
    print(
        "  hint: set " + " / ".join(_R2_CRED_VARS) + " to reach the registry these "
        "cells came from.",
        file=sys.stderr,
    )
    sys.exit(1)


def cmd_hub_pull(args):
    """Install a specific cell, or (with --all) refresh all stale installed cells.

    No arg: pull the named cell(s) unconditionally (install or re-fetch).
    --all: refresh installed cells that are stale vs the remote manifest,
    optionally filtered to the named cell(s) — this is the folded
    `cmd_update` behavior, cell-filter fix included.

    Fails LOUD: a named cell that cannot be resolved is an error with a non-zero
    exit, never a silent skip. See _fail_on_missing / _guard_provenance /
    _guard_r2_credentials.
    """
    from flex.hub.manifest import (
        fetch_manifest,
        diff_manifest,
        download_cell,
        _r2_auth_configured,
    )
    from flex.registry import list_cells, register_cell, CELLS_DIR

    _guard_r2_credentials()                       # GUARD 3 — before any network

    remote = fetch_manifest()
    cells = list(getattr(args, "cells", None) or [])
    authed = _r2_auth_configured()
    origin = "private" if authed else "public"

    local_by_name = {c["name"]: c for c in list_cells()}

    if cells:
        _guard_provenance(cells, local_by_name, authed)   # GUARD 2

    if getattr(args, "all", False):
        installed = [c for c in local_by_name.values() if c.get("source_url")]
        if cells:
            installed = [c for c in installed if c.get("name") in cells]
        diffs = diff_manifest(remote, installed)
        for name, status in diffs.items():
            if status == "stale":
                entry = remote.get(name)
                if entry:
                    dest = download_cell(entry, CELLS_DIR)
                    register_cell(name=name, path=str(dest), source_url=entry.url,
                                  checksum=entry.checksum, origin=origin)
                    print(f"  {name}: updated")
        # GUARD 1 — a named cell we were asked to refresh but could not resolve.
        _fail_on_missing([n for n in cells if n not in remote], remote, authed)
        return

    if not cells:
        print("  Specify cell name(s) to pull, or use --all to refresh installed cells.", file=sys.stderr)
        sys.exit(1)

    for name in cells:
        entry = remote.get(name)
        if not entry:
            continue                              # collected below — never silent
        dest = download_cell(entry, CELLS_DIR)
        register_cell(name=name, path=str(dest), cell_type=entry.cell_type,
                      source_url=entry.url, checksum=entry.checksum, origin=origin)
        print(f"  {name}: installed")

    # GUARD 1 (PRIMARY). Cells that DID resolve are installed above — we don't roll
    # back good work — but the run still exits non-zero and names what was missed.
    _fail_on_missing([n for n in cells if n not in remote], remote, authed)


def cmd_hub_push(args):
    """Publish cells via the flex.hub push submodule.

    An optional capability, unavailable unless publishing support is
    installed. Without it, this prints a clear message and exits
    non-zero instead of tracebacking.

    Note: no --public/--private scope semantics here. Access-scoped
    serving is out of scope for this CLI.
    """
    from flex.hub import _push_api

    _, _, push_all = _push_api()
    if push_all is None:
        print("  publishing support is not installed", file=sys.stderr)
        sys.exit(1)

    cell_names = list(getattr(args, "cells", None) or []) or None
    try:
        push_all(cell_names=cell_names, dry_run=getattr(args, "dry_run", False))
    except EnvironmentError as e:
        print(f"  {e}", file=sys.stderr)
        sys.exit(1)


def cmd_hub_status(args):
    """Show relay + publish state."""
    from argparse import Namespace
    from rich.console import Console

    console = Console()
    cmd_hub_relay(Namespace(status=True, stop=False))

    from flex.hub import _push_api
    _, _, push_all = _push_api()
    if push_all is not None:
        console.print("  Publish   [green]available[/green] (publishing support installed)")
    else:
        console.print("  Publish   [dim]unavailable[/dim] (publishing support not installed)")


def cmd_hub_relay(args):
    """Toggle remote MCP access.

    Creates or removes ``~/.flex/machine_id`` and restarts services so the
    MCP server picks up the new state on next startup, exposing a stable
    remote endpoint that MCP clients can connect to.
    """
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text

    console = Console()

    # Superseded, retained for compatibility with an existing consumer,
    # not advertised.
    # Dependency check — websockets is not a declared package dependency.
    try:
        import websockets  # noqa: F401
    except ImportError:
        console.print("  [red]relay requires the websockets package.[/red]")
        return

    if sys.platform == "win32":
        console.print("  [yellow]Relay not supported on Windows yet.[/yellow]")
        console.print("  [dim]Windows uses per-session stdio MCP transport — no persistent service to tunnel.[/dim]")
        return

    machine_id_path = FLEX_HOME / "machine_id"
    flag_path = FLEX_HOME / "relay_enabled"

    # Inline relay-state check keeps this command self-contained.
    def _enabled() -> bool:
        if flag_path.exists():
            return True
        # Migration: old installs used machine_id presence as the signal
        if machine_id_path.exists():
            try:
                flag_path.touch()
            except OSError:
                return False
            return True
        return False

    # --status: show current state
    if getattr(args, 'status', False):
        if _enabled() and machine_id_path.exists():
            mid = machine_id_path.read_text().strip()
            console.print(f"  Relay [green]active[/green]")
            console.print(f"  Endpoint: https://{mid}.getflex.dev/sse")
        else:
            console.print("  Relay [dim]disabled[/dim]")
        return

    # --stop: drop the flag, preserve machine_id, restart services
    # machine_id persists so the claude.ai Connector URL stays stable
    # across toggle cycles.
    if getattr(args, 'stop', False):
        if not flag_path.exists():
            console.print("  Relay already disabled")
            return
        flag_path.unlink()
        _kill_pid_services()
        _start_services_direct()
        console.print("  Relay [green]disabled[/green]")
        console.print("  [dim]Services restarted (localhost-only mode)[/dim]")
        return

    # Default: enable relay
    FLEX_HOME.mkdir(parents=True, exist_ok=True)
    if not machine_id_path.exists():
        import uuid
        machine_id_path.write_text(uuid.uuid4().hex[:8])
    flag_path.touch()

    mid = machine_id_path.read_text().strip()
    url = f"https://{mid}.getflex.dev/sse"

    # Restart services so MCP picks up the new machine_id and opens the WS
    _kill_pid_services()
    _start_services_direct()
    time.sleep(2)  # allow WS handshake

    # Health check — poll /health endpoint (up to 10s)
    connected = False
    try:
        import urllib.request
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(
                    f"https://{mid}.getflex.dev/health", timeout=3
                ) as resp:
                    data = json.loads(resp.read())
                    if data.get("flex_connected", False):
                        connected = True
                        break
            except Exception:
                pass
            time.sleep(1)
    except Exception:
        pass

    status_line = Text()
    if connected:
        status_line.append("connected", style="green")
    else:
        status_line.append("waiting for connection...", style="yellow")

    panel_content = Text()
    panel_content.append("Cloud access enabled\n\n", style="cyan")
    panel_content.append(f"Endpoint    {url}\n", style="bold")
    panel_content.append("Status      ")
    panel_content.append_text(status_line)
    panel_content.append("\n\n")
    panel_content.append("Paste into claude.ai → Settings → MCP:\n", style="dim")
    panel_content.append(f"  {url}\n", style="bold")
    panel_content.append("\nDisable: ", style="dim")
    panel_content.append("flex relay --stop", style="dim")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
    console.print()


def cmd_relay(args):
    """Thin top-level `flex relay` alias for `flex hub relay`.

    `flex hub relay` is the canonical form; this alias is kept because an
    existing consumer references the top-level `flex relay` command directly.
    """
    return cmd_hub_relay(args)


# ============================================================
# raw search implementation
# ============================================================

def _open_cell_for_search(cell_name: str, query: str | None = None):
    """Open a cell, warming vectors only when the query actually needs them.

    Routes through the same resolution as the MCP path (flex.engine.build_vec_state
    / register_vec_udf) so a tagged cell (e.g. `vec:model=nomic-v1.5`) serves
    identically here as it does over MCP or flex.retrieve.execute — no more
    stale-default-embedder divergence for the CLI path."""
    from flex.registry import resolve_cell
    from flex.core import open_cell_readonly

    path = resolve_cell(cell_name)
    if path is None:
        from flex.registry import retired_cell_message
        retired = retired_cell_message(cell_name)
        if retired:
            print(retired, file=sys.stderr)
            sys.exit(1)
        print(f"Cell '{cell_name}' not found.", file=sys.stderr)
        print("Run 'flex init' first, then use Claude Code to build your index.", file=sys.stderr)
        sys.exit(1)

    db = open_cell_readonly(path)

    needs_vectors = 'vec_ops(' in (query or '')
    if query and query.lstrip().startswith('@'):
        preset_name = query.lstrip()[1:].split(None, 1)[0]
        try:
            from flex.retrieve.presets import PresetLoader

            preset = PresetLoader(db).load(preset_name)
            needs_vectors = any(
                'vec_ops(' in str(item.get('sql') or '').lower()
                for item in preset.get('queries', [])
            )
        except (KeyError, ValueError, sqlite3.Error):
            needs_vectors = False

    # Structural SQL/recovery must not pay to load a multi-gigabyte vector
    # matrix. Semantic queries still use the exact shared engine resolver.
    try:
        if not needs_vectors:
            return db
        from flex import engine as _engine
        from pathlib import Path as _Path

        db_path = _Path(path)
        mtime = db_path.stat().st_mtime if db_path.exists() else 0
        state = _engine.build_vec_state(cell_name, db, mtime)
        if state:
            _engine.register_vec_udf(db, state)
    except ImportError:
        pass  # vec_ops won't work but plain SQL is fine

    return db


def _format_results(result_json: str, as_json: bool = False) -> str:
    """Format query results for terminal output."""
    if as_json:
        return result_json

    data = json.loads(result_json)
    if isinstance(data, dict) and "error" in data:
        return f"Error: {data['error']}"
    if not isinstance(data, list) or len(data) == 0:
        return "No results."

    # Simple table format
    keys = list(data[0].keys())
    # Compute column widths
    widths = {k: len(k) for k in keys}
    for row in data:
        for k in keys:
            val = str(row.get(k, ""))
            if len(val) > 80:
                val = val[:77] + "..."
            widths[k] = max(widths[k], len(val))

    # Cap total width
    lines = []
    header = "  ".join(k.ljust(widths[k]) for k in keys)
    lines.append(header)
    lines.append("  ".join("-" * widths[k] for k in keys))
    for row in data:
        vals = []
        for k in keys:
            val = str(row.get(k, ""))
            if len(val) > 80:
                val = val[:77] + "..."
            vals.append(val.ljust(widths[k]))
        lines.append("  ".join(vals))

    return "\n".join(lines)


def cmd_search(args):
    """Execute a query against a cell."""
    # Lazy import — avoids pulling in mcp deps at CLI startup
    from flex.mcp_server import execute_query

    query = args.query
    if query.startswith('!'):
        query = query[1:].lstrip()
    if args.cell == "ledger":
        # Ledger is self-provisioning at its query boundary. Route both reads
        # and mutations through that shared kernel so a raw shell query has the
        # same first-use behavior as MCP and does not require an install verb.
        from flex.mcp_server import _execute_cell_query
        print(_execute_cell_query(args.cell, query))
        return

    db = _open_cell_for_search(args.cell, query)
    try:
        result = execute_query(db, query, cell=args.cell)
        print(result)
    finally:
        db.close()


# ============================================================
# flex sync
# ============================================================

from flex.modules.specs import asset_modules_for, enrichment_stubs_from, normalize_cell_type, stock_subdirs


def _find_view_dir(cell_name: str, cell_type: str | None) -> Path | None:
    """Resolve the curated view directory for a cell.

    ~/.flex/views/ takes precedence (user library — editable, git-tracked).
    Falls back to module stock/views/ (auto-discovered from PKG_ROOT/modules/).
    No hardcoded module list — any module with stock/views/ is found.
    """
    key = cell_type or cell_name
    for module_name in asset_modules_for(key, "views_from") or [normalize_cell_type(key) or key]:
        user_dir = Path.home() / '.flex' / 'views' / module_name
        if user_dir.exists() and any(user_dir.glob('*.sql')):
            return user_dir

        stock_dirs = stock_subdirs(module_name, "views_from", "views")
        stock = stock_dirs[0] if stock_dirs else None
        if stock is not None:
            return stock

    return None


def _find_view_dirs(cell_name: str, cell_type: str | None) -> list[Path]:
    """Resolve view directories in install order: stock base, then user overrides."""
    key = cell_type or cell_name
    dirs: list[Path] = []
    modules = asset_modules_for(key, "views_from") or [normalize_cell_type(key) or key]
    for module_name in modules:
        dirs.extend(stock_subdirs(module_name, "views_from", "views"))

        user_dir = Path.home() / '.flex' / 'views' / module_name
        if user_dir.exists() and any(user_dir.glob('*.sql')):
            dirs.append(user_dir)

    return dirs


# Stub list lives in flex.modules.claude_code (single source of truth for the
# coding-agent substrate). Specs declare which substrate they borrow from.
from flex.modules.claude_code import ENRICHMENT_STUBS as _CC_ENRICHMENT_STUBS


class _EnrichmentStubMap(dict):
    def get(self, key, default=None):
        source = enrichment_stubs_from(key)
        if source in ("claude_code", "claude-code"):
            return _CC_ENRICHMENT_STUBS
        return super().get(key, default)


_ENRICHMENT_STUBS = _EnrichmentStubMap({
    'claude_code': _CC_ENRICHMENT_STUBS,
    'claude-code': _CC_ENRICHMENT_STUBS,
})



def _check_fts(conn, cell_name: str):
    """Check FTS5 consistency for a cell connection."""
    chunks_count = conn.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
    # chunks_fts
    try:
        fts_count = conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
        if fts_count != chunks_count:
            print(f"    chunks_fts drift ({fts_count} vs {chunks_count}) — rebuilding")
            conn.execute("INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')")
            conn.commit()
        else:
            print(f"    chunks_fts ok ({fts_count} rows)")
    except Exception:
        print(f"    chunks_fts not present (skip)")
    # content_fts
    try:
        content_count = conn.execute("SELECT COUNT(*) FROM _raw_content").fetchone()[0]
        content_fts_count = conn.execute("SELECT COUNT(*) FROM content_fts").fetchone()[0]
        if content_fts_count != content_count:
            print(f"    content_fts drift ({content_fts_count} vs {content_count}) — rebuilding")
            conn.execute("INSERT INTO content_fts(content_fts) VALUES('rebuild')")
            conn.commit()
    except Exception:
        pass  # content_fts may not exist on all cell types


def cmd_sync(args):
    """Bring all three layers (code, data, services) into parity."""
    import sqlite3
    import time

    from flex.registry import list_cells, resolve_cell
    from flex.manage.install_presets import ensure_cell_presets
    from flex.views import regenerate_views, install_views

    cells = list_cells()
    if not cells and args.cell is not None:
        print("No cells registered. Run 'flex init' first.")
        return

    target = args.cell  # None = all cells

    print("flex sync")
    print()

    # ---- Phase 1: Database query catalogs ----
    print("[1/5] Database query catalogs")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        db_path = resolve_cell(name)
        if not db_path or not db_path.exists():
            print(f"  {name}: SKIP (not found)")
            continue
        try:
            with sqlite3.connect(str(db_path), timeout=30) as conn:
                changed = ensure_cell_presets(conn, cell.get("cell_type"))
            print(f"  {name}: {'seeded' if changed else 'ok'}")
        except sqlite3.Error as error:
            print(f"  {name}: FAILED ({error})")
    # ---- Phase 2: Cell sync (stubs + curated views + auto views + FTS5) ----
    print()
    print("[2/5] Cell sync")
    for cell in cells:
        name = cell['name']
        if target and name != target:
            continue
        db_path = resolve_cell(name)
        if not db_path or not db_path.exists():
            print(f"  {name}: SKIP (not found)")
            continue
        conn = None
        try:
            conn = sqlite3.connect(str(db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")

            # Stubs
            cell_type = cell.get('cell_type')
            for ddl in _ENRICHMENT_STUBS.get(cell_type, []):
                conn.execute(ddl)
            conn.commit()

            # Curated views
            for view_dir in _find_view_dirs(name, cell_type):
                install_views(conn, view_dir)

            # Auto views
            regenerate_views(conn)
            conn.commit()

            # FTS5 consistency
            _check_fts(conn, name)

            # Summary
            try:
                views = [r[0] for r in conn.execute(
                    "SELECT name FROM _views ORDER BY name"
                ).fetchall()]
                print(f"  {name}: ok ({len(views)} views [{', '.join(views)}])")
            except Exception:
                print(f"  {name}: ok")
        except Exception as e:
            print(f"  {name}: FAILED ({e})")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ---- Phase 3: Services ----
    print()
    print("[3/5] Services")

    # Install service units if missing (recovery from partial init)
    if sys.platform == "linux":
        worker_unit = SYSTEMD_DIR / "flex-worker.service"
        refresh_timer = SYSTEMD_DIR / "flex-refresh.timer"
        worker_text = worker_unit.read_text() if worker_unit.exists() else ""
        custom_worker = worker_unit.exists() and "flex.daemon" not in worker_text
        if not worker_unit.exists() or (not refresh_timer.exists() and not custom_worker):
            print("  Installing missing systemd units")
            try:
                _install_systemd()
            except Exception as e:
                print(f"  systemd install FAILED: {e}")
        elif custom_worker:
            pass  # custom unit — don't overwrite
        elif "flex.daemon" in worker_text and "--no-refresh" not in worker_text:
            print("  Updating systemd units for local-only worker")
            try:
                _install_systemd()
            except Exception as e:
                print(f"  systemd update FAILED: {e}")

        for service in ["flex-worker", "flex-mcp"]:
            try:
                subprocess.run(
                    ["systemctl", "--user", "restart", service],
                    check=True, capture_output=True, timeout=10,
                )
                print(f"  {service}: restarted")
            except subprocess.CalledProcessError as e:
                print(f"  {service}: FAILED ({e.stderr.decode().strip()})")
            except subprocess.TimeoutExpired:
                print(f"  {service}: restart timed out")
            except FileNotFoundError:
                print(f"  {service}: SKIP (systemctl not found)")

    elif sys.platform == "darwin":
        worker_plist = LAUNCHD_DIR / "dev.getflex.worker.plist"
        refresh_plist = LAUNCHD_DIR / "dev.getflex.refresh.plist"
        worker_text = worker_plist.read_text() if worker_plist.exists() else ""
        custom_worker = worker_plist.exists() and "flex.daemon" not in worker_text
        if not worker_plist.exists() or (not refresh_plist.exists() and not custom_worker):
            print("  Installing missing launchd agents")
            try:
                _install_launchd()
            except Exception as e:
                print(f"  launchd install FAILED: {e}")
        elif custom_worker:
            pass  # custom agent — don't overwrite
        elif "flex.daemon" in worker_text and "--no-refresh" not in worker_text:
            print("  Updating launchd agents for local-only worker")
            try:
                _install_launchd()
            except Exception as e:
                print(f"  launchd update FAILED: {e}")
        else:
            # Kill any PID-managed processes before launchd takes over
            _kill_pid_services()
            uid = os.getuid()
            for label in ["dev.getflex.worker", "dev.getflex.mcp"]:
                try:
                    subprocess.run(
                        ["launchctl", "kickstart", "-k",
                         f"user/{uid}/{label}"],
                        capture_output=True, timeout=10,
                    )
                    print(f"  {label}: restarted")
                except Exception as e:
                    print(f"  {label}: FAILED ({e})")

    # Verify services actually started (all platforms)
    time.sleep(1)
    worker_ok, mcp_ok = _verify_services()
    if not worker_ok or not mcp_ok:
        _start_services_direct()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
    if not worker_ok:
        print("  worker: FAILED (could not start)")
    if not mcp_ok:
        print("  MCP: FAILED (could not start)")

    # ---- Phase 4: MCP wiring ----
    print()
    print("[4/5] MCP wiring")
    try:
        if CLAUDE_JSON.exists():
            _cfg = json.loads(CLAUDE_JSON.read_text())
            if "flex" not in _cfg.get("mcpServers", {}):
                print("  adding (missing from init)")
                _patch_claude_json()
            else:
                print("  ok")
        else:
            print("  creating ~/.claude.json")
            _patch_claude_json()
    except Exception as e:
        print(f"  FAILED ({e})")

    # ---- Phase 5: Optional enrichment rebuild ----
    if args.full:
        print()
        print("[5/5] Enrichment rebuild (claude_code)")
        receipt = None
        try:
            receipt = _quiesce_services_for_exclusive_work()
            t0 = time.time()
            result = subprocess.run(
                [sys.executable, "-m", "flex.modules.claude_code.manage.rebuild_all"],
                capture_output=True, text=True, timeout=600,
                cwd=str(PKG_ROOT.parent),
            )
            elapsed = time.time() - t0
            if result.returncode == 0:
                # Print last few lines of output
                lines = result.stdout.strip().splitlines()
                for line in lines[-10:]:
                    print(f"  {line}")
                print(f"  done in {elapsed:.1f}s")
            else:
                print(f"  FAILED (exit {result.returncode})")
                for line in result.stderr.strip().splitlines()[-5:]:
                    print(f"  {line}")
        except subprocess.TimeoutExpired:
            print("  TIMEOUT (>600s)")
        except Exception as e:
            print(f"  ERROR: {e}")
        finally:
            if receipt is not None:
                try:
                    _resume_services_after_exclusive_work(receipt)
                except RuntimeError as error:
                    print(f"  ERROR: services were not restored: {error}")

    print()
    print("Sync complete.")


# ============================================================
# flex remove
# ============================================================

def _human_size(n: int) -> str:
    """Format bytes as human-readable."""
    if n < 1024:
        return f"{n}B"
    elif n < 1048576:
        return f"{n / 1024:.0f}KB"
    elif n < 1073741824:
        return f"{n / 1048576:.0f}MB"
    return f"{n / 1073741824:.1f}GB"




def cmd_remove(args):
    """Delete and unregister a cell."""
    from flex.registry import resolve_cell, unregister_cell

    for name in args.cells:
        cell_path = resolve_cell(name)
        if cell_path and cell_path.exists():
            cell_path.unlink()
            print(f"  {name}: deleted {cell_path}")

        if unregister_cell(name):
            print(f"  {name}: unregistered")
        else:
            print(f"  {name}: not found in registry")


# ============================================================
# flex module
# ============================================================

def _module_install_root() -> Path:
    from flex.modules.specs import user_modules_root
    return user_modules_root()


def _safe_module_name(name: str) -> str:
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError(f"invalid module name: {name!r}")
    return name


def _module_dest(name: str) -> Path:
    root = _module_install_root().resolve()
    dest = root / _safe_module_name(name)
    try:
        rel_ok = dest.is_relative_to(root)
    except AttributeError:
        rel_ok = str(dest).startswith(str(root))
    if not rel_ok:
        raise ValueError(f"module path escapes install root: {name!r}")
    return dest


def cmd_module(args):
    """Manage local external modules."""
    action = getattr(args, "module_action", None)
    if action == "install":
        return cmd_module_install(args)
    if action == "list":
        return cmd_module_list(args)
    if action == "remove":
        return cmd_module_remove(args)
    print("  Expected one of: install, list, remove")


def cmd_module_install(args):
    """Install a local module folder into ~/.flex/modules."""
    src = Path(args.path).expanduser().resolve()
    if not src.exists() or not src.is_dir():
        raise SystemExit(f"module source is not a directory: {src}")
    if not (src / "install.py").exists():
        raise SystemExit(f"module source must contain install.py: {src}")

    name = _safe_module_name(args.name or src.name)
    root = _module_install_root()
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    dest = _module_dest(name)
    if dest.exists() or dest.is_symlink():
        if not args.force:
            raise SystemExit(f"module already installed: {name} ({dest})")
        if dest.is_symlink() or dest.is_file():
            dest.unlink()
        else:
            shutil.rmtree(dest)

    editable = bool(getattr(args, "editable", False))
    copy = bool(getattr(args, "copy", False))
    if editable and copy:
        raise SystemExit("--editable and --copy are mutually exclusive")

    if editable:
        dest.symlink_to(src, target_is_directory=True)
        mode = "linked"
    else:
        ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
        shutil.copytree(src, dest, ignore=ignore)
        mode = "copied"
    if not editable:
        _write_module_provenance(dest, _module_provenance(src, dest, name, mode))
    print(f"  {name}: {mode} {src} -> {dest}")


def cmd_module_list(args):
    """List modules installed under ~/.flex/modules."""
    root = _module_install_root()
    if not root.exists():
        print("  No external modules installed.")
        return
    found = False
    for path in sorted(root.iterdir()):
        if path.name.startswith("_"):
            continue
        install_py = path / "install.py"
        if not install_py.exists():
            continue
        found = True
        target = path.resolve() if path.is_symlink() else path
        suffix = f" -> {target}" if path.is_symlink() else ""
        if getattr(args, "verbose", False):
            meta_path = path / ".flex-module.json"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                except Exception:
                    meta = {}
                mode = meta.get("install_mode") or ("linked" if path.is_symlink() else "copied")
                ref = meta.get("git_ref")
                if not ref and meta.get("git_head"):
                    ref = meta["git_head"][:12]
                ref = ref or "unknown"
                dirty = " dirty" if meta.get("git_dirty") else ""
                print(f"  {path.name}  {mode}  {ref}{dirty}  {target}")
            else:
                mode = "linked" if path.is_symlink() else "copied"
                print(f"  {path.name}  {mode}  unknown  {target}")
        else:
            print(f"  {path.name}{suffix}")
    if not found:
        print("  No external modules installed.")


def cmd_module_remove(args):
    """Remove a module installed under ~/.flex/modules."""
    dest = _module_dest(args.name)
    if not dest.exists() and not dest.is_symlink():
        print(f"  {args.name}: not installed")
        return
    if dest.is_symlink() or dest.is_file():
        dest.unlink()
    else:
        shutil.rmtree(dest)
    print(f"  {args.name}: removed")


# ============================================================
# flex soma
# ============================================================

def cmd_soma(args):
    """Dispatch SOMA maintenance subcommands."""
    action = getattr(args, "soma_action", None)
    if action == "backfill-identity":
        return cmd_soma_backfill_identity(args)
    print("  Expected one of: backfill-identity")


def cmd_soma_backfill_identity(args):
    """Stamp _edges_fs_identity on existing cells without a rebuild."""
    from flex.modules.soma.manage.backfill_identity import report, run

    results = run([args.cell] if args.cell else None, dry_run=args.dry_run)
    code = report(results, dry_run=args.dry_run)
    if code:
        sys.exit(code)


def cmd_edges(args):
    """Dispatch edge-table maintenance subcommands."""
    action = getattr(args, "edges_action", None)
    if action == "dedupe":
        return cmd_edges_dedupe(args)
    print("  Expected one of: dedupe")


def cmd_edges_dedupe(args):
    """Dedupe _edges_source / _edges_delegations on existing cells.

    Fixes cells whose INSERT OR IGNORE never had a uniqueness constraint to
    ignore against, so every re-ingest appended duplicate edge rows. See
    flex/manage/dedupe_edges.py.
    """
    from flex.manage.dedupe_edges import report, run

    results = run([args.cell] if args.cell else None, dry_run=args.dry_run, vacuum=args.vacuum)
    code = report(results, dry_run=args.dry_run)
    if code:
        sys.exit(code)


def cmd_status(args):
    """Show cell health, lifecycle, and refresh status."""
    from flex.health import refresh_problems, watch_problem, watch_problems
    from flex.registry import classify_refresh_state, list_cells
    import sqlite3 as _sqlite3

    def _counts(path):
        try:
            db = _sqlite3.connect(path, timeout=5)
            chunks = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
            sources = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
            db.close()
            return chunks, sources
        except Exception:
            return None, None

    def _fmt_age(seconds):
        if seconds is None:
            return "—"
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m"
        if seconds < 86400:
            return f"{seconds // 3600}h"
        return f"{seconds // 86400}d"

    cells = list_cells()
    if not cells:
        print("No cells registered. Run 'flex init' first.")
        return

    # Default user-facing status shows only discoverable cells. Operator/debug
    # views can pass --all to include inactive or unlisted cells.
    if not args.all:
        cells = [c for c in cells if not c.get('unlisted') and c.get('active', 1)]

    if getattr(args, 'problems', False):
        problems = refresh_problems(cells, include_unlisted=True) + watch_problems(cells, include_unlisted=True)
        if args.json:
            import json
            print(json.dumps(problems, indent=2))
            return
        if not problems:
            print("No refresh lifecycle problems.")
            return
        print(f"{'CELL':<24} {'STATUS':<14} {'SEVERITY':<8} {'AGE':>5} REASON")
        print("-" * 88)
        for problem in problems:
            print(
                f"{problem['cell'][:23]:<24} {problem['status'][:13]:<14} "
                f"{problem['severity']:<8} {problem['age']:>5} {problem['reason']}"
            )
        print("\nNext:")
        for problem in problems:
            print(f"  {problem['cell']}: {problem['next']}")
        return

    if args.json:
        import json
        out = []
        for c in cells:
            state = classify_refresh_state(c)
            wp = watch_problem(c, state=state)
            chunks, sources = _counts(c['path'])
            entry = {
                'name': c['name'],
                'cell_type': c.get('cell_type'),
                'lifecycle': c.get('lifecycle', 'static'),
                'active': bool(c.get('active', 1)),
                'unlisted': bool(c.get('unlisted', 0)),
                'discoverable': bool(c.get('active', 1)) and not bool(c.get('unlisted', 0)),
                'warm_on_startup': bool(c.get('active', 1)) and not bool(c.get('unlisted', 0)),
                'refresh_status': c.get('refresh_status'),
                'effective_refresh_status': state['effective_refresh_status'],
                'last_refresh_at': c.get('last_refresh_at'),
                'refresh_interval': c.get('refresh_interval'),
                'refresh_due': state['refresh_due'],
                'refresh_stale': state['refresh_stale'],
                'refresh_overdue': state['refresh_overdue'],
                'refresh_never_run': state['refresh_never_run'],
                'refresh_age_s': state['refresh_age_s'],
                'refresh_running_for_s': state['refresh_running_for_s'],
                'path': c.get('path'),
                'refresh_module': c.get('refresh_module'),
                'refresh_script': c.get('refresh_script'),
                'watch_path': c.get('watch_path'),
                'watch_pattern': c.get('watch_pattern'),
                'watch_problem': wp['problem'] if wp else None,
            }
            entry['chunks'] = chunks
            entry['sources'] = sources
            out.append(entry)
        print(json.dumps(out, indent=2))
        return

    # Table format
    print(
        f"{'CELL':<24} {'TYPE':<14} {'FLAGS':<5} {'LIFECYCLE':<10} "
        f"{'STATUS':<14} {'AGE':>5} {'DUE':<3} {'CHUNKS':>8} {'SOURCES':>8}"
    )
    print("-" * 101)

    for c in cells:
        name = c['name'][:23]
        ct = (c.get('cell_type') or '—')[:13]
        lc = (c.get('lifecycle') or 'static')[:9]
        state = classify_refresh_state(c)
        rs = state['effective_refresh_status'][:13]
        age = _fmt_age(state['refresh_age_s'])
        due = "yes" if state['refresh_due'] else "no"
        flags = (
            ("A" if c.get('active', 1) else "I") +
            ("U" if c.get('unlisted') else "L")
        )
        chunks, sources = _counts(c['path'])
        chunks = '?' if chunks is None else chunks
        sources = '?' if sources is None else sources

        print(
            f"{name:<24} {ct:<14} {flags:<5} {lc:<10} {rs:<14} "
            f"{age:>5} {due:<3} {chunks:>8} {sources:>8}"
        )

    # Summary
    total_refresh = sum(1 for c in cells if c.get('lifecycle') == 'refresh')
    total_watch = sum(1 for c in cells if c.get('lifecycle') == 'watch')
    errors = sum(1 for c in cells if (c.get('refresh_status') or '').startswith('error'))
    stale = sum(1 for c in cells if classify_refresh_state(c)['refresh_stale'])
    overdue = sum(1 for c in cells if classify_refresh_state(c)['refresh_overdue'])
    due = sum(1 for c in cells if classify_refresh_state(c)['refresh_due'])
    print(
        f"\n{len(cells)} cells ({total_refresh} refresh, {total_watch} watch, "
        f"{due} due, {overdue} overdue, {stale} stale-running, {errors} errors)"
    )


def cmd_warm(args):
    """Manage the startup warm-set: which cells build VectorCaches at startup
    (instant first query) vs lazy-load on first use. The lazy path is cheap now
    that drifted caches refresh off the query thread — an unused cell costs no
    RAM until queried, and only its first query pays a one-time build."""
    from flex import registry

    cells = registry.list_cells()
    names = {c['name'] for c in cells}
    targets = list(getattr(args, 'cells', []) or [])

    if not targets:
        active = sorted(c['name'] for c in cells
                        if c.get('active', 1) and not c.get('unlisted', 0))
        lazy = sorted(c['name'] for c in cells if not c.get('active', 1))
        print(f"Warm at startup ({len(active)}):")
        for n in active:
            print(f"  ● {n}")
        print(f"\nLazy — load on first query ({len(lazy)}):")
        for n in lazy:
            print(f"  ○ {n}")
        print("\nflex warm <name...>          mark warm   "
              "| --off lazy | --only set the whole warm-set | --restart apply now")
        return

    unknown = [t for t in targets if t not in names]
    if unknown:
        print(f"Unknown cell(s): {', '.join(unknown)}")
        return

    if getattr(args, 'only', False):
        changed = 0
        for c in cells:
            want = c['name'] in targets
            if bool(c.get('active', 1)) != want:
                registry.set_active(c['name'], want)
                changed += 1
        print(f"Warm-set is now exactly ({len(targets)}): {', '.join(sorted(targets))}"
              f"  [{changed} changed]")
    else:
        on = not getattr(args, 'off', False)
        for t in targets:
            registry.set_active(t, on)
        print(f"Set {len(targets)} cell(s) {'warm at startup' if on else 'lazy'}: "
              f"{', '.join(targets)}")

    if getattr(args, 'restart', False):
        import subprocess
        subprocess.run(["systemctl", "--user", "restart", "flex-mcp"], check=False)
        print("Restarted flex-mcp — warm-set applied.")
    else:
        print("Restart to apply: systemctl --user restart flex-mcp")


def cmd_reembed(args):
    """Convert cell(s) onto the standard (Nomic) embedding model.

    The sanctioned per-cell model-upgrade path: each
    cell's `_raw_chunks`/`_raw_sources` embedding column is OVERWRITTEN with
    Nomic-768 vectors via copy-then-atomic-swap — never in-place on the live
    database. The
    live cell keeps serving MiniLM correctly for its full duration; it only
    flips, atomically, once the copy is fully re-embedded and verified.
    Backs up each cell db before touching it. Skips cells already
    `vec:model=nomic-v1.5-fp32` unless `--force`.
    """
    from flex.compile.reembed import reembed_cell
    from flex import registry

    names = list(getattr(args, 'cells', []) or [])
    had_error = False
    if names:
        targets = []
        for n in names:
            path = registry.resolve_cell(n)
            if path is None:
                print(f"{n}: ERROR — unknown cell")
                had_error = True
                continue
            targets.append((n, path))
    else:
        targets = [(c['name'], Path(c['path'])) for c in registry.list_cells()]

    if not targets:
        print("No cells to reembed.")
        if had_error:
            raise SystemExit(1)
        return

    dry_run = bool(getattr(args, 'dry_run', False))
    force = bool(getattr(args, 'force', False))
    receipt = None
    if not dry_run:
        try:
            receipt = _quiesce_services_for_exclusive_work()
        except RuntimeError as error:
            print(f"ERROR — reembed did not start: {error}")
            raise SystemExit(1) from error

    try:
        for name, path in targets:
            if not dry_run:
                print(f"{name}: migrating to nomic-v1.5-fp32 (services paused until verified swap)")
            result = reembed_cell(name, dry_run=dry_run, force=force)
            status = result.get('status')
            if status == 'converted':
                print(f"{name}: converted ({result['chunks']} chunks)  "
                      f"backup={result['backup']}")
            elif status == 'skipped':
                print(f"{name}: skipped ({result['reason']})")
            elif status == 'dry-run':
                note = f"  — {result['note']}" if result.get('note') else ""
                print(f"{name}: {result['pending']} pending / {result['chunks']} total chunks, "
                      f"~{result['eta_seconds']}s ETA{note}")
            else:
                had_error = True
                print(f"{name}: ERROR — {result.get('reason')}"
                      + (f"  backup={result['backup']}" if result.get('backup') else ""))
    finally:
        if receipt is not None:
            try:
                _resume_services_after_exclusive_work(receipt)
            except RuntimeError as error:
                had_error = True
                print(f"ERROR — reembed finished but services were not restored: {error}")
    if had_error:
        raise SystemExit(1)


def cmd_health(args):
    """Show compact operational health."""
    from flex.health import (
        refresh_problems, refresh_summary, watch_problems, watch_summary, watcher_summary,
    )
    from flex.registry import list_cells
    import json

    all_cells = list_cells()
    cells = all_cells
    if not args.all:
        cells = [c for c in cells if not c.get('unlisted') and c.get('active', 1)]
    refresh = refresh_summary(cells, include_unlisted=True)
    watch = watch_summary(cells, include_unlisted=True)
    # Locate claude_code via the already-fetched (and, in tests, mocked)
    # cell list rather than calling resolve_cell() independently — keeps
    # this call from bypassing test isolation around the registry.
    claude_cell = next((c for c in all_cells if c.get('name') == 'claude_code'), None)
    watcher = watcher_summary(cell_path=claude_cell.get('path') if claude_cell else None)
    problems = refresh_problems(cells, include_unlisted=True) + watch_problems(cells, include_unlisted=True, worker=watch["worker"])
    summary = {
        "status": "degraded" if (
            refresh["status"] == "degraded"
            or watch["status"] == "degraded"
            or watcher["status"] == "degraded"
        ) else "ok",
        "refresh": refresh,
        "watch": watch,
        "watcher": watcher,
        "problems": problems,
    }

    if args.json:
        print(json.dumps(summary, indent=2))
        return

    print(f"status: {summary['status']}")
    print(
        f"refresh: {refresh['refresh_cells']} cells, {refresh['problems']} problems, "
        f"{refresh['due']} due, {refresh['stale_running']} stale-running, "
        f"{refresh['overdue']} overdue, {refresh['errors']} errors"
    )
    print(f"watcher (claude_code): {watcher['status']}"
          + (f" — {watcher['reason']}" if watcher.get('reason') else ""))
    if not problems:
        return
    print()
    print(f"{'CELL':<24} {'STATUS':<14} {'SEVERITY':<8} {'AGE':>5} REASON")
    print("-" * 88)
    for problem in problems:
        print(
            f"{problem['cell'][:23]:<24} {problem['status'][:13]:<14} "
            f"{problem['severity']:<8} {problem['age']:>5} {problem['reason']}"
        )



# ============================================================
# Main
# ============================================================

def _gnu_flex_proxy():
    """Detect GNU flex usage and transparently forward to the real lexer.

    GNU flex (the lexer generator) ships with Xcode on macOS and is common
    on Linux dev machines.  Our `flex` binary can shadow it.  Instead of
    breaking the user's C build, we detect GNU-style invocations and exec
    the real thing.
    """
    argv = sys.argv[1:]
    if not argv:
        return  # bare `flex` — ours

    # Our commands — definitely not GNU flex
    our_commands = {
        "init", "search", "sync", "remove", "status", "health",
        "module", "relay", "hub", "index", "warm", "reembed", "edges", "soma",
        "-h", "--help",
    }
    if argv[0] in our_commands:
        return

    # Heuristics: does this look like GNU flex?
    gnu_extensions = {".l", ".lex", ".ll", ".l++", ".lxx"}
    gnu_flags = {
        "-o", "--outfile", "--header-file", "--header",
        "-C", "-Ca", "-Ce", "-Cf", "-CF", "-Cm", "-Cr",
        "-d", "--debug",
        "-i", "--case-insensitive",
        "-l", "--lex-compat",
        "-L", "--noline",
        "-s", "--nodefault",
        "-t", "--stdout",
        "-v", "--verbose",
        "-V", "--version",
        "-w", "--nowarn",
        "-B", "--batch",
        "-I", "--interactive",
        "-P", "--prefix",
        "-S", "--skel",
        "--nounistd", "--bison-bridge", "--bison-locations",
        "--posix", "--noansi-definitions", "--noansi-prototypes",
    }

    looks_gnu = False
    for arg in argv:
        if any(arg.endswith(ext) for ext in gnu_extensions):
            looks_gnu = True
            break
        if arg in gnu_flags or any(arg.startswith(f + "=") for f in gnu_flags):
            looks_gnu = True
            break

    if not looks_gnu:
        return

    # Find the real GNU flex — skip ourselves
    my_path = os.path.realpath(shutil.which("flex") or "")
    real_flex = None

    # Check well-known paths first
    for candidate in ["/usr/bin/flex", "/usr/local/opt/flex/bin/flex"]:
        if os.path.isfile(candidate) and os.path.realpath(candidate) != my_path:
            real_flex = candidate
            break

    # Fall back to which -a
    if not real_flex:
        try:
            all_paths = subprocess.check_output(
                ["which", "-a", "flex"], text=True, stderr=subprocess.DEVNULL
            ).strip().split("\n")
            for p in all_paths:
                if os.path.realpath(p) != my_path:
                    real_flex = p
                    break
        except subprocess.CalledProcessError:
            pass

    if real_flex:
        os.execv(real_flex, [real_flex] + argv)
        # execv replaces the process — never returns
    else:
        print(
            "This is getflex (AI knowledge engine), not GNU flex (lexer generator).\n"
            "GNU flex doesn't appear to be installed on this system.\n"
            "\n"
            "  Install GNU flex:  apt install flex  /  brew install flex\n"
            "  Use getflex:       flex init  |  flex search  |  flex sync",
            file=sys.stderr,
        )
        sys.exit(1)


def _rewrite_legacy_index_command(argv: list[str]) -> list[str]:
    """Map the retired source-ingest spelling onto the compiler surface."""
    if argv and argv[0] == "index":
        print(
            "`flex index PATH` is now `flex compile PATH`; "
            "@index is reserved for navigational cell projections.",
            file=sys.stderr,
        )
        return ["compile", *argv[1:]]
    return argv


def main():
    _gnu_flex_proxy()
    from flex.registry import load_plugins
    load_plugins()
    # Register built-in SDK commands such as `flex compile` before CLI parsing.
    import flex.sdk  # noqa: F401
    # Register cell-distribution hooks (daemon refresh, private publish-on-refresh)
    # backing `flex hub`.
    try:
        import flex.hub  # noqa: F401
    except ImportError:
        pass

    raw_argv = sys.argv[1:]
    if raw_argv and raw_argv[0] == "core":
        raw_argv = raw_argv[1:]
    elif raw_argv and raw_argv[0] in {"context", "sessions", "memory"}:
        print(
            f"`flex {raw_argv[0]}` project commands are deprecated.\n"
            "Use the Flex MCP tool for agent retrieval, or `flex search --cell <name> <query>` for raw CLI debugging.",
            file=sys.stderr,
        )
        sys.exit(2)

    # `flex index PATH` predated the navigational @index contract. Keep old
    # scripts working, but advertise only the precise compiler vocabulary.
    raw_argv = _rewrite_legacy_index_command(raw_argv)

    parser = argparse.ArgumentParser(
        prog="flex",
        description="Your AI sessions, searchable forever.",
    )
    sub = parser.add_subparsers(dest="command")

    # flex init — dispatcher; each installable module registers its own flags
    _install_modules = _discover_install_modules()
    _module_help_lines = []
    for _name in sorted(_install_modules):
        _sum = _install_modules[_name]['summary']
        _module_help_lines.append(f"{_name}: {_sum}" if _sum else _name)
    _module_help = (
        "Module to install. Surface verbs: "
        "filesystem --path PATH (mixed Markdown/code/text, embedded by default; "
        "add --obsidian or --no-embed). Compatibility aliases: "
        + (", ".join(sorted(_install_modules)) or "(none)")
    )
    _surface_help_lines = [
        "filesystem: compile a mixed folder into one watched Nomic cell",
        "  add --obsidian for vault semantics; add --no-embed for structural-only",
    ]
    init_p = sub.add_parser(
        "init",
        help="Initialize flex (base or with a module)",
        description="Initialize flex. Without --module, installs base flex.\n\n"
                    "Surface verbs:\n  " + "\n  ".join(_surface_help_lines) + "\n\n"
                    "Installable modules:\n  " + "\n  ".join(_module_help_lines),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    init_p.add_argument("--module", default=None, help=_module_help)
    # --embed is a one-cycle compatibility no-op: embedding is now the default.
    init_p.add_argument("--embed", action="store_true",
                        help="filesystem: compatibility no-op (embeddings are the default)")
    init_p.add_argument("--no-embed", action="store_true",
                        help="filesystem: disable semantic vectors; keep identical structure")
    for _entry in _install_modules.values():
        _reg = getattr(_entry['module'], 'register_args', None)
        if callable(_reg):
            try:
                _reg(init_p)
            except Exception as _e:
                print(f"[cli] register_args({_entry['folder']}) failed: {_e}", file=sys.stderr)

    # flex search — direct SQL/preset access to one registered cell
    search_p = sub.add_parser("search", help="Query a registered Flex cell")
    search_p.add_argument("query", help="SQL query, @preset, or vec_ops expression")
    search_p.add_argument("--cell", default="claude_code", help="Cell to query (default: claude_code)")
    search_p.add_argument("--json", action="store_true", help="Output raw JSON")

    # flex sync
    sync_p = sub.add_parser("sync", help="Bring code, data, and services into parity")
    sync_p.add_argument("--cell", default=None, help="Sync specific cell only (default: all)")
    sync_p.add_argument("--full", action="store_true", help="Also rebuild enrichments (~2min)")

    # flex module — local external module management
    module_p = sub.add_parser("module", help="Manage local external modules")
    module_sub = module_p.add_subparsers(dest="module_action")
    module_install_p = module_sub.add_parser("install", help="Install a local module folder")
    module_install_p.add_argument("path", help="Folder containing install.py")
    module_install_p.add_argument("--name", default=None, help="Installed module name (default: folder name)")
    install_mode = module_install_p.add_mutually_exclusive_group()
    install_mode.add_argument("--copy", action="store_true", help="Copy module snapshot (default)")
    install_mode.add_argument("--editable", action="store_true", help="Symlink module for local development")
    module_install_p.add_argument("--force", action="store_true", help="Replace an existing installed module")
    module_list_p = module_sub.add_parser("list", help="List installed external modules")
    module_list_p.add_argument("--verbose", action="store_true", help="Show install provenance")
    module_remove_p = module_sub.add_parser("remove", help="Remove an installed external module")
    module_remove_p.add_argument("name", help="Installed module name")

    # flex soma — SOMA identity maintenance utilities
    soma_p = sub.add_parser("soma", help="SOMA identity maintenance utilities")
    soma_sub = soma_p.add_subparsers(dest="soma_action")
    soma_backfill_p = soma_sub.add_parser(
        "backfill-identity",
        help="Stamp _edges_fs_identity on existing cells without a rebuild",
    )
    soma_backfill_p.add_argument("--cell", default=None,
                                  help="Only this cell (default: all registered cells)")
    soma_backfill_p.add_argument("--dry-run", action="store_true",
                                  help="Report per-cell coverage only, write nothing")

    edges_p = sub.add_parser("edges", help="Edge-table maintenance utilities")
    edges_sub = edges_p.add_subparsers(dest="edges_action")
    edges_dedupe_p = edges_sub.add_parser(
        "dedupe",
        help="Dedupe _edges_source / _edges_delegations on existing cells (fixes stale duplicate rows)",
    )
    edges_dedupe_p.add_argument("--cell", default=None,
                                 help="Only this cell (default: all registered cells)")
    edges_dedupe_p.add_argument("--dry-run", action="store_true",
                                 help="Report counts only, write nothing")
    edges_dedupe_p.add_argument("--vacuum", action="store_true",
                                 help="VACUUM cells that were actually deduped")

    # flex hub — view/pull/push cells + status (two-level dispatch,
    # mirroring the `soma`/`module` groups above)
    hub_p = sub.add_parser("hub", help="Browse, pull, and publish cells")
    # metavar override hides the undocumented `relay` choice (below) from the
    # `flex hub --help` choices summary entirely — hub_action has no plugin
    # extensibility, so a static list here is safe (unlike the top-level parser).
    hub_sub = hub_p.add_subparsers(dest="hub_action", metavar="{view,pull,push,status}")

    hub_view_p = hub_sub.add_parser("view", help="List remote cells and local sync status")
    hub_view_p.add_argument("--json", action="store_true", help="Machine-readable output")

    hub_pull_p = hub_sub.add_parser(
        "pull",
        help="Install cell(s), or refresh stale installed cells with --all",
    )
    hub_pull_p.add_argument("cells", nargs="*", help="Cell name(s) to pull/refresh")
    hub_pull_p.add_argument("--all", action="store_true",
                            help="Refresh all stale installed cells (filtered to any named cells)")

    hub_push_p = hub_sub.add_parser(
        "push",
        help="Publish cell(s) (requires publishing support to be installed)",
    )
    hub_push_p.add_argument("cells", nargs="*", help="Cell name(s) to push")
    hub_push_p.add_argument("--all", action="store_true", help="Push all configured cells")
    hub_push_p.add_argument("--dry-run", action="store_true", help="Show what would be uploaded")

    # relay is superseded and slated for retirement. Kept dispatchable (not
    # removed — an existing consumer still calls this form) but not
    # advertised: no help text (omitted from the detailed listing) and
    # excluded from hub_sub's choices metavar above (omitted from the
    # summary line too).
    hub_relay_p = hub_sub.add_parser("relay")
    hub_relay_p.add_argument("--stop", action="store_true",
                             help="Disable relay and restart services")
    hub_relay_p.add_argument("--status", action="store_true",
                             help="Show current relay URL if enabled")

    hub_status_p = hub_sub.add_parser("status", help="Show hub connection + publish state")

    # flex relay — thin top-level alias for `flex hub relay` (kept because an
    # existing consumer references this form directly). Superseded; no help
    # text so it's absent from the detailed top-level listing (the top-level
    # parser takes plugin-registered commands too, so unlike hub_sub it can't
    # safely use a static metavar override — the bare name may still appear
    # in the top-level choices summary, but with zero description).
    relay_p = sub.add_parser("relay")
    relay_p.add_argument("--stop", action="store_true",
                         help="Disable relay and restart services")
    relay_p.add_argument("--status", action="store_true",
                         help="Show current relay URL if enabled")

    # Plugin commands — modules register their own subcommands via hooks
    from flex.registry import get_hook
    _register_commands = get_hook("register_cli_commands")
    if _register_commands:
        _register_commands(sub)

    # flex status
    stat_p = sub.add_parser("status", help="Show cell health and refresh status")
    stat_p.add_argument("--json", action="store_true", help="Machine-readable output")
    stat_p.add_argument("--all", action="store_true", help="Include unlisted cells")
    stat_p.add_argument("--problems", action="store_true", help="Only show refresh lifecycle problems")

    # flex health
    health_p = sub.add_parser("health", help="Show operational health summary")
    health_p.add_argument("--json", action="store_true", help="Machine-readable output")
    health_p.add_argument("--all", action="store_true", help="Include unlisted cells")

    # flex warm — which cells build VectorCaches at startup vs lazy-load
    warm_p = sub.add_parser("warm", help="Manage the startup warm-set (vs lazy-load)")
    warm_p.add_argument("cells", nargs="*", help="Cell names to toggle (none = show current warm-set)")
    warm_p.add_argument("--off", action="store_true", help="Mark the named cells lazy (load on first query)")
    warm_p.add_argument("--only", action="store_true", help="Warm ONLY the named cells; mark all others lazy")
    warm_p.add_argument("--restart", action="store_true", help="Restart flex-mcp to apply now")

    # flex reembed — convert cell(s) onto the standard (Nomic) embedding model
    reembed_p = sub.add_parser(
        "reembed",
        help="Convert cell(s) onto the standard embedding model (copy-then-atomic-swap)")
    reembed_p.add_argument("cells", nargs="*", help="Cell names to reembed (none = all registered cells)")
    reembed_p.add_argument("--dry-run", action="store_true",
                           help="Report pending/total chunk counts + ETA without writing anything")
    reembed_p.add_argument("--force", action="store_true",
                           help="Reembed even if already vec:model=nomic-v1.5-fp32")

    _register_extra = get_hook("register_extra_commands")
    if _register_extra:
        _register_extra(sub)

    args = parser.parse_args(raw_argv)
    try:
        if args.command == "init":
            cmd_init(args)
        elif args.command == "search":
            cmd_search(args)
        elif args.command == "sync":
            cmd_sync(args)
        elif args.command == "module":
            cmd_module(args)
        elif args.command == "soma":
            cmd_soma(args)
        elif args.command == "edges":
            cmd_edges(args)
        elif args.command == "hub":
            cmd_hub(args)
        elif args.command == "remove":
            cmd_remove(args)
        elif args.command == "status":
            cmd_status(args)
        elif args.command == "health":
            cmd_health(args)
        elif args.command == "warm":
            cmd_warm(args)
        elif args.command == "reembed":
            cmd_reembed(args)
        elif args.command == "relay":
            cmd_relay(args)
        elif hasattr(args, 'func'):
            args.func(args)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        print("\n  Interrupted. Run flex init again to resume.")
        sys.exit(130)


if __name__ == "__main__":
    main()
