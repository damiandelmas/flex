"""OS-authoritative admission for Flex's single semantic work lane.

The lock is deliberately a file-descriptor lease, not a pid file.  ``flock``
is released by the kernel when an owning process dies, while the adjacent JSON
receipt is only useful to people reading diagnostics.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def flex_home() -> Path:
    """Resolve FLEX_HOME at use time so isolated callers need no reload."""
    return Path(os.environ.get("FLEX_HOME", Path.home() / ".flex")).resolve()


def _paths() -> tuple[Path, Path]:
    home = flex_home()
    return home / "semantic-work.lock", home / "semantic-work.json"


@dataclass
class HeavyWorkLease:
    """A non-blocking semantic-work lease returned by :func:`try_heavy_lease`."""

    fd: Any | None
    metadata_path: Path
    acquired: bool

    def close(self) -> None:
        if self.fd is None:
            return
        fd, self.fd = self.fd, None
        try:
            # Removing a receipt is best effort.  It is never consulted for
            # admission, so an interrupted write or a stale file is harmless.
            self.metadata_path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
        finally:
            fd.close()

    def __enter__(self) -> "HeavyWorkLease":
        return self

    def __exit__(self, *_unused) -> None:
        self.close()


def try_heavy_lease(*, detail: str | None = None,
                    timeout_s: float = 0) -> HeavyWorkLease:
    """Acquire the semantic lane, waiting at most ``timeout_s`` seconds.

    ``timeout_s=0`` preserves the capture worker's nonblocking contract.  A
    bounded caller polls ``LOCK_NB`` at a short monotonic interval instead of
    spinning; this lets queued external refreshes claim the next release even
    while a capture process keeps attempting its own nonblocking leases.
    """
    lock_path, metadata_path = _paths()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = lock_path.open("a+")
    try:
        timeout = max(0.0, float(timeout_s))
    except (TypeError, ValueError):
        timeout = 0.0
    deadline = time.monotonic() + timeout
    while True:
        try:
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                fd.close()
                return HeavyWorkLease(None, metadata_path, False)
            # A modest sleep avoids an unfair userspace spin while keeping the
            # release-to-waiter latency comfortably below a capture tick.
            time.sleep(min(0.05, remaining))

    receipt = {
        "owner": "flex-semantic",
        "pid": os.getpid(),
        "started_at": time.time(),
        "detail": detail or "",
    }
    try:
        tmp = metadata_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(receipt, sort_keys=True))
        tmp.replace(metadata_path)
    except OSError:
        pass
    return HeavyWorkLease(fd, metadata_path, True)


def heavy_work_active() -> dict[str, Any]:
    """Return diagnostic state; only ``active`` comes from the OS lock.

    The caller cannot infer correctness from the optional JSON receipt.  In
    particular a stale receipt with an unlocked lock reports inactive.
    """
    lease = try_heavy_lease(detail="diagnostic probe")
    if lease.acquired:
        lease.close()
        active = False
    else:
        active = True
    _, metadata_path = _paths()
    metadata: dict[str, Any] | None = None
    try:
        parsed = json.loads(metadata_path.read_text())
        metadata = parsed if isinstance(parsed, dict) else None
    except (OSError, ValueError, TypeError):
        pass
    return {"active": active, "metadata": metadata}
