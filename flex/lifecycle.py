"""Registry-driven lifecycle coordination.

This deliberately owns scheduling only. Provider refresh modules still own
source signatures, parsing, validation, publication, and their receipts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable


Materialize = Callable[..., dict | None]


class LifecycleCoordinator:
    """Run one declared lifecycle pass through one materialization boundary."""

    def __init__(self, materialize: Materialize):
        self._materialize = materialize

    def local_watch_pass(
        self, *, eligible: Callable[[dict], bool] | None = None,
    ) -> dict[str, str]:
        """Reconcile declared local watch cells after a provider signature probe.

        A positive probe is the only condition that invokes real provider
        materialization. The provider remains responsible for publishing its
        structural generation and recording deferred semantic debt.
        """
        from flex.registry import discover_watched

        results: dict[str, str] = {}
        for cell in discover_watched():
            if eligible is not None and not eligible(cell):
                continue
            name = cell.get("name")
            if not name:
                continue
            try:
                probe = self._materialize(name, dry_run=True, quiet=True)
                changed = isinstance(probe, dict) and (
                    probe.get("changed") is True or probe.get("needs_resync") is True
                )
                if not changed:
                    continue
                stats = self._materialize(name, quiet=True)
                results[name] = (
                    "deferred" if isinstance(stats, dict) and stats.get("deferred")
                    else "ok" if stats is not None else "error: no stats"
                )
            except Exception as exc:
                results[name] = f"error: {exc}"
        return results

    def remote_refresh_pass(self, *, force: bool = False) -> dict[str, str]:
        """Refresh only registry-declared remote/scheduled cells that are due."""
        from flex.admission import try_heavy_lease
        from flex.registry import classify_refresh_state, discover_refreshable
        from flex.refresh import _heavy_admission_wait_s

        now = datetime.fromtimestamp(__import__("time").time(), timezone.utc)
        due = [
            cell for cell in discover_refreshable()
            if force or classify_refresh_state(cell, now)["refresh_due"]
        ]
        if not due:
            return {}
        results: dict[str, str] = {}
        with try_heavy_lease(
            detail="registry remote refreshes", timeout_s=_heavy_admission_wait_s()
        ) as lease:
            if not lease.acquired:
                return {str(cell["name"]): "deferred" for cell in due}
            for cell in due:
                name = str(cell["name"])
                try:
                    stats = self._materialize(name, scheduled=True, _heavy_admitted=True)
                    results[name] = (
                        "deferred" if isinstance(stats, dict) and stats.get("deferred")
                        else "ok" if stats is not None else "error: no stats"
                    )
                except Exception as exc:
                    results[name] = f"error: {exc}"
        return results

    def active_append_pass(self, runner: Callable[[set[str]], dict | None]) -> dict | None:
        """Run the declared active-append detector through one owner.

        Unlike a generic watch probe, an append detector has a provider-native
        durable cursor and must be scheduled at its own short cadence. The
        Registry still selects the eligible cells; the provider runner owns
        parsing, validation, publication, and cursor receipts.
        """
        from flex.registry import list_cells

        names = {
            str(cell["name"])
            for cell in list_cells()
            if cell.get("lifecycle") == "watch"
            # ``None`` is the one-version migration state for an existing
            # Codex install; new registrations declare active_append
            # explicitly. It remains provider-scoped rather than making all
            # watches implicitly active append detectors.
            and (cell.get("detector") == "active_append"
                 or (cell.get("detector") is None and cell.get("cell_type") == "codex"))
            and cell.get("active", 1)
        }
        return runner(names) if names else None


def coordinator(materialize: Materialize) -> LifecycleCoordinator:
    """Explicit factory keeps refresh module imports out of daemon startup."""
    return LifecycleCoordinator(materialize)
