"""Public install-time enrichment pipeline for coding-agent cells.

Runs the CC enrichment sequence — warmup classification, source pooling,
source/file/delegation graphs, fingerprints, repo attribution, community
labels — plus preset + view installation. Exposed here (not hidden in
install.py) so other coding-agent modules can reuse the pipeline in place.

Historical name: `claude_code.install._run_enrichment_quiet`. Promoted to
public; `cell_type` + `skip_delegation` added. CC defaults preserve
original behavior; `claude_code/install.py::_run_enrichment_quiet` is now
a back-compat shim that forwards here.
"""

from __future__ import annotations

import contextlib
import io
import sqlite3


def run_enrichment(
    conn: sqlite3.Connection,
    *,
    cell_type: str = "claude-code",
    skip_delegation: bool = False,
    progress_cb=None,
) -> tuple[int, list[str]]:
    """Run the CC enrichment pipeline on a coding-agent cell.

    Args:
        conn: open cell connection (CC canonical schema).
        cell_type: which module's presets to install.
            Defaults to 'claude-code'. Other coding-agent modules pass their
            own cell_type.
        skip_delegation: omit `rebuild_delegation_graph`.
        progress_cb: optional callable(step_name: str).

    Returns (n_communities, failed_step_names).
    """
    try:
        from flex.modules.claude_code.manage.rebuild_all import (
            rebuild_warmup_types, reembed_sources, rebuild_source_graph,
            rebuild_community_labels, rebuild_file_graph, rebuild_delegation_graph,
        )
        from flex.modules.claude_code.manage.enrich_summary import run as run_fingerprints
        from flex.modules.claude_code.manage.enrich_soma_repos import run as _register_soma_repos
        from flex.modules.claude_code.manage.enrich_repo_project import run as run_repo_project
        from flex.views import regenerate_views, install_views
    except ImportError:
        return 0, []

    steps: list[tuple[str, callable]] = [
        ("warmup types",     lambda: rebuild_warmup_types(conn)),
        ("source pooling",   lambda: reembed_sources(conn)),
        ("source graph",     lambda: rebuild_source_graph(conn)),
        ("file graph",       lambda: rebuild_file_graph(conn)),
    ]
    if not skip_delegation:
        steps.append(("delegation graph", lambda: rebuild_delegation_graph(conn)))
    steps.extend([
        ("fingerprints",     lambda: run_fingerprints(conn)),
        ("repo registry",    lambda: _register_soma_repos(conn)),
        ("repo attribution", lambda: run_repo_project(conn)),
        ("community labels", lambda: rebuild_community_labels(conn)),
    ])

    failures: list[str] = []
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        for step, fn in steps:
            if progress_cb:
                progress_cb(step)
            try:
                fn()
            except Exception:
                failures.append(step)

        if progress_cb:
            progress_cb("presets")
        try:
            from flex.manage.install_presets import install_presets, _preset_dirs_for
            for pd in _preset_dirs_for(cell_type):
                if pd.exists():
                    install_presets(conn, pd)
            # Fallback: compatible cells still get the public coding-agent presets.
            if cell_type != 'claude-code':
                for pd in _preset_dirs_for('claude-code')[1:]:
                    if pd.exists():
                        install_presets(conn, pd)
            conn.commit()
            n_presets = conn.execute("SELECT COUNT(*) FROM _presets").fetchone()[0]
            if n_presets == 0:
                failures.append("presets (0 installed)")
        except Exception:
            failures.append("presets")

        if progress_cb:
            progress_cb("views")
        try:
            from flex.cli import _find_view_dirs
            # Stock lives under CC; compatible coding-agent modules reuse it.
            view_dirs = _find_view_dirs("claude_code", cell_type)
            if not view_dirs:
                view_dirs = _find_view_dirs("claude_code", "claude-code")
            for view_dir in view_dirs:
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
