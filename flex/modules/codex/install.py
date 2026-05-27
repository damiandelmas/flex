"""codex install hook — transpiler spec + Claude Code substrate."""

from __future__ import annotations

from flex.modules.claude_code.coding_agent_install import (
    register_common_args,
    run_from_spec,
)
from flex.modules.codex.compile.worker import DEFAULT_CODEX_SESSIONS_DIR


MODULE_SUMMARY = "index ~/.codex/sessions — programmable memory for codex CLI"

MODULE = {
    "cell_type": "codex",
    "maturity": "canonical",
    "license_intent": "MIT-compatible core module",
    "release_posture": "public",
    "description": "Codex CLI session provenance. Each doc is a session, each chunk is a prompt, assistant turn, tool call, or tool result.",
    "default_cell_name": "codex",
    "source_arg": "--codex-dir",
    "source_label": "codex sessions",
    "source_help": "Path to codex sessions root (default: ~/.codex/sessions)",
    "default_source": DEFAULT_CODEX_SESSIONS_DIR,
    "missing_hint": "install codex CLI and run at least one session.",
    "transpile": "flex.modules.codex.compile.worker:transpile",
    "signature": "flex.modules.codex.compile.worker:compute_dir_signature",
    "signature_meta_keys": ("codex_dir_total_size", "codex_dir_file_count"),
    "source_meta_key": "codex_source_path",
    "refresh_module": "flex.modules.codex.refresh",
    "watch_pattern": "**/rollout-*.jsonl",
    "substrate": "claude_code",
    "soma_level": "L3",
    "views_from": ("claude_code",),
    "presets_from": ("claude_code", "soma"),
    "instructions_from": ("codex", "claude_code"),
    "enrichment_stubs_from": "claude_code",
    "query_examples": ("@orient", "@digest", "@file path='src/foo.py'"),
}


def register_args(parser) -> None:
    register_common_args(
        parser,
        source_flag=MODULE["source_arg"],
        source_help=MODULE["source_help"],
        default_name=MODULE["default_cell_name"],
    )


def run(args, console) -> None:
    run_from_spec(args, console, MODULE)
