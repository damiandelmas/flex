"""DeepSeek Harness install hook — source spec over the shared substrate."""

from __future__ import annotations

from flex.modules.claude_code.coding_agent_install import (
    register_common_args,
    run_from_spec,
)
from flex.modules.deepseek.compile.worker import DEFAULT_DSH_HOME


MODULE_SUMMARY = "index DeepSeek Harness sessions — append-only JSONL agent memory"

MODULE = {
    "cell_type": "deepseek",
    "maturity": "experimental",
    "license_intent": "MIT-compatible core module",
    "release_posture": "public",
    "description": (
        "DeepSeek Harness session provenance. The native source is one "
        "session.jsonl(.zstd) artifact per session; every event remains "
        "recoverable through DeepSeek sidecars."
    ),
    "default_cell_name": "deepseek",
    "source_arg": "--dsh-dir",
    "source_label": "DeepSeek sessions",
    "source_help": "Path to the DeepSeek Harness home (default: ~/.dsh)",
    "default_source": DEFAULT_DSH_HOME,
    "missing_hint": "install DeepSeek Harness and run at least one session.",
    "transpile": "flex.modules.deepseek.compile.worker:transpile",
    "signature": "flex.modules.deepseek.compile.worker:compute_source_signature",
    "signature_meta_keys": ("deepseek_source_signature", "deepseek_source_size"),
    "source_meta_key": "deepseek_source_path",
    "refresh_module": "flex.modules.deepseek.refresh",
    "watch_pattern": "**/session.jsonl*",
    "substrate": "claude_code",
    "soma_level": "L3",
    "views_from": ("claude_code",),
    "presets_from": ("claude_code", "soma"),
    "instructions_from": ("deepseek", "claude_code"),
    "enrichment_stubs_from": "claude_code",
    "skill": "flex:sessions:deepseek",
    "query_examples": (
        "@orient",
        "@digest",
        "@story session='session-...'",
        "SELECT * FROM _types_deepseek_session ORDER BY created_at_ms DESC",
    ),
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
