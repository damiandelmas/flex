"""Pi coding-agent install hook over the shared session substrate."""

from __future__ import annotations

from flex.modules.claude_code.coding_agent_install import (
    register_common_args,
    run_from_spec,
)
from flex.modules.pi.compile.worker import DEFAULT_PI_SESSIONS


MODULE_SUMMARY = "index Pi sessions — append-only JSONL conversation trees"

MODULE = {
    "cell_type": "pi",
    "maturity": "experimental",
    "license_intent": "MIT-compatible core module",
    "release_posture": "public",
    "description": (
        "Pi coding-agent session provenance. The native source is one versioned "
        "JSONL tree per session; every entry and branch edge remains recoverable "
        "through Pi sidecars."
    ),
    "default_cell_name": "pi",
    "source_arg": "--pi-sessions-dir",
    "source_label": "Pi sessions",
    "source_help": "Path to Pi's session root (default: ~/.pi/agent/sessions)",
    "default_source": DEFAULT_PI_SESSIONS,
    "missing_hint": "install Pi and run at least one persisted session.",
    "transpile": "flex.modules.pi.compile.worker:transpile",
    "signature": "flex.modules.pi.compile.worker:compute_source_signature",
    "signature_meta_keys": ("pi_source_signature", "pi_source_size"),
    "source_meta_key": "pi_source_path",
    "refresh_module": "flex.modules.pi.refresh",
    "watch_pattern": "**/*.jsonl",
    "substrate": "claude_code",
    "soma_level": "L3",
    "views_from": ("claude_code",),
    "presets_from": ("claude_code", "soma"),
    "instructions_from": ("pi", "claude_code"),
    "enrichment_stubs_from": "claude_code",
    "skill": "flex:sessions:pi",
    "query_examples": (
        "@orient",
        "@digest",
        "@story session='01a0...'",
        "SELECT * FROM _types_pi_session ORDER BY created_at_ms DESC",
        "SELECT * FROM _types_pi_entry WHERE source_id = '01a0...' ORDER BY file_position",
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
