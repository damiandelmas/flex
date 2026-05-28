"""goose install hook — transpiler spec + Claude Code substrate."""

from __future__ import annotations

from flex.modules.claude_code.coding_agent_install import (
    register_common_args,
    run_from_spec,
)
from flex.modules.goose.compile.worker import DEFAULT_GOOSE_DB


MODULE_SUMMARY = "index goose sessions.db — programmable memory for goose agents"

MODULE = {
    "cell_type": "goose",
    "description": "Goose session provenance. Each doc is a session, each chunk is a prompt, assistant turn, tool call, or tool result.",
    "default_cell_name": "goose",
    "source_arg": "--goose-db",
    "source_label": "goose db",
    "source_help": "Path to goose sessions.db (default: ~/.local/share/goose/sessions/sessions.db)",
    "default_source": DEFAULT_GOOSE_DB,
    "missing_hint": "install goose and run at least one session.",
    "transpile": "flex.modules.goose.compile.worker:transpile",
    "signature_meta_keys": ("goose_db_size",),
    "source_meta_key": "goose_db_path",
    "refresh_module": "flex.modules.goose.refresh",
    "watch_pattern": "sessions.db",
    "substrate": "claude_code",
    "soma_level": "L3",
    "views_from": ("claude_code",),
    "presets_from": ("claude_code", "soma"),
    "instructions_from": ("goose", "claude_code"),
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
