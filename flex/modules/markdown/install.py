"""obsidian / markdown install hook.

Called by the CLI dispatcher when `flex init --module obsidian` runs.
Handles vault auto-detection, compile_vault(), services, and MCP wiring.
The folder is `markdown` but the CLI surface is `obsidian` — the module
handles both Obsidian vaults and plain markdown trees.
"""

import sys
import time
from pathlib import Path


CLI_NAME = "obsidian"
MODULE_SUMMARY = "index Obsidian vault, start worker, register MCP"


def register_args(parser) -> None:
    """Register --vault and --name on the init subparser."""
    # argparse rejects duplicate flags; skip if already present (e.g. during
    # parser re-entry or test harnesses that call register_args twice).
    _existing = {a.option_strings[0] for a in parser._actions if a.option_strings}
    if '--vault' not in _existing:
        parser.add_argument(
            '--vault', default=None,
            help='Path to Obsidian vault (--module obsidian only). Auto-detected if omitted.',
        )
    if '--name' not in _existing:
        parser.add_argument(
            '--name', default=None,
            help='Cell name override (--module obsidian only).',
        )


def _detect_vault() -> list[Path]:
    """Scan common locations for Obsidian vaults. Returns list of candidates."""
    candidates: list[Path] = []

    for check in [
        Path.home() / 'vault',
        Path.home() / 'Obsidian',
        Path.home() / 'obsidian',
        Path.home() / 'Documents' / 'Obsidian',
        Path.home() / 'Documents' / 'vault',
    ]:
        if check.is_dir() and (check / '.obsidian').is_dir():
            candidates.append(check)

    wsl_user = Path('/mnt/c/Users')
    if wsl_user.exists():
        for user_dir in wsl_user.iterdir():
            if not user_dir.is_dir() or user_dir.name.startswith('.'):
                continue
            for check in [
                user_dir / 'vault',
                user_dir / 'Obsidian',
                user_dir / 'Documents' / 'Obsidian',
            ]:
                if check.is_dir() and (check / '.obsidian').is_dir():
                    candidates.append(check)

    return candidates


def run(args, console) -> None:
    """Install obsidian module: detect vault → compile → services → MCP."""
    from rich.panel import Panel
    from rich.text import Text

    from flex.cli import (
        _install_launchd, _install_systemd, _patch_claude_json,
        _start_services_direct, _verify_services,
    )

    vault_path = getattr(args, 'vault', None)

    # Auto-detect
    if not vault_path:
        candidates = _detect_vault()
        if len(candidates) == 1:
            vault_path = str(candidates[0])
            console.print(f"  Vault detected      [green]{vault_path}[/green]")
        elif len(candidates) > 1:
            console.print("  [yellow]Multiple vaults found:[/yellow]")
            for i, c in enumerate(candidates):
                console.print(f"    [{i + 1}] {c}")
            console.print()
            console.print("  Specify with: [bold]flex init --module obsidian --vault /path/to/vault[/bold]")
            console.print()
            return
        else:
            console.print("  [yellow]No Obsidian vault found.[/yellow]")
            console.print()
            console.print("  Specify with: [bold]flex init --module obsidian --vault /path/to/vault[/bold]")
            console.print()
            return

    vp = Path(vault_path).resolve()
    if not vp.exists():
        console.print(f"  [red]Vault not found: {vault_path}[/red]")
        return

    cell_name = getattr(args, 'name', None) or vp.name
    cell_type = 'obsidian' if (vp / '.obsidian').is_dir() else 'markdown'

    console.print(f"  Indexing vault       [bold]{vp}[/bold]")
    console.print(f"  Cell name            {cell_name}")
    console.print(f"  Cell type            {cell_type}")
    console.print()

    try:
        from flex.modules.markdown.compile.init import compile_vault
        db = compile_vault(vp, name=cell_name, cell_type=cell_type)
        chunks = db.execute("SELECT COUNT(*) FROM _raw_chunks").fetchone()[0]
        sources = db.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
        db.close()
        console.print(f"  vault               [green]{sources} notes, {chunks} chunks[/green]")
    except Exception as e:
        console.print(f"  vault               [red]error: {e}[/red]")
        raise

    # Services + MCP wiring
    if sys.platform != "win32":
        _install_systemd() or _install_launchd()
        time.sleep(1)
        worker_ok, mcp_ok = _verify_services()
        if not worker_ok or not mcp_ok:
            _start_services_direct()
            time.sleep(1)
            worker_ok, mcp_ok = _verify_services()
        _status = lambda ok: "[green]running[/green]" if ok else "[red]failed[/red]"
        console.print(f"  worker             {_status(worker_ok)}")
        console.print(f"  MCP                {_status(mcp_ok)}")

    _patch_claude_json()
    console.print()

    panel_content = Text()
    panel_content.append("Flex is ready.\n\n", style="cyan")
    panel_content.append("Vault indexed         ", style="")
    panel_content.append(f"{cell_name}\n", style="green")
    panel_content.append("MCP Server            ", style="")
    panel_content.append("http://localhost:7134/mcp\n\n", style="green")
    panel_content.append("  flex search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@orient"\n', style="bold")
    panel_content.append("  flex search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@hubs"\n', style="bold")
    panel_content.append("  flex search --cell ", style="bold")
    panel_content.append(f"{cell_name} ", style="bold green")
    panel_content.append('"@ghost-notes"\n', style="bold")
    console.print(Panel(panel_content, padding=(1, 2), highlight=False))
    console.print()
