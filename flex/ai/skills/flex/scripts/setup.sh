#!/usr/bin/env bash
# flex setup — installs flex and verifies the MCP server is running.
# Run this if the flex_search MCP tool is not available.
#
# What it does:
#   1. Checks if flex is already installed
#   2. If not, installs via curl (hermetic venv at ~/.flex/venv/)
#   3. Verifies MCP server is registered
#
# Safe to re-run. flex init is idempotent.

set -e

# Check if flex is already installed and MCP is wired
if command -v flex &>/dev/null && [ -f ~/.claude.json ] && grep -q '"flex"' ~/.claude.json 2>/dev/null; then
    echo "flex is installed and MCP is registered."
    echo "Restart Claude Code or type /mcp to connect."
    exit 0
fi

# Install
echo "Installing flex..."
curl -sSL https://getflex.dev/install.sh | bash

echo ""
echo "Done. Restart Claude Code or type /mcp to verify."
