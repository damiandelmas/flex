#!/bin/bash
# Swap claude_code cell between full and 30d versions.
# Usage: swap_cell.sh [30d|full]

CELL=$(sqlite3 ~/.flex/registry.db "SELECT path FROM cells WHERE name='claude_code'")
if [[ -z "$CELL" ]]; then echo "claude_code cell not found"; exit 1; fi

case "${1:-status}" in
  30d)
    systemctl --user stop flex-worker 2>/dev/null
    mv "$CELL" "${CELL}.full"
    mv "${CELL}.30d" "$CELL"
    systemctl --user start flex-worker 2>/dev/null
    echo "Swapped to 30d cell"
    ;;
  full)
    systemctl --user stop flex-worker 2>/dev/null
    mv "$CELL" "${CELL}.30d"
    mv "${CELL}.full" "$CELL"
    systemctl --user start flex-worker 2>/dev/null
    echo "Swapped to full cell"
    ;;
  status)
    SIZE=$(du -h "$CELL" | cut -f1)
    if [[ -f "${CELL}.30d" ]]; then
      echo "Active: full ($SIZE) | 30d available"
    elif [[ -f "${CELL}.full" ]]; then
      echo "Active: 30d ($SIZE) | full available"
    else
      echo "Active: $SIZE (no swap target)"
    fi
    ;;
  *)
    echo "Usage: swap_cell.sh [30d|full|status]"
    ;;
esac
