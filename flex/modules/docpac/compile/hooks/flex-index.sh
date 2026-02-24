#!/bin/bash
#
# Flex incremental index hook
# Triggers on Write/Edit to */context/**/*.md files
# Queues file for async indexing by worker
#

# Get file path from hook input
FILE_PATH=$(jq -r '.tool_input.file_path // empty' 2>/dev/null)

# Skip if no file path or not a context markdown file
[[ -z "$FILE_PATH" ]] && exit 0
[[ "$FILE_PATH" != *"/context/"* ]] && exit 0
[[ "$FILE_PATH" != *.md ]] && exit 0

# Skip excluded paths
[[ "$FILE_PATH" == *"/buffer/"* ]] && exit 0
[[ "$FILE_PATH" == *"/_raw/"* ]] && exit 0
[[ "$FILE_PATH" == *"/_stale/"* ]] && exit 0
[[ "$FILE_PATH" == *"/_qmem/"* ]] && exit 0
[[ "$FILE_PATH" == *"/onboard/"* ]] && exit 0
[[ "$FILE_PATH" == *"/cache/"* ]] && exit 0

# Queue file for indexing (SQLite WAL — dedup via UNIQUE on path)
QUEUE_DB="$HOME/.flex/queue.db"
mkdir -p "$(dirname "$QUEUE_DB")"
sqlite3 "$QUEUE_DB" "CREATE TABLE IF NOT EXISTS pending (path TEXT PRIMARY KEY, ts INTEGER); INSERT OR REPLACE INTO pending VALUES ('$FILE_PATH', $(date +%s))" 2>/dev/null
