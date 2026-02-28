#!/bin/bash
#
# UserPromptSubmit Hook - Capture user prompts to Flex queue
#
# Same queue as PostToolUse, different event type
#

set -uo pipefail

QUEUE_DB="${HOME}/.flex/queue.db"
mkdir -p "$(dirname "$QUEUE_DB")"

# Parse input
INPUT=$(cat)
SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')
CWD=$(echo "$INPUT" | jq -r '.cwd // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

# Extract full user prompt (no truncation)
PROMPT=$(echo "$INPUT" | jq -r '.prompt // empty')

# Need basics
if [[ -z "$SESSION_ID" ]] || [[ -z "$PROMPT" ]]; then
    exit 0
fi

# Get message number
MSG_NUM="0"
if [[ -n "$TRANSCRIPT_PATH" ]] && [[ -f "$TRANSCRIPT_PATH" ]]; then
    MSG_NUM=$(wc -l < "$TRANSCRIPT_PATH" 2>/dev/null || echo "0")
fi

TS=$(date +%s)

# Build event - tool="UserPrompt" to distinguish from tool calls
EVENT=$(jq -cn \
    --arg tool "UserPrompt" \
    --arg prompt "$PROMPT" \
    --arg session "$SESSION_ID" \
    --arg msg "$MSG_NUM" \
    --arg cwd "$CWD" \
    --arg ts "$TS" \
    '{
        tool: $tool,
        prompt: $prompt,
        session: $session,
        msg: ($msg | tonumber),
        cwd: $cwd,
        ts: ($ts | tonumber)
    }')

# SQLite queue — hook must create table inline (may fire before worker starts)
echo "$EVENT" | __FLEX_PYTHON__ -c "
import sqlite3, sys
event = sys.stdin.read()
db = sqlite3.connect('${QUEUE_DB}', timeout=5)
db.execute('CREATE TABLE IF NOT EXISTS claude_code_pending (session_id TEXT NOT NULL, ts INTEGER NOT NULL, payload TEXT NOT NULL)')
db.execute('CREATE INDEX IF NOT EXISTS idx_claude_code_ts ON claude_code_pending(ts)')
db.execute('INSERT INTO claude_code_pending VALUES (?, ?, ?)', ('${SESSION_ID}', ${TS}, event))
db.commit()
db.close()
" 2>/dev/null || true

exit 0
