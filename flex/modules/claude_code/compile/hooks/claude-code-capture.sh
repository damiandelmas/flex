#!/bin/bash
#
# PostToolUse Hook - Queue events for async Flex capture
#
# FAST PATH: Just INSERT into SQLite queue (~2ms)
# Worker daemon processes queue in background (stays warm)
#

set -uo pipefail

QUEUE_DB="${HOME}/.flex/queue.db"
mkdir -p "$(dirname "$QUEUE_DB")"

# Parse input
INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name // empty')
SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')
CWD=$(echo "$INPUT" | jq -r '.cwd // empty')

# Need basics
if [[ -z "$TOOL_NAME" ]] || [[ -z "$SESSION_ID" ]]; then
    exit 0
fi

# Initialize fields
FILE_PATH=""
OLD_STRING=""
NEW_STRING=""
WRITE_CONTENT=""
PATTERN=""
COMMAND=""
URL=""
QUERY=""
PROMPT=""
SUBAGENT=""
SPAWNED_AGENT=""
TODOS=""
SKILL=""
QUESTIONS=""
WEB_CONTENT=""
WEB_STATUS=""

# Extract tool-specific fields based on category
case "$TOOL_NAME" in
    Write|Edit|Read)
        FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // .tool_input.filePath // empty')
        OLD_STRING=$(echo "$INPUT" | jq -r '.tool_input.old_string // empty')
        NEW_STRING=$(echo "$INPUT" | jq -r '.tool_input.new_string // empty')
        WRITE_CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // empty' | head -c 500000)
        ;;
    MultiEdit)
        FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
        ;;
    NotebookEdit)
        FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.notebook_path // empty')
        ;;
    Grep)
        PATTERN=$(echo "$INPUT" | jq -r '.tool_input.pattern // empty')
        FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.path // empty')
        ;;
    Glob)
        PATTERN=$(echo "$INPUT" | jq -r '.tool_input.pattern // empty')
        FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.path // empty')
        ;;
    Bash)
        COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')
        ;;
    KillShell)
        COMMAND=$(echo "$INPUT" | jq -r '.tool_input.shell_id // empty')
        ;;
    WebFetch)
        URL=$(echo "$INPUT" | jq -r '.tool_input.url // empty')
        PROMPT=$(echo "$INPUT" | jq -r '.tool_input.prompt // empty')
        WEB_STATUS=$(echo "$INPUT" | jq -r '.tool_response.code // empty')
        WEB_CONTENT=$(echo "$INPUT" | jq -r '.tool_response.result // empty' | head -c 500000)
        ;;
    WebSearch)
        QUERY=$(echo "$INPUT" | jq -r '.tool_input.query // empty')
        ;;
    Task)
        SUBAGENT=$(echo "$INPUT" | jq -r '.tool_input.subagent_type // empty')
        PROMPT=$(echo "$INPUT" | jq -r '.tool_input.prompt // empty' | head -c 500)
        RESULT_TEXT=$(echo "$INPUT" | jq -r '.tool_result // .result // ""')
        SPAWNED_AGENT=$(echo "$RESULT_TEXT" | grep -o 'agentId: [a-f0-9]*' | head -1 | sed 's/agentId: //' || true)
        ;;
    TaskOutput)
        COMMAND=$(echo "$INPUT" | jq -r '.tool_input.task_id // empty')
        ;;
    TodoWrite)
        TODOS=$(echo "$INPUT" | jq -r '.tool_input.todos | length | tostring' 2>/dev/null || echo "0")
        ;;
    EnterPlanMode|ExitPlanMode)
        ;;
    AskUserQuestion)
        QUESTIONS=$(echo "$INPUT" | jq -r '.tool_input.questions | length | tostring' 2>/dev/null || echo "0")
        ;;
    Skill)
        SKILL=$(echo "$INPUT" | jq -r '.tool_input.skill // empty')
        ;;
    SlashCommand)
        COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')
        ;;
    mcp__*)
        COMMAND=$(echo "$INPUT" | jq -c '.tool_input // {}' | head -c 500)
        ;;
    *)
        COMMAND=$(echo "$INPUT" | jq -c '.tool_input // {}' | head -c 200)
        ;;
esac

# Skip temp files, node_modules
if [[ -n "$FILE_PATH" ]]; then
    if [[ "$FILE_PATH" =~ ^/tmp/ ]] || \
       [[ "$FILE_PATH" =~ node_modules ]] || \
       [[ "$FILE_PATH" =~ \.git/ ]]; then
        exit 0
    fi
fi

# Skip noisy bash commands
if [[ "$TOOL_NAME" == "Bash" ]] && [[ -n "$COMMAND" ]]; then
    SKIP_PATTERN='^(ls|pwd|which|type|file|stat)( |$)'
    if [[ "$COMMAND" =~ $SKIP_PATTERN ]]; then
        exit 0
    fi
fi

# Get message number
MSG_NUM="0"
if [[ -n "$TRANSCRIPT_PATH" ]] && [[ -f "$TRANSCRIPT_PATH" ]]; then
    MSG_NUM=$(wc -l < "$TRANSCRIPT_PATH" 2>/dev/null || echo "0")
fi

# Timestamp
TS=$(date +%s)

# Build queue event JSON (compact, one line per event)
EVENT=$(jq -cn \
    --arg tool "$TOOL_NAME" \
    --arg file "$FILE_PATH" \
    --arg old_string "$OLD_STRING" \
    --arg new_string "$NEW_STRING" \
    --arg write_content "$WRITE_CONTENT" \
    --arg pattern "$PATTERN" \
    --arg command "$COMMAND" \
    --arg url "$URL" \
    --arg query "$QUERY" \
    --arg prompt "$PROMPT" \
    --arg subagent "$SUBAGENT" \
    --arg spawned_agent "$SPAWNED_AGENT" \
    --arg todos "$TODOS" \
    --arg skill "$SKILL" \
    --arg questions "$QUESTIONS" \
    --arg web_content "$WEB_CONTENT" \
    --arg web_status "$WEB_STATUS" \
    --arg session "$SESSION_ID" \
    --arg msg "$MSG_NUM" \
    --arg cwd "$CWD" \
    --arg ts "$TS" \
    '{
        tool: $tool,
        file: (if $file != "" then $file else null end),
        old_string: (if $old_string != "" then $old_string else null end),
        new_string: (if $new_string != "" then $new_string else null end),
        write_content: (if $write_content != "" then $write_content else null end),
        pattern: (if $pattern != "" then $pattern else null end),
        command: (if $command != "" then $command else null end),
        url: (if $url != "" then $url else null end),
        query: (if $query != "" then $query else null end),
        prompt: (if $prompt != "" then $prompt else null end),
        subagent: (if $subagent != "" then $subagent else null end),
        spawned_agent: (if $spawned_agent != "" then ("agent-" + $spawned_agent) else null end),
        todos: (if $todos != "" and $todos != "0" then ($todos | tonumber) else null end),
        skill: (if $skill != "" then $skill else null end),
        questions: (if $questions != "" and $questions != "0" then ($questions | tonumber) else null end),
        web_content: (if $web_content != "" then $web_content else null end),
        web_status: (if $web_status != "" then ($web_status | tonumber) else null end),
        session: $session,
        msg: ($msg | tonumber),
        cwd: $cwd,
        ts: ($ts | tonumber)
    } | with_entries(select(.value != null))')

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
