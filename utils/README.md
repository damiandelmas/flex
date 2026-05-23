# utils

Small standalone helpers that support Flex recovery workflows but are not part of the main `flex` CLI.

## flex-resume

`flex-resume` resumes a Claude Code session by session id or prefix from any project directory. It searches `~/.claude/projects`, links the session into the current project's Claude session folder when needed, reads the session's recorded working directory, and runs `claude -r <session-id>`.

```bash
utils/flex-resume <session-id-or-prefix>
```

This is useful after Flex finds the session that contains lost work or a missing change trail.
