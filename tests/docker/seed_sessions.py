"""
Seed ~/.claude/projects/ with minimal JSONL sessions
that simulate a real new-user environment.

Covers: Read, Edit, Write, Task (delegation), and enough sessions
for the source graph to have edges (≥3 required).
"""
import json
from pathlib import Path

PROJ_DIR = Path.home() / ".claude" / "projects" / "-home-testuser-projects-myapp"
PROJ_DIR.mkdir(parents=True, exist_ok=True)

SESSION_A = "aaaaaaaa-0000-0000-0000-000000000001"
SESSION_B = "aaaaaaaa-0000-0000-0000-000000000002"
SESSION_C = "aaaaaaaa-0000-0000-0000-000000000003"
SESSION_D = "aaaaaaaa-0000-0000-0000-000000000004"
CHILD_AGENT = "agent-cccccccc-0000-0000-0000-000000000001"

sessions = {
    SESSION_A: [
        {
            "type": "user",
            "uuid": "uuid-a001",
            "timestamp": "2026-02-19T10:00:00Z",
            "message": {"role": "user", "content": "Fix the auth bug in auth.py"},
            "cwd": "/home/testuser/projects/myapp",
            "parentUuid": None,
        },
        {
            "type": "assistant",
            "uuid": "uuid-a002",
            "timestamp": "2026-02-19T10:00:05Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll look at auth.py and fix it."},
                    {
                        "type": "tool_use",
                        "id": "toolu_0001",
                        "name": "Read",
                        "input": {"file_path": "/home/testuser/projects/myapp/auth.py"},
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_0001",
                        "content": "def auth(u, p):\n    return u == 'admin'\n",
                    },
                    {
                        "type": "tool_use",
                        "id": "toolu_0002",
                        "name": "Edit",
                        "input": {
                            "file_path": "/home/testuser/projects/myapp/auth.py",
                            "old_string": "return u == 'admin'",
                            "new_string": "return check_password(u, p)",
                        },
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_0002",
                        "content": "File edited successfully.",
                    },
                ],
            },
            "cwd": "/home/testuser/projects/myapp",
        },
    ],
    SESSION_B: [
        {
            "type": "user",
            "uuid": "uuid-b001",
            "timestamp": "2026-02-20T09:00:00Z",
            "message": {"role": "user", "content": "Create a utils module"},
            "cwd": "/home/testuser/projects/myapp",
            "parentUuid": None,
        },
        {
            "type": "assistant",
            "uuid": "uuid-b002",
            "timestamp": "2026-02-20T09:00:10Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll create utils.py."},
                    {
                        "type": "tool_use",
                        "id": "toolu_0003",
                        "name": "Write",
                        "input": {
                            "file_path": "/home/testuser/projects/myapp/utils.py",
                            "content": "def helper():\n    pass\n",
                        },
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_0003",
                        "content": "File written.",
                    },
                ],
            },
            "cwd": "/home/testuser/projects/myapp",
        },
    ],
    # Session C — adds a third session so the source graph can build edges
    SESSION_C: [
        {
            "type": "user",
            "uuid": "uuid-c001",
            "timestamp": "2026-02-21T11:00:00Z",
            "message": {"role": "user", "content": "Add tests for the auth module"},
            "cwd": "/home/testuser/projects/myapp",
            "parentUuid": None,
        },
        {
            "type": "assistant",
            "uuid": "uuid-c002",
            "timestamp": "2026-02-21T11:00:05Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll create test_auth.py with unit tests."},
                    {
                        "type": "tool_use",
                        "id": "toolu_0004",
                        "name": "Write",
                        "input": {
                            "file_path": "/home/testuser/projects/myapp/test_auth.py",
                            "content": "import pytest\nfrom auth import auth\n\ndef test_valid():\n    assert auth('admin', 'secret')\n",
                        },
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_0004",
                        "content": "File written.",
                    },
                    {
                        "type": "tool_use",
                        "id": "toolu_0005",
                        "name": "Read",
                        "input": {"file_path": "/home/testuser/projects/myapp/auth.py"},
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_0005",
                        "content": "def auth(u, p):\n    return check_password(u, p)\n",
                    },
                ],
            },
            "cwd": "/home/testuser/projects/myapp",
        },
    ],
    # Session D — contains a Task delegation (spawns a child agent)
    SESSION_D: [
        {
            "type": "user",
            "uuid": "uuid-d001",
            "timestamp": "2026-02-22T14:00:00Z",
            "message": {"role": "user", "content": "Refactor the database layer and run tests"},
            "cwd": "/home/testuser/projects/myapp",
            "parentUuid": None,
        },
        {
            "type": "assistant",
            "uuid": "uuid-d002",
            "timestamp": "2026-02-22T14:00:05Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll delegate the test run to a sub-agent."},
                    {
                        "type": "tool_use",
                        "id": "toolu_0006",
                        "name": "Task",
                        "input": {
                            "description": "Run test suite",
                            "prompt": "Run all tests and report results",
                            "subagent_type": "general-purpose",
                        },
                    },
                ],
            },
            "cwd": "/home/testuser/projects/myapp",
        },
        # Progress entry — this is where agentId lives
        {
            "type": "progress",
            "uuid": "uuid-d003",
            "timestamp": "2026-02-22T14:00:10Z",
            "parentToolUseID": "toolu_0006",
            "data": {"agentId": CHILD_AGENT},
        },
        {
            "type": "user",
            "uuid": "uuid-d004",
            "timestamp": "2026-02-22T14:00:30Z",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_0006",
                        "content": "All 5 tests passed.",
                    },
                ],
            },
            "cwd": "/home/testuser/projects/myapp",
        },
    ],
}

for sid, entries in sessions.items():
    p = PROJ_DIR / f"{sid}.jsonl"
    with open(p, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")

print(f"Seeded {len(sessions)} sessions in {PROJ_DIR}")
