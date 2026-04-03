#!/usr/bin/env python3
"""
Soft file operation detection from Bash commands.

Parses Bash command text to infer file writes, edits, moves, etc.
Returns detected operations with confidence levels.
"""

import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

@dataclass
class SoftFileOp:
    file_path: str
    inferred_op: str  # write, edit, delete, move, copy, read
    confidence: str   # low, medium, high
    detection_method: str = "parsed"

def detect_file_ops(command: str, cwd: str = None) -> list[SoftFileOp]:
    """
    Detect file operations from a Bash command.

    Returns list of SoftFileOp with inferred operations.
    """
    ops = []

    # Normalize command
    cmd = command.strip()

    # Pattern: heredoc writes
    # cat << 'EOF' > /path/file
    # cat <<EOF > file
    heredoc = re.findall(r"cat\s+<<\s*['\"]?(\w+)['\"]?\s*>\s*([^\s;&|]+)", cmd)
    for _, filepath in heredoc:
        ops.append(SoftFileOp(
            file_path=_resolve_path(filepath, cwd),
            inferred_op="write",
            confidence="high"
        ))

    # Pattern: echo/printf redirect (overwrite) - but not >>
    # echo "..." > /path/file
    # printf "..." > file
    echo_write = re.findall(r"(?:echo|printf)\s+.*?(?<!>)>\s*([^\s;&|>]+)", cmd)
    for filepath in echo_write:
        if not filepath.startswith("&"):  # skip &1, &2
            ops.append(SoftFileOp(
                file_path=_resolve_path(filepath, cwd),
                inferred_op="write",
                confidence="medium"
            ))

    # Pattern: append redirect
    # echo "..." >> /path/file
    echo_append = re.findall(r">>\s*([^\s;&|]+)", cmd)
    for filepath in echo_append:
        ops.append(SoftFileOp(
            file_path=_resolve_path(filepath, cwd),
            inferred_op="edit",
            confidence="medium"
        ))

    # Pattern: sed in-place
    # sed -i 's/.../g' file
    # sed -i.bak 's/.../g' file
    sed_inplace = re.findall(r"sed\s+-i(?:\.\w+)?\s+['\"]?s.*?['\"]?\s+([^\s;&|]+)", cmd)
    for filepath in sed_inplace:
        ops.append(SoftFileOp(
            file_path=_resolve_path(filepath, cwd),
            inferred_op="edit",
            confidence="high"
        ))

    # Pattern: tee (write)
    # ... | tee /path/file
    # ... | tee -a /path/file (append)
    tee_write = re.findall(r"\|\s*tee\s+(?:-a\s+)?([^\s;&|]+)", cmd)
    for filepath in tee_write:
        is_append = "-a" in cmd[:cmd.find(filepath)]
        ops.append(SoftFileOp(
            file_path=_resolve_path(filepath, cwd),
            inferred_op="edit" if is_append else "write",
            confidence="high"
        ))

    # Pattern: cp (copy)
    # cp src dst
    # cp -r src dst
    cp_match = re.findall(r"cp\s+(?:-\w+\s+)*([^\s]+)\s+([^\s;&|]+)", cmd)
    for src, dst in cp_match:
        ops.append(SoftFileOp(
            file_path=_resolve_path(dst, cwd),
            inferred_op="copy",
            confidence="high"
        ))

    # Pattern: mv (move)
    # mv src dst
    mv_match = re.findall(r"mv\s+(?:-\w+\s+)*([^\s]+)\s+([^\s;&|]+)", cmd)
    for src, dst in mv_match:
        ops.append(SoftFileOp(
            file_path=_resolve_path(dst, cwd),
            inferred_op="move",
            confidence="high"
        ))
        # Also track source as "deleted" from original location
        ops.append(SoftFileOp(
            file_path=_resolve_path(src, cwd),
            inferred_op="delete",
            confidence="high"
        ))

    # Pattern: rm (delete)
    # rm file
    # rm -rf dir
    rm_match = re.findall(r"rm\s+(?:-\w+\s+)*([^\s;&|]+)", cmd)
    for filepath in rm_match:
        if not filepath.startswith("-"):
            ops.append(SoftFileOp(
                file_path=_resolve_path(filepath, cwd),
                inferred_op="delete",
                confidence="high"
            ))

    # Pattern: touch (create/update)
    # touch file
    touch_match = re.findall(r"touch\s+([^\s;&|]+)", cmd)
    for filepath in touch_match:
        ops.append(SoftFileOp(
            file_path=_resolve_path(filepath, cwd),
            inferred_op="write",
            confidence="medium"
        ))

    # Pattern: mkdir (create directory)
    # mkdir -p /path/dir
    mkdir_match = re.findall(r"mkdir\s+(?:-\w+\s+)*([^\s;&|]+)", cmd)
    for dirpath in mkdir_match:
        ops.append(SoftFileOp(
            file_path=_resolve_path(dirpath, cwd),
            inferred_op="write",
            confidence="high"
        ))

    # Pattern: python/node writing
    # python3 script.py > output.txt
    script_redirect = re.findall(r"(?:python3?|node|ruby)\s+[^\s]+\s+.*?>\s*([^\s;&|>]+)", cmd)
    for filepath in script_redirect:
        if not filepath.startswith("&"):
            ops.append(SoftFileOp(
                file_path=_resolve_path(filepath, cwd),
                inferred_op="write",
                confidence="low"
            ))

    # ===== READ OPERATIONS =====
    # Skip heredocs - they look like cat but are writes
    is_heredoc = bool(re.search(r"<<\s*['\"]?\w+['\"]?", cmd))

    if not is_heredoc:
        # Pattern: cat file (simple read)
        # cat /path/file
        # cat -n /path/file
        cat_read = re.findall(r"cat\s+(?:-[a-zA-Z]+\s+)*([/~][^\s|><&;]+)", cmd)
        for filepath in cat_read:
            if not filepath.startswith("-"):
                ops.append(SoftFileOp(
                    file_path=_resolve_path(filepath, cwd),
                    inferred_op="read",
                    confidence="high"
                ))

    # Pattern: head/tail (partial read)
    # head -30 /path/file
    # tail -f /path/file
    for read_cmd in ["head", "tail"]:
        pattern = rf"{read_cmd}\s+(?:-[a-zA-Z0-9]+\s+)*([/~][^\s|><&;]+)"
        for filepath in re.findall(pattern, cmd):
            if not filepath.startswith("-"):
                ops.append(SoftFileOp(
                    file_path=_resolve_path(filepath, cwd),
                    inferred_op="read",
                    confidence="high"
                ))

    # Pattern: less/more (pager read)
    for read_cmd in ["less", "more"]:
        pattern = rf"{read_cmd}\s+(?:-[a-zA-Z]+\s+)*([/~][^\s|><&;]+)"
        for filepath in re.findall(pattern, cmd):
            if not filepath.startswith("-"):
                ops.append(SoftFileOp(
                    file_path=_resolve_path(filepath, cwd),
                    inferred_op="read",
                    confidence="high"
                ))

    # Pattern: wc (count read)
    # wc -l /path/file
    wc_read = re.findall(r"wc\s+(?:-[a-zA-Z]+\s+)*([/~][^\s|><&;]+)", cmd)
    for filepath in wc_read:
        if not filepath.startswith("-"):
            ops.append(SoftFileOp(
                file_path=_resolve_path(filepath, cwd),
                inferred_op="read",
                confidence="high"
            ))

    # Pattern: file/stat (metadata read)
    for read_cmd in ["file", "stat"]:
        pattern = rf"{read_cmd}\s+(?:-[a-zA-Z]+\s+)*([/~][^\s|><&;]+)"
        for filepath in re.findall(pattern, cmd):
            if not filepath.startswith("-"):
                ops.append(SoftFileOp(
                    file_path=_resolve_path(filepath, cwd),
                    inferred_op="read",
                    confidence="medium"
                ))

    # Pattern: checksum commands
    # md5sum /path/file
    # sha256sum /path/file
    for hash_cmd in ["md5sum", "sha256sum", "sha1sum", "shasum"]:
        pattern = rf"{hash_cmd}\s+(?:-[a-zA-Z]+\s+)*([/~][^\s|><&;]+)"
        for filepath in re.findall(pattern, cmd):
            if not filepath.startswith("-"):
                ops.append(SoftFileOp(
                    file_path=_resolve_path(filepath, cwd),
                    inferred_op="read",
                    confidence="high"
                ))

    # Pattern: grep/rg with file path (search read)
    # grep "pattern" /path/file
    # grep -A5 "pattern" /path/file
    # rg "pattern" /path/file
    grep_pattern = r"(?:grep|rg)\s+(?:-[a-zA-Z0-9]+\s+)*(?:['\"][^'\"]+['\"]\s+|[^\s\-][^\s]*\s+)([/~][^\s|><&;]+)"
    for filepath in re.findall(grep_pattern, cmd):
        if not filepath.startswith("-"):
            ops.append(SoftFileOp(
                file_path=_resolve_path(filepath, cwd),
                inferred_op="read",
                confidence="high"
            ))

    # Deduplicate by file_path, keeping highest confidence
    seen = {}
    confidence_rank = {"high": 3, "medium": 2, "low": 1}
    for op in ops:
        key = (op.file_path, op.inferred_op)
        if key not in seen or confidence_rank[op.confidence] > confidence_rank[seen[key].confidence]:
            seen[key] = op

    return list(seen.values())

def _resolve_path(filepath: str, cwd: str = None) -> str:
    """Resolve relative paths to absolute."""
    if not filepath:
        return filepath

    # Already absolute
    if filepath.startswith("/"):
        return filepath

    # Home directory
    if filepath.startswith("~"):
        return str(Path(filepath).expanduser())

    # Relative - use cwd if available
    if cwd:
        return str(Path(cwd) / filepath)

    return filepath
