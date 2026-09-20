# Pi session cell

This cell is a source-faithful projection of Pi's append-only JSONL session
trees. Begin with `@orient`, then use the shared coding-agent presets for
sessions, messages, tools, files, and failures.

Use `_types_pi_session` for Pi header, leaf, branch, model, thinking, usage, and
title metadata. Use `_types_pi_entry` when exact tree lineage or native entry
payloads matter. `is_active_branch = 1` marks the path Pi would reconstruct for
its current leaf; entries off that path remain queryable rather than being
discarded.
