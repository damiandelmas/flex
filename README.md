<p align="center">
  <img src="assets/banner.png" alt="flex" width="100%">
</p>

# flex

[![PyPI](https://img.shields.io/pypi/v/getflex)](https://pypi.org/project/getflex/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)

your claude code sessions are a knowledge base — you just can't search them yet.

install flex and immediately get an MCP tool for claude code to search and retrieve information from all of your conversations. index any local folder too with `flex index`.

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

## how it works

1. **index your data**

- `flex init` for claude code sessions
- `flex index ./docs` for local files

flex builds a SQLite database with your messages, embeddings, and structured views.

2. **query it**

- ask your agent via MCP
- or use CLI:

```bash
flex search --cell claude_code "@digest"
```

queries are just SQL or presets.

## local-first

your entire knowledge base is one file on your machine.

```bash
ls ~/.flex/cells/
claude_code.db    284M

sqlite3 claude_code.db "SELECT COUNT(*) FROM sessions"
4547
```

no servers to manage, no external services required. everything runs locally.

---

```bash
curl -sSL https://getflex.dev/install.sh | bash
```

MIT · Python 3.12 · SQLite

[getflex.dev](https://getflex.dev) · [x](https://x.com/damian_delmas)
