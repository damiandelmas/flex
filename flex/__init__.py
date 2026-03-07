"""
Flex — SQL-first agentic knowledge engine.

The AI writes SQL. The schema speaks for itself.

Domains:
  compile/     deterministic parsing, source → chunks
  manage/      offline graph intelligence → enrichment columns
  retrieve/    query execution: vec_ops, presets, direct SQL
  core.py      infrastructure: cell loading, SQL, view generation
  onnx/        embedding model (shared by compile and manage)
"""
