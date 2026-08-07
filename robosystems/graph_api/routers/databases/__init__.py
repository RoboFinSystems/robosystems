"""Per-database routers: lifecycle, query, schema, backup/restore, memory, swap."""

from . import (
  backup,
  management,
  memory,
  metrics,
  query,
  restore,
  schema,
  semantic_memory,
  swap,
  tables,
  vector_search,
)

__all__ = [
  "backup",
  "management",
  "memory",
  "metrics",
  "query",
  "restore",
  "schema",
  "semantic_memory",
  "swap",
  "tables",
  "vector_search",
]
