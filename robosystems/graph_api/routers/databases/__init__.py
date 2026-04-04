"""
Database management routers.
"""

from . import (
  backup,
  management,
  memory,
  metrics,
  query,
  restore,
  schema,
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
  "swap",
  "tables",
  "vector_search",
]
