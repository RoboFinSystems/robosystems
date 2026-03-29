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
  "tables",
  "vector_search",
]
