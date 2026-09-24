"""FastAPI dependency functions for multi-tenant graph database resolution."""

from .helpers import (
  optional_entity,
  optional_user_graph,
  require_entity,
  require_graph_category,
  require_user_graph,
)

__all__ = [
  "optional_entity",
  "optional_user_graph",
  "require_entity",
  "require_graph_category",
  "require_user_graph",
]
