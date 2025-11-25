"""
FastAPI dependency functions for multi-tenant graph database resolution.

Simplified for graph databases-only architecture with multi-backend support.
"""

from .auth import (
  get_graph_database,
  get_graph_repository_with_auth,
  get_universal_repository_with_auth,
)
from .repositories import (
  get_graph_repository_dependency,
  get_user_graph_repository,
  get_shared_repository,
  get_main_repository,
  get_sec_repository,
)
from .helpers import (
  require_entity,
  optional_entity,
  require_user_graph,
  optional_user_graph,
  require_graph_category,
)

__all__ = [
  # Auth dependencies
  "get_graph_database",
  "get_graph_repository_with_auth",
  "get_universal_repository_with_auth",
  # Repository dependencies
  "get_graph_repository_dependency",
  "get_user_graph_repository",
  "get_shared_repository",
  "get_main_repository",
  "get_sec_repository",
  # Helper dependencies
  "require_entity",
  "optional_entity",
  "require_user_graph",
  "optional_user_graph",
  "require_graph_category",
]
