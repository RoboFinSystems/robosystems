"""
FastAPI dependency functions for multi-tenant graph database resolution.

Simplified for graph databases-only architecture with multi-backend support.
"""

from .auth import (
  get_graph_database,
  get_graph_repository_with_auth,
  get_universal_repository_with_auth,
)
from .helpers import (
  optional_entity,
  optional_user_graph,
  require_entity,
  require_graph_category,
  require_user_graph,
)
from .repositories import (
  get_graph_repository_dependency,
  get_main_repository,
  get_sec_repository,
  get_shared_repository,
  get_user_graph_repository,
)

__all__ = [
  # Auth dependencies
  "get_graph_database",
  # Repository dependencies
  "get_graph_repository_dependency",
  "get_graph_repository_with_auth",
  "get_main_repository",
  "get_sec_repository",
  "get_shared_repository",
  "get_universal_repository_with_auth",
  "get_user_graph_repository",
  "optional_entity",
  "optional_user_graph",
  # Helper dependencies
  "require_entity",
  "require_graph_category",
  "require_user_graph",
]
