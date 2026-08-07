"""Graph database routing, allocation, admission control, and rate limiting."""

from robosystems.config.graph_tier import GraphTier
from robosystems.graph_api.core.ladybug import Engine, Repository

from .base import GraphEngineInterface, GraphOperation
from .repository import (
  UniversalRepository,
  create_universal_repository,
  create_universal_repository_with_auth,
  get_repository_type,
  is_api_repository,
  is_direct_repository,
)
from .router import (
  GraphRouter,
  get_graph_repository,
  get_graph_router,
  get_universal_repository,
)
from .types import (
  AccessPattern,
  ConnectionPattern,
  GraphCategory,
  NodeType,
  RepositoryType,
)

__all__ = [
  "AccessPattern",
  "ConnectionPattern",
  "Engine",
  "GraphCategory",
  # Base abstractions
  "GraphEngineInterface",
  "GraphOperation",
  "GraphRouter",
  "GraphTier",
  # Type definitions
  "NodeType",
  # Graph components
  "Repository",
  "RepositoryType",
  # Repository wrapper
  "UniversalRepository",
  "create_universal_repository",
  "create_universal_repository_with_auth",
  # Primary interface (recommended)
  "get_graph_repository",
  "get_graph_router",
  "get_repository_type",
  "get_universal_repository",
  "is_api_repository",
  "is_direct_repository",
]
