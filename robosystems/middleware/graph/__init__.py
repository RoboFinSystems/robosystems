"""
Graph middleware for graph database operations.

Simplified architecture for graph databases-only graph database access.
"""

# Graph middleware components
from .router import (
  get_graph_repository,
  get_universal_repository,
  get_graph_router,
  GraphRouter,
)

# Repository wrapper
from .repository import (
  UniversalRepository,
  create_universal_repository,
  create_universal_repository_with_auth,
  is_api_repository,
  is_direct_repository,
  get_repository_type,
)

# Type definitions (canonical source)
from .types import (
  NodeType,
  RepositoryType,
  ConnectionPattern,
  AccessPattern,
  GraphCategory,
)
from robosystems.config.graph_tier import GraphTier

# Base abstractions
from .base import GraphEngineInterface, GraphOperation

# Graph database implementations (no circular dependency now)
from robosystems.graph_api.core.ladybug import Repository, Engine

__all__ = [
  # Primary interface (recommended)
  "get_graph_repository",
  "get_universal_repository",
  "get_graph_router",
  "GraphRouter",
  # Graph components
  "Repository",
  "Engine",
  # Repository wrapper
  "UniversalRepository",
  "create_universal_repository",
  "create_universal_repository_with_auth",
  "is_api_repository",
  "is_direct_repository",
  "get_repository_type",
  # Type definitions
  "NodeType",
  "RepositoryType",
  "ConnectionPattern",
  "AccessPattern",
  "GraphCategory",
  "GraphTier",
  # Base abstractions
  "GraphEngineInterface",
  "GraphOperation",
]
