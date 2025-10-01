"""
Graph middleware for Kuzu database operations.

Simplified architecture for Kuzu-only graph database access.
"""

# Graph middleware components
from .router import (
  get_graph_repository,
  get_universal_repository,
  get_graph_router,
  GraphRouter,
)

from .engine import Repository, Engine

# Repository wrapper
from .repository import (
  UniversalRepository,
  create_universal_repository,
  create_universal_repository_with_auth,
  is_api_repository,
  is_direct_repository,
  get_repository_type,
)

# Cluster management
from .clusters import (
  ClusterConfig,
  RepositoryType,
  get_cluster_for_entity_graphs,
  get_cluster_for_shared_repository,
)


# Base abstractions
from .base import GraphEngineInterface, GraphOperation

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
  # Cluster management
  "ClusterConfig",
  "RepositoryType",
  "get_cluster_for_entity_graphs",
  "get_cluster_for_shared_repository",
  # Base abstractions
  "GraphEngineInterface",
  "GraphOperation",
]
