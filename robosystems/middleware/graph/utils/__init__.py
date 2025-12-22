"""
Multi-tenant utility functions for graph database operations.

This module provides utilities for handling multi-tenant database operations,
including database name resolution, validation, configuration management,
and cluster operations.
"""

from ..types import ConnectionPattern, GraphIdentity, GraphTypeRegistry
from .database import (
  ensure_database_with_schema,
  ensure_database_with_schema_sync,
  get_database_name,
  get_database_path_for_graph,
  get_max_databases_per_node,
  get_repository_database_name,
  is_multitenant_mode,
  list_shared_repositories,
  log_database_operation,
)
from .identity import (
  get_access_pattern,
  get_graph_cluster_type,
  get_graph_identity,
  get_graph_routing,
  get_migration_status,
  get_repository_type_from_graph_id,
  is_user_graph,
  log_cluster_operation,
  validate_graph_access,
  validate_repository_access,
)
from .subgraph import (
  FULL_SUBGRAPH_PATTERN,
  PARENT_GRAPH_PATTERN,
  SUBGRAPH_NAME_PATTERN,
  SubgraphInfo,
  SubgraphType,
  construct_subgraph_id,
  generate_unique_subgraph_name,
  is_parent_graph,
  is_subgraph,
  parse_subgraph_id,
  split_graph_hierarchy,
  validate_parent_graph_id,
  validate_subgraph_name,
)
from .validation import (
  get_sec_database_name,
  is_sec_database,
  is_shared_repository,
  validate_database_creation,
  validate_graph_id,
  validate_sec_access,
)

# Backward compatibility alias
AccessPattern = ConnectionPattern


class MultiTenantUtils:
  """Utility class for multi-tenant database operations."""

  SHARED_REPOSITORIES = GraphTypeRegistry.SHARED_REPOSITORIES

  # Validation methods
  is_multitenant_mode = staticmethod(is_multitenant_mode)
  validate_graph_id = staticmethod(validate_graph_id)
  validate_database_creation = staticmethod(validate_database_creation)
  is_sec_database = staticmethod(is_sec_database)
  get_sec_database_name = staticmethod(get_sec_database_name)
  validate_sec_access = staticmethod(validate_sec_access)
  is_shared_repository = staticmethod(is_shared_repository)

  # Database methods
  get_database_name = staticmethod(get_database_name)
  get_repository_database_name = staticmethod(get_repository_database_name)
  list_shared_repositories = staticmethod(list_shared_repositories)
  log_database_operation = staticmethod(log_database_operation)
  get_database_path_for_graph = staticmethod(get_database_path_for_graph)
  get_max_databases_per_node = staticmethod(get_max_databases_per_node)
  ensure_database_with_schema = staticmethod(ensure_database_with_schema)
  ensure_database_with_schema_sync = staticmethod(ensure_database_with_schema_sync)

  # Identity methods
  get_graph_identity = staticmethod(get_graph_identity)
  get_graph_routing = staticmethod(get_graph_routing)
  validate_graph_access = staticmethod(validate_graph_access)
  get_graph_cluster_type = staticmethod(get_graph_cluster_type)
  is_user_graph = staticmethod(is_user_graph)
  get_repository_type_from_graph_id = staticmethod(get_repository_type_from_graph_id)
  validate_repository_access = staticmethod(validate_repository_access)
  get_access_pattern = staticmethod(get_access_pattern)
  log_cluster_operation = staticmethod(log_cluster_operation)
  get_migration_status = staticmethod(get_migration_status)

  @staticmethod
  def check_database_limits() -> None:
    """
    Check if the current number of databases exceeds the configured limit.

    Currently implemented as a no-op. Database limits are enforced at the
    instance level based on LBUG_MAX_DATABASES_PER_NODE.
    """
    pass


__all__ = [
  "FULL_SUBGRAPH_PATTERN",
  "PARENT_GRAPH_PATTERN",
  "SUBGRAPH_NAME_PATTERN",
  # Backward compatibility
  "AccessPattern",
  "ConnectionPattern",
  # Types
  "GraphIdentity",
  "GraphTypeRegistry",
  # Main class
  "MultiTenantUtils",
  "SubgraphInfo",
  # Subgraph functions
  "SubgraphType",
  "construct_subgraph_id",
  "ensure_database_with_schema",
  "ensure_database_with_schema_sync",
  "generate_unique_subgraph_name",
  "get_access_pattern",
  "get_database_name",
  "get_database_path_for_graph",
  "get_graph_cluster_type",
  # Identity functions
  "get_graph_identity",
  "get_graph_routing",
  "get_max_databases_per_node",
  "get_migration_status",
  "get_repository_database_name",
  "get_repository_type_from_graph_id",
  "get_sec_database_name",
  # Database functions
  "is_multitenant_mode",
  "is_parent_graph",
  "is_sec_database",
  "is_shared_repository",
  "is_subgraph",
  "is_user_graph",
  "list_shared_repositories",
  "log_cluster_operation",
  "log_database_operation",
  "parse_subgraph_id",
  "split_graph_hierarchy",
  "validate_database_creation",
  "validate_graph_access",
  # Validation functions
  "validate_graph_id",
  "validate_parent_graph_id",
  "validate_repository_access",
  "validate_sec_access",
  "validate_subgraph_name",
]
