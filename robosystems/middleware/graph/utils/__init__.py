"""Multi-tenant graph utilities: database name resolution, graph identity
and routing, subgraph parsing, and identifier validation."""

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
  is_shared_repository,
  is_shared_repository_or_subgraph,
  validate_database_creation,
  validate_graph_id,
)

AccessPattern = ConnectionPattern


class MultiTenantUtils:
  """Utility class for multi-tenant database operations."""

  # Validation methods
  is_multitenant_mode = staticmethod(is_multitenant_mode)
  validate_graph_id = staticmethod(validate_graph_id)
  validate_database_creation = staticmethod(validate_database_creation)
  is_shared_repository = staticmethod(is_shared_repository)
  is_shared_repository_or_subgraph = staticmethod(is_shared_repository_or_subgraph)

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


__all__ = [
  "FULL_SUBGRAPH_PATTERN",
  "PARENT_GRAPH_PATTERN",
  "SUBGRAPH_NAME_PATTERN",
  "AccessPattern",
  "ConnectionPattern",
  "GraphIdentity",
  "GraphTypeRegistry",
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
  # Database functions
  "is_multitenant_mode",
  "is_parent_graph",
  "is_shared_repository",
  "is_shared_repository_or_subgraph",
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
  "validate_subgraph_name",
]
