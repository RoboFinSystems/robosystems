"""
Graph identity utilities.

Functions for resolving graph identity, routing, and access patterns.
"""

from typing import Any

from robosystems.config import env
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType

from ..types import (
  AccessPattern as GraphAccessPattern,
)
from ..types import (
  ConnectionPattern,
  GraphIdentity,
  GraphTypeRegistry,
)
from .database import get_database_name
from .validation import is_shared_repository


def get_graph_identity(graph_id: str, session: Any | None = None) -> GraphIdentity:
  """
  Get complete graph identity including category and type.

  Args:
      graph_id: Graph identifier
      session: Optional database session for lookup

  Returns:
      GraphIdentity with full type information
  """
  return GraphTypeRegistry.identify_graph(graph_id, session=session)


def get_graph_routing(graph_id: str, session: Any | None = None) -> dict[str, Any]:
  """
  Get routing information for a graph based on its type.

  Args:
      graph_id: Graph identifier
      session: Optional database session for lookup

  Returns:
      Dict with routing configuration including cluster type, access mode, etc.
  """
  identity = get_graph_identity(graph_id, session=session)
  routing_info = identity.get_routing_info()

  routing_info["database_name"] = get_database_name(graph_id)
  routing_info["graph_identity"] = identity

  return routing_info


def validate_graph_access(
  graph_id: str,
  required_access: GraphAccessPattern,
  user_permissions: dict[str, Any] | None = None,
) -> bool:
  """
  Validate if the requested access pattern is allowed for this graph.

  Args:
      graph_id: Graph identifier
      required_access: Required access pattern
      user_permissions: Optional user permissions to check

  Returns:
      bool: True if access is allowed
  """
  identity = get_graph_identity(graph_id)
  _allowed_access = identity.get_access_pattern()

  if identity.is_shared_repository and required_access == GraphAccessPattern.READ_WRITE:
    logger.warning(f"Write access requested for shared repository {graph_id}, denying")
    return False

  if identity.is_system_graph and required_access != GraphAccessPattern.RESTRICTED:
    logger.warning(
      f"Non-restricted access requested for system graph {graph_id}, denying"
    )
    return False

  return True


def get_graph_cluster_type(graph_id: str) -> str:
  """
  Determine which cluster type should handle this graph.

  Args:
      graph_id: Graph identifier

  Returns:
      str: Cluster type ("user_writer", "shared_writer", etc.)
  """
  identity = get_graph_identity(graph_id)

  if identity.is_shared_repository:
    return "shared_writer"
  elif identity.is_user_graph:
    return "user_writer"
  else:
    return "system"


def is_user_graph(graph_id: str) -> bool:
  """
  Check if this is a user-created graph.

  Args:
      graph_id: Graph identifier

  Returns:
      bool: True if this is a user graph
  """
  identity = get_graph_identity(graph_id)
  return identity.is_user_graph


def get_repository_type_from_graph_id(graph_id: str):
  """
  Get the RepositoryType enum value from a graph_id.

  Args:
      graph_id: Graph identifier

  Returns:
      RepositoryType: The repository type enum value

  Raises:
      ValueError: If graph_id is not a known repository
  """
  from robosystems.models.iam import RepositoryType

  repository_mapping = {
    "sec": RepositoryType.SEC,
    "industry": RepositoryType.INDUSTRY,
    "economic": RepositoryType.ECONOMIC,
  }

  if graph_id not in repository_mapping:
    raise ValueError(f"Unknown repository graph_id: {graph_id}")

  return repository_mapping[graph_id]


def validate_repository_access(
  graph_id: str, user_id: str, operation_type: str = "read"
) -> bool:
  """
  Validate that a user has access to a shared repository.

  Args:
      graph_id: Repository identifier
      user_id: User ID to check
      operation_type: Type of operation (read, write, admin)

  Returns:
      bool: True if user has appropriate access
  """
  if not is_shared_repository(graph_id):
    return False

  from robosystems.database import session
  from robosystems.models.iam import (
    UserRepository,
  )
  from robosystems.models.iam import (
    UserRepositoryAccessLevel as RepositoryAccessLevel,
  )

  from .database import get_repository_database_name

  repository_name = get_repository_database_name(graph_id)
  access_level = UserRepository.get_user_access_level(
    user_id, repository_name, session()
  )

  has_access = False
  if operation_type == "read":
    has_access = access_level in [
      RepositoryAccessLevel.READ,
      RepositoryAccessLevel.WRITE,
      RepositoryAccessLevel.ADMIN,
    ]
  elif operation_type == "write":
    has_access = access_level in [
      RepositoryAccessLevel.WRITE,
      RepositoryAccessLevel.ADMIN,
    ]
  elif operation_type == "admin":
    has_access = access_level == RepositoryAccessLevel.ADMIN
  else:
    has_access = False

  if has_access:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(user_id),
      details={
        "action": "repository_access_granted",
        "repository": repository_name,
        "operation_type": operation_type,
        "access_level": access_level.value if access_level else None,
      },
      risk_level="low",
    )
  else:
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(user_id),
      resource=f"repository:{repository_name}",
      action=operation_type,
    )

  return has_access


def get_access_pattern() -> ConnectionPattern:
  """
  Get the preferred graph database access pattern.

  Returns:
      ConnectionPattern: The access pattern to use
  """
  pattern = env.LBUG_ACCESS_PATTERN.lower()
  try:
    return ConnectionPattern(pattern)
  except ValueError:
    logger.warning(f"Invalid LBUG_ACCESS_PATTERN: {pattern}, using api_auto")
    return ConnectionPattern.API_AUTO


def log_cluster_operation(
  operation: str, cluster_id: str, graph_id: str, **kwargs
) -> None:
  """
  Log cluster operation for monitoring and debugging.

  Args:
      operation: Operation description
      cluster_id: Cluster identifier
      graph_id: Graph identifier
      **kwargs: Additional context
  """
  context = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
  logger.info(
    f"Graph Cluster Operation: {operation} | "
    f"Cluster: {cluster_id} | Graph: {graph_id}"
    f"{' | ' + context if context else ''}"
  )


def get_migration_status() -> dict[str, Any]:
  """
  Get the current graph database migration status.

  Returns:
      Dict: Migration status information
  """
  from .database import get_max_databases_per_node

  return {
    "access_pattern": get_access_pattern().value,
    "max_databases_per_node": get_max_databases_per_node(),
    "shared_repositories": {
      "sec_engine": "ladybug",
    },
    "environment": env.ENVIRONMENT,
  }
