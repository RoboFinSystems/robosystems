"""Graph identity utilities.

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
  """Get complete graph identity including category and type."""
  return GraphTypeRegistry.identify_graph(graph_id, session=session)


def get_graph_routing(graph_id: str, session: Any | None = None) -> dict[str, Any]:
  """Get routing information for a graph based on its type."""
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
  """Validate if the requested access pattern is allowed for this graph."""
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
  """Determine which cluster type should handle this graph."""
  identity = get_graph_identity(graph_id)

  if identity.is_shared_repository:
    return "shared_writer"
  elif identity.is_user_graph:
    return "user_writer"
  else:
    return "system"


def is_user_graph(graph_id: str) -> bool:
  """Check if this is a user-created graph."""
  identity = get_graph_identity(graph_id)
  return identity.is_user_graph


def get_repository_type_from_graph_id(graph_id: str) -> str:
  """Validate that graph_id is a known shared repository and return it."""
  if not is_shared_repository(graph_id):
    raise ValueError(f"Unknown repository graph_id: {graph_id}")

  return graph_id


def validate_repository_access(
  graph_id: str, user_id: str, operation_type: str = "read"
) -> bool:
  """Validate that a user has access to a shared repository.

  For subgraphs (e.g., "sec_historical"), access is checked against the
  parent repository ("sec") since subgraphs inherit parent permissions.
  """
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph,
    resolve_shared_repository_parent,
  )

  if not is_shared_repository_or_subgraph(graph_id):
    return False
  from robosystems.database import SessionFactory
  from robosystems.models.core import (
    UserRepository,
  )
  from robosystems.models.core import (
    UserRepositoryAccessLevel as RepositoryAccessLevel,
  )

  from .database import get_repository_database_name

  # Resolve subgraph to parent for permission check
  parent_repo_id = resolve_shared_repository_parent(graph_id)
  repository_name = get_repository_database_name(parent_repo_id)

  # Use a short-lived session instead of the scoped session proxy.
  # The scoped session is tied to the request lifecycle via
  # DatabaseSessionMiddleware, which holds connections until the entire
  # request completes.  For MCP endpoints that run for minutes, this
  # exhausts the connection pool.
  _sess = SessionFactory()
  try:
    access_level = UserRepository.get_user_access_level(user_id, repository_name, _sess)
  finally:
    _sess.close()

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
  """Get the preferred graph database access pattern."""
  pattern = env.LBUG_ACCESS_PATTERN.lower()
  try:
    return ConnectionPattern(pattern)
  except ValueError:
    logger.warning(f"Invalid LBUG_ACCESS_PATTERN: {pattern}, using api_auto")
    return ConnectionPattern.API_AUTO


def log_cluster_operation(
  operation: str, cluster_id: str, graph_id: str, **kwargs
) -> None:
  """Log cluster operation for monitoring and debugging."""
  context = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
  logger.info(
    f"Graph Cluster Operation: {operation} | "
    f"Cluster: {cluster_id} | Graph: {graph_id}"
    f"{' | ' + context if context else ''}"
  )


def get_migration_status() -> dict[str, Any]:
  """Get the current graph database migration status."""
  from robosystems.config.shared_repositories import get_all_repository_ids

  from .database import get_max_databases_per_node

  return {
    "access_pattern": get_access_pattern().value,
    "max_databases_per_node": get_max_databases_per_node(),
    "shared_repositories": {
      repo_id: {"engine": "ladybug"} for repo_id in get_all_repository_ids()
    },
    "environment": env.ENVIRONMENT,
  }
