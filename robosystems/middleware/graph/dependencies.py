"""
FastAPI dependency functions for multi-tenant Kuzu graph database resolution.

Simplified for graph databases-only architecture.
"""

from typing import Optional

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user,
  get_current_user_with_graph,
)
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.models.iam import User
from robosystems.models.iam.user_graph import UserGraph
from robosystems.models.iam.user_repository import (
  RepositoryAccessLevel,
  UserRepository,
)
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .engine import Repository
from .router import get_graph_repository
from .types import AccessPattern


async def get_graph_database(
  graph_id: str,
  current_user: User = Depends(get_current_user),
  required_access: Optional[AccessPattern] = None,
  db: Session = Depends(get_db_session),
) -> str:
  """
  Resolve the database name for any graph type.

  Args:
      graph_id: Graph identifier
      current_user: Authenticated user
      required_access: Optional required access pattern

  Returns:
      Database name for the graph

  Raises:
      HTTPException: If graph not found or user doesn't have access
  """
  try:
    # Get graph identity and routing info
    routing_info = MultiTenantUtils.get_graph_routing(graph_id)
    identity = routing_info["graph_identity"]

    # Validate access if required
    if required_access:
      if not MultiTenantUtils.validate_graph_access(graph_id, required_access):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied: {required_access.value} not allowed for {identity.category.value} graph",
        )

    # Check user permissions based on graph type
    if identity.is_shared_repository:
      # Check shared repository access
      access_level = UserRepository.get_user_access_level(
        str(current_user.id), graph_id, db
      )

      if access_level == RepositoryAccessLevel.NONE:
        logger.warning(
          f"User {current_user.id} attempted access to shared repository {graph_id} without permission"
        )
        SecurityAuditLogger.log_authorization_denied(
          user_id=str(current_user.id),
          resource=f"shared_repository:{graph_id}",
          action=required_access.value if required_access else "access",
        )
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to shared repository {graph_id}",
        )

      # Check if required access level is satisfied
      if required_access:
        required_level_map = {
          AccessPattern.READ_ONLY: RepositoryAccessLevel.READ,
          AccessPattern.READ_WRITE: RepositoryAccessLevel.WRITE,
        }

        required_level = required_level_map.get(
          required_access, RepositoryAccessLevel.READ
        )

        # Check if user has sufficient access level
        level_hierarchy = [
          RepositoryAccessLevel.NONE,
          RepositoryAccessLevel.READ,
          RepositoryAccessLevel.WRITE,
          RepositoryAccessLevel.ADMIN,
        ]

        if level_hierarchy.index(access_level) < level_hierarchy.index(required_level):
          raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Insufficient access level for {graph_id}. Required: {required_level.value}, have: {access_level.value}",
          )

    elif identity.is_user_graph:
      # Check user graph access via UserGraph table
      if not UserGraph.user_has_access(str(current_user.id), graph_id, db):
        logger.warning(
          f"User {current_user.id} attempted access to user graph {graph_id} without permission"
        )
        SecurityAuditLogger.log_authorization_denied(
          user_id=str(current_user.id),
          resource=f"user_graph:{graph_id}",
          action=required_access.value if required_access else "access",
        )
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to user graph {graph_id}",
        )

      # Check admin access if required
      if required_access == AccessPattern.READ_WRITE:
        if not UserGraph.user_has_admin_access(str(current_user.id), graph_id, db):
          raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Admin access required for graph {graph_id}",
          )

    database_name = routing_info["database_name"]
    logger.debug(f"Resolved database for graph {graph_id}: {database_name}")
    return database_name

  except Exception as e:
    logger.error(f"Error resolving graph database for {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found or access denied",
    )


async def get_graph_repository_dependency(
  graph_id: str,
  current_user: User = Depends(get_current_user),
  operation_type: str = Query("read", description="Operation type: read or write"),
) -> Repository:
  """
  Get a repository for any graph type with proper routing.

  Args:
      graph_id: Graph identifier
      current_user: Authenticated user
      operation_type: Operation type (read/write)

  Returns:
      Configured Repository instance

  Raises:
      HTTPException: If graph not found or user doesn't have access
  """
  try:
    # Get graph routing info
    routing_info = MultiTenantUtils.get_graph_routing(graph_id)
    identity = routing_info["graph_identity"]

    # Validate operation type
    if operation_type == "write" and identity.is_shared_repository:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Write operations not allowed on shared repository {graph_id}",
      )

    # Get repository with proper routing
    repository = await get_graph_repository(graph_id, operation_type=operation_type)

    logger.debug(
      f"Created {identity.category.value} repository for graph {graph_id} "
      f"(operation: {operation_type})"
    )
    return repository

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error creating repository for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to access graph {graph_id}",
    )


# ============================================================================
# NEW TYPE-AWARE DEPENDENCY FUNCTIONS
# ============================================================================


async def get_user_graph_repository(
  graph_id: str,
  current_user: User = Depends(get_current_user),
  operation_type: str = Query("write", description="Operation type: read or write"),
  db: Session = Depends(get_db_session),
) -> Repository:
  """
  Get a repository specifically for user-created graphs.

  Validates that:
  1. The graph is a user graph (not shared repository)
  2. The user has access via UserGraph table
  3. The requested operation is allowed

  Args:
      graph_id: User graph identifier
      current_user: Authenticated user
      operation_type: Operation type (read/write)

  Returns:
      Repository instance for the user graph

  Raises:
      HTTPException: If not a user graph or access denied
  """
  # Verify this is a user graph
  identity = MultiTenantUtils.get_graph_identity(graph_id)
  if not identity.is_user_graph:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Graph {graph_id} is not a user graph",
    )

  # Check UserGraph table for permissions
  if not UserGraph.user_has_access(str(current_user.id), graph_id, db):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"user_graph:{graph_id}",
      action=operation_type,
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to user graph {graph_id}",
    )

  # Check admin access for write operations
  if operation_type == "write":
    user_graph = UserGraph.get_by_user_and_graph(str(current_user.id), graph_id, db)
    if user_graph and user_graph.role not in ["admin", "member"]:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Write access denied. Your role: {user_graph.role}",
      )

  return await get_graph_repository_dependency(graph_id, current_user, operation_type)


async def get_shared_repository(
  repository_name: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
) -> Repository:
  """
  Get a repository for a shared data repository.

  Validates that:
  1. The repository is a known shared repository
  2. The user has read access permissions
  3. Returns read-only repository

  Args:
      repository_name: Shared repository name (sec, industry, etc.)
      current_user: Authenticated user

  Returns:
      Read-only repository instance

  Raises:
      HTTPException: If not a valid repository or access denied
  """
  # Verify this is a shared repository
  if not MultiTenantUtils.is_shared_repository(repository_name):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"{repository_name} is not a valid shared repository",
    )

  # Check shared repository access permissions
  if not UserRepository.user_has_access(str(current_user.id), repository_name, db):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"shared_repository:{repository_name}",
      action="read",
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to shared repository {repository_name}. Please subscribe to access this data.",
    )

  # Always read-only for shared repositories
  return await get_graph_repository_dependency(repository_name, current_user, "read")


async def get_main_repository(
  current_user: User = Depends(get_current_user),
) -> Repository:
  """
  Get a repository for the main/default database.

  Args:
      current_user: Authenticated user

  Returns:
      Configured Repository instance for main database
  """
  try:
    # Use "default" as the main database identifier
    repository = await get_graph_repository("default", operation_type="write")

    logger.debug("Created main Kuzu repository")
    return repository

  except Exception as e:
    logger.error(f"Error creating main repository: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to access main database",
    )


async def get_graph_repository_with_auth(
  graph_id: str,
  current_user: User,
  operation_type: str = "write",
  db: Session = Depends(get_db_session),
) -> Repository:
  """
  Get a graph repository with user authorization.

  Args:
      graph_id: Graph/database identifier
      current_user: Authenticated user
      operation_type: "read" or "write"

  Returns:
      Configured Repository instance

  Raises:
      HTTPException: If graph not found or user doesn't have access
  """
  try:
    # Implement proper authorization logic
    identity = MultiTenantUtils.get_graph_identity(graph_id)

    # Check permissions based on graph type
    if identity.is_shared_repository:
      # Check shared repository access
      if not UserRepository.user_has_access(str(current_user.id), graph_id, db):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to shared repository {graph_id}",
        )

      # Write operations not allowed on shared repositories
      if operation_type == "write":
        access_level = UserRepository.get_user_access_level(
          str(current_user.id), graph_id, db
        )
        if access_level not in [
          RepositoryAccessLevel.WRITE,
          RepositoryAccessLevel.ADMIN,
        ]:
          raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Write access denied to shared repository {graph_id}. Your access level: {access_level.value}",
          )

    elif identity.is_user_graph:
      # Check user graph access
      if not UserGraph.user_has_access(str(current_user.id), graph_id, db):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to user graph {graph_id}",
        )

    # Log the successful graph access attempt
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,  # Could add GRAPH_ACCESS_GRANTED
      user_id=str(current_user.id),
      details={
        "action": "graph_repository_access",
        "graph_id": graph_id,
        "operation_type": operation_type,
        "user_email": current_user.email,
        "graph_type": identity.category.value,
      },
      risk_level="low",
    )

    repository = await get_graph_repository(graph_id, operation_type=operation_type)

    logger.debug(
      f"Created repository for graph {graph_id} with {operation_type} access"
    )
    return repository

  except Exception as e:
    # Log failed graph access attempt
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"graph_database:{graph_id}",
      action=operation_type,
    )

    logger.error(f"Error creating repository for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to access graph {graph_id}",
    )


def require_entity(
  current_user: User = Depends(get_current_user_with_graph),
) -> str:
  """
  Require that the user has a selected entity.

  Args:
      current_user: Authenticated user with graph info

  Returns:
      Entity identifier

  Raises:
      HTTPException: If no entity is selected
  """
  selected_entity = getattr(current_user, "selected_entity", None)
  if not selected_entity:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No entity selected. Please select a entity first.",
    )

  return selected_entity


def optional_entity(
  current_user: User = Depends(get_current_user_with_graph),
) -> Optional[str]:
  """
  Optionally get the user's selected entity.

  Args:
      current_user: Authenticated user with graph info

  Returns:
      Entity identifier if selected, None otherwise
  """
  return getattr(current_user, "selected_entity", None)


async def get_sec_repository() -> Repository:
  """
  Get a repository for the shared SEC database.

  Returns:
      Configured Repository instance for SEC data
  """
  try:
    # SEC database is a shared repository
    repository = await get_graph_repository("sec", operation_type="read")

    logger.debug("Created SEC Kuzu repository")
    return repository

  except Exception as e:
    logger.error(f"Error creating SEC repository: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to access SEC database",
    )


# Note: get_shared_repository is defined above in the new type-aware section


async def get_universal_repository_with_auth(
  graph_id: str,
  current_user: User,
  operation_type: str = "write",
  db: Session = Depends(get_db_session),
):
  """
  Get a universal repository wrapper with user authorization.

  This provides a unified interface that handles both sync and async repositories
  automatically, eliminating the need for conditional awaiting in application code.

  Args:
      graph_id: Graph/database identifier
      current_user: Authenticated user
      operation_type: "read" or "write"

  Returns:
      UniversalRepository instance

  Raises:
      HTTPException: If graph not found or user doesn't have access
  """
  try:
    from .repository import UniversalRepository

    # Implement proper authorization logic using the existing function
    repository = await get_graph_repository_with_auth(
      graph_id=graph_id,
      current_user=current_user,
      operation_type=operation_type,
      db=db,
    )

    # Log the universal repository access
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(current_user.id),
      details={
        "action": "universal_repository_access",
        "graph_id": graph_id,
        "operation_type": operation_type,
        "user_email": current_user.email,
      },
      risk_level="low",
    )

    universal_repo = UniversalRepository(repository)

    logger.debug(
      f"Created universal repository for graph {graph_id} with {operation_type} access"
    )
    return universal_repo

  except Exception as e:
    # Log failed graph access attempt
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"graph_database:{graph_id}",
      action=operation_type,
    )

    logger.error(f"Error creating universal repository for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to access graph {graph_id}",
    )


# ============================================================================
# HELPER FUNCTIONS FOR COMMON PATTERNS
# ============================================================================


def require_user_graph(
  current_user: User = Depends(get_current_user_with_graph),
) -> str:
  """
  Require that the user has a selected user graph (not shared repository).

  Args:
      current_user: Authenticated user with graph info

  Returns:
      User graph identifier

  Raises:
      HTTPException: If no graph selected or selected graph is not a user graph
  """
  graph_id = getattr(current_user, "selected_graph", None)
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No graph selected. Please select a graph first.",
    )

  identity = MultiTenantUtils.get_graph_identity(graph_id)
  if not identity.is_user_graph:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Selected graph {graph_id} is not a user graph",
    )

  return graph_id


def optional_user_graph(
  current_user: User = Depends(get_current_user_with_graph),
) -> Optional[str]:
  """
  Optionally get the user's selected graph if it's a user graph.

  Args:
      current_user: Authenticated user with graph info

  Returns:
      User graph identifier if selected and is a user graph, None otherwise
  """
  graph_id = getattr(current_user, "selected_graph", None)
  if not graph_id:
    return None

  identity = MultiTenantUtils.get_graph_identity(graph_id)
  return graph_id if identity.is_user_graph else None


def require_graph_category(
  category: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> str:
  """
  Require that the user has selected a graph of a specific category.

  Args:
      category: Required graph category (user, shared, system)
      current_user: Authenticated user with graph info

  Returns:
      Graph identifier

  Raises:
      HTTPException: If no graph selected or wrong category
  """
  graph_id = getattr(current_user, "selected_graph", None)
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No graph selected. Please select a graph first.",
    )

  identity = MultiTenantUtils.get_graph_identity(graph_id)
  if identity.category.value != category:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Selected graph must be of category '{category}', but got '{identity.category.value}'",
    )

  return graph_id
