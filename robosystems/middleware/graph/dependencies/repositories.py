"""
Repository factory dependencies.

FastAPI dependency functions for creating graph repositories.
"""

from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.models.iam import User
from robosystems.models.iam.graph_user import GraphUser
from robosystems.models.iam.user_repository import UserRepository
from robosystems.security import SecurityAuditLogger

if TYPE_CHECKING:
  from robosystems.graph_api.core.ladybug import Repository

from ..router import get_graph_repository


async def get_graph_repository_dependency(
  graph_id: str,
  current_user: User = Depends(get_current_user),
  operation_type: str = Query("read", description="Operation type: read or write"),
) -> "Repository":
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
    routing_info = MultiTenantUtils.get_graph_routing(graph_id)
    identity = routing_info["graph_identity"]

    if operation_type == "write" and identity.is_shared_repository:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Write operations not allowed on shared repository {graph_id}",
      )

    repository = await get_graph_repository(graph_id, operation_type=operation_type)

    logger.debug(
      f"Created {identity.category.value} repository for graph {graph_id} "
      f"(operation: {operation_type})"
    )
    return repository

  except HTTPException:
    raise
  except Exception as e:
    logger.exception(f"Error creating repository for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to access graph {graph_id}",
    )


async def get_user_graph_repository(
  graph_id: str,
  current_user: User = Depends(get_current_user),
  operation_type: str = Query("write", description="Operation type: read or write"),
  db: Session = Depends(get_db_session),
) -> "Repository":
  """
  Get a repository specifically for user-created graphs.

  Validates that:
  1. The graph is a user graph (not shared repository)
  2. The user has access via GraphUser table
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
  identity = MultiTenantUtils.get_graph_identity(graph_id)
  if not identity.is_user_graph:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Graph {graph_id} is not a user graph",
    )

  if not GraphUser.user_has_access(str(current_user.id), graph_id, db):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"user_graph:{graph_id}",
      action=operation_type,
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to user graph {graph_id}",
    )

  if operation_type == "write":
    user_graph = GraphUser.get_by_user_and_graph(str(current_user.id), graph_id, db)
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
) -> "Repository":
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
  if not MultiTenantUtils.is_shared_repository(repository_name):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"{repository_name} is not a valid shared repository",
    )

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

  return await get_graph_repository_dependency(repository_name, current_user, "read")


async def get_main_repository(
  current_user: User = Depends(get_current_user),
) -> "Repository":
  """
  Get a repository for the main/default database.

  Args:
      current_user: Authenticated user

  Returns:
      Configured Repository instance for main database
  """
  try:
    repository = await get_graph_repository("default", operation_type="write")

    logger.debug("Created main graph repository")
    return repository

  except Exception as e:
    logger.error(f"Error creating main repository: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to access main database",
    )


async def get_sec_repository() -> "Repository":
  """
  Get a repository for the shared SEC database.

  Returns:
      Configured Repository instance for SEC data
  """
  try:
    repository = await get_graph_repository("sec", operation_type="read")

    logger.debug("Created SEC graph repository")
    return repository

  except Exception as e:
    logger.error(f"Error creating SEC repository: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to access SEC database",
    )
