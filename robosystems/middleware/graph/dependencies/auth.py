"""
Graph access authorization dependencies.

FastAPI dependency functions for validating user access to graphs.
"""


from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.graph_api.core.ladybug import Repository
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.models.iam import User
from robosystems.models.iam.graph_user import GraphUser
from robosystems.models.iam.user_repository import (
  RepositoryAccessLevel,
  UserRepository,
)
from robosystems.security import SecurityAuditLogger, SecurityEventType

from ..router import get_graph_repository
from ..types import AccessPattern


async def get_graph_database(
  graph_id: str,
  current_user: User = Depends(get_current_user),
  required_access: AccessPattern | None = None,
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
    routing_info = MultiTenantUtils.get_graph_routing(graph_id, session=db)
    identity = routing_info["graph_identity"]

    if required_access:
      if not MultiTenantUtils.validate_graph_access(graph_id, required_access):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied: {required_access.value} not allowed for {identity.category.value} graph",
        )

    if identity.is_shared_repository:
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

      if required_access:
        required_level_map = {
          AccessPattern.READ_ONLY: RepositoryAccessLevel.READ,
          AccessPattern.READ_WRITE: RepositoryAccessLevel.WRITE,
        }

        required_level = required_level_map.get(
          required_access, RepositoryAccessLevel.READ
        )

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
      if not GraphUser.user_has_access(str(current_user.id), graph_id, db):
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

      if required_access == AccessPattern.READ_WRITE:
        if not GraphUser.user_has_admin_access(str(current_user.id), graph_id, db):
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
    identity = MultiTenantUtils.get_graph_identity(graph_id)

    if identity.is_shared_repository:
      if not UserRepository.user_has_access(str(current_user.id), graph_id, db):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to shared repository {graph_id}",
        )

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
      if not GraphUser.user_has_access(str(current_user.id), graph_id, db):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to user graph {graph_id}",
        )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
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

  except HTTPException as exc:
    raise exc
  except Exception as e:
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"graph_database:{graph_id}",
      action=operation_type,
    )

    logger.exception(f"Error creating repository for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to access graph {graph_id}",
    )


async def get_universal_repository_with_auth(
  graph_id: str,
  current_user: User,
  operation_type: str = "write",
  db: Session = Depends(get_db_session),
):
  """
  Get a universal repository wrapper with user authorization.

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
    from ..repository import UniversalRepository

    repository = await get_graph_repository_with_auth(
      graph_id=graph_id,
      current_user=current_user,
      operation_type=operation_type,
      db=db,
    )

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

  except HTTPException as exc:
    raise exc
  except Exception as e:
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
