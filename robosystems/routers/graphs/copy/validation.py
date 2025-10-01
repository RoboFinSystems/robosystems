"""
Validation utilities for copy operations.

This module provides permission checking, tier limits, and other
validation logic for copy operations.
"""

from typing import Dict, Optional
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, UserGraph
from robosystems.models.iam.user_repository import UserRepository, RepositoryAccessLevel
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.logger import logger


async def validate_copy_permissions(
  graph_id: str,
  current_user: User,
  session: Session,
) -> None:
  """
  Validate user has write permissions to the graph.

  Args:
      graph_id: Target graph identifier
      current_user: Current authenticated user
      session: Database session

  Raises:
      HTTPException: If user lacks write permissions
  """

  # For shared repositories, users need explicit write permission
  shared_repos = ["sec", "industry", "economic"]
  if graph_id.lower() in shared_repos:
    # Check shared repository access
    access_level = UserRepository.get_user_access_level(
      str(current_user.id), graph_id, session
    )

    # Need at least WRITE level for copy operations
    if access_level not in [RepositoryAccessLevel.WRITE, RepositoryAccessLevel.ADMIN]:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "user_id": current_user.id,
          "graph_id": graph_id,
          "action": "copy_write_denied",
          "repository_type": "shared",
        },
        risk_level="medium",
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Write access denied for shared repository: {graph_id}",
      )
  else:
    # For user graphs, check ownership
    # Check if this is the user's own graph
    user_graph = (
      session.query(UserGraph)
      .filter(UserGraph.graph_id == graph_id, UserGraph.user_id == current_user.id)
      .first()
    )

    if not user_graph:
      # Also check if user has delegated access
      # This could be through company membership or explicit sharing
      # For now, we'll just deny if not owner
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "user_id": current_user.id,
          "graph_id": graph_id,
          "action": "copy_write_denied",
          "repository_type": "user",
        },
        risk_level="medium",
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Access denied for graph: {graph_id}",
      )

  logger.info(f"User {current_user.id} authorized for copy to {graph_id}")


def get_tier_limits(user: User) -> Dict[str, any]:
  """
  Get tier-based limits for copy operations.

  Args:
      user: User object with subscription tier

  Returns:
      Dictionary of limits based on user's tier
  """

  tier = getattr(user, "subscription_tier", "standard")

  limits = {
    "standard": {
      "max_file_size_gb": 10,
      "timeout_seconds": 900,  # 15 minutes
      "concurrent_operations": 1,
      "max_files_per_operation": 100,
      "daily_copy_operations": 50,
    },
    "enterprise": {
      "max_file_size_gb": 50,
      "timeout_seconds": 1800,  # 30 minutes
      "concurrent_operations": 3,
      "max_files_per_operation": 500,
      "daily_copy_operations": 500,
    },
    "premium": {
      "max_file_size_gb": 100,
      "timeout_seconds": 3600,  # 60 minutes
      "concurrent_operations": 5,
      "max_files_per_operation": 1000,
      "daily_copy_operations": -1,  # Unlimited
    },
  }

  return limits.get(tier, limits["standard"])


def validate_size_limits(
  max_file_size_gb: Optional[int], tier_limits: Dict[str, any], user_id: str
) -> None:
  """
  Validate file size against tier limits.

  Args:
      max_file_size_gb: Requested maximum file size
      tier_limits: User's tier limits
      user_id: User ID for logging

  Raises:
      HTTPException: If size exceeds tier limits
  """

  if max_file_size_gb:
    max_allowed = tier_limits["max_file_size_gb"]
    if max_file_size_gb > max_allowed:
      logger.warning(
        f"User {user_id} requested {max_file_size_gb}GB "
        f"but tier limit is {max_allowed}GB"
      )
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"File size limit {max_file_size_gb}GB exceeds tier limit {max_allowed}GB",
      )


def calculate_timeout(
  tier_limits: Dict[str, any], extended_timeout: bool = False
) -> int:
  """
  Calculate timeout based on tier and request.

  Args:
      tier_limits: User's tier limits
      extended_timeout: Whether extended timeout was requested

  Returns:
      Timeout in seconds
  """

  base_timeout = tier_limits["timeout_seconds"]

  if extended_timeout:
    # Double timeout but cap at 2 hours
    return min(base_timeout * 2, 7200)

  return base_timeout
