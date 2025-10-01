"""User limits service for centralized limit checking and management."""

import logging
from datetime import datetime, timezone
from typing import Tuple, Optional
from sqlalchemy.orm import Session

from ..models.iam import UserLimits
from ..models.iam.user_usage_tracking import UserUsageTracking, UsageType

logger = logging.getLogger(__name__)


class UserLimitsService:
  """Service class for user limits and subscription management."""

  def __init__(self, session: Session):
    self.session = session

  def check_and_enforce_limits(
    self, user_id: str, action_type: str
  ) -> Tuple[bool, str]:
    """
    Check if user can perform the requested action based on their limits.

    Args:
        user_id: User ID to check limits for
        action_type: Type of action being attempted

    Returns:
        Tuple of (can_perform: bool, reason: str)
    """
    logger.info(f"Checking limits for user {user_id}, action: {action_type}")

    user_limits = UserLimits.get_or_create_for_user(user_id, self.session)

    if action_type == "create_user_graph":
      # This is the only limit we still check - as a safety valve
      return user_limits.can_create_user_graph(self.session)
    else:
      # All other limits are handled by middleware
      logger.debug(f"Action type {action_type} - limits handled by middleware")
      return True, "Rate limiting handled by middleware"

  def record_api_call(
    self, user_id: str, endpoint: Optional[str] = None, graph_id: Optional[str] = None
  ) -> None:
    """Record an API call for rate limiting tracking."""
    try:
      UserUsageTracking.record_usage(
        user_id=user_id,
        usage_type=UsageType.API_CALL,
        session=self.session,
        endpoint=endpoint,
        graph_id=graph_id,
        auto_commit=False,  # Let caller manage transaction
      )
      logger.debug(f"Recorded API call for user {user_id}, endpoint: {endpoint}")
    except Exception as e:
      logger.error(f"Failed to record API call for user {user_id}: {e}")
      # Don't raise - usage tracking shouldn't break the API

  def record_sec_import(
    self, user_id: str, graph_id: Optional[str] = None, resource_count: int = 1
  ) -> None:
    """Record a SEC import for rate limiting tracking."""
    try:
      UserUsageTracking.record_usage(
        user_id=user_id,
        usage_type=UsageType.SEC_IMPORT,
        session=self.session,
        graph_id=graph_id,
        resource_count=resource_count,
        auto_commit=False,  # Let caller manage transaction
      )
      logger.debug(f"Recorded SEC import for user {user_id}, count: {resource_count}")
    except Exception as e:
      logger.error(f"Failed to record SEC import for user {user_id}: {e}")
      # Don't raise - usage tracking shouldn't break the API

  def get_user_usage_stats(self, user_id: str) -> dict:
    """
    Get comprehensive usage statistics for a user.

    Args:
        user_id: User ID to get stats for

    Returns:
        Dictionary with usage statistics
    """
    logger.info(f"Getting usage stats for user {user_id}")

    user_limits = UserLimits.get_or_create_for_user(user_id, self.session)
    base_stats = user_limits.get_current_usage(self.session)

    # Add detailed usage tracking stats
    usage_stats = UserUsageTracking.get_user_usage_stats(user_id, self.session)
    base_stats["usage_tracking"] = usage_stats

    return base_stats

  def update_user_limits(self, user_id: str, max_user_graphs: int) -> None:
    """
    Update the max graphs limit for a user.

    Args:
        user_id: User ID to update
        max_user_graphs: New maximum number of graphs allowed
    """
    logger.info(f"Updating max_user_graphs for user {user_id} to {max_user_graphs}")

    user_limits = UserLimits.get_or_create_for_user(user_id, self.session)
    user_limits.max_user_graphs = max_user_graphs
    user_limits.updated_at = datetime.now(timezone.utc)
    self.session.commit()

    logger.info(f"Successfully updated max_user_graphs for user {user_id}")
