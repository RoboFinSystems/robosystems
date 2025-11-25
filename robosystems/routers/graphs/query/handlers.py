"""
Query helper functions.

This module provides helper utilities for query execution.
"""

from typing import Any

from robosystems.middleware.graph.utils import MultiTenantUtils


def get_query_operation_type(graph_id: str) -> str:
  """
  Determine the correct operation type for query operations.

  For consistency with distributed LadybugDB architecture:
  - User graphs: Always use 'write' to ensure writer cluster routing
  - Shared repositories: Use 'read' for reader cluster routing

  Args:
      graph_id: Graph database identifier

  Returns:
      Operation type: 'read' or 'write'
  """
  if MultiTenantUtils.is_shared_repository(graph_id):
    return "read"
  else:
    return "write"


def get_user_priority(user: Any) -> int:
  """
  Get query priority based on user subscription tier.

  Args:
      user: User object with potential subscription

  Returns:
      Priority value (lower is higher priority)
  """
  from robosystems.config.query_queue import QueryQueueConfig

  if hasattr(user, "subscription") and user.subscription:
    tier = (
      user.subscription.billing_plan.name if user.subscription.billing_plan else None
    )
    return QueryQueueConfig.get_priority_for_user(tier)
  return QueryQueueConfig.DEFAULT_PRIORITY
