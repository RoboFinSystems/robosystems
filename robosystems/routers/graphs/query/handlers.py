"""Helper utilities shared by the query execution routes."""

from typing import Any


def get_query_operation_type(graph_id: str) -> str:
  """Determine the operation type that drives LadybugDB cluster routing.

  User graphs always resolve to `write` so they land on the writer cluster;
  shared repositories and their subgraphs resolve to `read` for the reader
  cluster.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    return "read"
  else:
    return "write"


def get_user_priority(user: Any) -> int:
  """Query priority for a user's subscription tier; lower sorts first."""
  from robosystems.config.query_queue import QueryQueueConfig

  if hasattr(user, "subscription") and user.subscription:
    tier = (
      user.subscription.billing_plan.name if user.subscription.billing_plan else None
    )
    return QueryQueueConfig.get_priority_for_user(tier)
  return QueryQueueConfig.DEFAULT_PRIORITY
