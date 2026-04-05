"""Subscription enforcement middleware for graph operations."""

import threading
import time
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ...config import env
from ...config.defaults import CacheDefaults
from ...config.graph_tier import GraphTier
from ...logger import get_logger
from ...models.core import Graph, GraphStatus, OrgUser
from ...models.core.billing import (
  BillingCustomer,
  BillingSubscription,
  SubscriptionStatus,
)

logger = get_logger(__name__)

# In-memory cache: graph_id -> (subscription_result, expires_at)
# subscription_result is one of: "active", "no_subscription", "canceled_grace", "canceled_expired"
_subscription_cache: dict[str, tuple[str, float]] = {}
_subscription_lock = threading.Lock()


def _get_cached_subscription(graph_id: str) -> str | None:
  with _subscription_lock:
    entry = _subscription_cache.get(graph_id)
    if entry and entry[1] > time.time():
      return entry[0]
    if entry:
      del _subscription_cache[graph_id]
    return None


def _cache_subscription(graph_id: str, result: str) -> None:
  with _subscription_lock:
    _subscription_cache[graph_id] = (result, time.time() + CacheDefaults.SHORT)


def invalidate_subscription_cache(graph_id: str) -> None:
  """Call when subscription changes (cancel, reactivate, etc.)."""
  with _subscription_lock:
    _subscription_cache.pop(graph_id, None)


def check_can_provision_graph(
  user_id: str,
  requested_tier: GraphTier,
  session: Session,
) -> tuple[bool, str | None]:
  """Check if a user can provision a new graph.

  Args:
      user_id: The user ID
      requested_tier: The requested graph tier
      session: Database session

  Returns:
      Tuple of (can_provision, error_message)
  """
  # Get user's organization - billing is org-level, not user-level
  org_user = session.query(OrgUser).filter(OrgUser.user_id == user_id).first()

  if not org_user:
    logger.error(
      f"User {user_id} has no organization - cannot check billing",
      extra={"user_id": user_id},
    )
    return (False, "User is not a member of any organization")

  # Get or create billing customer for the user's organization
  billing_customer = BillingCustomer.get_or_create(org_user.org_id, session)

  can_provision, error_message = billing_customer.can_provision_resources(
    environment=env.ENVIRONMENT, billing_enabled=env.BILLING_ENABLED
  )

  if not can_provision:
    logger.warning(
      f"User {user_id} (org {org_user.org_id}) cannot provision graph: {error_message}",
      extra={
        "user_id": user_id,
        "org_id": org_user.org_id,
        "requested_tier": requested_tier.value,
      },
    )
  else:
    logger.info(
      f"User {user_id} (org {org_user.org_id}) authorized to provision {requested_tier.value} graph",
      extra={
        "user_id": user_id,
        "org_id": org_user.org_id,
        "requested_tier": requested_tier.value,
      },
    )

  return (can_provision, error_message)


def check_graph_subscription_active(
  graph_id: str,
  session: Session,
) -> tuple[bool, str | None]:
  """Check if a graph has an active subscription.

  Args:
      graph_id: The graph ID
      session: Database session

  Returns:
      Tuple of (is_active, error_message)
  """
  subscription = BillingSubscription.get_by_resource(
    resource_type="graph", resource_id=graph_id, session=session
  )

  if not subscription:
    logger.warning(f"No subscription found for graph {graph_id}")
    if env.BILLING_ENABLED:
      return (False, "No active subscription for this graph.")
    return (True, None)

  if subscription.status != SubscriptionStatus.ACTIVE.value:
    status = subscription.status
    error_messages: dict[str, str] = {
      SubscriptionStatus.PENDING.value: "Subscription is pending activation.",
      SubscriptionStatus.PAUSED.value: "Subscription is paused. Please reactivate.",
      SubscriptionStatus.CANCELED.value: "Subscription has been canceled.",
      SubscriptionStatus.PAST_DUE.value: "Subscription is past due. Please update payment.",
      SubscriptionStatus.UNPAID.value: "Subscription is unpaid. Please make payment.",
    }
    error_message = error_messages.get(status, f"Subscription status is {status}.")  # type: ignore
    return (False, error_message)

  return (True, None)


def require_graph_access(
  graph_id: str, session: Session, require_write: bool = False
) -> Graph:
  """Validate graph is accessible for the requested operation type.

  Checks two layers:
  1. Graph status (queued/provisioning -> 409, suspended -> 403, deprovisioned -> 404)
  2. Subscription status (grace period: reads OK, writes blocked)

  Args:
      graph_id: The graph ID to check
      session: Database session
      require_write: Whether a write operation is being attempted

  Returns:
      The Graph if access is allowed.

  Raises:
      HTTPException: If access is denied.
  """
  graph = Graph.get_by_id(graph_id, session, include_deprovisioned=True)

  if not graph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Graph not found.",
    )

  # Layer 1: Graph lifecycle status
  graph_status = graph.status or GraphStatus.ACTIVE.value

  if graph_status == GraphStatus.DEPROVISIONED.value:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Graph not found.",
    )

  if graph_status == GraphStatus.SUSPENDED.value:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Subscription has ended. Resubscribe to access this graph.",
    )

  # Layer 2: Subscription check (only when billing is enabled)
  if not env.BILLING_ENABLED:
    return graph

  # Skip subscription check for shared repositories
  if graph.is_repository:
    return graph

  # Check in-memory subscription cache first
  cached = _get_cached_subscription(graph_id)
  if cached == "active":
    return graph
  elif cached == "canceled_grace":
    if require_write:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Subscription canceled. Write operations disabled until subscription ends.",
      )
    return graph
  elif cached == "canceled_expired":
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Subscription has ended. Resubscribe to access this graph.",
    )
  elif cached == "no_subscription":
    return graph

  # Cache miss — query DB
  subscription = BillingSubscription.get_by_resource(
    resource_type="graph", resource_id=graph_id, session=session
  )

  if not subscription:
    logger.warning(f"No subscription found for graph {graph_id}")
    _cache_subscription(graph_id, "no_subscription")
    return graph

  if subscription.status == "canceled":
    now = datetime.now(UTC)
    ends_at = subscription.ends_at

    if ends_at and ends_at > now:
      # Grace period: reads OK, writes blocked
      _cache_subscription(graph_id, "canceled_grace")
      if require_write:
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail="Subscription canceled. Write operations disabled until subscription ends.",
        )
      return graph

    # Past grace period
    _cache_subscription(graph_id, "canceled_expired")
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Subscription has ended. Resubscribe to access this graph.",
    )

  _cache_subscription(graph_id, "active")
  return graph
