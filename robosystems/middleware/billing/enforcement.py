"""Subscription enforcement middleware for graph operations."""

from typing import Optional, Tuple
from sqlalchemy.orm import Session

from ...models.billing import BillingCustomer, BillingSubscription, SubscriptionStatus
from ...models.iam import OrgUser
from ...config.graph_tier import GraphTier
from ...config import env
from ...logger import get_logger

logger = get_logger(__name__)


def check_can_provision_graph(
  user_id: str,
  requested_tier: GraphTier,
  session: Session,
) -> Tuple[bool, Optional[str]]:
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
) -> Tuple[bool, Optional[str]]:
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
