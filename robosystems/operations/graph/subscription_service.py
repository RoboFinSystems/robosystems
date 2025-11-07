"""Service for managing graph database subscriptions."""

import logging
from typing import List
from sqlalchemy.orm import Session

from ...models.billing import BillingCustomer, BillingSubscription
from ...models.iam.graph_credits import GraphTier
from ...config import BillingConfig, env

logger = logging.getLogger(__name__)

BILLING_PREMIUM_PLANS_ENABLED = env.BILLING_PREMIUM_PLANS_ENABLED
BILLING_ENABLED = env.BILLING_ENABLED
ENVIRONMENT = env.ENVIRONMENT


def get_available_plans() -> List[str]:
  """Get list of available billing plans based on environment settings."""
  if not BILLING_PREMIUM_PLANS_ENABLED and ENVIRONMENT == "dev":
    return ["kuzu-standard"]
  else:
    return ["kuzu-standard", "kuzu-large", "kuzu-xlarge"]


def is_payment_required() -> bool:
  """Check if payment authentication is required."""
  if not BILLING_ENABLED:
    return False
  if ENVIRONMENT == "dev":
    return False
  return True


def get_max_plan_tier() -> str:
  """Get the maximum plan tier allowed in current environment."""
  if not BILLING_PREMIUM_PLANS_ENABLED and ENVIRONMENT == "dev":
    return "kuzu-standard"
  return "kuzu-xlarge"


class GraphSubscriptionService:
  """Service for managing graph database subscriptions and billing."""

  def __init__(self, session: Session):
    """Initialize subscription service with database session."""
    self.session = session

  def create_graph_subscription(
    self,
    user_id: str,
    graph_id: str,
    plan_name: str = "kuzu-standard",
    tier: GraphTier = GraphTier.KUZU_STANDARD,
  ) -> BillingSubscription:
    """Create a billing subscription for a graph database.

    Args:
        user_id: User ID (billing owner)
        graph_id: Graph database ID
        plan_name: Billing plan name
        tier: Graph tier

    Returns:
        BillingSubscription instance
    """
    available_plans = get_available_plans()
    if plan_name not in available_plans:
      max_tier = get_max_plan_tier()
      logger.warning(f"Plan '{plan_name}' not available, using '{max_tier}' instead")
      plan_name = max_tier

    plan_config = BillingConfig.get_subscription_plan(plan_name)
    if not plan_config:
      raise ValueError(f"Billing plan '{plan_name}' not found")

    existing = BillingSubscription.get_by_resource(
      resource_type="graph", resource_id=graph_id, session=self.session
    )

    if existing:
      logger.warning(f"Subscription already exists for graph {graph_id}")
      return existing

    BillingCustomer.get_or_create(user_id, self.session)

    subscription = BillingSubscription.create_subscription(
      user_id=user_id,
      resource_type="graph",
      resource_id=graph_id,
      plan_name=plan_name,
      base_price_cents=plan_config["base_price_cents"],
      session=self.session,
      billing_interval="monthly",
    )

    subscription.activate(self.session)

    logger.info(
      f"Created billing subscription for graph {graph_id} with plan {plan_name}",
      extra={
        "user_id": user_id,
        "graph_id": graph_id,
        "plan_name": plan_name,
        "subscription_id": subscription.id,
      },
    )

    return subscription
