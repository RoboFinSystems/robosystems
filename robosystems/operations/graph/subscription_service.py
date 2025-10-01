"""Service for managing graph database subscriptions (entity-specific databases)."""

import logging
from datetime import datetime, timezone
from typing import List
from dateutil.relativedelta import relativedelta

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ...models.iam import (
  GraphSubscription,
  SubscriptionStatus,
)
from ...config import BillingConfig, env

logger = logging.getLogger(__name__)

# Billing configuration from environment
BILLING_PREMIUM_PLANS_ENABLED = env.BILLING_PREMIUM_PLANS_ENABLED
BILLING_ENABLED = env.BILLING_ENABLED
ENVIRONMENT = env.ENVIRONMENT


def get_available_plans() -> List[str]:
  """Get list of available billing plans based on environment settings."""
  if not BILLING_PREMIUM_PLANS_ENABLED and ENVIRONMENT == "dev":
    # In dev with premium disabled, only allow standard
    return ["standard"]
  else:
    # All plans available
    return ["standard", "enterprise", "premium"]


def is_payment_required() -> bool:
  """Check if payment authentication is required."""
  if not BILLING_ENABLED:
    return False  # No payment when billing disabled
  if ENVIRONMENT == "dev":
    return False  # No payment in dev
  return True  # Payment required in prod/staging with billing enabled


def get_max_plan_tier() -> str:
  """Get the maximum plan tier allowed in current environment."""
  if not BILLING_PREMIUM_PLANS_ENABLED and ENVIRONMENT == "dev":
    return "standard"
  return "premium"


class GraphSubscriptionService:
  """Service for managing graph database subscriptions and billing plans."""

  def __init__(self, session: Session):
    """Initialize subscription service with database session."""
    self.session = session

  def create_graph_subscription(
    self,
    user_id: str,
    graph_id: str,
    plan_name: str = "standard",
  ) -> GraphSubscription:
    """
    Create a subscription for a specific graph database.

    Args:
        user_id: User ID
        graph_id: Graph database ID
        plan_name: Billing plan name

    Returns:
        GraphSubscription instance
    """
    # Check if plan is available in current environment
    available_plans = get_available_plans()
    if plan_name not in available_plans:
      # Downgrade to max available plan
      max_tier = get_max_plan_tier()
      logger.warning(
        f"Plan '{plan_name}' not available in dev environment, using '{max_tier}' instead"
      )
      plan_name = max_tier

    # Get the billing plan from config
    plan_config = BillingConfig.get_subscription_plan(plan_name)

    if not plan_config:
      raise ValueError(f"Billing plan '{plan_name}' not found")

    # Check if subscription already exists
    existing = (
      self.session.query(GraphSubscription)
      .filter(
        GraphSubscription.user_id == user_id,
        GraphSubscription.graph_id == graph_id,
      )
      .first()
    )

    if existing:
      logger.warning(f"Subscription already exists for graph {graph_id}")
      return existing

    # Calculate billing period
    now = datetime.now(timezone.utc)
    period_start = now
    period_end = self._get_next_billing_date(now)

    # Create subscription
    subscription = GraphSubscription(
      user_id=user_id,
      graph_id=graph_id,
      plan_name=plan_name,
      status=SubscriptionStatus.ACTIVE.value,
      current_period_start=period_start,
      current_period_end=period_end,
    )

    self.session.add(subscription)

    try:
      self.session.commit()
      logger.info(f"Created subscription for graph {graph_id} with plan {plan_name}")
      return subscription
    except SQLAlchemyError as e:
      self.session.rollback()
      logger.error(f"Failed to create subscription: {e}")
      raise

  def _get_next_billing_date(self, from_date: datetime) -> datetime:
    """Calculate next billing date (1 month from given date)."""
    # Use relativedelta to handle month-end dates correctly
    # e.g., Jan 31 + 1 month = Feb 28/29, not an invalid Feb 31
    return from_date + relativedelta(months=1)
