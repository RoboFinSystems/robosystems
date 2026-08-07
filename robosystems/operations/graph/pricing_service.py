"""Monthly bill calculation for a single graph.

Pricing is a flat monthly price per graph — no usage metering.
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy.orm import Session

from ...config import BillingConfig
from ...models.core import GraphUsage
from ...models.core.billing import BillingSubscription

logger = logging.getLogger(__name__)


class GraphPricingService:
  """Calculate what a graph costs for a billing period."""

  def __init__(self, session: Session):
    self.session = session

  def get_subscription_plan(self, user_id: str, graph_id: str) -> dict | None:
    """The graph's billing plan, defaulting to the standard tier."""
    from ...models.core.billing import BillingCustomer

    customer = BillingCustomer.get_by_user_id(user_id, self.session)
    if not customer:
      return BillingConfig.get_subscription_plan("ladybug-standard")

    subscription = (
      self.session.query(BillingSubscription)
      .filter(
        BillingSubscription.org_id == customer.org_id,
        BillingSubscription.resource_type == "graph",
        BillingSubscription.resource_id == graph_id,
        BillingSubscription.status == "active",
      )
      .first()
    )

    if subscription and subscription.plan_name:
      return BillingConfig.get_subscription_plan(subscription.plan_name)

    # Default to ladybug-standard plan if no subscription
    return BillingConfig.get_subscription_plan("ladybug-standard")

  def calculate_graph_monthly_bill(
    self,
    user_id: str,
    graph_id: str,
    year: int,
    month: int,
  ) -> dict:
    """
    Calculate monthly bill for a specific graph database.

    Uses the graph's subscription plan for pricing.
    Storage is included in the tier (no overage charges).
    """
    # Get the billing plan
    plan = self.get_subscription_plan(user_id, graph_id)
    if not plan:
      raise ValueError("No billing plan found")

    # Get usage records for the month
    usage_records = (
      self.session.query(GraphUsage)
      .filter(
        GraphUsage.user_id == user_id,
        GraphUsage.graph_id == graph_id,
        GraphUsage.billing_year == year,
        GraphUsage.billing_month == month,
      )
      .all()
    )

    if not usage_records:
      # No usage data - return base subscription cost
      return {
        "graph_id": graph_id,
        "billing_period": {"year": year, "month": month},
        "plan": {
          "name": plan["name"],
          "display_name": plan["display_name"],
        },
        "usage": {
          "total_gb_hours": 0,
          "avg_size_gb": 0,
          "max_size_gb": 0,
          "total_queries": 0,
          "measurement_count": 0,
        },
        "charges": {
          "base_monthly": float(Decimal(plan["base_price_cents"]) / 100),
          "total": float(Decimal(plan["base_price_cents"]) / 100),
        },
        "generated_at": datetime.now(UTC).isoformat(),
      }

    # Calculate usage metrics
    usage_metrics = self._calculate_usage_metrics(usage_records)

    # Base subscription price
    base_price = (Decimal(plan["base_price_cents"]) / 100).quantize(
      Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    return {
      "graph_id": graph_id,
      "billing_period": {"year": year, "month": month},
      "plan": {
        "name": plan["name"],
        "display_name": plan["display_name"],
      },
      "usage": usage_metrics,
      "charges": {
        "base_monthly": float(base_price),
        "total": float(base_price),
      },
      "generated_at": datetime.now(UTC).isoformat(),
    }

  def _calculate_usage_metrics(self, usage_records: list) -> dict:
    """Calculate usage metrics from hourly records."""
    total_gb_hours = sum(record.size_gb for record in usage_records)
    total_queries = sum(record.query_count for record in usage_records)
    max_size_gb = max(record.size_gb for record in usage_records)
    avg_size_gb = total_gb_hours / len(usage_records) if usage_records else 0

    return {
      "total_gb_hours": float(total_gb_hours),
      "avg_size_gb": float(avg_size_gb),
      "max_size_gb": float(max_size_gb),
      "total_queries": total_queries,
      "measurement_count": len(usage_records),
    }
