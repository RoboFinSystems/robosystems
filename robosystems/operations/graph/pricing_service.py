"""
Graph database pricing service.

Calculates monthly bills for individual graph databases based on:
- Fixed monthly price per database
- Storage overage charges
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy.orm import Session

from ...models.iam import GraphUsage
from ...models.billing import BillingSubscription
from ...config import BillingConfig

logger = logging.getLogger(__name__)


class GraphPricingService:
  """Service for calculating per-graph database pricing."""

  def __init__(self, session: Session):
    """Initialize pricing service with database session."""
    self.session = session

  def get_subscription_plan(self, user_id: str, graph_id: str) -> Optional[Dict]:
    """Get the billing plan for a graph subscription."""
    from ...models.billing import BillingCustomer

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
  ) -> Dict:
    """
    Calculate monthly bill for a specific graph database.

    Uses the graph's subscription plan for pricing.
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
      # No usage data - return zero bill
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
          "included_gb": plan["included_gb"],
          "overage_gb": 0,
        },
        "charges": {
          "base_monthly": 0,
          "storage_overage": 0,
          "total": 0,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
      }

    # Calculate usage metrics
    usage_metrics = self._calculate_usage_metrics(usage_records)

    # Calculate charges
    charges = self._calculate_charges(plan, usage_metrics["avg_size_gb"])

    return {
      "graph_id": graph_id,
      "billing_period": {"year": year, "month": month},
      "plan": {
        "name": plan["name"],
        "display_name": plan["display_name"],
      },
      "usage": {
        **usage_metrics,
        "included_gb": plan["included_gb"],
        "overage_gb": charges["overage_gb"],
      },
      "charges": {
        "base_monthly": charges["base_monthly"],
        "storage_overage": charges["storage_overage"],
        "total": charges["total"],
      },
      "generated_at": datetime.now(timezone.utc).isoformat(),
    }

  def _calculate_usage_metrics(self, usage_records: list) -> Dict:
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

  def _calculate_charges(self, plan: Dict, avg_size_gb: float) -> Dict:
    """Calculate charges based on plan and usage.

    NOTE: This method is deprecated. Storage is now credit-based rather than GB-based.
    New storage billing uses daily credit consumption at 0.05 credits per GB.
    This method is kept for legacy billing compatibility only.
    """
    # Convert cents to dollars
    base_price = Decimal(plan["base_price_cents"]) / 100
    overage_price_per_gb = Decimal(plan["overage_price_cents_per_gb"]) / 100

    # Calculate overage
    avg_gb = Decimal(str(avg_size_gb))
    included_gb = Decimal(str(plan["included_gb"]))
    overage_gb = max(avg_gb - included_gb, Decimal("0"))

    # Calculate costs
    overage_cost = overage_gb * overage_price_per_gb
    total_cost = base_price + overage_cost

    # Round to 2 decimal places
    total_cost = total_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    overage_cost = overage_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    return {
      "base_monthly": float(base_price),
      "storage_overage": float(overage_cost),
      "total": float(total_cost),
      "overage_gb": float(overage_gb),
    }
