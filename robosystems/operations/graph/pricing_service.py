"""
Graph database pricing service.

Calculates monthly bills for individual graph databases based on:
- Fixed monthly price per database
- Storage overage charges
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy.orm import Session

from ...config import BillingConfig
from ...models.billing import BillingSubscription
from ...models.iam import GraphUsage

logger = logging.getLogger(__name__)


class GraphPricingService:
  """Service for calculating per-graph database pricing."""

  def __init__(self, session: Session):
    """Initialize pricing service with database session."""
    self.session = session

  def get_subscription_plan(self, user_id: str, graph_id: str) -> dict | None:
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
  ) -> dict:
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
        "generated_at": datetime.now(UTC).isoformat(),
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

  def _calculate_charges(self, plan: dict, avg_size_gb: float) -> dict:
    """Calculate charges based on plan and usage.

    NOTE: Storage overage is now credit-based (1 credit/GB/day) rather than dollar-based.
    This method only returns the base subscription cost. Storage overage is tracked
    separately via the credit system in CreditService.charge_storage_overage().
    """
    # Convert cents to dollars
    base_price = Decimal(plan["base_price_cents"]) / 100

    # Calculate overage GB for informational purposes only
    # Actual billing is handled via credits (1 credit/GB/day)
    avg_gb = Decimal(str(avg_size_gb))
    included_gb = Decimal(str(plan["included_gb"]))
    overage_gb = max(avg_gb - included_gb, Decimal("0"))

    # Round to 2 decimal places
    base_price = base_price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    return {
      "base_monthly": float(base_price),
      "storage_overage": 0.0,  # Now handled via credit system
      "total": float(base_price),  # Base only; overage via credits
      "overage_gb": float(overage_gb),
      "overage_billing": "credit-based",  # 1 credit/GB/day
    }
